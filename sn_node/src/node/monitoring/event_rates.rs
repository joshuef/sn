// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

// TODO: real-time configurable by operator (or by section..?)
const MAX_CPU_LOAD: f64 = 15.0; // unit: percent

const ORDER: Ordering = Ordering::SeqCst;

#[derive(Debug, Clone)]
pub(super) struct EventRates {
    name: String,
    period: u8,
    default_load_per_event: f64,
    samples: Arc<RwLock<VecDeque<AtomicUsize>>>,
    weighted_value: Arc<RwLock<f64>>,
    initial_value: f64,
}

/// Calculates the max number of (arbitrary) "events" per second that can be
/// allowed, to stay at or below the CPU load defined by the [`MAX_CPU_LOAD`] constant.
///
/// By frequently updating an instance of [`EventRates`] with the measured CPU load during
/// a period (which should be equal to the value of the [`period`] argument passed to the `new` fn)
/// a weighted average of the max number of allowed events is obtained.
///
/// This weighted value is the output of the [`EventRates`] component, and is obtained by calling
/// the [`max_events_per_s`] fn.
///
/// NOTE: The load per [`period`] value is updated every interval given by
/// the frequency with which the [´update`] fn is called. It is thus the caller that
/// is responsible for maintaining a fix frequency to get a uniform weighted value.
impl EventRates {
    pub(super) fn new(name: String, initial_value: f64, period: u8) -> Self {
        Self {
            name,
            period,
            default_load_per_event: MAX_CPU_LOAD / initial_value, // unit: percent-seconds per msg
            samples: Arc::new(RwLock::new(VecDeque::from([AtomicUsize::new(0)]))),
            weighted_value: Arc::new(RwLock::new(initial_value)),
            initial_value,
        }
    }

    /// The output of this component, i.e. the rate limit.
    pub(super) async fn max_events_per_s(&self) -> f64 {
        *self.weighted_value.read().await
    }

    /// Count an "event" (something that happened).
    pub(super) async fn increment(&self) {
        // increments the latest entry, which represents current interval
        if let Some(entry) = self.samples.read().await.back() {
            let _ = entry.fetch_add(1, ORDER);
        }
    }

    /// Updates the calculations with a recent measurement of CPU load.
    ///
    /// Call this with a fix frequency of once per unit of the [`period`] passed into the [`new`] fn,
    /// (i.e., if the period is 15 _minutes_, [`update`] fn shall be called every _minute_).
    ///
    /// Adds the measured load during a period which shall equal the [`period`] passed into [`new`] fn,
    /// and calculates a new moving average of [CPU load-percent-seconds per event].
    /// This moving average is used to calculate a new [`weighted_value`] of "max events per second" (given the desired max CPU load).
    pub(super) async fn update(&self, period_load: f64) {
        let (events_per_period, number_of_samples) = {
            let samples = self.samples.read().await;
            let number_of_samples = samples.len();
            let event_count: usize = samples.iter().map(|s| s.load(ORDER)).sum();
            // unit: events per [period]
            let event_count = usize::max(1, event_count) as f64;
            let events_per_period =
                f64::max(self.initial_value, event_count / number_of_samples as f64);
            (events_per_period, number_of_samples)
        };

        // unit: percent-seconds per event
        // (percent-seconds per [period] / events per [period] => percent-seconds per event)
        let load_per_event = period_load / events_per_period;

        // number of samples will start at 1 and will increment with every
        // update until it reaches and stays at [`period`].
        // [time unit] is the chosen period (and update frequency) time unit.
        debug!(
            "Load per {} {:?} ({} [time unit] moving avg)",
            self.name, load_per_event, number_of_samples
        );

        // unit: events / s
        // (percent / percent-seconds per event => events / s)
        let max_events_per_s = MAX_CPU_LOAD / f64::min(self.default_load_per_event, load_per_event);

        *self.weighted_value.write().await = max_events_per_s;

        debug!(
            "Max {} per s {:?} ({} [chosen period] moving avg)",
            self.name, max_events_per_s, number_of_samples
        );

        let mut samples = self.samples.write().await;

        // if full, drop oldest entry
        if number_of_samples == self.period as usize {
            let _ = samples.pop_front();
        }

        // insert new
        samples.push_back(AtomicUsize::new(0));
    }
}
