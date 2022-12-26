// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Instrumentation for statemaps: <https://github.com/TritonDataCenter/statemap>

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;

/// Log the metadata for a statemap, this should be called as soon as the system starts up.
pub fn log_metadata() {
    let metadata = json!({
        "title": "The Various States of Safe Network",
        "start": [0, 0], // [seconds, nanoseconds] since unix epoch
        "states": State::metadata_json()
    });

    trace!("STATEMAP_METADATA: {metadata}");
}

/// Log a statemap entry, call this whenever you would like to log a state transition.
pub fn log_state(entity: String, state: State) {
    let time = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(t) => t,
        Err(e) => {
            error!("STATEMAP_ENTRY: failed to read system time: {e:?}");
            return;
        }
    };

    let entry = serde_json::json!({
        "time": time.as_nanos().to_string(),
        "entity": entity,
        "state": state as usize,
    });

    trace!("STATEMAP_ENTRY: {entry}")
}

/// States used for generating statemaps
pub enum State {
    Idle,
    HandleMsg,
    Comms,
    Dysfunction,
    ClientMsg,
    Dkg,
    Agreement,
    Membership,
    Handover,
    Replication,
    AntiEntropy,
    Relocate,
    Join,
    Propose,
    Node,
    Data,
}

impl State {
    pub fn metadata_json() -> serde_json::Value {
        // Colors generated with https://mokole.com/palette.html
        serde_json::json!({
            "Idle": { "value": Self::Idle as usize, "color": "#f9f9f9" },
            "HandleMsg": { "value": Self::HandleMsg as usize, "color": "#7f0000" },
            "Comms": { "value": Self::Comms as usize, "color": "#808000" },
            "Dysfunction": { "value": Self::Dysfunction as usize, "color": "#000080" },
            "Data": { "value": Self::Data as usize, "color": "#ff0000" },
            "ClientMsg": { "value": Self::ClientMsg as usize, "color": "#00ced1" },
            "Dkg": { "value": Self::Dkg as usize, "color": "#ffa500" },
            "Agreement": { "value": Self::Agreement as usize, "color": "#7fff00" },
            "Membership": { "value": Self::Membership as usize, "color": "#e9967a" },
            "Handover": { "value": Self::Handover as usize, "color": "#0000ff" },
            "Replication": { "value": Self::Replication as usize, "color": "#ff00ff" },
            "AntiEntropy": { "value": Self::AntiEntropy as usize, "color": "#1e90ff" },
            "Relocate": { "value": Self::Relocate as usize, "color": "#ffff54" },
            "Join": { "value": Self::Join as usize, "color": "#dda0dd" },
            "Propose": { "value": Self::Propose as usize, "color": "#ff1493" },
            "Node": { "value": Self::Node as usize, "color": "#98fb98" },
        })
    }
}
