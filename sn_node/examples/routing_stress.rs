// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#![recursion_limit = "256"]

use sn_interface::{
    messaging::{data::Error::FailedToWriteFile, system::SystemMsg, DstLocation, MsgId},
    types::Cache,
};
use sn_node::node::{
    create_test_max_capacity_and_root_storage, Config, Event as RoutingEvent, NodeApi,
    NodeElderChange,
};

use bls::PublicKey;
use eyre::{eyre, Context, Error, Result};
use futures::{
    future,
    stream::{self, StreamExt},
    Stream,
};
use itertools::Itertools;
use rand::{
    distributions::{Distribution, WeightedIndex},
    Rng,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::BufWriter,
    net::SocketAddr,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tokio::{
    sync::mpsc::{self, Sender},
    time, // task,
};
use tokio_util::time::delay_queue::DelayQueue;
use tracing_subscriber::EnvFilter;
use xor_name::{Prefix, XorName};
use yansi::{Color, Style};

// Minimal delay between two consecutive prints of the network status.
const MIN_PRINT_DELAY: Duration = Duration::from_millis(500);

// Time after which we stop tracking a probe message, regardless of its state (delivered or not).
const PROBE_WINDOW: Duration = Duration::from_secs(60);

/// Stress test for sn-routing.
#[derive(Debug, StructOpt)]
struct Options {
    /// Enable logging. Takes path to a file to log to or "-" to log to stdout. If omitted, logging
    /// is disabled.
    #[structopt(short, long, name = "PATH")]
    log: Option<String>,
    /// How many probe messages to send per second.
    ///
    /// Probe messages are used to determine network health. The higher the percentage of
    /// successfully delivered probe messages, the healthier the network is.
    #[structopt(short, long, default_value = "1")]
    probe_frequency: f64,
    /// Churn schedule: DURATION1 JOINS1 DROPS1 DURATION2 JOINS2 DROPS2, ...
    ///
    /// Given as a list of numbers which are taken in groups of three. Each triple defines one churn
    /// period. The first number defines the duration of the period in seconds, the second number
    /// defines the number of nodes to add during this period and the third number defines the
    /// number of nodes to remove. The actual times of the churn events are chosen randomly within
    /// the period. Multiple periods can be specified and are then executed sequentially. If the
    /// number of entries is not divisible by three, the missing numbers are assumed to be zeroes.
    ///
    /// Example
    ///
    /// 10 5 0 20 15 8
    ///
    /// Defines a schedule where there is 5 nodes added and none removed during the first
    /// 10 seconds and then 15 nodes added and 8 nodes removed during the subsequent 20 seconds.
    /// No more churn (apart from internal churn due to relocation) is be generated afterwards.
    schedule: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Options::from_args();

    let local = tokio::task::LocalSet::new();

    let _res = local
        .run_until(async move {
            if opts.schedule.is_empty() {
                return Err(eyre!(
                    "Must specify churn schedule (run with --help for more info)."
                ));
            }

            if opts.probe_frequency <= 0.0 {
                return Err(eyre!("Probe frequency must be greater than zero."));
            }

            // Init logging.
            let _log_guard = if let Some(path) = opts.log {
                let builder =
                    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env());

                if path == "-" {
                    builder.init();
                    None
                } else {
                    let file = File::create(path)?;
                    let file = BufWriter::new(file);
                    let (writer, guard) = tracing_appender::non_blocking(file);

                    builder.with_writer(writer).init();
                    Some(guard)
                }
            } else {
                None
            };

            let schedule = ChurnSchedule::parse(&opts.schedule)?;

            let (event_tx, mut event_rx) = mpsc::channel(20);
            let mut network = Network::new();

            // Create the genesis node
            network.create_node(event_tx.clone()).await;

            let mut churn_events = schedule.events();

            let probe_interval = Duration::from_secs_f64(1.0 / opts.probe_frequency);
            let mut probes = time::interval(probe_interval);

            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        if let Some(event) = event {
                            network.handle_event(event).await?
                        } else {
                            break
                        }
                    }
                    event = churn_events.next() => {
                        match event {
                            Some(ChurnEvent::Join) => {
                                network.create_node(event_tx.clone()).await
                            }
                            Some(ChurnEvent::Drop) => {
                                network.remove_random_node()
                            }
                            None => unreachable!()
                        }
                    }
                    _ = probes.tick() => network.send_probes().await?,
                }
            }

            Ok(())
        })
        .await;

    Ok(())
}

#[allow(clippy::large_enum_variant)]
enum Event {
    // Node successfully joined the network.
    JoinSuccess { id: u64, node: NodeApi },
    // Node failed to join the network.
    JoinFailure { id: u64, error: Error },
    // Node fired a routing event.
    Routing { id: u64, event: RoutingEvent },
}

#[allow(clippy::large_enum_variant)]
enum Node {
    // Node is bootstrapping into the network for the first time.
    Joining,
    // Node has joined the network and is either a member of a section or being relocated
    Joined {
        node: NodeApi,
        name: XorName,
        age: u8,
        prefix: Prefix,
        is_relocating: bool,
        // `Some` if this node is elder, otherwise `None`.
        elder: Option<ElderState>,
    },
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct ElderState {
    key: bls::PublicKey,
    num_elders: usize,
}

struct Network {
    nodes: BTreeMap<u64, Node>,
    next_id: u64,
    start_time: Instant,
    print_time: Instant,
    probe_tracker: ProbeTracker,
    stats: Stats,
    theme: Theme,
}

impl Network {
    fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            next_id: 0,
            start_time: Instant::now(),
            print_time: Instant::now() - MIN_PRINT_DELAY,
            probe_tracker: ProbeTracker::default(),
            stats: Stats::default(),
            theme: Theme::default(),
        }
    }

    // Create new node and let it join the network.
    async fn create_node(&mut self, event_tx: Sender<Event>) {
        let bootstrap_nodes = self.get_bootstrap_nodes().await;

        let id = self.new_node_id();
        let _prev = self.nodes.insert(id, Node::Joining);
        self.stats.join_attempts += 1;

        let config = Config {
            first: bootstrap_nodes.is_empty(),
            hard_coded_contacts: bootstrap_nodes,
            ..Default::default()
        };

        let _ = add_node(id, config, event_tx).await;

        self.try_print_status();
    }

    // Remove a random node where the probability of a node to be removed is inversely proportional
    // to its age. That is, younger nodes are more likely to be dropped than older nodes. More
    // specifically, a node with age N is twice a likely to be dropped than a node with age N + 1.
    fn remove_random_node(&mut self) {
        let weighted_ids: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|(id, node)| match node {
                Node::Joined { age, .. } => Some((*id, 1.0 / 2f64.powf(*age as f64))),
                Node::Joining => None,
            })
            .collect();

        let dist =
            if let Ok(dist) = WeightedIndex::new(weighted_ids.iter().map(|(_, weight)| *weight)) {
                dist
            } else {
                return;
            };

        let index = dist.sample(&mut rand::thread_rng());
        let id = weighted_ids[index].0;

        if let Some(node) = self.nodes.remove(&id) {
            self.stats.drops += 1;

            if let Node::Joined {
                is_relocating: true,
                ..
            } = node
            {
                // We dropped a node that is being relocated. Count this as success to avoid
                // this showing up as relocation failure which it isn't.
                self.stats.relocation_successes += 1;
            }

            self.try_print_status();
        }
    }

    async fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::JoinSuccess { id, node } => {
                let name = node.name();
                let age = node.age();
                let prefix = node.our_prefix();

                let _prev = self.nodes.insert(
                    id,
                    Node::Joined {
                        node,
                        name,
                        age,
                        prefix,
                        is_relocating: false,
                        elder: None,
                    },
                );
                self.stats.join_successes += 1;
            }
            Event::JoinFailure { id, error } => {
                println!("{}: {}", self.theme.error.paint("join failure"), error);
                let _prev = self.nodes.remove(&id);
                self.stats.join_failures += 1;
            }
            Event::Routing { id, event } => match event {
                RoutingEvent::EldersChanged {
                    elders,
                    self_status_change,
                    ..
                }
                | RoutingEvent::SectionSplit {
                    elders,
                    self_status_change,
                    ..
                } => {
                    if let Some(Node::Joined {
                        name,
                        prefix,
                        elder,
                        ..
                    }) = self.nodes.get_mut(&id)
                    {
                        let old_prefix = *prefix;
                        *prefix = elders.prefix;

                        if elders.added.contains(name) || elders.remaining.contains(name) {
                            *elder = Some(ElderState {
                                key: elders.key,
                                num_elders: elders.remaining.len() + elders.added.len(),
                            });
                        } else if elders.removed.contains(name) && old_prefix == elders.prefix {
                            *elder = None;
                        }
                    }
                    match self_status_change {
                        NodeElderChange::Promoted => self.stats.promotions += 1,
                        NodeElderChange::Demoted => self.stats.demotions += 1,
                        NodeElderChange::None => (),
                    };
                }
                RoutingEvent::RelocationStarted { .. } => {
                    if let Some(Node::Joined { is_relocating, .. }) = self.nodes.get_mut(&id) {
                        *is_relocating = true;
                        self.stats.relocation_attempts += 1;
                    }
                }
                RoutingEvent::Relocated { .. } => {
                    if let Some(Node::Joined {
                        node,
                        name,
                        age,
                        is_relocating,
                        ..
                    }) = self.nodes.get_mut(&id)
                    {
                        *name = node.name();
                        *age = node.age();
                        *is_relocating = false;
                        self.stats.relocation_successes += 1;
                    }
                }
                RoutingEvent::MessageReceived { dst, .. } => {
                    let (dst, dst_section_pk) = match dst {
                        DstLocation::Section { name, section_pk } => (name, section_pk),
                        DstLocation::Node { name, section_pk } => (name, section_pk),
                        DstLocation::EndUser(_) => {
                            return Err(eyre!("unexpected probe message dst: {:?}", dst))
                        }
                    };

                    self.probe_tracker.receive(&dst, &dst_section_pk).await;
                }
                _ => {
                    // Currently ignore the other event variants. This might change in the future,
                    // if we come up with something interesting to use those events for.
                }
            },
        }

        self.try_print_status();

        Ok(())
    }

    fn new_node_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).expect("too many nodes");
        id
    }

    // Returns the socket addresses to bootstrap against.
    async fn get_bootstrap_nodes(&self) -> BTreeSet<SocketAddr> {
        // Number of bootstrap contacts to use. Use more than one to increase the chance of
        // successful bootstrap in case some of the bootstrap nodes get dropped.
        //
        // FIXME: seems there is a bug which causes a hang if more than one contact is used.
        // Need to investigate.
        const COUNT: usize = 1;

        // Use the oldest nodes in the network as the bootstrap contacts.
        let mut nodes = BTreeSet::new();
        for (node, _) in self
            .nodes
            .values()
            .filter_map(|node| match node {
                Node::Joined { node, age, .. } => Some((node, age)),
                Node::Joining => None,
            })
            .sorted_by(|(_, lhs_age), (_, rhs_age)| lhs_age.cmp(rhs_age).reverse())
            .take(COUNT)
        {
            let _prev = nodes.insert(node.our_connection_info());
        }

        nodes
    }

    // Send messages to probe network health.
    async fn send_probes(&mut self) -> Result<()> {
        // Cache the (src, dst) pairs of sent messages to ensure every node from the same
        // section sends the same message.
        let mut cache = BTreeMap::new();

        let nodes = self.nodes.values().filter_map(|node| match node {
            Node::Joined { node, prefix, .. } => Some((node, prefix)),
            Node::Joining => None,
        });

        for (node, prefix) in nodes {
            let dst = *cache
                .entry(prefix)
                .or_insert_with(|| prefix.substituted_in(xor_name::rand::random()));

            let nodes_section = node
                .matching_section(&node.name())
                .await
                .context("No pk found for our node's section")?;

            let nodes_section_pk = nodes_section.section_key();
            if self.try_send_probe(node, dst).await? {
                self.probe_tracker
                    .send(*prefix, dst, nodes_section_pk)
                    .await;
            }
        }

        self.probe_tracker.prune();

        self.try_print_status();

        Ok(())
    }

    async fn try_send_probe(&self, node: &NodeApi, dst: XorName) -> Result<bool> {
        let public_key_set = if let Ok(public_key_set) = node.public_key_set() {
            public_key_set
        } else {
            // The node doesn't have BLS keys. Skip.
            return Ok(false);
        };

        // There can be a significant delay between a node being relocated and us receiving the
        // `Relocated` event. Using the current node name instead of the one reported by the last
        // `Relocated` event reduced send errors due to src location mismatch which would cause the
        // section health to appear lower than it actually is.

        // just some valid message
        let node_msg = SystemMsg::NodeMsgError {
            error: FailedToWriteFile,
            correlation_id: MsgId::new(),
        };

        let dst_location = DstLocation::Section {
            name: dst,
            section_pk: public_key_set.public_key(),
        };

        let wire_msg = node.sign_single_src_msg(node_msg, dst_location)?;

        match node.send_msg_to_nodes(wire_msg).await {
            Ok(()) => Ok(true),
            Err(error) => Err(error).context(format!("failed to send probe by {}", node.name())),
        }
    }

    fn try_print_status(&mut self) {
        if self.print_time.elapsed() > MIN_PRINT_DELAY {
            self.print_time = Instant::now();
            self.print_status();
        }
    }

    fn print_status(&self) {
        let mut num_active = 0;
        let mut num_joining = 0;
        let mut num_relocating = 0;
        let mut num_elders = 0;
        let mut ages = vec![0; 256];
        let mut sections = BTreeMap::new();

        for node in self.nodes.values() {
            match node {
                Node::Joining => {
                    num_joining += 1;
                }
                Node::Joined {
                    age,
                    prefix,
                    is_relocating,
                    elder,
                    ..
                } => {
                    if *is_relocating {
                        num_relocating += 1;
                    } else {
                        num_active += 1;
                    }

                    if elder.is_some() {
                        num_elders += 1;
                    }

                    ages[*age as usize] += 1;
                    *sections.entry(prefix).or_insert(0) += 1;
                }
            }
        }

        println!();
        println!(
            "{:18} {}",
            self.theme.label.paint("duration:"),
            self.theme.value.paint(format_args!(
                "{:.1}s",
                self.start_time.elapsed().as_secs_f64()
            ))
        );
        println!(
            "{:18} total: {}, active: {}, joining: {}, relocating: {}",
            self.theme.label.paint("nodes:"),
            self.theme.value.paint(self.nodes.len()),
            self.theme.value.paint(num_active),
            self.theme.value.paint(num_joining),
            self.theme.value.paint(num_relocating),
        );
        println!(
            "{:18} elders: {}, {}",
            self.theme.label.paint("age distribution:"),
            self.theme.value.paint(num_elders),
            ages.into_iter()
                .enumerate()
                .filter(|(_, count)| *count > 0)
                .format_with(", ", |(age, count), f| f(&format_args!(
                    "{}: {}",
                    age,
                    self.theme.value.paint(count)
                )))
        );
        println!(
            "{:18} {}",
            self.theme.label.paint("section members:"),
            sections
                .iter()
                .format_with(", ", |(prefix, count), f| f(&format_args!(
                    "({:b}): {}",
                    prefix,
                    self.theme.value.paint(count)
                )))
        );

        println!(
            "{:18} {}",
            self.theme.label.paint("section health:"),
            self.probe_tracker
                .status()
                .filter(|(prefix, ..)| sections.contains_key(prefix))
                .format_with(", ", |(prefix, delivered, sent), f| {
                    let percent = percent(delivered, sent);

                    f(&format_args!(
                        "({:b}): {}",
                        prefix,
                        self.theme
                            .health(percent)
                            .paint(format_args!("{:.0}%", percent)),
                    ))
                })
        );

        let failure_style = if self.stats.join_failures == 0 {
            self.theme.value
        } else {
            self.theme.error
        };

        println!(
            "{:18} join attempts: {} (successes: {}, failures: {}), drops: {}",
            self.theme.label.paint("churn:"),
            self.theme.value.paint(self.stats.join_attempts),
            self.theme.value.paint(format_args!(
                "{} ({:.0}%)",
                self.stats.join_successes,
                percent(self.stats.join_successes, self.stats.join_attempts)
            )),
            failure_style.paint(format_args!(
                "{} ({:.0}%)",
                self.stats.join_failures,
                percent(self.stats.join_failures, self.stats.join_attempts)
            )),
            self.theme.value.paint(self.stats.drops)
        );

        println!(
            "{:18} attempts: {}, successes: {}",
            self.theme.label.paint("relocations:"),
            self.theme.value.paint(self.stats.relocation_attempts),
            self.theme.value.paint(format_args!(
                "{} ({:.0}%)",
                self.stats.relocation_successes,
                percent(
                    self.stats.relocation_successes,
                    self.stats.relocation_attempts
                )
            ))
        );

        println!(
            "{:18} promotions: {}, demotions: {}",
            self.theme.label.paint("elder churn:"),
            self.theme.value.paint(self.stats.promotions),
            self.theme.value.paint(self.stats.demotions)
        );

        println!();
    }
}

async fn add_node(id: u64, mut config: Config, event_tx: Sender<Event>) -> Result<()> {
    let (_, root_dir) = create_test_max_capacity_and_root_storage()?;

    config.root_dir = Some(root_dir);
    let joining_timeout = Duration::from_secs(3 * 60);

    let (node, mut events) = match NodeApi::new(&config, joining_timeout).await {
        Ok(output) => output,
        Err(error) => {
            let _res = event_tx
                .send(Event::JoinFailure {
                    id,
                    error: error.into(),
                })
                .await;
            return Ok(());
        }
    };

    let _res = event_tx.send(Event::JoinSuccess { id, node }).await;

    while let Some(event) = events.next().await {
        if event_tx.send(Event::Routing { id, event }).await.is_err() {
            break;
        }
    }

    Ok(())
}

// Schedule that defines when churn events happen.
#[derive(Debug)]
struct ChurnSchedule(Vec<ChurnPeriod>);

impl ChurnSchedule {
    fn parse(input: &[String]) -> Result<Self> {
        let vec: Result<Vec<_>> = input
            .chunks(3)
            .map(|chunk| {
                let duration: f64 = chunk.get(0).map(|n| n.parse()).unwrap_or(Ok(0.0))?;
                let duration = Duration::from_secs_f64(duration);

                let joins = chunk.get(1).map(|n| n.parse()).unwrap_or(Ok(0))?;
                let drops = chunk.get(2).map(|n| n.parse()).unwrap_or(Ok(0))?;

                Ok(ChurnPeriod {
                    duration,
                    joins,
                    drops,
                })
            })
            .collect();

        Ok(Self(vec?))
    }

    // Returns a stream yielding the churn events at their scheduled times.
    fn events(&self) -> impl Stream<Item = ChurnEvent> + Unpin {
        let mut rng = rand::thread_rng();
        let mut start = Duration::default();
        let mut queue = DelayQueue::new();

        for period in &self.0 {
            let end = start + period.duration;

            for _ in 0..period.joins {
                queue.insert(
                    ChurnEvent::Join,
                    if start < end {
                        rng.gen_range(start..end)
                    } else {
                        start
                    },
                );
            }

            for _ in 0..period.drops {
                queue.insert(
                    ChurnEvent::Drop,
                    if start < end {
                        rng.gen_range(start..end)
                    } else {
                        start
                    },
                );
            }

            start = end;
        }

        // never yield again once the schedule is exhausted
        queue
            .filter_map(|result| future::ready(result.ok()))
            .map(|expired| expired.into_inner())
            .chain(stream::pending())
    }
}

#[derive(Debug)]
struct ChurnPeriod {
    duration: Duration,
    joins: usize,
    drops: usize,
}

enum ChurnEvent {
    Join,
    Drop,
}

#[derive(Default)]
struct Stats {
    join_attempts: usize,
    join_failures: usize,
    join_successes: usize,
    drops: usize,
    relocation_attempts: usize,
    relocation_successes: usize,
    promotions: usize,
    demotions: usize,
}

#[derive(Clone)]
enum ProbeState {
    Pending(PublicKey),
    Success,
}

#[derive(Default)]
struct ProbeTracker {
    sections: BTreeMap<Prefix, Cache<XorName, ProbeState>>,
}

use futures::executor::block_on as block;

impl ProbeTracker {
    async fn send(&mut self, src: Prefix, dst: XorName, current_public_key: PublicKey) {
        let cache = self
            .sections
            .entry(src)
            .or_insert_with(|| Cache::with_expiry_duration(PROBE_WINDOW));
        if cache.get(&dst).await.is_none() {
            cache
                .set(dst, ProbeState::Pending(current_public_key), None)
                .await;
        }
    }

    async fn receive(&mut self, dst: &XorName, received_public_key: &PublicKey) {
        let result = &self
            .sections
            .iter()
            .find_map(|(prefix, cache)| block(cache.get(dst)).map(|pk| (prefix, pk, cache)));
        let (_prefix, probe_state, cache) = match result {
            None => return,
            Some(state) => state,
        };

        if let ProbeState::Pending(pk) = probe_state {
            if received_public_key == pk {
                cache.set(*dst, ProbeState::Success, None).await;
            }
        }
    }

    // Returns iterator that yields the numbers of (delivered, sent) probe messages for each section.
    fn status(&self) -> impl Iterator<Item = (&Prefix, usize, usize)> {
        self.sections.iter().map(|(prefix, section)| {
            let success = block(section.count(|(_, state)| match state.object {
                ProbeState::Success => true,
                ProbeState::Pending(_) => false,
            }));

            (prefix, success, block(section.len()))
        })
    }

    fn prune(&mut self) {
        let remove: Vec<_> = self
            .sections
            .iter()
            .filter(|(_, section)| block(section.is_empty()))
            .map(|(prefix, _)| *prefix)
            .collect();

        for prefix in remove {
            let _prev = self.sections.remove(&prefix);
        }
    }
}

struct Theme {
    label: Style,
    value: Style,
    error: Style,
}

impl Theme {
    fn health(&self, health: f64) -> Style {
        if health > 80.0 {
            Style::new(Color::Green).bold()
        } else if health > 50.0 {
            Style::new(Color::Yellow).bold()
        } else {
            Style::new(Color::Red).bold()
        }
    }
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            label: Style::new(Color::Magenta).bold(),
            value: Style::new(Color::Blue).bold(),
            error: Style::new(Color::Red).bold(),
        }
    }
}

fn percent(num: usize, den: usize) -> f64 {
    if den > 0 {
        100.0 * num as f64 / den as f64
    } else {
        0.0
    }
}
