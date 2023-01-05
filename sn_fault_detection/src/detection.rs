// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{get_mean_of, std_deviation, FaultDetection};

use itertools::Itertools;
use std::collections::BTreeMap;
use xor_name::XorName;

use std::time::Duration;
static RECENT_ISSUE_DURATION: Duration = Duration::from_secs(60 * 10); // 10 minutes

/// How many standard devs before we consider a node faulty
static STD_DEVS_AWAY: usize = 1;

static CONN_WEIGHTING: f32 = 5.0;
static OP_WEIGHTING: f32 = 1.0;
static KNOWLEDGE_WEIGHTING: f32 = 5.0;
static DKG_WEIGHTING: f32 = 10.0; // there are quite a lot of DKG msgs that go out atm, so can't weight this too heavily
static AE_PROBE_WEIGHTING: f32 = 10.0;

#[derive(Clone, Debug)]
/// Represents the different type of issues that can be recorded by the Fault Detection
/// system.
/// Issues have a xorname so they can be reliable assignd to the same nodes
pub enum IssueType {
    /// Represents an AeProbeMsg to be tracked by Fault Detection.
    AeProbeMsg,
    /// Represents a Dkg issue to be tracked by Fault Detection.
    Dkg,
    /// Represents a communication issue to be tracked by Fault Detection.
    Communication,
    /// Represents a knowledge issue to be tracked by Fault Detection.
    Knowledge,
    /// Represents a pending request operation issue to be tracked by Fault Detection.
    RequestOperation,
}

#[derive(Debug)]
pub struct ScoreResults {
    pub communication_scores: BTreeMap<XorName, f32>,
    pub dkg_scores: BTreeMap<XorName, f32>,
    pub knowledge_scores: BTreeMap<XorName, f32>,
    pub op_scores: BTreeMap<XorName, f32>,
    pub probe_scores: BTreeMap<XorName, f32>,
}

impl FaultDetection {
    /// Calculate the scores of all nodes being tracked and return them in a node -> score map.
    /// There is a map for each type of issue.
    ///
    /// The 'score' for a node is the number of issues logged against that node, minus the average
    /// of the number of issues at all the other nodes.
    ///
    /// These scores can then be used to highlight nodes that have a higher score than some
    /// particular ratio.
    pub fn calculate_scores(&self) -> ScoreResults {
        let mut communication_scores = BTreeMap::new();
        let mut knowledge_scores = BTreeMap::new();
        let mut op_scores = BTreeMap::new();
        let mut dkg_scores = BTreeMap::new();
        let mut probe_scores = BTreeMap::new();

        for node in &self.nodes {
            let _ = dkg_scores.insert(
                *node,
                self.calculate_node_score_for_type(node, &IssueType::Dkg),
            );
            let _ = probe_scores.insert(
                *node,
                self.calculate_node_score_for_type(node, &IssueType::AeProbeMsg),
            );
            let _ = communication_scores.insert(
                *node,
                self.calculate_node_score_for_type(node, &IssueType::Communication),
            );
            let _ = knowledge_scores.insert(
                *node,
                self.calculate_node_score_for_type(node, &IssueType::Knowledge),
            );
            let _ = op_scores.insert(
                *node,
                self.calculate_node_score_for_type(node, &IssueType::RequestOperation),
            );
        }

        ScoreResults {
            communication_scores,
            dkg_scores,
            knowledge_scores,
            op_scores,
            probe_scores,
        }
    }

    /// get the node's score, relative to the average for all nodes being tracked
    fn calculate_node_score_for_type(&self, node: &XorName, issue_type: &IssueType) -> f32 {
        let node_issue_count = self.get_node_issue_count_for_type(node, issue_type);

        // we can shortcircuit here
        if node_issue_count == 0 {
            return 0.0;
        }

        debug!("node {node} {issue_type:?} count: {:?}", node_issue_count);
        let mut other_node_counts = Vec::new();
        for itr in &self.nodes {
            if itr == node {
                continue;
            }
            other_node_counts.push(self.get_node_issue_count_for_type(itr, issue_type) as f32);
        }
        let average = get_mean_of(&other_node_counts).unwrap_or(1.0);

        node_issue_count.saturating_sub(average as usize) as f32
    }

    fn get_node_issue_count_for_type(&self, node: &XorName, issue_type: &IssueType) -> usize {
        match issue_type {
            IssueType::Communication => {
                if let Some(issues) = self.communication_issues.get(node) {
                    issues.len()
                } else {
                    0
                }
            }
            IssueType::Dkg => {
                if let Some(issues) = self.dkg_issues.get(node) {
                    issues.len()
                } else {
                    0
                }
            }
            IssueType::AeProbeMsg => {
                if let Some(issues) = self.probe_issues.get(node) {
                    issues.len()
                } else {
                    0
                }
            }
            IssueType::Knowledge => {
                if let Some(issues) = self.knowledge_issues.get(node) {
                    issues.len()
                } else {
                    0
                }
            }
            IssueType::RequestOperation => {
                if let Some(issues) = self.unfulfilled_ops.get(node) {
                    issues.len()
                } else {
                    0
                }
            }
        }
    }

    /// get scores mapped by name, to score and z-score, which is std dev's from the mean
    fn get_weighted_scores(&self) -> BTreeMap<XorName, usize> {
        trace!("Getting weighted scores");
        let scores = self.calculate_scores();
        let ops_scores = scores.op_scores;
        let conn_scores = scores.communication_scores;
        let dkg_scores = scores.dkg_scores;
        let knowledge_scores = scores.knowledge_scores;
        let probe_scores = scores.probe_scores;

        let mut pre_standardised_scores = BTreeMap::default();
        let mut scores_only = vec![];
        // now we loop to get the scores per xorname, so we can then avg etc
        for (name, score) in ops_scores {
            let ops_score = score * OP_WEIGHTING;

            let node_conn_score = *conn_scores.get(&name).unwrap_or(&1.0);
            let node_conn_score = node_conn_score * CONN_WEIGHTING;

            let node_dkg_score = *dkg_scores.get(&name).unwrap_or(&1.0);
            let node_dkg_score = node_dkg_score * DKG_WEIGHTING;

            let node_knowledge_score = *knowledge_scores.get(&name).unwrap_or(&1.0);
            let node_knowledge_score = node_knowledge_score * KNOWLEDGE_WEIGHTING;

            let node_probe_score = *probe_scores.get(&name).unwrap_or(&1.0);
            let node_probe_score = node_probe_score * AE_PROBE_WEIGHTING;

            let final_score = ops_score
                + node_conn_score
                + node_knowledge_score
                + node_dkg_score
                + node_probe_score;
            debug!(
                "Node {name} has a final score of {final_score} |
                (Conns score({node_conn_score}), Dkg score({node_dkg_score}), |
                Knowledge score({node_knowledge_score}), Ops score({score})), AeProbe score ({node_probe_score})"
            );

            scores_only.push(final_score);
            let _prev = pre_standardised_scores.insert(name, final_score as usize);
        }

        let mean = get_mean_of(&scores_only);
        let std_dev = std_deviation(&scores_only).unwrap_or(0.0);

        trace!("avg weighted score across all nodes: {mean:?}");
        trace!("std dev: {std_dev:?}");

        // now we store the z-score
        let mut final_scores = BTreeMap::default();

        let threshold = STD_DEVS_AWAY * std_dev as usize;
        debug!(
            "____Threshold is {STD_DEVS_AWAY:?} std devs away, which is {:?}",
            threshold
        );

        for (name, score) in pre_standardised_scores {
            let meaned = if let Some(mean) = mean {
                score.saturating_sub(mean as usize)
            } else {
                score
            };

            let zscore = meaned.saturating_sub(std_dev as usize);

            if zscore > threshold && zscore > 0 {
                trace!("Initial score for {name:?} is {score:?}");
                let _existed = final_scores.insert(name, zscore);
                debug!("Final Z-score for {name} is {zscore:?}");
            }
        }

        final_scores
    }

    fn cleanup_time_sensistive_checks(&mut self) {
        for issues in &mut self.communication_issues.values_mut() {
            issues.retain(|time| time.elapsed() < RECENT_ISSUE_DURATION);
        }

        for issues in &mut self.probe_issues.values_mut() {
            issues.retain(|time| time.elapsed() < RECENT_ISSUE_DURATION);
        }

        for issues in &mut self.knowledge_issues.values_mut() {
            issues.retain(|time| time.elapsed() < RECENT_ISSUE_DURATION);
        }

        for issues in &mut self.dkg_issues.values_mut() {
            issues.retain(|time| time.elapsed() < RECENT_ISSUE_DURATION);
        }
        for issues in &mut self.unfulfilled_ops.values_mut() {
            issues.retain(|time| time.elapsed() < RECENT_ISSUE_DURATION);
        }
    }

    /// Get a list of nodes that are faulty
    /// (the nodes must all `ProposeOffline` over a faulty node and then _immediately_ vote it off. So any other membershipn changes in flight could block this.
    /// thus, we need to be callling this function often until nodes are removed.)
    pub fn get_faulty_nodes(&mut self) -> Vec<XorName> {
        self.cleanup_time_sensistive_checks();

        let final_scores = self.get_weighted_scores();

        // sort into vec of highest scores first
        let faulty_nodes = final_scores
            .iter()
            .sorted_by(|a, b| Ord::cmp(&b.1, &a.1))
            .map(|(name, _score)| {
                info!("FaultDetection: Adding {name} as faulty node");
                *name
            })
            .collect_vec();

        faulty_nodes
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{detection::IssueType, tests::init_test_logger, FaultDetection};

    use eyre::bail;
    use proptest::prelude::*;
    use tokio::runtime::Runtime;
    use xor_name::{rand::random as random_xorname, XorName};

    #[derive(Debug, Clone)]
    enum NodeQualityScored {
        Bad(f32),
        Good(f32),
    }

    impl NodeQualityScored {
        fn get_failure_rate(&self) -> &f32 {
            match self {
                Self::Good(r) => r,
                Self::Bad(r) => r,
            }
        }
    }

    /// In a standard network startup (as of 24/06/22)
    /// we see:
    /// 0 op requests
    /// 2407 `DkgBroadcastVote` DKG (each are tracked as an eror until a respnose comes in...) this is total across all nodes...
    ///
    /// This includes:
    /// 510 "tracker: Dkg..." (the initial black mark)
    /// ~2394 "Logging Dkg session as responded to in faults." (aka removing a black mark) < -- we're not simulating this,
    /// only the stains that stick... So in reality, over time we' see 0 DKG issues in a normal startup
    /// ~469 "tracker: Know"
    /// ~230 "tracker: Communication""
    /// 0 "tracker: `PendingOp`..." (equally a lot of these are being responded to...)
    fn generate_network_startup_msg_issues() -> impl Strategy<Value = IssueType> {
        // higher numbers here are more frequent
        prop_oneof![
        0 => Just(IssueType::Communication),
        230 => Just(IssueType::Dkg),
        0 => Just(IssueType::AeProbeMsg),
        100 => Just(IssueType::Knowledge),
        ]
    }

    /// Generate proptest issues, in a range from 1000 `to...max_uantity`
    fn generate_msg_issues(
        min: usize,
        max: usize,
    ) -> impl Strategy<Value = Vec<(IssueType, XorName, f32)>> {
        let issue_name_for_direction = generate_xorname();
        prop::collection::vec(
            (
                generate_network_startup_msg_issues(),
                issue_name_for_direction,
                0.0..1.0f32,
            ),
            min..max + 1,
        )
    }

    /// Generate proptest issues, in a range from 1000 `to...max_quantity`
    /// issues had a name for reliably routing
    /// issues come with a random f32 0-1 to use as our test against `NodeQuality`
    fn generate_startup_issues(
        min: usize,
        max: usize,
    ) -> impl Strategy<Value = Vec<(IssueType, XorName, f32)>> {
        let issue_name_for_direction = generate_xorname();
        prop::collection::vec(
            (
                generate_network_startup_msg_issues(),
                issue_name_for_direction,
                0.0..1.0f32,
            ),
            min..max + 1,
        )
    }

    fn generate_xorname() -> impl Strategy<Value = XorName> {
        // get a random string
        let str_val = "[1-9]{32}[a-zA-Z]{32}[1-9]{32}[a-zA-Z]{32}[1-9]{32}[a-zA-Z]{32}";

        str_val.prop_map(|s| XorName::from_content(s.as_bytes()))
    }

    /// Generate proptest nodes, each a Xorname, this will generate nodes with different `NodeQualities`
    fn generate_nodes_and_quality(
        min: usize,
        max: usize,
    ) -> impl Strategy<Value = Vec<(XorName, NodeQualityScored)>> {
        prop::collection::vec(
            (
                generate_xorname(),
                prop_oneof![
                    // 3 x as likely to have good nodes vs bad
                    // good nodes fail only 2.5% of the time
                    3 => Just(NodeQualityScored::Good(0.025)),
                    // bad nodes fail 80% of the time
                    1 => Just(NodeQualityScored::Bad(0.80)),

                ],
            ),
            min..max,
        )
        .prop_filter(
            "there should be at least two good and one bad node",
            |nodes| {
                let mut good_len: f32 = 0.0;
                let mut bad_len: f32 = 0.0;

                for (_name, quality) in nodes {
                    match quality {
                        NodeQualityScored::Good(_) => good_len += 1.0,
                        NodeQualityScored::Bad(_) => bad_len += 1.0,
                    }
                }

                let byzantine_level = good_len / 3.0;

                // we have at least one bad node
                bad_len >= 1.0 &&
                // at least two good
                good_len >=2.0 &&
                // we're not overly byzantine (ie no more than 30% bad)
                byzantine_level >= 1.0 &&
                // otherwise, 3 good and 2 bad nodes
                byzantine_level > bad_len
            },
        )
    }

    /// for a given issue and a "Root address" to base elder selection off, this returns
    /// the nodes we should target for this specific issue:
    /// eg if DKG, it's the closest to the `root_addr`
    /// if anything else, we base it off issue name closeness
    fn get_nodes_suffering_issue(
        issue: IssueType,
        issue_location: XorName,
        root: XorName,
        nodes: &[(XorName, NodeQualityScored)],
        elders_count: usize,
    ) -> Vec<(XorName, NodeQualityScored)> {
        if matches!(issue, IssueType::Dkg) || matches!(issue, IssueType::AeProbeMsg) {
            nodes
                .iter()
                .sorted_by(|lhs, rhs| root.clone().cmp_distance(&lhs.0, &rhs.0))
                .take(elders_count)
                .cloned()
                .collect::<Vec<_>>()
        } else {
            // we use the "issue location" to determine which four nodes to send to
            // this should therefore be reproducible amongst proptest retries/shrinking etc
            nodes
                .iter()
                .sorted_by(|lhs, rhs| issue_location.cmp_distance(&lhs.0, &rhs.0))
                // and we simul-send it to 4 nodes
                .take(1)
                .cloned()
                .collect::<Vec<_>>()
        }
    }

    proptest! {
        #[test]
        #[allow(clippy::unwrap_used)]
        fn pt_calculate_scores_should_include_all_nodes_in_score_map(
            node_count in 4..50usize, issue_type in generate_network_startup_msg_issues())
        {
            Runtime::new().unwrap().block_on(async {
                let nodes = (0..node_count).map(|_| random_xorname()).collect::<Vec<XorName>>();
                let mut fault_detection = FaultDetection::new(nodes.clone());
                for _ in 0..5 {
                    fault_detection.track_issue(
                        nodes[0], issue_type.clone());
                }

                let score_results = fault_detection
                    .calculate_scores();
                match issue_type {
                    IssueType::Dkg => {
                        assert_eq!(score_results.dkg_scores.len(), node_count);
                    },
                    IssueType::AeProbeMsg => {
                        assert_eq!(score_results.probe_scores.len(), node_count);
                    },
                    IssueType::Communication => {
                        assert_eq!(score_results.communication_scores.len(), node_count);
                    },
                    IssueType::Knowledge => {
                        assert_eq!(score_results.knowledge_scores.len(), node_count);
                    },
                    IssueType::RequestOperation => {
                        assert_eq!(score_results.op_scores.len(), node_count);
                    },
                }
            })
        }

        #[test]
        #[allow(clippy::unwrap_used)]
        fn pt_calculate_scores_one_node_with_issues_should_have_higher_score_and_others_should_have_zero(
            node_count in 4..50usize, issue_count in 1..50, issue_type in generate_network_startup_msg_issues())
        {

            init_test_logger();
            let _outer_span = tracing::info_span!("...........").entered();

            Runtime::new().unwrap().block_on(async {

                let nodes = (0..node_count).map(|_| random_xorname()).collect::<Vec<XorName>>();
                let mut fault_detection = FaultDetection::new(nodes.clone());

                // one node keeps getting the issues applied to it
                for _ in 0..issue_count {
                    fault_detection.track_issue(
                        nodes[0], issue_type.clone());
                }

                let score_results = fault_detection
                    .calculate_scores();

                    let scores = match issue_type {
                    IssueType::Dkg => {
                        score_results.dkg_scores
                    },
                    IssueType::AeProbeMsg => {
                        score_results.probe_scores
                    },
                    IssueType::Communication => {
                        score_results.communication_scores
                    },
                    IssueType::Knowledge => {
                        score_results.knowledge_scores
                    },
                    IssueType::RequestOperation => {
                        score_results.op_scores
                    },
                };


                debug!("Actual node score: {:?}", scores.get(&nodes[0]).unwrap());
                assert!(*scores.get(&nodes[0]).unwrap() > 0 as f32);
                for node in nodes.iter().take(node_count).skip(1) {
                    assert_eq!(*scores.get(node).unwrap(), 0.0);
                }
            })
        }


        #[test]
        #[allow(clippy::unwrap_used)]
        /// Test that gives a range of nodes and a few bad nodes,
        /// we then check that we can reliably detect those nodes
        ///
        /// We do not want false positives, We do want -- over longer timeframes -- to find all bad nodes... there's a tough balance to strike here.
        /// Given that the tests _must_ terminate, there will be some instances where a bad node may not be found. But we can assume as long as we're
        /// getting _some_ that most will be caught over the long term. So we opt to check that every bad node we get from fault detection is indeed bad,
        /// and that we don't exceed the count of bad_nodes per test
        ///
        /// "Nodes" are just random xornames,
        /// each issue has a random xorname attached to it to, and is sent to 4 nodes... each of which will fail a % of the time, depending on the
        /// NodeQuality (Good or Bad)
        fn pt_detect_correct_or_less_amount_of_faulty_nodes_with_full_elder_set(
            nodes in generate_nodes_and_quality(3,30), issues in generate_msg_issues(100,500))
            {
                let elders_in_dkg = 7;
                init_test_logger();
                let _outer_span = tracing::info_span!("pt_correct_less").entered();

                let mut good_len = 0;
                let mut bad_len = 0;

                for (_node, quality) in &nodes {
                    match quality {
                        NodeQualityScored::Good(_) => good_len += 1,
                        NodeQualityScored::Bad(_) => bad_len += 1,
                    }
                }

                debug!("Good {good_len}");
                debug!("Bad {bad_len}");

                // random xorname to pick 7 nodes as "elders" for DKG
                let random_xorname_root = nodes[0].0;


                let _res = Runtime::new().unwrap().block_on(async {
                    // track faults of all_nodes
                    let all_node_names = nodes.clone().iter().map(|(name, _)| *name).collect::<Vec<XorName>>();

                    let mut fault_detection = FaultDetection::new(all_node_names);

                    // Now we loop through each issue/msg
                    for (issue, issue_location, fail_test ) in issues {
                        let target_nodes = get_nodes_suffering_issue(issue.clone(), issue_location, random_xorname_root, &nodes, elders_in_dkg);

                        // now we track our issue, but only if that node fails to passes muster...
                        for (node, quality) in target_nodes {
                            // if our random fail test is less than the failure rate.
                            let failure_chance = quality.get_failure_rate();
                            let msg_failed = &fail_test < failure_chance;

                            if msg_failed {
                                fault_detection.track_issue(
                                    node, issue.clone());
                            }

                        }
                    }
                    // now we can see what we have...
                    let faulty_nodes_found = fault_detection.get_faulty_nodes();

                    info!("======================");
                    info!("faulty nodes found len {:?}:, expected {:}", faulty_nodes_found.len(), bad_len );
                    info!("======================");

                    // over a long enough time span, we should catch those bad nodes...
                    // So long as fault detection isn't returning _more_ than the bad node count, this can pass
                    assert!(faulty_nodes_found.len() <= bad_len, "checking {} faulty nodes found is equal or less than the {} actual bad nodes in test", faulty_nodes_found.len(), bad_len);

                    // check that these were indeed bad nodes
                    for bad_node in faulty_nodes_found {
                        if let Some((_, quality)) = nodes.iter().find(|(name, _)| {name == &bad_node }) {
                            match quality {
                                NodeQualityScored::Good(_) => bail!("identified a good node as bad"),
                                NodeQualityScored::Bad(_) => {
                                    // everything is fine
                                }
                            }
                        }
                        else {
                            bail!("bad node not found in our original node set!?")
                        }

                    }

                    Ok(())
                });
        }


        #[test]
        #[allow(clippy::unwrap_used)]
        /// Test to check if we have more DKG messages, that bad nodes are found, within our expected issue count
        /// we then check that we can reliably detect those nodes
        ///
        /// We do not want false positives, We do want -- over longer timeframes -- to find all bad nodes... there's a tough balance to strike here.
        /// Given that the tests _must_ terminate, there will be some instances where a bad node may not be found. But we can assume as long as we're
        /// getting _some_ that most will be caught over the long term. So we opt to check that every bad node we get from fault detection is indeed bad,
        /// and that we don't exceed the count of bad_nodes per test
        ///
        /// "Nodes" are just random xornames,
        /// each issue has a random xorname attached to it to, and is sent to 4 nodes... each of which will fail a % of the time, depending on the
        /// NodeQuality (Good or Bad)
        fn pt_detect_dkg_bad_nodes(
            // ~1500 msgs total should get us ~500 dkg which would be representative
            nodes in generate_nodes_and_quality(3,30), issues in generate_startup_issues(100,1000))
            {
                init_test_logger();
                let elders_in_dkg = 7;
                let _outer_span = tracing::info_span!("pt_dkg").entered();
                let mut good_len = 0;
                let mut bad_len = 0;
                let random_xorname_root = nodes[0].0;

                for (_, quality) in &nodes {
                    match quality {
                        NodeQualityScored::Good(_) => good_len += 1,
                        NodeQualityScored::Bad(_) => bad_len += 1,
                    }
                }

                debug!("Good {good_len}");
                debug!("Bad {bad_len}");

                let _res = Runtime::new().unwrap().block_on(async {
                // add fault detection of our all_nodes
                let all_node_names = nodes.clone().iter().map(|(name, _)| *name).collect::<Vec<XorName>>();

                let mut fault_detection = FaultDetection::new(all_node_names);

                // Now we loop through each issue/msg
                for (issue, issue_location, fail_test ) in issues {

                    let target_nodes = get_nodes_suffering_issue(issue.clone(), issue_location, random_xorname_root, &nodes, elders_in_dkg);

                    // we send each message to all nodes in this situation where we're looking at elder comms alone over dkg
                    // now we track our issue, but only if that node fails to passes muster...
                    for (node, quality) in target_nodes.clone() {
                        // if our random fail test is less than the quality failure rate.
                        let failure_chance = quality.get_failure_rate();
                        let msg_failed = &fail_test < failure_chance;

                        if msg_failed {
                            fault_detection.track_issue(
                                node, issue.clone());
                        }

                    }
                }
                // now we can see what we have...
                    let faulty_nodes_found = fault_detection.get_faulty_nodes();

                info!("======================");
                info!("faulty nodes found len {:?}:, expected {:}?", faulty_nodes_found.len(), bad_len );
                info!("======================");

                // over a long enough time span, we should catch those bad nodes...
                // So long as fault detection isn't returning _more_ than the bad node count, this can pass
                assert!(faulty_nodes_found.len() <= bad_len, "checking {} faulty nodes found is less or equal to the {} actual bad nodes in test", faulty_nodes_found.len(), bad_len);

                // check that these were indeed bad nodes
                for bad_node in faulty_nodes_found {
                    if let Some((_, quality)) = nodes.iter().find(|(name, _)| {name == &bad_node }) {
                        match quality {
                            NodeQualityScored::Good(_) => bail!("identified a good node as bad"),
                            NodeQualityScored::Bad(_) => {
                                // everything is fine
                            }
                        }
                    }
                    else {
                        bail!("bad node not found in our original node set!?")
                    }

                }
                Ok(())
            });
        }

        #[test]
        #[allow(clippy::unwrap_used)]
        /// Test to check if we have unresponded to AeProbe msgs
        ///
        /// "Nodes" are just random xornames,
        /// each issue has a random xorname attached to it to, and is sent to 4 nodes... each of which will fail a % of the time, depending on the
        /// NodeQuality (Good or Bad)
        fn pt_detect_unresponsive_elders(
            // ~1500 msgs total should get us ~500 dkg which would be representative
            nodes in generate_nodes_and_quality(2,7), issues in generate_startup_issues(500,2500))
            {
                init_test_logger();
                let _outer_span = tracing::info_span!("detect unresponsive elders").entered();
                let mut good_len = 0;
                let mut bad_len = 0;
                let random_xorname_root = nodes[0].0;

                for (_, quality) in &nodes {
                    match quality {
                        NodeQualityScored::Good(_) => good_len += 1,
                        NodeQualityScored::Bad(_) => bad_len += 1,
                    }
                }

                debug!("Good {good_len}");
                debug!("Bad {bad_len}");

                let _res = Runtime::new().unwrap().block_on(async {
                    // track faults of all_nodes
                    let all_node_names = nodes.clone().iter().map(|(name, _)| *name).collect::<Vec<XorName>>();

                    let mut fault_detection = FaultDetection::new(all_node_names);

                    // Now we loop through each issue/msg
                    for (issue, issue_location, fail_test ) in issues {
                        // this will be all ndoes in this test as we have up to 7 elders
                        let target_nodes = get_nodes_suffering_issue(issue.clone(), issue_location, random_xorname_root, &nodes, nodes.len());

                        // we send each message to all nodes in this situation where we're looking at elder comms alone over dkg
                        // now we track our issue, but only if that node fails to passes muster...
                        for (node, quality) in target_nodes.clone() {
                            // if our random fail test is less than the quality failure rate.
                            let failure_chance = quality.get_failure_rate();
                            let msg_failed = &fail_test < failure_chance;

                            if msg_failed {
                                fault_detection.track_issue(
                                    node, issue.clone());
                            }

                        }
                    }
                    // now we can see what we have...
                    let faulty_nodes_found = fault_detection.get_faulty_nodes();

                    info!("======================");
                    info!("faulty nodes found len {:?}:, expected {:}?", faulty_nodes_found.len(), bad_len );
                    info!("======================");

                    // over a long enough time span, we should catch those bad nodes...
                    // So long as fault detection isn't returning _more_ than the bad node count, this can pass
                    assert!(faulty_nodes_found.len() <= bad_len, "checking {} faulty nodes found is less or equal to the {} actual bad nodes in test", faulty_nodes_found.len(), bad_len);

                    // check that these were indeed bad nodes
                    for bad_node in faulty_nodes_found {
                        if let Some((_, quality)) = nodes.iter().find(|(name, _)| {name == &bad_node }) {
                            match quality {
                                NodeQualityScored::Good(_) => bail!("identified a good node as bad"),
                                NodeQualityScored::Bad(_) => {
                                    // everything is fine
                                }
                            }
                        }
                        else {
                            bail!("bad node not found in our original node set!?")
                        }

                    }
                    Ok(())
            });
        }

        #[test]
        #[allow(clippy::unwrap_used)]
        fn pt_calculate_scores_when_all_nodes_have_the_same_number_of_issues_scores_should_all_be_zero(
            node_count in 4..50, issue_count in 0..50, issue_type in generate_network_startup_msg_issues())
        {
            Runtime::new().unwrap().block_on(async {
                let nodes = (0..node_count).map(|_| random_xorname()).collect::<Vec<XorName>>();
                let mut fault_detection = FaultDetection::new(nodes.clone());
                for node in &nodes {
                    for _ in 0..issue_count {
                        fault_detection.track_issue(
                            *node, issue_type.clone());
                    }
                }

                let score_results = fault_detection
                    .calculate_scores();
                let scores = match issue_type {
                    IssueType::Communication => {
                        score_results.communication_scores
                    },
                    IssueType::AeProbeMsg => {
                        score_results.probe_scores
                    },
                    IssueType::Dkg => {
                        score_results.dkg_scores
                    },
                    IssueType::Knowledge => {
                        score_results.knowledge_scores
                    },
                    IssueType::RequestOperation => {
                        score_results.op_scores
                    },
                };
                for node in &nodes {
                    assert_eq!(*scores.get(node).unwrap(), 0.0);
                }
            })
        }
    }
}

#[cfg(test)]
mod ops_tests {
    use crate::{tests::init_test_logger, FaultDetection, IssueType};
    use xor_name::{rand::random as random_xorname, XorName};

    #[tokio::test]
    async fn unfulfilled_ops_leads_to_node_classified_as_faulty() {
        init_test_logger();
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut fault_detection = FaultDetection::new(nodes.clone());

        // as this is normal, we should not detect anything off
        assert_eq!(
            fault_detection.get_faulty_nodes().len(),
            0,
            "Node should not yet be faulty"
        );

        // adding more issues though, and we should see this node as faulty
        for _ in 0..30 {
            fault_detection.track_issue(nodes[0], IssueType::RequestOperation);
        }

        // Now we should start detecting...
        assert_eq!(
            fault_detection.get_faulty_nodes().len(),
            1,
            "node should be found as faulty"
        );
    }
}

#[cfg(test)]
mod comm_tests {
    use crate::{FaultDetection, IssueType};

    use eyre::Error;
    use xor_name::{rand::random as random_xorname, XorName};

    type Result<T, E = Error> = std::result::Result<T, E>;

    // Above this, nodes should be sus
    // this is only counting last RECENT minutes atm
    pub(crate) const NORMAL_CONNECTION_PROBLEM_COUNT: usize = 50;

    #[tokio::test]
    async fn conn_fault_is_tolerant_of_norms() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();

        let mut fault_detection = FaultDetection::new(nodes.clone());

        for node in &nodes {
            for _ in 0..NORMAL_CONNECTION_PROBLEM_COUNT {
                fault_detection.track_issue(*node, IssueType::Communication);
            }
        }

        assert_eq!(
            fault_detection.get_faulty_nodes().len(),
            0,
            "no nodes are faulty"
        );

        Ok(())
    }
}

#[cfg(test)]
mod knowledge_tests {
    use crate::tests::init_test_logger;
    use crate::{FaultDetection, IssueType};

    use eyre::Error;
    use xor_name::{rand::random as random_xorname, XorName};

    type Result<T, E = Error> = std::result::Result<T, E>;

    // some example numbers as guidance
    // 5 here means we have some tolerance for AE rounds while nodes are getting up to speed on churn/split
    pub(crate) const NORMAL_KNOWLEDGE_ISSUES: usize = 70;

    #[tokio::test]
    async fn knowledge_fault_is_tolerant_of_norms() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();

        let mut fault_detection = FaultDetection::new(nodes.clone());

        // Write data NORMAL_KNOWLEDGE_ISSUES times to the 10 nodes
        for node in &nodes {
            for _ in 0..NORMAL_KNOWLEDGE_ISSUES {
                fault_detection.track_issue(*node, IssueType::Knowledge);
            }
        }

        // Assert there are not any faulty nodes
        // This is because all of them are within the tolerance ratio of each other
        assert_eq!(
            fault_detection.get_faulty_nodes().len(),
            0,
            "no nodes are faulty"
        );

        Ok(())
    }

    #[tokio::test]
    async fn knowledge_fault_is_not_too_sharp() -> Result<()> {
        init_test_logger();

        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();

        let mut fault_detection = FaultDetection::new(nodes.clone());

        // Add a new nodes
        let new_node = random_xorname();
        fault_detection.add_new_node(new_node);

        // Add just one issue to all, this gets us a baseline avg to not overly skew results
        for node in nodes {
            fault_detection.track_issue(node, IssueType::Knowledge);
        }

        // Add just one knowledge issue...
        for _ in 0..1 {
            fault_detection.track_issue(new_node, IssueType::Knowledge);
        }

        let faulty_nodes = fault_detection.get_faulty_nodes();

        // Assert that the new node is not faulty
        assert!(
            !faulty_nodes.contains(&new_node),
            "our node should not be faulty"
        );
        assert_eq!(faulty_nodes.len(), 0, "no node is faulty node");

        Ok(())
    }
}
