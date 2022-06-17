// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::dkg::KeyedSig;
use sn_consensus::Generation;
use sn_interface::messaging::system::SectionAuth;
use sn_interface::network_knowledge::SectionAuthorityProvider;
use std::collections::BTreeMap;
use xor_name::Prefix;
type Entry = (SectionAuth<SectionAuthorityProvider>, KeyedSig);
use std::{cell::RefCell, rc::Rc};

// Helper structure to make sure we process a split by updating info about both our section and the
// sibling section at the same time.
pub(crate) struct SplitBarrier(Rc<RefCell<BTreeMap<Generation, Vec<Entry>>>>);

impl SplitBarrier {
    pub(crate) fn new() -> Self {
        Self(Rc::new(RefCell::new(BTreeMap::default())))
    }

    // Pass an agreed-on proposal for `NewElders` through this function. If there is no split, it
    // returns it unchanged. If there is a split and we've seen the agreement for only one
    // subsection so far, it caches it and returns nothing. Otherwise it returns both proposals.
    //
    // Note: in case of a fork, it can return more than two proposals. In that case one of the
    // proposals will be for one subsection and all the others for the other subsection.
    #[instrument(skip(self), level = "trace", name = "split barrier processing")]
    pub(crate) async fn process(
        &mut self,
        our_prefix: &Prefix,
        section_auth: SectionAuth<SectionAuthorityProvider>,
        keyed_sig: KeyedSig,
        generation: Generation,
    ) -> Vec<Entry> {
        debug!("Processing split barrier for gen: {generation}");
        if !section_auth.prefix().is_extension_of(our_prefix) {
            // Not a split, no need to cache.
            return vec![(section_auth, keyed_sig)];
        }

        let mut gen_map_guard = self.0.borrow_mut();
        let gen_entries = gen_map_guard.remove(&generation).unwrap_or_default();

        // Split detected. Find all cached siblings.
        let (mut give, mut keep): (Vec<Entry>, Vec<Entry>) =
            gen_entries
                .into_iter()
                .partition(|(cached_section_auth, _)| {
                    cached_section_auth.prefix() == section_auth.prefix().sibling()
                });

        if give.is_empty() {
            // No sibling found. Cache this update until we see the sibling update.
            keep.push((section_auth, keyed_sig));
            let _prev = gen_map_guard.insert(generation, keep);
            vec![]
        } else {
            // Sibling found. We can proceed with the update.
            give.push((section_auth, keyed_sig));
            give
        }
    }
}
