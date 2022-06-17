// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::api::dispatcher::Dispatcher;

use std::sync::Arc;
use xor_name::Prefix;

pub(crate) struct LogCtx {
    cmds_dispatcher: Arc<Dispatcher>,
}

impl LogCtx {
    pub(crate) fn new(cmds_dispatcher: Arc<Dispatcher>) -> Self {
        Self { cmds_dispatcher }
    }

    pub(crate) async fn prefix(&self) -> Prefix {
        self.cmds_dispatcher.node.network_knowledge().prefix()
    }
}
