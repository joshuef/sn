// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

// this gets us to_string easily enough
use strum_macros::{Display as StrumDisplay, EnumIter, EnumString};

/// Internal log marker, to be used in tests asserts.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, EnumIter, StrumDisplay, EnumString)]
#[allow(missing_docs)]
pub enum LogMarker {
    // Stats
    TotalNetworkElders,
    // Split
    StillElderAfterSplit,
    SplitSuccess,
    SplitAttempt,
    NewPrefix,
    AeSendUpdateToSiblings,
    AgreementOfMembership,
    // ProposalAgreed
    ProposalAgreed,
    // Handover
    HandoverConsensusTrigger,
    HandoverMsgBeingHandled,
    HandoverConsensusTermination,
    HandoverSendingAeUpdateRequest,
    HandoverAeRequestReceived,
    // Malice
    DeviantsDetected,
    ProposeOffline,
    AcceptedNodeAsOffline,
    IgnoredNodeAsOffline,
    VotedOffline,
    // Messaging
    ClientMsgToBeForwarded,
    MsgReceived,
    ClientMsgToBeHandled,
    NodeMsgToBeHandled,
    // Membership
    MembershipVotesBeingHandled,
    MembershipSendingAeUpdateRequest,
    MembershipAeRequestReceived,
    GossippingMembershipVotes,
    // Data
    DataStoreReceivedAtElder,
    DataQueryReceviedAtElder,
    // Chunks
    StoringChunk,
    StoredNewChunk,
    DataResponseReceviedFromAdult,
    ChunkQueryReceviedAtElder,
    ChunkQueryReceviedAtStoringNode,
    // Data reorganisation
    RequestForAnyMissingData,
    DataReorganisationUnderway,
    QueuingMissingReplicatedData,
    SendingMissingReplicatedData,
    // Register
    RegisterWrite,
    RegisterQueryReceivedAtElder,
    RegisterQueryReceivedAtAdult,
    // Routing cmds
    CmdHandlingSpawned,
    CmdProcessStart,
    CmdProcessEnd,
    CmdProcessingError,
    // DKG + Promotion
    SendDkgStart,
    PromotedToElder,
    DemotedFromElder,
    DkgBroadcastEphemeralPubKey,
    DkgBroadcastFirstVote,
    DkgHandleEphemeralPubKey,
    DkgBroadcastVote,
    DkgVotesHandling,
    DkgComplete,
    HandlingDkgSuccessfulOutcome,
    HandlingNewEldersAgreement,
    HandlingNewSectionsAgreement,
    NewSignedSap,
    NewKeyShareStored,
    TriggeringPromotionAndDemotion,
    // Anti-Entropy
    AeResendAfterRetry,
    AeResendAfterRedirect,
    AeSendRedirect,
    AeSendRetryAsOutdated,
    AeSendRetryDstPkFail,
    // Sending messages
    SendToNodes,
    SendResourceProofChallenge,
    SendJoinRequest,
    SendJoinAsRelocatedResponse,
    SendJoinRejected,
    SendNodeApproval,
    // Approved to join
    ReceivedJoinApproval,
    // Connections
    ConnectionOpened,
    ConnectionClosed,
    IncomingConnection,
    ReceiveCompleted,
    ConnectionReused,
    // Relocation
    RelocateStart,
    RelocateEnded,
}
