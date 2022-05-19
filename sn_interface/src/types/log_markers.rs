// Copyright 2022 MaidSafe.net limited.
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
    AgreementOfOnline,
    // ProposalAgreed
    ProposalAgreed,
    // Handover
    HandoverConsensusTrigger,
    HandoverMsgBeingHandled,
    HandoverConsensusTermination,
    // Malice
    DeviantsDetected,
    ProposeOffline,
    AcceptedNodeAsOffline,
    IgnoredNodeAsOffline,
    // Messaging
    ServiceMsgToBeHandled,
    SystemMsgToBeHandled,
    // Membership
    MembershipVotesBeingHandled,
    MembershipSendingAeUpdateRequest,
    MembershipAeRequestReceived,
    // Data
    DataStoreReceivedAtElder,
    DataQueryReceviedAtElder,
    // Chunks
    StoringChunk,
    StoredNewChunk,
    ChunkQueryResponseReceviedFromAdult,
    ChunkQueryReceviedAtElder,
    ChunkQueryReceviedAtAdult,
    // Data reorganisation
    RequestForAnyMissingData,
    DataReorganisationUnderway,
    FetchingMissingReplicateData,
    SendingMissingReplicatedData,
    // Register
    RegisterWrite,
    RegisterQueryReceivedAtElder,
    RegisterQueryReceivedAtAdult,
    // Routing cmds
    DispatchHandleMsgCmd,
    DispatchSendMsgCmd,
    CmdHandlingSpawned,
    CmdProcessStart,
    CmdProcessEnd,
    CmdProcessingError,
    // DKG + Promotion
    SendDkgStart,
    PromotedToElder,
    DemotedFromElder,
    DkgSendFailureObservation,
    DkgBroadcastMsg,
    DkgMessageHandling,
    DkgSessionComplete,
    DkgSessionAlreadyCompleted,
    HandlingDkgSuccessfulOutcome,
    HandlingNewEldersAgreement,
    NewSignedSap,
    NewKeyShareStored,
    TriggeringPromotionAndDemotion,
    // Anti-Entropy
    AeResendAfterRetry,
    AeResendAfterAeRedirect,
    AeSendRedirect,
    AeSendRetryAsOutdated,
    AeSendRetryDstPkFail,
    // Sending messages
    SendOrHandle,
    SendDirect,
    SendDirectToNodes,
    SendResourceProofChallenge,
    SendJoinRequest,
    SendJoinAsRelocatedResponse,
    SendJoinRejected,
    SendJoinRedirected,
    SendJoinRetryNotCorrectKey,
    SendJoinRetryAgeIssue,
    SendJoinRetryNotAdult,
    SendJoinsDisallowed,
    SendDKGUnderway,
    SendNodeApproval,
    // Approved to join
    ReceivedJoinApproval,
    // Connections
    ConnectionOpened,
    ConnectionClosed,
    ConnectionReused,
    // Relocation
    RelocateStart,
    RelocateEnd,
}
