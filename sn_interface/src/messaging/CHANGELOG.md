# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

### [36.0.1](https://github.com/maidsafe/sn_messaging/compare/v36.0.0...v36.0.1) (2021-06-14)

## [36.0.0](https://github.com/maidsafe/sn_messaging/compare/v35.0.0...v36.0.0) (2021-06-14)


### ⚠ BREAKING CHANGES

* rename Proven to SectionSigned, MemberInfo to NodeState, PeerState

### api

* rename Proven to SectionSigned, MemberInfo to NodeState, PeerState ([08d6929](https://github.com/maidsafe/sn_messaging/commit/08d69293713cb8caea4449dbca4d119961403b92))

## [35.0.0](https://github.com/maidsafe/sn_messaging/compare/v34.0.0...v35.0.0) (2021-06-09)


### ⚠ BREAKING CHANGES

* **routing:** adds a new routing message variant

### Features

* **routing:** add variant to signal elders to start the connectivity ([710c44e](https://github.com/maidsafe/sn_messaging/commit/710c44ef5d619aab8757f3a69f8e4e5959fd9456))

## [34.0.0](https://github.com/maidsafe/sn_messaging/compare/v33.0.0...v34.0.0) (2021-06-08)


### ⚠ BREAKING CHANGES

* **section_info:** SectionInfoMsg payload changed as well as it's name.

* **section_info:** use SectionAuthorityProvider for SectionInfo messages payload ([5436fbf](https://github.com/maidsafe/sn_messaging/commit/5436fbfe545bbc5f723db93b82291ace36cdcca5))

## [33.0.0](https://github.com/maidsafe/sn_messaging/compare/v32.0.0...v33.0.0) (2021-06-07)


### ⚠ BREAKING CHANGES

* **join:** new Join messages are not backward compatible.

* **join:** re-organising all messages related to node joining flow ([2a94692](https://github.com/maidsafe/sn_messaging/commit/2a94692e03d5bcfe04f700cf6b7f0090af5cdbaf))

## [32.0.0](https://github.com/maidsafe/sn_messaging/compare/v31.0.0...v32.0.0) (2021-06-07)


### ⚠ BREAKING CHANGES

* refactor of SAP
removal of bls_signature_aggregator
rename Proof to Signed

### api

* SAP refactor ([402fd3f](https://github.com/maidsafe/sn_messaging/commit/402fd3fd8fa1cf9721135f17c1ccb5ed35f6f081))

## [31.0.0](https://github.com/maidsafe/sn_messaging/compare/v30.0.0...v31.0.0) (2021-06-04)


### ⚠ BREAKING CHANGES

* **messages:** adds new variant to the get-section-response

* **messages:** add GetSectionResponse::NotAllowed variant ([846e39e](https://github.com/maidsafe/sn_messaging/commit/846e39edc1beef57cd39d563d2ed86b17791235c))

## [30.0.0](https://github.com/maidsafe/sn_messaging/compare/v29.0.0...v30.0.0) (2021-06-04)


### ⚠ BREAKING CHANGES

* **variant:** affects sn_routing's Variant handling

### Features

* **variant:** remove ConnectivityComplaint variant ([71c0b5f](https://github.com/maidsafe/sn_messaging/commit/71c0b5f72ba429ee396764b6b08567706d970e9d))

## [29.0.0](https://github.com/maidsafe/sn_messaging/compare/v28.0.0...v29.0.0) (2021-06-03)


### ⚠ BREAKING CHANGES

* It's actually insufficient payment, as we dont check the balance of a wallet when doing ops

### Features

* InsufficientBalance error renamed ([8063c67](https://github.com/maidsafe/sn_messaging/commit/8063c67ba4eb6c565aaaedc6e2adc17f1ed57340))

## [28.0.0](https://github.com/maidsafe/sn_messaging/compare/v27.0.1...v28.0.0) (2021-06-01)


### ⚠ BREAKING CHANGES

* removal of proofchain alters RoutingMsg type

### Features

* send proofchain only for specific messages ([642401b](https://github.com/maidsafe/sn_messaging/commit/642401b73d3bce47de09512d8afec783353c819e))

### [27.0.1](https://github.com/maidsafe/sn_messaging/compare/v27.0.0...v27.0.1) (2021-05-31)

## [27.0.0](https://github.com/maidsafe/sn_messaging/compare/v26.0.0...v27.0.0) (2021-05-30)


### ⚠ BREAKING CHANGES

* **routingMsg:** RoutingMsg now contains its full hierarchy rather than just serialised msg bytes.

### Features

* **routingMsg:** adding all RoutingMsg definitions ([0a50f63](https://github.com/maidsafe/sn_messaging/commit/0a50f63cc02539a8d0e8f2625a49c3248c568d9c))

## [26.0.0](https://github.com/maidsafe/sn_messaging/compare/v25.1.1...v26.0.0) (2021-05-28)


### ⚠ BREAKING CHANGES

* Update DstLocation Direct->DirectAndUnrouted for clarity.

### Features

* Update DstLocation Direct->DirectAndUnrouted for clarity. ([2cfef24](https://github.com/maidsafe/sn_messaging/commit/2cfef2434c7957c95d774416489b686d8483dd0c))

### [25.1.1](https://github.com/maidsafe/sn_messaging/compare/v25.1.0...v25.1.1) (2021-05-27)

## [25.1.0](https://github.com/maidsafe/sn_messaging/compare/v25.0.0...v25.1.0) (2021-05-26)


### Features

* enable to directly generate wiremsg ([9ab515d](https://github.com/maidsafe/sn_messaging/commit/9ab515d02a08bcc397b2fc387b10dbb111bb77b9))

## [25.0.0](https://github.com/maidsafe/sn_messaging/compare/v24.0.0...v25.0.0) (2021-05-25)


### ⚠ BREAKING CHANGES

* **header:** header messages are not backward compatible.

### Features

* **header:** include the message id in the header ([d2c49c7](https://github.com/maidsafe/sn_messaging/commit/d2c49c7a6ffbe0ea70ef090bc3834cdc41e14263))

## [24.0.0](https://github.com/maidsafe/sn_messaging/compare/v23.0.0...v24.0.0) (2021-05-24)


### ⚠ BREAKING CHANGES

* removing msg::Msg enum which breaks backward compatibility.

* removing unnecessary message definitions ([6997fc9](https://github.com/maidsafe/sn_messaging/commit/6997fc9d3fa5a3e7b4fd0cca50d6dd5100282b67))

## [23.0.0](https://github.com/maidsafe/sn_messaging/compare/v22.0.1...v23.0.0) (2021-05-21)


### ⚠ BREAKING CHANGES

* **section_info:** EndUser is now a struct with just the use xorname and a socket id xorname.

### Features

* **client:** add client signature to queries and commands ([eaa3b2a](https://github.com/maidsafe/sn_messaging/commit/eaa3b2acfce90c632c5f8464c90f4f1a095a0cdf))


* **section_info:** remove RegisterEndUser related messages ([da66f58](https://github.com/maidsafe/sn_messaging/commit/da66f58262f476e8732ea6955d60dee9737c618b))

### [22.0.1](https://github.com/maidsafe/sn_messaging/compare/v22.0.0...v22.0.1) (2021-05-15)

## [22.0.0](https://github.com/maidsafe/sn_messaging/compare/v21.1.0...v22.0.0) (2021-05-12)


### ⚠ BREAKING CHANGES

* **version:** Anti-entropy related changes before this commit
included breaking changes

* **version:** remove commented code ([8a2b058](https://github.com/maidsafe/sn_messaging/commit/8a2b058260df807a66ddaf2d649cb5bd145cdbf9))

## [21.1.0](https://github.com/maidsafe/sn_messaging/compare/v21.0.0...v21.1.0) (2021-05-10)


### Features

* **msg:** add convenience deserialize method ([7a83ab7](https://github.com/maidsafe/sn_messaging/commit/7a83ab7df527edd10ece17bcfa8478b204c2514a))
* **msg:** add convenience wrapper for client and node msgs ([06fd752](https://github.com/maidsafe/sn_messaging/commit/06fd75271b50dd5124c4b5f1ea35b84026a3d20b))
* **node-cmds:** add a couple of convenience functions to serialise/deserialise NodeCmdMessages ([9fd827d](https://github.com/maidsafe/sn_messaging/commit/9fd827d21d9a54afc6edc16dbead37030f84d81a))
* **nodemsg:** add general error enum variant ([80c7056](https://github.com/maidsafe/sn_messaging/commit/80c7056324f6391c9532889a9e8930aab655b7e2))
* **serialisation:** add source section public key to WireMsgHeader ([66320e3](https://github.com/maidsafe/sn_messaging/commit/66320e306a050df7c8294a7985a09269ed7a55ee))
* add a SupportingInfo message variant ([4ac0399](https://github.com/maidsafe/sn_messaging/commit/4ac03999cce1ed3b29fa07c548ce22aecd199746))


### Bug Fixes

* add GetChunk variant under NodeSystemQueryResponse fixing chunk-replication ([cf6f5d3](https://github.com/maidsafe/sn_messaging/commit/cf6f5d3c2287e1272c0b330a128e70806cf6372e))
* fix unresolved import ([a325254](https://github.com/maidsafe/sn_messaging/commit/a325254422a6fb6a27cdf1b96ad17c44dc0a35e4))
* post-rebase issues ([b55744d](https://github.com/maidsafe/sn_messaging/commit/b55744d090c19e4a1e3e899b1155a91fd98f68ea))
* re-add necessary msgs that were removed ([9643da6](https://github.com/maidsafe/sn_messaging/commit/9643da687b31cae189bfc714641012128e4ab3ac))
* **node_msg:** remove unused target_section_pk field ([898c1a9](https://github.com/maidsafe/sn_messaging/commit/898c1a95e6d0e74d2179e5921340fc9b479899e1))
* **node_msg:** rename and expose id function ([8deb221](https://github.com/maidsafe/sn_messaging/commit/8deb221298c4ce4a9345946f74cbd32f5544e90d))

## [21.0.0](https://github.com/maidsafe/sn_messaging/compare/v20.1.0...v21.0.0) (2021-05-06)


### ⚠ BREAKING CHANGES

* **storecost:** GetStoreCost query result content changed

### Features

* **storecost:** expand query result with more data ([c5656c2](https://github.com/maidsafe/sn_messaging/commit/c5656c2cabf24bc1d4d51b588ad90abb80bcb41a))

## [20.1.0](https://github.com/maidsafe/sn_messaging/compare/v20.0.1...v20.1.0) (2021-05-05)


### Features

* **api:** add new QueryResponse::is_success API ([8d1425e](https://github.com/maidsafe/sn_messaging/commit/8d1425e054797a4046eb5730cbe394facb3b9c21))

### [20.0.1](https://github.com/maidsafe/sn_messaging/compare/v20.0.0...v20.0.1) (2021-05-03)


### Bug Fixes

* **full-adults:** use BTreeSet to hold full adult information for ([85ecb70](https://github.com/maidsafe/sn_messaging/commit/85ecb70e1210f808ac51b9d219e17fdea810a002))

## [20.0.0](https://github.com/maidsafe/sn_messaging/compare/v19.0.1...v20.0.0) (2021-04-30)


### ⚠ BREAKING CHANGES

* **all:** this deprecates some of the messages

* **all:** refactor and remove messages no longer needed ([1b15dd6](https://github.com/maidsafe/sn_messaging/commit/1b15dd66d49b7197c5b00e92105f778ef5029855))

### [19.0.1](https://github.com/maidsafe/sn_messaging/compare/v19.0.0...v19.0.1) (2021-04-27)


### Bug Fixes

* **error:** use correct address for DataNotFound error ([1518e57](https://github.com/maidsafe/sn_messaging/commit/1518e57e77bb9a43dceab4f51bd6a98033844a40))

## [19.0.0](https://github.com/maidsafe/sn_messaging/compare/v18.0.1...v19.0.0) (2021-04-27)


### ⚠ BREAKING CHANGES

* **err:** this renames one of the Error variants

* **err:** rename NoSuchData to DataNotFound and add the address ([463c6c7](https://github.com/maidsafe/sn_messaging/commit/463c6c7235e16aa5e52224ad7371a4c1b74f002a))

### [18.0.1](https://github.com/maidsafe/sn_messaging/compare/v18.0.0...v18.0.1) (2021-04-26)

## [18.0.0](https://github.com/maidsafe/sn_messaging/compare/v17.0.1...v18.0.0) (2021-04-23)


### ⚠ BREAKING CHANGES

* **adult_ack:** NodeCmdResult removed from Message enum.

* **adult_ack:** use ChunkOpHandled in NodeEvent ([9b1ba47](https://github.com/maidsafe/sn_messaging/commit/9b1ba477a5f951ab9a1fe18a9906773478e2e231))

### [17.0.1](https://github.com/maidsafe/sn_messaging/compare/v17.0.0...v17.0.1) (2021-04-23)

## [17.0.0](https://github.com/maidsafe/sn_messaging/compare/v16.2.0...v17.0.0) (2021-04-21)


### ⚠ BREAKING CHANGES

* **dataexchange:** Updated members on ReceiveExistingData cmd.

### Features

* **dataexchange:** add structs ([3e7b68b](https://github.com/maidsafe/sn_messaging/commit/3e7b68ba3b1954beae8a5615643ca4c29160d60e))

## [16.2.0](https://github.com/maidsafe/sn_messaging/compare/v16.1.0...v16.2.0) (2021-04-21)


### Features

* **register:** adding messages for Register data type operations ([082f544](https://github.com/maidsafe/sn_messaging/commit/082f544a6bc889cbc75ca806998c06504e9dbad8))

## [16.1.0](https://github.com/maidsafe/sn_messaging/compare/v16.0.0...v16.1.0) (2021-04-21)


### Features

* **data:** add NodeQuery variants to facilitate sharing of data to New Elders ([487af89](https://github.com/maidsafe/sn_messaging/commit/487af895238412a1b8fa66fcab6501ff3630b13f))

## [16.0.0](https://github.com/maidsafe/sn_messaging/compare/v15.0.0...v16.0.0) (2021-04-21)


### ⚠ BREAKING CHANGES

* **msg:** this adds a new variant to the message enum

### Features

* **msg:** add message variant for NodeCmdResult ([f3a6e48](https://github.com/maidsafe/sn_messaging/commit/f3a6e487e18a29d7ba08b52edaf62b1b40be8feb))

## [15.0.0](https://github.com/maidsafe/sn_messaging/compare/v14.0.0...v15.0.0) (2021-04-21)


### ⚠ BREAKING CHANGES

* re-enable aggregate at source

### api

* re-enable aggregate at source ([2074770](https://github.com/maidsafe/sn_messaging/commit/207477079622c6fdc9e0ee44ba58d694bf9b3fe6))

## [14.0.0](https://github.com/maidsafe/sn_messaging/compare/v13.1.0...v14.0.0) (2021-04-13)


### ⚠ BREAKING CHANGES

* **chunks:** Node messages changed members.

* **chunks:** turn around the replication flow ([cc1e5e8](https://github.com/maidsafe/sn_messaging/commit/cc1e5e8901cf10c89c2fc0db31adc51e6968d4ee))

## [13.1.0](https://github.com/maidsafe/sn_messaging/compare/v13.0.0...v13.1.0) (2021-04-08)


### Features

* include the info when join is allowed ([16a5870](https://github.com/maidsafe/sn_messaging/commit/16a587007e024a109b052cb176670dc4a15dc5e0))

## [13.0.0](https://github.com/maidsafe/sn_messaging/compare/v12.0.0...v13.0.0) (2021-03-31)


### ⚠ BREAKING CHANGES

* Aggregation scheme variant removed.

* remove unused scheme variant ([27c7d86](https://github.com/maidsafe/sn_messaging/commit/27c7d86ebce2a734183aa0136478ea6f13c7a511))

## [12.0.0](https://github.com/maidsafe/sn_messaging/compare/v11.0.0...v12.0.0) (2021-03-30)


### ⚠ BREAKING CHANGES

* This reverts commits in release 10.0.0

* Revert release "10.0.0" ([158e3a3](https://github.com/maidsafe/sn_messaging/commit/158e3a312b4461d1d6a23aaefd3e426a17c6b503))

## [11.0.0](https://github.com/maidsafe/sn_messaging/compare/v10.0.0...v11.0.0) (2021-03-30)


### ⚠ BREAKING CHANGES

* **deps:** Changes to node reward messages

### Bug Fixes

* clarify api ([6db2924](https://github.com/maidsafe/sn_messaging/commit/6db29249540c3329a7e92c2f3187933ffa5f7159))


* **deps:** update sn_data_types ([2597df9](https://github.com/maidsafe/sn_messaging/commit/2597df926eef60298314d79764f5d0153c4225b6))

## [10.0.0](https://github.com/maidsafe/sn_messaging/compare/v9.0.0...v10.0.0) (2021-03-26)


### ⚠ BREAKING CHANGES

* Split message type into two, for passing process errors
or messages to be processed.

### Features

* add id func ([2a6ccea](https://github.com/maidsafe/sn_messaging/commit/2a6ccead51550424ea6ac9db6227dddfd9f9ebb7))
* add processing error reasons ([7455746](https://github.com/maidsafe/sn_messaging/commit/7455746f827b4cfd4bfa9fc34a4af46872e44688))
* expose processing error ([2237743](https://github.com/maidsafe/sn_messaging/commit/22377431752bb2430fcd57845c4d1e87526bbba8))
* functional msg id ([fd4062a](https://github.com/maidsafe/sn_messaging/commit/fd4062ab750f3c7ec49281ced38f5d548308b4ad))
* initial LazyError proposal ([2a971aa](https://github.com/maidsafe/sn_messaging/commit/2a971aab884a8ed4e8538262a92892f3fc5b6634))
* make headerinfo fields pub ([a868218](https://github.com/maidsafe/sn_messaging/commit/a86821861d58dba2838dc66197a99c15faef4d96))
* make source message optional ([943048b](https://github.com/maidsafe/sn_messaging/commit/943048baa6438339ed962b1e4885259fd3485b92))
* remove processErrorReason, just use standard error message ([72283d8](https://github.com/maidsafe/sn_messaging/commit/72283d86eb7703e0e8faa1f97d90a97c81cdf2ae))
* **GetSectionQuery:** use PK instead of Xor so we have a response pk ([49f29fb](https://github.com/maidsafe/sn_messaging/commit/49f29fb3a6709a902cbb9c6c2e23d2c29dd5f036))
* **Redirect:** provide elder name too ([7129f9d](https://github.com/maidsafe/sn_messaging/commit/7129f9d5e5f01235e42b87e0e15652241f7ae1c7))
* **serialisation:** add destination XorName and destination section public key to WireMsgHeader ([7c65ff1](https://github.com/maidsafe/sn_messaging/commit/7c65ff1eacf98d97ee6cebc1e7796b5981c54e36))
* **trait:** derive Clone Trait for multiple types ([eea57c2](https://github.com/maidsafe/sn_messaging/commit/eea57c276622b31e055fcfaaeaabd76f199d9d3c))
* one message to rule them all ([8cb9c49](https://github.com/maidsafe/sn_messaging/commit/8cb9c49405782b9bd313ea98435799f31bd445f2))


### Bug Fixes

* expose create_processing_error_msg ([0070d89](https://github.com/maidsafe/sn_messaging/commit/0070d899cee49a18be5477f2585a0283d8c02c08))
* missing export of NodeSystemQueryResponse ([1883557](https://github.com/maidsafe/sn_messaging/commit/1883557ff72acc72eeaf9c33425d7be722f4f08e))

## [9.0.0](https://github.com/maidsafe/sn_messaging/compare/v8.0.0...v9.0.0) (2021-03-22)


### ⚠ BREAKING CHANGES

* DT update. Naming and message structs for split
* SetupSections as opposed to GetSectionWallet, allows passing of sibling PK for proper setup of both section wallets

### Features

* Remove GetReplicaKeys trasnfer query ([c4c7a2a](https://github.com/maidsafe/sn_messaging/commit/c4c7a2a019ac9478f2a81d513117c1a21308d7f1))
* **cmds:** add CreateSectionWallet cmd ([8afb2cf](https://github.com/maidsafe/sn_messaging/commit/8afb2cf72252b2c737a695628920d725e12ce468))
* **msg_id:** generate from content ([386c092](https://github.com/maidsafe/sn_messaging/commit/386c0925c5ce974b8b08a634f7a98be6e03c297e))
* **msgs:** rename query ([9bb508b](https://github.com/maidsafe/sn_messaging/commit/9bb508b7b6f760cc214300450a6fda04d4d33528))
* **node:** add PromotedToElder event ([010bad2](https://github.com/maidsafe/sn_messaging/commit/010bad2595a9054c934f2464064b06cf2a654c13))
* **systemcmd:** extend with wallet proposals ([553adaa](https://github.com/maidsafe/sn_messaging/commit/553adaa67e54130964e04d8e845f38cbcaa60dfc))
* **transfers:** propagate the credit proof only ([059eb74](https://github.com/maidsafe/sn_messaging/commit/059eb7427e99a4aadd50129c81b355757a41fb1d))
* updates to message naming + sibling key passing ([05b0a32](https://github.com/maidsafe/sn_messaging/commit/05b0a32c2f9ef045f2c75e28af670899795c569d))
* updates to message naming, and removing sibling key passing ([75c9b0b](https://github.com/maidsafe/sn_messaging/commit/75c9b0b57708ef68667c5119029c297ae065f4d8))


### Bug Fixes

* remove leftover GetReplicaKeys refs ([ecde8bc](https://github.com/maidsafe/sn_messaging/commit/ecde8bc2d2c1b078579026f01d472feb5f32fe36))
* remove redundant origin field ([21bf1cb](https://github.com/maidsafe/sn_messaging/commit/21bf1cb88531f5268a0808ae558fb0609aa784e2))


* DT dep update ([b82b223](https://github.com/maidsafe/sn_messaging/commit/b82b2237fe67cc72d294d94f9cb61f31c2ee6ef6))

## [8.0.0](https://github.com/maidsafe/sn_messaging/compare/v7.0.1...v8.0.0) (2021-03-03)


### ⚠ BREAKING CHANGES

* **Seq:** Policy mutation operations are removed.

### Features

* **Seq:** upgrading sn_data_types to v0.16.0 and removing operations that are meant for mutating Seuquence's Policy ([306d8c1](https://github.com/maidsafe/sn_messaging/commit/306d8c16ea627f2aaed597d8c0df3698ab7d3a3e))

### [7.0.1](https://github.com/maidsafe/sn_messaging/compare/v7.0.0...v7.0.1) (2021-03-03)

## [7.0.0](https://github.com/maidsafe/sn_messaging/compare/v6.0.2...v7.0.0) (2021-02-26)


### ⚠ BREAKING CHANGES

* **api:** location scheme updated, breaking the current messaging api

### Features

* **api:** api updated ([4e11d0e](https://github.com/maidsafe/sn_messaging/commit/4e11d0ecf10eb1b9c5ead1ac5be0de1e079bff05))

### [6.0.2](https://github.com/maidsafe/sn_messaging/compare/v6.0.1...v6.0.2) (2021-02-26)

### [6.0.1](https://github.com/maidsafe/sn_messaging/compare/v6.0.0...v6.0.1) (2021-02-24)

## [6.0.0](https://github.com/maidsafe/sn_messaging/compare/v5.0.0...v6.0.0) (2021-02-24)


### ⚠ BREAKING CHANGES

* **location:** this adds a new variant to SrcLocation

### Features

* **location:** add support for accumulation at destination ([89cadad](https://github.com/maidsafe/sn_messaging/commit/89cadad9669295f2833f0a161acd252d04e4218a))

## [5.0.0](https://github.com/maidsafe/sn_messaging/compare/v4.0.5...v5.0.0) (2021-02-22)


### ⚠ BREAKING CHANGES

* **deps:** remove msgenvelope, change infrastructure msg

### Features

* **enduser:** add bootstrap msg variants ([129924e](https://github.com/maidsafe/sn_messaging/commit/129924e03eb020881322b1ce3d5412de70c02172))
* **enduser:** replace socketaddr with a hash ([45ac67f](https://github.com/maidsafe/sn_messaging/commit/45ac67f80dd010c3536a4632dcbf952d0f01a007))
* **messages:** implement location ([cf37569](https://github.com/maidsafe/sn_messaging/commit/cf37569d55515d35e5652c2c06f9ac3e8b3b7dbc))
* **messages:** remove MsgEnvelope ([d54b6c4](https://github.com/maidsafe/sn_messaging/commit/d54b6c42d119221f066d24109805b0995caf224b))


### Bug Fixes

* add string payload to invalid bootstrap error ([23ed16c](https://github.com/maidsafe/sn_messaging/commit/23ed16c2dd68f75c2554f7cc4d54c6fd6a9c7522))
* improve debug msg ([17daa0b](https://github.com/maidsafe/sn_messaging/commit/17daa0bca15a1d48a8869642969eca213196312e))
* post-rebase issues ([93578c7](https://github.com/maidsafe/sn_messaging/commit/93578c7b60fe8ec30dfbfd8d4371e8566284a9c1))


* **deps:** update sn_data_types ([555e4fb](https://github.com/maidsafe/sn_messaging/commit/555e4fbb3222ba0a46fd189c9c62bfd8052d9d19))

### [4.0.5](https://github.com/maidsafe/sn_messaging/compare/v4.0.4...v4.0.5) (2021-02-22)

### [4.0.4](https://github.com/maidsafe/sn_messaging/compare/v4.0.3...v4.0.4) (2021-02-18)

### [4.0.3](https://github.com/maidsafe/sn_messaging/compare/v4.0.2...v4.0.3) (2021-02-17)

### [4.0.2](https://github.com/maidsafe/sn_messaging/compare/v4.0.1...v4.0.2) (2021-02-16)

### [4.0.1](https://github.com/maidsafe/sn_messaging/compare/v4.0.0...v4.0.1) (2021-02-16)

## [4.0.0](https://github.com/maidsafe/sn_messaging/compare/v3.0.2...v4.0.0) (2021-02-15)


### ⚠ BREAKING CHANGES

* adds more infrastructure information to bootstrap and on section key errors
* Adds pk to messages and helper on MsgEnvelope

### Features

* add infrastructure information. ([9ca78b7](https://github.com/maidsafe/sn_messaging/commit/9ca78b78a8acf0cc3f6d9b9195a1483c66934d49))
* error messages related to target pk ([08d31d3](https://github.com/maidsafe/sn_messaging/commit/08d31d3f694bf92562499a498bc0b7dd903ff61c))
* make infra error its own type, use that in client::Error. ([122bc07](https://github.com/maidsafe/sn_messaging/commit/122bc0755078602a65275d4b7ccf2e8d759c8ef9))
* require a section key PK to be passed with all messages ([60f5240](https://github.com/maidsafe/sn_messaging/commit/60f5240ac8242d04e970773cdccfcb7ccd4a9e3e))

### [3.0.2](https://github.com/maidsafe/sn_messaging/compare/v3.0.1...v3.0.2) (2021-02-08)

### [3.0.1](https://github.com/maidsafe/sn_messaging/compare/v3.0.0...v3.0.1) (2021-02-08)

## [3.0.0](https://github.com/maidsafe/sn_messaging/compare/v2.0.0...v3.0.0) (2021-02-03)


### ⚠ BREAKING CHANGES

* **types:** moving client messages to its own module and publis namespace.

### Features

* **types:** adding Ping, NodeMessage and InfrastructureQuery definitions and support in serialisation ([dcd6b32](https://github.com/maidsafe/sn_messaging/commit/dcd6b321154714000d67c38137d1155433c4672a))

## [2.0.0](https://github.com/maidsafe/sn_messaging/compare/v1.6.1...v2.0.0) (2021-02-01)


### ⚠ BREAKING CHANGES

* rename money to token

* rename money to token ([eb53ef5](https://github.com/maidsafe/sn_messaging/commit/eb53ef577da48c9850e8997fcb91ebc6ae9fefd2))

### [1.6.1](https://github.com/maidsafe/sn_messaging/compare/v1.6.0...v1.6.1) (2021-02-01)

## [1.6.0](https://github.com/maidsafe/sn_messaging/compare/v1.5.5...v1.6.0) (2021-01-29)


### Features

* **genesis:** add msgs for genesis section init ([a808d3f](https://github.com/maidsafe/sn_messaging/commit/a808d3fbcf1ab10c8b21876ab177d97ffab47abc))
* **node_transfers:** add section payout error ([86114a5](https://github.com/maidsafe/sn_messaging/commit/86114a53593786ced19def470ddf262821d927ba))
* **nodeevents:** add SectionPayoutRegistered ([b782d19](https://github.com/maidsafe/sn_messaging/commit/b782d19cfa94a2d8b76cde714c3102dcfc9dc944))
* **rewards:** use share for payout validation ([041330b](https://github.com/maidsafe/sn_messaging/commit/041330bec25561e350a4fe28cc36cba4eb5f4d51))

### [1.5.5](https://github.com/maidsafe/sn_messaging/compare/v1.5.4...v1.5.5) (2021-01-15)

### [1.5.4](https://github.com/maidsafe/sn_messaging/compare/v1.5.3...v1.5.4) (2021-01-15)

### [1.5.3](https://github.com/maidsafe/sn_messaging/compare/v1.5.2...v1.5.3) (2021-01-13)

### [1.5.2](https://github.com/maidsafe/sn_messaging/compare/v1.5.1...v1.5.2) (2021-01-13)

### [1.5.1](https://github.com/maidsafe/sn_messaging/compare/v1.5.0...v1.5.1) (2021-01-13)


### Bug Fixes

* **verification:** verification of message_env checks underlying message, not itself ([ee90aef](https://github.com/maidsafe/sn_messaging/commit/ee90aef0c3164db4d57aba0022a6b82b941eec1b))

## [1.5.0](https://github.com/maidsafe/sn_messaging/compare/v1.4.0...v1.5.0) (2021-01-12)


### Features

* **errrors:** remove unexpectedNode error. ([effc838](https://github.com/maidsafe/sn_messaging/commit/effc838d7ff1d3297eced4026a5584f0ac90291b))

## [1.4.0](https://github.com/maidsafe/sn_messaging/compare/v1.3.0...v1.4.0) (2021-01-12)


### Features

* **errors:** add node relocation error ([cc8887f](https://github.com/maidsafe/sn_messaging/commit/cc8887f37b667242b861f8f9554c1cca0b64eb7d))

## [1.3.0](https://github.com/maidsafe/sn_messaging/compare/v1.2.0...v1.3.0) (2021-01-12)


### Features

* **serialisation:** add a size field to the wire message header and support only Msgpack serialisation type for protocol v1 ([b9eb6d6](https://github.com/maidsafe/sn_messaging/commit/b9eb6d6db6148a1554cf2d42e2a177f7ac6e0db7))
* **serialisation:** serialize to JSON with a wire message header ([806f3e4](https://github.com/maidsafe/sn_messaging/commit/806f3e4042c752cd69a3e0970e677e6affc37488))
* **serialisation:** support Msgpack serialisation type ([74870b1](https://github.com/maidsafe/sn_messaging/commit/74870b11bbe4e35d7887304bccf3d3e81362ac38))


### Bug Fixes

* **serialisation:** minor refactor and fix to Msgpack deserialisation logic ([d7c84e6](https://github.com/maidsafe/sn_messaging/commit/d7c84e6e1dd4f594613dac54ed2cc0ae0e958849))

## [1.2.0](https://github.com/maidsafe/sn_messaging/compare/v1.1.0...v1.2.0) (2021-01-05)


### Features

* **deps:** use crates.io sn_data_types ([0a4270a](https://github.com/maidsafe/sn_messaging/commit/0a4270a18100fa4d046d658f54553a8fcbcdf168))

## 1.1.0 (2021-01-05)


### Features

* more errors ([b8144bc](https://github.com/maidsafe/sn_messaging/commit/b8144bcbb88ee3bdcad3a9933c80c9fc2ac2ed76))
* **init:** initial port of messaging from sn_data_types ([10b874c](https://github.com/maidsafe/sn_messaging/commit/10b874c01e853a86f65947136498450bf5ff293d))

# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.
