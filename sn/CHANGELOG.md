# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.54.2 (2022-01-25)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 1 commit contributed to the release.
 - 0 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Merge #929 ([`c4ba2db`](https://github.com/maidsafe/safe_network/commit/c4ba2db749c5ff57a8c74eacb22307e2358182b2))
</details>

## v0.54.1 (2022-01-24)

### Bug Fixes

 - <csr-id-151993ac6442224079dc02bfe476d2dfbc1a411b/> put connection ensurance and sending within the same spawned thread

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 2 days passed between releases.
 - 4 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.54.1 ([`5a90ec6`](https://github.com/maidsafe/safe_network/commit/5a90ec673edcb36ae954b0af6144bae7d8243cd7))
    - put connection ensurance and sending within the same spawned thread ([`151993a`](https://github.com/maidsafe/safe_network/commit/151993ac6442224079dc02bfe476d2dfbc1a411b))
    - keep all logs by default ([`99df748`](https://github.com/maidsafe/safe_network/commit/99df748624a36e1d2457cb81f74e8cd8accb8633))
    - address some review comments ([`4e517a1`](https://github.com/maidsafe/safe_network/commit/4e517a1fd57e63a3b5381ecd0cdf9db4f762e03f))
</details>

## v0.54.0 (2022-01-22)

<csr-id-8f58c5d900ce00d90bf7421f34122f4ca5ff5601/>

### chore (BREAKING)

 - <csr-id-8f58c5d900ce00d90bf7421f34122f4ca5ff5601/> making section members list non optional in AntiEntropyUpdate message type


### Bug Fixes

 - <csr-id-8f5f7fd0c57db4a2cd29dfe2dc617b796fea97f4/> make client sending message non-blocking
 - <csr-id-36daaebd84e07c3ff55477e24598e6b9d0c0b314/> correct correlation_id for query response
 - <csr-id-56b6b11447010485c860a55e6dbd035d826091d1/> client wait longer for AE update got triggered
 - <csr-id-10123e6a3d18129fcb5c0090d409a02b2f762139/> using try_send to avoid blocking
 - <csr-id-1e4049fc944f8bcab0e51671219896854a3bffaa/> return CmdError to the caller
 - <csr-id-c0cc9807df988ca3a871720edadc4ff19e028f63/> sign to client msg with node's Ed key

### New Features

 - <csr-id-61e87de077bd47b3dd106dc1e1dd65aa1f5b2d0e/> retry intial client contact if it fails
   Previously we tried to connect to all known nodes, but if we reached the end of the list, we'd keep hitting the same nodes over and over.
   Now we fail after trying all candidates, and retry with a new xorname to get new candidates (if we know a prefixmap).
   We also mark the attempted connections as failed, so the client attempts to create
   fresh new connections instead of using possibly dead connections
 - <csr-id-a02f5f0de50c7c58b1cda55a14cb63d805c772c5/> clients mark closed connections as such
   And now check that the current connection is valid before trying to use it.
   Otherwise they reconnect
 - <csr-id-d88890261c2ca06498914714eefb56aff61673d5/> counting ACK/Err cmd response together and notify error to upper layer
 - <csr-id-939f353ed399d0148ba52225ca6d1676d3d7c04b/> timeout on waiting CmdAcks
 - <csr-id-b33b7dcb7fa6eb976bff7f38ac3d6ddc62e1977a/> send CmdAck on client cmd

### refactor (BREAKING)

 - <csr-id-959e909d8c61230ea143702858cb7b4c42caffbf/> removing the internal routing layer

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 28 commits contributed to the release.
 - 1 day passed between releases.
 - 22 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.54.0/sn_api-0.52.0/sn_cli-0.45.0 ([`0190f03`](https://github.com/maidsafe/safe_network/commit/0190f0305980bdaee30f9f2ab5eb5510149916db))
    - resolve clippy failures after rebase ([`274cc12`](https://github.com/maidsafe/safe_network/commit/274cc12ca37da5ff536a7b4ab59f803546ccefe9))
    - update remaining places ([`3dc2327`](https://github.com/maidsafe/safe_network/commit/3dc23278c6a4fabc250b27f4312f5c51f0f271a4))
    - make client sending message non-blocking ([`8f5f7fd`](https://github.com/maidsafe/safe_network/commit/8f5f7fd0c57db4a2cd29dfe2dc617b796fea97f4))
    - Merge #958 ([`437a113`](https://github.com/maidsafe/safe_network/commit/437a113e6e5736e4eb4287f41228806678a9762e))
    - correct correlation_id for query response ([`36daaeb`](https://github.com/maidsafe/safe_network/commit/36daaebd84e07c3ff55477e24598e6b9d0c0b314))
    - Merge #955 ([`b7581b9`](https://github.com/maidsafe/safe_network/commit/b7581b91ed7778b1b962a0fdfc7d33a65cc7098c))
    - client wait longer for AE update got triggered ([`56b6b11`](https://github.com/maidsafe/safe_network/commit/56b6b11447010485c860a55e6dbd035d826091d1))
    - Merge branch 'main' into simplify-sn-api ([`33ef052`](https://github.com/maidsafe/safe_network/commit/33ef0524ae238391f25c8fb340627c34ea79fcb2))
    - using try_send to avoid blocking ([`10123e6`](https://github.com/maidsafe/safe_network/commit/10123e6a3d18129fcb5c0090d409a02b2f762139))
    - removing the internal routing layer ([`959e909`](https://github.com/maidsafe/safe_network/commit/959e909d8c61230ea143702858cb7b4c42caffbf))
    - wait before attempting to make_contact again ([`1b3c0eb`](https://github.com/maidsafe/safe_network/commit/1b3c0eb4443cff3f4575164907a7a1fbf92cebe2))
    - remove one layer of indirection ([`3b5ce19`](https://github.com/maidsafe/safe_network/commit/3b5ce194213a7090ee83c02b0043700cda230796))
    - return CmdError to the caller ([`1e4049f`](https://github.com/maidsafe/safe_network/commit/1e4049fc944f8bcab0e51671219896854a3bffaa))
    - retry intial client contact if it fails ([`61e87de`](https://github.com/maidsafe/safe_network/commit/61e87de077bd47b3dd106dc1e1dd65aa1f5b2d0e))
    - sign to client msg with node's Ed key ([`c0cc980`](https://github.com/maidsafe/safe_network/commit/c0cc9807df988ca3a871720edadc4ff19e028f63))
    - clients mark closed connections as such ([`a02f5f0`](https://github.com/maidsafe/safe_network/commit/a02f5f0de50c7c58b1cda55a14cb63d805c772c5))
    - counting ACK/Err cmd response together and notify error to upper layer ([`d888902`](https://github.com/maidsafe/safe_network/commit/d88890261c2ca06498914714eefb56aff61673d5))
    - dont fail client send w/ closed conn ([`d96ded1`](https://github.com/maidsafe/safe_network/commit/d96ded11ca74e75dde3dcc0d0b865712895d3dcc))
    - ignore many_client tests ([`d84ac52`](https://github.com/maidsafe/safe_network/commit/d84ac520676b83f39d5cf356e89ac3ec2d6478cc))
    - timeout on waiting CmdAcks ([`939f353`](https://github.com/maidsafe/safe_network/commit/939f353ed399d0148ba52225ca6d1676d3d7c04b))
    - Merge #951 ([`7f42d4e`](https://github.com/maidsafe/safe_network/commit/7f42d4e38d3bbf5b946c499b6a3543e50096a036))
    - send CmdAck on client cmd ([`b33b7dc`](https://github.com/maidsafe/safe_network/commit/b33b7dcb7fa6eb976bff7f38ac3d6ddc62e1977a))
    - Merge branch 'main' into node-logrotate ([`4c86c16`](https://github.com/maidsafe/safe_network/commit/4c86c16fca4a5eb63c37c74344fa726542fa3422))
    - Merge #962 ([`29d01da`](https://github.com/maidsafe/safe_network/commit/29d01da5233fd2a10b30699b555a0d85d7a7409a))
    - update year on files modified 2022 ([`7a7752f`](https://github.com/maidsafe/safe_network/commit/7a7752f830785ec39d301e751dc75f228d43d595))
    - making section members list non optional in AntiEntropyUpdate message type ([`8f58c5d`](https://github.com/maidsafe/safe_network/commit/8f58c5d900ce00d90bf7421f34122f4ca5ff5601))
    - send CmdAck for client cmd during test ([`37d2522`](https://github.com/maidsafe/safe_network/commit/37d2522619e49212572aa37034e0ba1857679dc7))
</details>

## v0.53.0 (2022-01-20)

<csr-id-8f58c5d900ce00d90bf7421f34122f4ca5ff5601/>

### Documentation

 - <csr-id-1d36f019159296db6d5a6bb4ebeec8699ea39f96/> add doc for usage of DKG in safe_network
   - also moves README from the .github folder so it doesn't show up in the
   repo root

### New Features

 - <csr-id-f81ade56b2b43cfbf81c6112d4c687d5b540b101/> verify blobs QueryResponse from adults and penalise if faulty
 - <csr-id-5cf1dc87b67e9af9e50299a75cdaf9d571408e10/> Add jitter into client query request timing
   This should help prevent nodes being hammered over the same period.
   This PR also increases the default client query time to 500s.

### Bug Fixes

 - <csr-id-5102396bd1cc1bc8c7765a9c8da74abe8ff02a1e/> new rust v and new clippy warnings
 - <csr-id-d4634f0e5e89e22d6ab3c70ec9135bfb80360c7e/> make owner semantics unbreakable
 - <csr-id-5bd1198aa8bd7f1b78282fb02b262e38f9121d78/> include ServiceAuth in DataQuery to Adults
 - <csr-id-42d3b932f69c3680e8aaff488395406983728ed6/> only send data cmd to 3 elders
 - <csr-id-a1de919c3d96c3ea5f566178989c9db9af01e468/> Store logging guard outwith of optional feat block.
   A code block was added to not log when using tokio console.
   This was resulting in our  being dropped and no node logging!
   This stores the gaurd outwith this block.
 - <csr-id-39f2cc24c1ad706cb7700b8a89585c2098f589c4/> on ae update, use pre update adults for checks
   Previously we'd had the  checking current
   against current adults. Which obviously did nothing.
   
   Now we grab adults before we update from AE flows. So we _should_ see
   changes, and initiate reorganisational flows for chunks etc.
 - <csr-id-2d339a094f3ccf589304dcddf6a16956280ca7a4/> use async/await over BoxFuture
   this seems to affect the regular functioning of the node
 - <csr-id-841b917977a484568615b540b267e3192cc95ed5/> dont .await on the root task
 - <csr-id-b55a7044fae9e578cb10b82a3b1db2ab6400b3db/> revert to original way of creating the Runtime
   at first tokio-console didn't work without the use of #[tokio::main]
   turns out it was something else, preventing the console from starting up
 - <csr-id-0f0ef819f4b36e5056dfa7f1983238bd4752caba/> read from disk on cache miss
 - <csr-id-e18c88019d37ab4f7618dde1a90e19ddf94db1c7/> VersioinHash use Display for encode
 - <csr-id-edefd2d7860e6f79c07060e078cdaa433da9e804/> use latest PK to send DataExchange messages
 - <csr-id-687434b5cccc3b796e18e4356ce2114b72c2c1ad/> update config smoke test expected size
 - <csr-id-4542a579ac4baadaeb06c10592f6d65fe148bafc/> make register query op-ids unique
 - <csr-id-83ef7a66bb245e2303b80d98d6b8fa888b93d6ba/> make use of all the queries
 - <csr-id-d080586074dea44b53a9901cb2e85599cc997379/> wait for a full section's info
   Before a client can perform put/query we shoud have at least one full
   section's info to work properly
 - <csr-id-06f980874357872e16535b0371c7e0e15a8d0a1c/> set DEFAULT_CHUNK_COPY_COUNT to 4 once again

### chore (BREAKING)

 - <csr-id-8f58c5d900ce00d90bf7421f34122f4ca5ff5601/> making section members list non optional in AntiEntropyUpdate message type


### refactor (BREAKING)

 - <csr-id-f22e4dce22492079170cbaeb8c29b3911faaf89a/> removing MIN_AGE and the concept of mature node
   - Also refactoring routing internal API for querying section peers from its network knowledge.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 61 commits contributed to the release over the course of 13 calendar days.
 - 13 days passed between releases.
 - 35 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.53.0/sn_api-0.51.0/sn_cli-0.44.0 ([`923930a`](https://github.com/maidsafe/safe_network/commit/923930acb3769cfa7047954a1fee1853ec9e3062))
    - moving relocation module within routing::Core where it belongs ([`82f3997`](https://github.com/maidsafe/safe_network/commit/82f39977ec965429269a31639ce83be9749b5d80))
    - update config smoke test expected size ([`687434b`](https://github.com/maidsafe/safe_network/commit/687434b5cccc3b796e18e4356ce2114b72c2c1ad))
    - moving logic to check for split into a helper function ([`565711d`](https://github.com/maidsafe/safe_network/commit/565711d4bc58f18181d279e5f29d9adb3acb051a))
    - qp2p update/ clippy fixes and code comment cleanup ([`b06bc6e`](https://github.com/maidsafe/safe_network/commit/b06bc6eb0fd210b6abc9039d1f20ab8d93befc16))
    - Merge branch 'main' into fix-update-section ([`1b41609`](https://github.com/maidsafe/safe_network/commit/1b4160969789f706b49534dbeb08bd920ecdb80a))
    - allow 1gb uncompressed logs by default ([`f7f5936`](https://github.com/maidsafe/safe_network/commit/f7f59369d09a4d25a94328d031f00cc61b187eed))
    - lessen nesting and indentation ([`96a955e`](https://github.com/maidsafe/safe_network/commit/96a955e5b124db3250c3d0fd09926cec10322632))
    - Merge branch 'main' into node-logrotate ([`6bb5166`](https://github.com/maidsafe/safe_network/commit/6bb5166749e421964e8074ad7db003eaca5437f7))
    - re-enable init with populated register ([`4eef4d3`](https://github.com/maidsafe/safe_network/commit/4eef4d3a9356d6a0724c0c670809e8d9fd1f11c0))
    - solving new clippy findings ([`57749b7`](https://github.com/maidsafe/safe_network/commit/57749b7d0671423fe205447bc84d9f8bfc99f54b))
    - Merge #935 #937 ([`cbd1125`](https://github.com/maidsafe/safe_network/commit/cbd1125403511b25d6563b1f302a92f554ace43e))
    - Merge node-logrotate origin for rebase with main ([`6df7f6f`](https://github.com/maidsafe/safe_network/commit/6df7f6fec3ee9d37b44db188fd670e4b65796e8c))
    - store cap within the register ([`c222370`](https://github.com/maidsafe/safe_network/commit/c222370b52befaedebe895fcf7feb2c1da99aeaa))
    - removing MIN_AGE and the concept of mature node ([`f22e4dc`](https://github.com/maidsafe/safe_network/commit/f22e4dce22492079170cbaeb8c29b3911faaf89a))
    - Merge #942 ([`0cd2893`](https://github.com/maidsafe/safe_network/commit/0cd289366d9bd41b27df4d03ca80d4de03323915))
    - Merge branch 'main' into verify-blob ([`da43bd7`](https://github.com/maidsafe/safe_network/commit/da43bd72dbd5ffdf9b6bbdc735b0350f1b43e859))
    - Don't tie idle_timeout to query_timeout ([`252f81e`](https://github.com/maidsafe/safe_network/commit/252f81e155dce2b8d774c8999b730e763674d93f))
    - Apply rustfmt changes ([`8151219`](https://github.com/maidsafe/safe_network/commit/815121952c4f6bf85725828214fe439a647d7260))
    - Apply rustfmt changes ([`76b772c`](https://github.com/maidsafe/safe_network/commit/76b772c949626e9e5ffe6aef804a556aad2b8abd))
    - only check permissions once ([`a018022`](https://github.com/maidsafe/safe_network/commit/a018022e4eb298a9a66513c770c0edc1b415a886))
    - remove unused old file ([`40b00cb`](https://github.com/maidsafe/safe_network/commit/40b00cbd899c72889439b9f94b34b173ff3af837))
    - Merge #885 ([`72a3f12`](https://github.com/maidsafe/safe_network/commit/72a3f1269c9c38add9b88455837655f2bc33b551))
    - Merge branch 'main' into verify-blob ([`df7bf60`](https://github.com/maidsafe/safe_network/commit/df7bf601d1f30048aa33602094b7224ac043558e))
    - fix client config query timeout check ([`893df83`](https://github.com/maidsafe/safe_network/commit/893df83ec9a1c4231f09b4e7bad9c15ef2944036))
    - Remove a couple of commented out lines ([`54e7325`](https://github.com/maidsafe/safe_network/commit/54e7325bbdb9de25bd2f329122e2e132c5338544))
    - Remove a couple of commented out lines ([`3c40347`](https://github.com/maidsafe/safe_network/commit/3c40347abfdc4c7bd40ca702cf869a19abf67cb1))
    - fix additional wrongly setup test cases ([`941b83f`](https://github.com/maidsafe/safe_network/commit/941b83f3960c84cfee86a8c818233fbbc403c189))
    - on ae update, use pre update adults for checks ([`39f2cc2`](https://github.com/maidsafe/safe_network/commit/39f2cc24c1ad706cb7700b8a89585c2098f589c4))
    - use async/await over BoxFuture ([`2d339a0`](https://github.com/maidsafe/safe_network/commit/2d339a094f3ccf589304dcddf6a16956280ca7a4))
    - verify blobs QueryResponse from adults and penalise if faulty ([`f81ade5`](https://github.com/maidsafe/safe_network/commit/f81ade56b2b43cfbf81c6112d4c687d5b540b101))
    - Add jitter into client query request timing ([`5cf1dc8`](https://github.com/maidsafe/safe_network/commit/5cf1dc87b67e9af9e50299a75cdaf9d571408e10))
    - Provide dummy fmt::Debug for FileRotateAppender ([`49884c0`](https://github.com/maidsafe/safe_network/commit/49884c0bd545c297b9089250681b925acf932931))
    - Provide dummy fmt::Debug for FileRotateAppender ([`e07cfc0`](https://github.com/maidsafe/safe_network/commit/e07cfc040e415783d37ce91ed4af301b8bf19c15))
    - new rust v and new clippy warnings ([`5102396`](https://github.com/maidsafe/safe_network/commit/5102396bd1cc1bc8c7765a9c8da74abe8ff02a1e))
    - Store logging guard outwith of optional feat block. ([`a1de919`](https://github.com/maidsafe/safe_network/commit/a1de919c3d96c3ea5f566178989c9db9af01e468))
    - Merge #898 ([`99fd463`](https://github.com/maidsafe/safe_network/commit/99fd46375154a7efca6f795795832234f57e9f5c))
    - dont .await on the root task ([`841b917`](https://github.com/maidsafe/safe_network/commit/841b917977a484568615b540b267e3192cc95ed5))
    - Merge #930 ([`5606698`](https://github.com/maidsafe/safe_network/commit/560669861b6e35d7af8f82091a49cb35200cc7f5))
    - sn_node support for logrotate style logs (issue #938) ([`96adbf9`](https://github.com/maidsafe/safe_network/commit/96adbf995c72abd9d98235a76a36263b81a07adf))
    - sn_node support for logrotate style logs (issue #938) ([`faf27a5`](https://github.com/maidsafe/safe_network/commit/faf27a5c7a69c7484f3f873e08575e40de190e00))
    - make owner semantics unbreakable ([`d4634f0`](https://github.com/maidsafe/safe_network/commit/d4634f0e5e89e22d6ab3c70ec9135bfb80360c7e))
    - revert to original way of creating the Runtime ([`b55a704`](https://github.com/maidsafe/safe_network/commit/b55a7044fae9e578cb10b82a3b1db2ab6400b3db))
    - removing unused routing Command and API ([`56bed01`](https://github.com/maidsafe/safe_network/commit/56bed01344ca5ec74a49c2a41116ef76fb33e3b4))
    - make register query op-ids unique ([`4542a57`](https://github.com/maidsafe/safe_network/commit/4542a579ac4baadaeb06c10592f6d65fe148bafc))
    - add checks to ensure all nodes have joined ([`026458c`](https://github.com/maidsafe/safe_network/commit/026458c6afa9848bb58a694da6ae4b81196b8f19))
    - Merge branch 'main' into kill-the-blob ([`5a055ba`](https://github.com/maidsafe/safe_network/commit/5a055ba679e6a4f2cd92700af68f8b36ac12a544))
    - Merge branch 'main' into tokio-console ([`e67cf62`](https://github.com/maidsafe/safe_network/commit/e67cf629d7c36389bba3aaa5a921932cb0da4db1))
    - read from disk on cache miss ([`0f0ef81`](https://github.com/maidsafe/safe_network/commit/0f0ef819f4b36e5056dfa7f1983238bd4752caba))
    - cleanup and cover both public/private ([`08cde85`](https://github.com/maidsafe/safe_network/commit/08cde8586e018e19df6b35270bd999f45d30596e))
    - use AE on all client tests ([`fdda641`](https://github.com/maidsafe/safe_network/commit/fdda641b3874c425616352daf7db7429219bb858))
    - Merge branch 'main' into kill-the-blob ([`411ce5b`](https://github.com/maidsafe/safe_network/commit/411ce5b9d4c396484d2384324ae09d346c79013f))
    - Merge branch 'main' into tokio-console ([`1549a2e`](https://github.com/maidsafe/safe_network/commit/1549a2e1407b2ace0c301c7b5fa42803ed2674a8))
    - make use of all the queries ([`83ef7a6`](https://github.com/maidsafe/safe_network/commit/83ef7a66bb245e2303b80d98d6b8fa888b93d6ba))
    - wait for a full section's info ([`d080586`](https://github.com/maidsafe/safe_network/commit/d080586074dea44b53a9901cb2e85599cc997379))
    - Merge branch 'main' into kill-the-blob ([`9c5cd80`](https://github.com/maidsafe/safe_network/commit/9c5cd80c286308c6d075c5418d8a1650e87fddd5))
    - Merge #916 #918 #919 ([`5c4d3a9`](https://github.com/maidsafe/safe_network/commit/5c4d3a92ff28126468f07d599c6caf416661aba2))
    - Merge branch 'main' into tokio-console ([`5bebdf7`](https://github.com/maidsafe/safe_network/commit/5bebdf792e297d15d2d3acfb68f4654f67985e62))
    - include ServiceAuth in DataQuery to Adults ([`5bd1198`](https://github.com/maidsafe/safe_network/commit/5bd1198aa8bd7f1b78282fb02b262e38f9121d78))
    - Merge branch 'main' into kill-the-blob ([`fe814a6`](https://github.com/maidsafe/safe_network/commit/fe814a69e5ef5fbe4c62a056498ef88ce5897fef))
    - set DEFAULT_CHUNK_COPY_COUNT to 4 once again ([`06f9808`](https://github.com/maidsafe/safe_network/commit/06f980874357872e16535b0371c7e0e15a8d0a1c))
</details>

## v0.52.13 (2022-01-06)

### Bug Fixes

 - <csr-id-edefd2d7860e6f79c07060e078cdaa433da9e804/> use latest PK to send DataExchange messages

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 1 commit contributed to the release.
 - 1 commit where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.13/sn_api-0.50.6 ([`155ee03`](https://github.com/maidsafe/safe_network/commit/155ee032ee56cbbb34928f2d14529273ccb69559))
</details>

## v0.52.12 (2022-01-06)

### Bug Fixes

 - <csr-id-06f980874357872e16535b0371c7e0e15a8d0a1c/> set DEFAULT_CHUNK_COPY_COUNT to 4 once again
 - <csr-id-112d0b474b8dd141d0daf1302b80055482d65a15/> unnecessary fetch_add for loading

### New Features

 - <csr-id-878e4bb43865933502a22f7cefb861bb6d72195c/> locally track query listeners to avoid overwrite/removal
   Previously each query for an op_id overwrote the listener.
   This could lead to odd behaviour for parallel requests in the same client.
   Now we only remove the listener for our msg_id even if it's the same
   operation_id
 - <csr-id-fff2d52b700dfe7ec9a8909a0d5adf176de4c5c7/> substract space from used_space on register delete
 - <csr-id-1b8681838d810aa2b4ef0abfaf9106678ff7cebb/> impl Ordering for NetworkPrefixMap based on it's length

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 10 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 9 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.12 ([`4f29c28`](https://github.com/maidsafe/safe_network/commit/4f29c285a0b48220df1f1c6c52c4b487350eae08))
    - increase testnet startup interval on CI only. ([`f1afc59`](https://github.com/maidsafe/safe_network/commit/f1afc5933dc782bc6a7840cd12cebb32a189a5df))
    - clients and node read/write to a global prefix_map ([`8128231`](https://github.com/maidsafe/safe_network/commit/81282318ae2da1793e66f28f0c8b3c0b2272a529))
    - substract space from used_space on register delete ([`fff2d52`](https://github.com/maidsafe/safe_network/commit/fff2d52b700dfe7ec9a8909a0d5adf176de4c5c7))
    - Merge branch 'main' into some_detailed_logging ([`eedd75c`](https://github.com/maidsafe/safe_network/commit/eedd75c266d39e4f290b894fa38fb5e237722722))
    - unnecessary fetch_add for loading ([`112d0b4`](https://github.com/maidsafe/safe_network/commit/112d0b474b8dd141d0daf1302b80055482d65a15))
    - sn_cli-0.43.1 ([`db51539`](https://github.com/maidsafe/safe_network/commit/db515397771f117b3bf095e1a4afb897eb4acafe))
    - move to adults ([`7928ce6`](https://github.com/maidsafe/safe_network/commit/7928ce6411d237078f7ed3ba83823f438f3a991f))
    - rename dest to dst ([`bebdae9`](https://github.com/maidsafe/safe_network/commit/bebdae9d52d03bd13b679ee19446452990d1e2cf))
    - impl Ordering for NetworkPrefixMap based on it's length ([`1b86818`](https://github.com/maidsafe/safe_network/commit/1b8681838d810aa2b4ef0abfaf9106678ff7cebb))
</details>

## v0.52.11 (2022-01-06)

### Bug Fixes

 - <csr-id-42d3b932f69c3680e8aaff488395406983728ed6/> only send data cmd to 3 elders

### Documentation

 - <csr-id-1d36f019159296db6d5a6bb4ebeec8699ea39f96/> add doc for usage of DKG in safe_network
   - also moves README from the .github folder so it doesn't show up in the
   repo root

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 3 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.11/sn_api-0.50.5/sn_cli-0.43.2 ([`99d012e`](https://github.com/maidsafe/safe_network/commit/99d012ef529df78ef4c84f5e6ea99d3a77414797))
    - Merge branch 'main' into tokio-console ([`3626696`](https://github.com/maidsafe/safe_network/commit/3626696d32a4955a2078800feb899d1fb7246891))
    - Merge #917 ([`0eb6439`](https://github.com/maidsafe/safe_network/commit/0eb643910098ab6021561e5b997b6289be9e2c57))
    - add doc for usage of DKG in safe_network ([`1d36f01`](https://github.com/maidsafe/safe_network/commit/1d36f019159296db6d5a6bb4ebeec8699ea39f96))
    - only send data cmd to 3 elders ([`42d3b93`](https://github.com/maidsafe/safe_network/commit/42d3b932f69c3680e8aaff488395406983728ed6))
    - Merge branch 'main' into kill-the-blob ([`40268a5`](https://github.com/maidsafe/safe_network/commit/40268a598aea8d14c1dbeb1c00712b9f9a664ef8))
</details>

## v0.52.10 (2022-01-05)

### Bug Fixes

 - <csr-id-ad202c9d24da5a5e47d93a3793d665e1b844b38d/> dont .await on threads spawned for subtasks
 - <csr-id-e18c88019d37ab4f7618dde1a90e19ddf94db1c7/> VersioinHash use Display for encode

### New Features

 - <csr-id-c47aeca2618d54a8b3d7b21c82f6ac6e62acd10c/> refactor node to support tokio-console and resolve issues

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 11 commits contributed to the release.
 - 9 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.10 ([`7b0cd4d`](https://github.com/maidsafe/safe_network/commit/7b0cd4d183a9f82d1d78dbb56572e5bf36714a0a))
    - ignore GA failing test ([`233e64a`](https://github.com/maidsafe/safe_network/commit/233e64af3b2af63bbba06ba8f43d0a7becece913))
    - dont .await on threads spawned for subtasks ([`ad202c9`](https://github.com/maidsafe/safe_network/commit/ad202c9d24da5a5e47d93a3793d665e1b844b38d))
    - VersioinHash use Display for encode ([`e18c880`](https://github.com/maidsafe/safe_network/commit/e18c88019d37ab4f7618dde1a90e19ddf94db1c7))
    - misc fixes ([`9f1bd81`](https://github.com/maidsafe/safe_network/commit/9f1bd81b269ec06fd5d379ab4b07f18f814da865))
    - Merge branch 'main' into kill-the-blob ([`6f89f12`](https://github.com/maidsafe/safe_network/commit/6f89f129ece75dee45f311d30e52ca71b6b7bc98))
    - Merge #840 #906 #912 #915 ([`099d41b`](https://github.com/maidsafe/safe_network/commit/099d41b4a576343df636190b4f2956e0f9d7a1a5))
    - log EntryHash human readable ([`bf16c5e`](https://github.com/maidsafe/safe_network/commit/bf16c5ea7051386064233443921438cbbd79d907))
    - remove unused dep async recursion ([`012e91a`](https://github.com/maidsafe/safe_network/commit/012e91a60e01cd2ce9155d5c56045f211865ff2c))
    - refactor node to support tokio-console and resolve issues ([`c47aeca`](https://github.com/maidsafe/safe_network/commit/c47aeca2618d54a8b3d7b21c82f6ac6e62acd10c))
    - ties up the loose ends in unified data flow ([`9c9a537`](https://github.com/maidsafe/safe_network/commit/9c9a537ad12cc809540df321297c8552c52a8648))
</details>

## v0.52.9 (2022-01-04)

### Bug Fixes

 - <csr-id-edefd2d7860e6f79c07060e078cdaa433da9e804/> use latest PK to send DataExchange messages

### New Features

 - <csr-id-fff2d52b700dfe7ec9a8909a0d5adf176de4c5c7/> substract space from used_space on register delete

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.9/sn_api-0.50.4 ([`a64c7e0`](https://github.com/maidsafe/safe_network/commit/a64c7e0414b77f545cb8cdbf64af0fb7212d1f2e))
    - Merge branch 'main' into kill-the-blob ([`7d38c3d`](https://github.com/maidsafe/safe_network/commit/7d38c3df14d03c042b645ad05be6cd3cc540d631))
    - use latest PK to send DataExchange messages ([`edefd2d`](https://github.com/maidsafe/safe_network/commit/edefd2d7860e6f79c07060e078cdaa433da9e804))
</details>

## v0.52.8 (2022-01-04)

### New Features

 - <csr-id-a7531b434591c39fc91bdffc413fcb2d6ed47e7a/> restore NotEnoughSpace checks and reunite reg and chunk storage tracking
 - <csr-id-19d7d3ad04a428485738ffc916b4f14388ad10d5/> optimise disk space checks by doing them less often
 - <csr-id-c47aeca2618d54a8b3d7b21c82f6ac6e62acd10c/> refactor node to support tokio-console and resolve issues
 - <csr-id-878e4bb43865933502a22f7cefb861bb6d72195c/> locally track query listeners to avoid overwrite/removal
   Previously each query for an op_id overwrote the listener.
   This could lead to odd behaviour for parallel requests in the same client.
   Now we only remove the listener for our msg_id even if it's the same
   operation_id

### Bug Fixes

 - <csr-id-112d0b474b8dd141d0daf1302b80055482d65a15/> unnecessary fetch_add for loading
 - <csr-id-ad202c9d24da5a5e47d93a3793d665e1b844b38d/> dont .await on threads spawned for subtasks
 - <csr-id-9f89966e02c3e0ba0297377b4efdf88a31ec1e87/> restore original behavior

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 6 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.8 ([`5214d5e`](https://github.com/maidsafe/safe_network/commit/5214d5e7f84a3c1cf213097a5d55bfb293f03324))
    - some detailed logging for debugging ([`6ccb792`](https://github.com/maidsafe/safe_network/commit/6ccb792c18481ffd8218cd7c27b28d8a10d1f528))
    - restore NotEnoughSpace checks and reunite reg and chunk storage tracking ([`a7531b4`](https://github.com/maidsafe/safe_network/commit/a7531b434591c39fc91bdffc413fcb2d6ed47e7a))
    - restore original behavior ([`9f89966`](https://github.com/maidsafe/safe_network/commit/9f89966e02c3e0ba0297377b4efdf88a31ec1e87))
    - rename blob to file ([`c790077`](https://github.com/maidsafe/safe_network/commit/c790077bebca691f974000278d5525f4b011b8a7))
    - locally track query listeners to avoid overwrite/removal ([`878e4bb`](https://github.com/maidsafe/safe_network/commit/878e4bb43865933502a22f7cefb861bb6d72195c))
</details>

## v0.52.7 (2022-01-04)

### New Features

 - <csr-id-19d7d3ad04a428485738ffc916b4f14388ad10d5/> optimise disk space checks by doing them less often

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 6 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.7 ([`40d1844`](https://github.com/maidsafe/safe_network/commit/40d1844e0b28578e8b8c6b270151dbb86961a766))
    - record it in an atomic usize ([`da3bbe1`](https://github.com/maidsafe/safe_network/commit/da3bbe16084b71ec42343035087848c8f6996ec4))
    - optimise disk space checks by doing them less often ([`19d7d3a`](https://github.com/maidsafe/safe_network/commit/19d7d3ad04a428485738ffc916b4f14388ad10d5))
    - reorder network startup ([`b35d0cc`](https://github.com/maidsafe/safe_network/commit/b35d0ccd0305e3e87a9070bc2a57287dbe6b2633))
    - use zsh to repeat e2e tests ([`2ebd1b4`](https://github.com/maidsafe/safe_network/commit/2ebd1b4fcab47bc86980860379891bb041ff2aa4))
    - remove unneeded retry_loop from reg tests ([`bd0382f`](https://github.com/maidsafe/safe_network/commit/bd0382fef77947935584418ee91720001f5f269c))
</details>

## v0.52.6 (2022-01-04)

### New Features

 - <csr-id-cc2c55a24bbe3edc63fd6a3a8553b10330960495/> discard JoinResponse messages in dispatcher
   Dispatcher is only created when we have joined the network already. Therefore we can safely ignore JoinResponses there.
   
   We now error out of the msg handling flow and so will log this unexpected message at the node.
 - <csr-id-158043cba04983a35f66df825dc803c68f3ea454/> Move command limit to service msgs only.
   Require a permit or drop a service msg level command. This should _hopefully_ stop mem leak due to waiting to handle messages coming in from clients
 - <csr-id-01ea1017749a1644737ff8654378f9db70b8a988/> Add hard limit to concurrent commands
   It may be that nodes can be overwhelmed when too many messages come in.
   
   Here there's a naiive impl to drop commands (and msgs) from being handled when we're overwhelmed
 - <csr-id-3058bf1a50be8a88ac0c8cb4a66278db7e186957/> reenable using constants for message priority
   and wait on higher priority message completion before spawning new
   Command tasks
   
   This reverts commit 6d1cdc64078de06a43281d924f58d01b615e9268.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 20 commits contributed to the release.
 - 20 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.6/sn_api-0.50.2 ([`0a70425`](https://github.com/maidsafe/safe_network/commit/0a70425fb314de4c165da54fdc29a127ae900d81))
    - discard JoinResponse messages in dispatcher ([`cc2c55a`](https://github.com/maidsafe/safe_network/commit/cc2c55a24bbe3edc63fd6a3a8553b10330960495))
    - rename + use glob const ([`838c495`](https://github.com/maidsafe/safe_network/commit/838c495c8858b85c693da1a3e45baafa57ba03ea))
    - refactor wait until higher_prio ([`1caad35`](https://github.com/maidsafe/safe_network/commit/1caad35e0e744e50b2bd15dda8dbd3adbacb87c7))
    - reduce semaphore wait timeout ([`78e41a3`](https://github.com/maidsafe/safe_network/commit/78e41a3d8387f9b53bfd5e078ae7aa44fe1ea6d4))
    - add 5mb many client test ([`36ba7d5`](https://github.com/maidsafe/safe_network/commit/36ba7d5e85e304d6d0ff3210429923beed77d25b))
    - make unstable-command-prioritisation required for wait ([`04ee3e7`](https://github.com/maidsafe/safe_network/commit/04ee3e74e5573f903be29cd89416ce9e5758cf00))
    - Move command limit to service msgs only. ([`158043c`](https://github.com/maidsafe/safe_network/commit/158043cba04983a35f66df825dc803c68f3ea454))
    - Add hard limit to concurrent commands ([`01ea101`](https://github.com/maidsafe/safe_network/commit/01ea1017749a1644737ff8654378f9db70b8a988))
    - disable waiting for higher prio messages before continuing ([`a9913a0`](https://github.com/maidsafe/safe_network/commit/a9913a0f7140d302fcaf24264fc1982f2ad3d06b))
    - change dkg interval ([`9717114`](https://github.com/maidsafe/safe_network/commit/97171142548772a466188f2e6d9f24072f28640d))
    - improve formatting of priority match statements ([`a7e7908`](https://github.com/maidsafe/safe_network/commit/a7e7908537d63e4071323a59cbbd036edcff41ab))
    - limit time waiting to acquire priority permit ([`18cee44`](https://github.com/maidsafe/safe_network/commit/18cee44f08aa4f83ad477cc82a29525e9d339e0c))
    - tidy up some log messages ([`1d19c02`](https://github.com/maidsafe/safe_network/commit/1d19c02668dfa3739a350b15c7310daec93d9837))
    - put command prioritisation behind a feature flag. ([`2492ea8`](https://github.com/maidsafe/safe_network/commit/2492ea84e9fcba5d19022e171ec6b60c341ee59b))
    - add PermitInfo type ([`8884f94`](https://github.com/maidsafe/safe_network/commit/8884f9453a859bd63b378337aab326889d153768))
    - stop everything if something more important is going on ([`690b24f`](https://github.com/maidsafe/safe_network/commit/690b24f14c3183640d04d048c2a7f4ac79f6e6c7))
    - ensure child commands dont remove root permit early ([`3b3a381`](https://github.com/maidsafe/safe_network/commit/3b3a38130bd6943b7c53b1cf74321d89dd4af1da))
    - use constants for cmd priorities ([`8effd08`](https://github.com/maidsafe/safe_network/commit/8effd08c16cfd2e0715ee0d00092e267f72a8cf0))
    - reenable using constants for message priority ([`3058bf1`](https://github.com/maidsafe/safe_network/commit/3058bf1a50be8a88ac0c8cb4a66278db7e186957))
</details>

## v0.52.5 (2022-01-04)

### Bug Fixes

 - <csr-id-9f89966e02c3e0ba0297377b4efdf88a31ec1e87/> restore original behavior
 - <csr-id-22f6dbcba8067ef777b7bea0393673bb669893b4/> make all atomic ops relaxed

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.5 ([`ab00eca`](https://github.com/maidsafe/safe_network/commit/ab00eca916d6ed8a0a137004a6b9fd24e7217a70))
    - make all atomic ops relaxed ([`22f6dbc`](https://github.com/maidsafe/safe_network/commit/22f6dbcba8067ef777b7bea0393673bb669893b4))
</details>

## v0.52.4 (2022-01-04)

### Bug Fixes

 - <csr-id-9f89966e02c3e0ba0297377b4efdf88a31ec1e87/> restore original behavior
 - <csr-id-a9e753de1465d3f6abeb4ccf9a5b31fc3a2172f5/> add decrease fn

### New Features

 - <csr-id-cc2c55a24bbe3edc63fd6a3a8553b10330960495/> discard JoinResponse messages in dispatcher
   Dispatcher is only created when we have joined the network already. Therefore we can safely ignore JoinResponses there.
   
   We now error out of the msg handling flow and so will log this unexpected message at the node.
 - <csr-id-158043cba04983a35f66df825dc803c68f3ea454/> Move command limit to service msgs only.
   Require a permit or drop a service msg level command. This should _hopefully_ stop mem leak due to waiting to handle messages coming in from clients
 - <csr-id-01ea1017749a1644737ff8654378f9db70b8a988/> Add hard limit to concurrent commands
   It may be that nodes can be overwhelmed when too many messages come in.
   
   Here there's a naiive impl to drop commands (and msgs) from being handled when we're overwhelmed
 - <csr-id-3058bf1a50be8a88ac0c8cb4a66278db7e186957/> reenable using constants for message priority
   and wait on higher priority message completion before spawning new
   Command tasks
   
   This reverts commit 6d1cdc64078de06a43281d924f58d01b615e9268.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 4 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.4/sn_api-0.50.1 ([`4bb2adf`](https://github.com/maidsafe/safe_network/commit/4bb2adf52efdac6187fffc299018bf13f3398e14))
    - do not skip AE check for AE-Probe messages ([`0a5fca9`](https://github.com/maidsafe/safe_network/commit/0a5fca96af4d2627b842591775e77c09201ed655))
    - add decrease fn ([`a9e753d`](https://github.com/maidsafe/safe_network/commit/a9e753de1465d3f6abeb4ccf9a5b31fc3a2172f5))
    - set testnet interval to 10s by default once again ([`3af8ddb`](https://github.com/maidsafe/safe_network/commit/3af8ddbee91f3403b86914d352a970e366d1fa40))
</details>

## v0.52.3 (2022-01-03)

### refactor (BREAKING)

 - <csr-id-e9a9cc1096e025d88f19390ad6ba7398f71bc800/> removing unnecessary Error type definitions

### New Features

 - <csr-id-5fadc027b4b7dd942275ef6041b2bdb92b062bed/> remove unused kv store, cleanup chunk store
 - <csr-id-7f1ead2f0f33558583989a7314d2c121a6f1280a/> disk chunk store
 - <csr-id-eb30133e773124b46cdad6e6fa7f3c65f066946a/> make read and delete async

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 9 commits contributed to the release.
 - 9 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.3 ([`2924661`](https://github.com/maidsafe/safe_network/commit/292466119e2d99c36043e7f2247b1bde9ec9ced9))
    - fmt ([`d54c955`](https://github.com/maidsafe/safe_network/commit/d54c955aa768ab08ef8193b7e36cb96822bc6cb8))
    - do not check if space available ([`6b9b159`](https://github.com/maidsafe/safe_network/commit/6b9b1590bce9423130210007ddd6e9c14b51819d))
    - remove repetitive code ([`be89899`](https://github.com/maidsafe/safe_network/commit/be8989971c68ac5aee43380223000a1f400252fc))
    - make read and delete async ([`eb30133`](https://github.com/maidsafe/safe_network/commit/eb30133e773124b46cdad6e6fa7f3c65f066946a))
    - typos and shortcuts ([`72f79d4`](https://github.com/maidsafe/safe_network/commit/72f79d46fc56fdda9215f8a9d6f95bcdf323a66f))
    - remove unused kv store, cleanup chunk store ([`5fadc02`](https://github.com/maidsafe/safe_network/commit/5fadc027b4b7dd942275ef6041b2bdb92b062bed))
    - disk chunk store ([`7f1ead2`](https://github.com/maidsafe/safe_network/commit/7f1ead2f0f33558583989a7314d2c121a6f1280a))
    - removing unnecessary Error type definitions ([`e9a9cc1`](https://github.com/maidsafe/safe_network/commit/e9a9cc1096e025d88f19390ad6ba7398f71bc800))
</details>

## v0.52.2 (2022-01-03)

### refactor (BREAKING)

 - <csr-id-e9a9cc1096e025d88f19390ad6ba7398f71bc800/> removing unnecessary Error type definitions

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.2 ([`d490127`](https://github.com/maidsafe/safe_network/commit/d490127b17d53a7648f9e97aae690b232188b034))
    - fix docs to match parameters ([`9d7e684`](https://github.com/maidsafe/safe_network/commit/9d7e6843701465a13de7b528768273bad02e920e))
</details>

## v0.52.1 (2022-01-03)

### Bug Fixes

 - <csr-id-f00de3a9cbc43eabeb0d46804a92b88204a48ea4/> respond only once to client for every chunk query

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 11 calendar days.
 - 11 days passed between releases.
 - 5 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.1/sn_api-0.48.0/sn_cli-0.41.0 ([`e38925e`](https://github.com/maidsafe/safe_network/commit/e38925e07d69432db310fc8ec9803200ea964ab2))
    - further integrate and organize dir ([`36dbea0`](https://github.com/maidsafe/safe_network/commit/36dbea03d879c07f922be36a124ad8d44c3c2d0e))
    - move routing into node dir ([`48ef44e`](https://github.com/maidsafe/safe_network/commit/48ef44e9db01d74119a2b1c9f7e7dae4ce988c57))
    - remove expired queries from pending queries cache ([`619d142`](https://github.com/maidsafe/safe_network/commit/619d142de8999d536e41ac5fe402a94d934689fb))
    - respond only once to client for every chunk query ([`f00de3a`](https://github.com/maidsafe/safe_network/commit/f00de3a9cbc43eabeb0d46804a92b88204a48ea4))
</details>

## v0.52.0 (2021-12-22)

### Bug Fixes

 - <csr-id-f00de3a9cbc43eabeb0d46804a92b88204a48ea4/> respond only once to client for every chunk query

### refactor (BREAKING)

 - <csr-id-1188ed58eed443b4b8c65b591376f2f9a21acc0d/> minor refactor to error types definitions

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 day passed between releases.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.52.0/sn_api-0.47.0/sn_cli-0.40.0 ([`6b59ad8`](https://github.com/maidsafe/safe_network/commit/6b59ad852f89f033caf2b3c7dfcfa3019f8129e8))
    - minor refactor to error types definitions ([`1188ed5`](https://github.com/maidsafe/safe_network/commit/1188ed58eed443b4b8c65b591376f2f9a21acc0d))
</details>

## v0.51.7 (2021-12-20)

### Bug Fixes

 - <csr-id-44e57667f48b1fd9bce154652ae2108603a35c11/> send_query_with_retry_count now uses that retry count in its calculations

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 3 days passed between releases.
 - 4 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.51.7 ([`c76c3ab`](https://github.com/maidsafe/safe_network/commit/c76c3ab638188cba38911f037829c209fcc45fc3))
    - reduce query attempts ([`07e19b5`](https://github.com/maidsafe/safe_network/commit/07e19b53cd8eaa777f4c83369d2ee1076c75fe4f))
    - send_query_with_retry_count now uses that retry count in its calculations ([`44e5766`](https://github.com/maidsafe/safe_network/commit/44e57667f48b1fd9bce154652ae2108603a35c11))
    - removing exponential backoff when retrying queries ([`069013b`](https://github.com/maidsafe/safe_network/commit/069013b032b7fd8d8a58ca0d75f6ea357abf5593))
</details>

## v0.51.6 (2021-12-17)

### New Features

 - <csr-id-1078e59be3a58ffedcd3c1460385b4bf00f18f6b/> use upload_and_verify by default in safe_client

### Bug Fixes

 - <csr-id-f083ea9200a1fccfc7bd21117f34c118702a7a70/> fix: adult choice per chunk and handling db errors

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.51.6 ([`79b2d0a`](https://github.com/maidsafe/safe_network/commit/79b2d0a3f52de0335323773936dee9bdbe12a0cf))
    - adult choice per chunk and handling db errors ([`f083ea9`](https://github.com/maidsafe/safe_network/commit/f083ea9200a1fccfc7bd21117f34c118702a7a70))
</details>

## v0.51.5 (2021-12-16)

### New Features

 - <csr-id-1078e59be3a58ffedcd3c1460385b4bf00f18f6b/> use upload_and_verify by default in safe_client
 - <csr-id-3e91a43676e1252a872a24872db8f91c729bfb15/> keeping Chunks wich were retrieved in a local client cache

### Bug Fixes

 - <csr-id-d8ad6a9b1d6a530b7f597bccf6a6bed6d8546ac0/> populate client sender success counter

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 3 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.51.5 ([`45df3d7`](https://github.com/maidsafe/safe_network/commit/45df3d71cc4b3185602b9d27b8cb0f5bf65a4b43))
    - attempt to retrieve all bytes during upload_and_verify ([`9cf7c72`](https://github.com/maidsafe/safe_network/commit/9cf7c72a94386f2cbe6f803be970c6debfbcb99b))
    - use upload_and_verify by default in safe_client ([`1078e59`](https://github.com/maidsafe/safe_network/commit/1078e59be3a58ffedcd3c1460385b4bf00f18f6b))
</details>

## v0.51.4 (2021-12-16)

### New Features

 - <csr-id-3e91a43676e1252a872a24872db8f91c729bfb15/> keeping Chunks wich were retrieved in a local client cache

### Bug Fixes

 - <csr-id-d8ad6a9b1d6a530b7f597bccf6a6bed6d8546ac0/> populate client sender success counter

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.51.4 ([`17d7906`](https://github.com/maidsafe/safe_network/commit/17d7906656bec401d6b39cc3551141112a3d77c4))
    - populate client sender success counter ([`d8ad6a9`](https://github.com/maidsafe/safe_network/commit/d8ad6a9b1d6a530b7f597bccf6a6bed6d8546ac0))
</details>

## v0.51.3 (2021-12-16)

### New Features

 - <csr-id-3e91a43676e1252a872a24872db8f91c729bfb15/> keeping Chunks wich were retrieved in a local client cache

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 3 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.51.3/sn_api-0.46.1 ([`9be440b`](https://github.com/maidsafe/safe_network/commit/9be440b36db07e1c04ab688b44ef91e4a56ed576))
    - defining a const for Chunks cache size ([`92cdb53`](https://github.com/maidsafe/safe_network/commit/92cdb53391652651bfe9a47c5a0261ba10f38148))
    - keeping Chunks wich were retrieved in a local client cache ([`3e91a43`](https://github.com/maidsafe/safe_network/commit/3e91a43676e1252a872a24872db8f91c729bfb15))
</details>

## v0.51.2 (2021-12-16)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.51.2 ([`595541b`](https://github.com/maidsafe/safe_network/commit/595541b83284a5c5b60fbc00e47b1146117d7613))
    - add more logs regarding storage levels ([`f522d9a`](https://github.com/maidsafe/safe_network/commit/f522d9aad0071cd3b47b1a3e4c178b0100ec71d6))
</details>

## v0.51.1 (2021-12-15)

### New Features

 - <csr-id-591ce5f6dfa14143114fbd16c1d632a8dbe4a2d1/> Use Peers to ensure connection reuse

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 4 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.51.1 ([`dcbb67f`](https://github.com/maidsafe/safe_network/commit/dcbb67fc699d7cb1f3a2c4632bcb8a5738916091))
    - add recursion limit for clippy ([`e52a0c0`](https://github.com/maidsafe/safe_network/commit/e52a0c063f747e0be1525f07f8f759f4b9d042a7))
    - Use Peers to ensure connection reuse ([`591ce5f`](https://github.com/maidsafe/safe_network/commit/591ce5f6dfa14143114fbd16c1d632a8dbe4a2d1))
    - move Peer to root of sn ([`df87fcf`](https://github.com/maidsafe/safe_network/commit/df87fcf42c46dc28e6926394c120fbf2c715e54a))
</details>

## v0.51.0 (2021-12-15)

### New Features (BREAKING)

 - <csr-id-134dfa29b3698fb233095194305d9bbbba2875c7/> different flavors of upload data API, with/without verifying successful upload

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.51.0 ([`c685838`](https://github.com/maidsafe/safe_network/commit/c685838d8f9c10b0f4e7541fe201862bb84e8555))
    - add in distinct client errors for comms failure ([`7c34940`](https://github.com/maidsafe/safe_network/commit/7c34940401b0115105d9b818b9f93c39d7669eed))
</details>

## v0.50.0 (2021-12-14)

### New Features (BREAKING)

 - <csr-id-134dfa29b3698fb233095194305d9bbbba2875c7/> different flavors of upload data API, with/without verifying successful upload

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.50.0 ([`653f653`](https://github.com/maidsafe/safe_network/commit/653f653a775a101679904ab75c8012a72dfdedfb))
    - different flavors of upload data API, with/without verifying successful upload ([`134dfa2`](https://github.com/maidsafe/safe_network/commit/134dfa29b3698fb233095194305d9bbbba2875c7))
</details>

## v0.49.3 (2021-12-14)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.49.3 ([`36ca20e`](https://github.com/maidsafe/safe_network/commit/36ca20e606899ecbdea24d845c34ba11ab889cf7))
    - adding a test for retrieving Blob with range over data length ([`edb8de8`](https://github.com/maidsafe/safe_network/commit/edb8de8b4d923e97d68eed40a7953f38461b0281))
</details>

## v0.49.2 (2021-12-14)

### New Features

 - <csr-id-86ba4234a29137518c73b18becbf018993e104a8/> on initial contact put all known elders into the contact pool.
   Previously if we knew a sap, we only took 3 nodes
 - <csr-id-99add55c5ca5a3e3da2130797083dd449da2f7cd/> make contact via register get
   We previously use a chunk get, but this in itself will cause more network messaging than a simple register get which can be dealt with by elders only
 - <csr-id-2bdc03578f3d9144a097a947ab44d0c1286f6180/> use backoff during make contact instead of standard_wait.
   This should help aleviate any pressure on already struggling nodes, especially if a low wait was set to make tests run faster eg (where ae may not always be needed

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.49.2 ([`62d7479`](https://github.com/maidsafe/safe_network/commit/62d747969b739172910aabca6fcb273d2827fc8a))
    - set default AE_WAIT to be 0 ([`dd50d1d`](https://github.com/maidsafe/safe_network/commit/dd50d1d860aa4ca60b6c0d5a525b45d88ddf432e))
</details>

## v0.49.1 (2021-12-13)

### New Features

 - <csr-id-86ba4234a29137518c73b18becbf018993e104a8/> on initial contact put all known elders into the contact pool.
   Previously if we knew a sap, we only took 3 nodes
 - <csr-id-99add55c5ca5a3e3da2130797083dd449da2f7cd/> make contact via register get
   We previously use a chunk get, but this in itself will cause more network messaging than a simple register get which can be dealt with by elders only
 - <csr-id-2bdc03578f3d9144a097a947ab44d0c1286f6180/> use backoff during make contact instead of standard_wait.
   This should help aleviate any pressure on already struggling nodes, especially if a low wait was set to make tests run faster eg (where ae may not always be needed

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 5 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.49.1 ([`69ae8c2`](https://github.com/maidsafe/safe_network/commit/69ae8c20e91dd9959ebfa5456efdf9c218a9d66f))
    - set DEFAULT_OPERATION_TIMEOUT to 120s ([`2e7bc0b`](https://github.com/maidsafe/safe_network/commit/2e7bc0b782da6231f54edc440fa555fa754d294c))
    - on initial contact put all known elders into the contact pool. ([`86ba423`](https://github.com/maidsafe/safe_network/commit/86ba4234a29137518c73b18becbf018993e104a8))
    - make contact via register get ([`99add55`](https://github.com/maidsafe/safe_network/commit/99add55c5ca5a3e3da2130797083dd449da2f7cd))
    - use backoff during make contact instead of standard_wait. ([`2bdc035`](https://github.com/maidsafe/safe_network/commit/2bdc03578f3d9144a097a947ab44d0c1286f6180))
</details>

## v0.49.0 (2021-12-13)

<csr-id-88c78e8129e5092bd120d0fc6c9696673550be9d/>

### New Features

 - <csr-id-35467d3e886d2824c4f9e4586666cab7a7960e54/> limit the relocations at one time

### Bug Fixes

 - <csr-id-8955fcf9d69e869725177340d1de6b6b1e7a203b/> read_from client API was incorrectly using provided length value as an end index
   - Minor refactoring in sn_api moving the SafeData struct into its own file.

### chore (BREAKING)

 - <csr-id-88c78e8129e5092bd120d0fc6c9696673550be9d/> rename enum variants for improved clarity on flows

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release over the course of 3 calendar days.
 - 3 days passed between releases.
 - 6 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.49.0 ([`6f5516d`](https://github.com/maidsafe/safe_network/commit/6f5516d8bb677462ea6def46aa65a1094767d68c))
    - replace use of deprecated dalek function ([`1a81c8f`](https://github.com/maidsafe/safe_network/commit/1a81c8f04f947d2b83d3cd726c00ad66927f5225))
    - tidy up some log messages ([`a569474`](https://github.com/maidsafe/safe_network/commit/a569474a8be9c11ab73ec7ad1ad157f69827b4d3))
    - read_from client API was incorrectly using provided length value as an end index ([`8955fcf`](https://github.com/maidsafe/safe_network/commit/8955fcf9d69e869725177340d1de6b6b1e7a203b))
    - limit the relocations at one time ([`35467d3`](https://github.com/maidsafe/safe_network/commit/35467d3e886d2824c4f9e4586666cab7a7960e54))
    - rename enum variants for improved clarity on flows ([`88c78e8`](https://github.com/maidsafe/safe_network/commit/88c78e8129e5092bd120d0fc6c9696673550be9d))
</details>

## v0.48.0 (2021-12-10)

### New Features

 - <csr-id-fd31bd2ef5ccc9149f8f0a2844c52af60bff3840/> use Vec of DataCmd instead of wrapper struct
 - <csr-id-84ebec0e1b2b3e1107d673734125aadbdb108472/> upgrade batch to write ahead log with index

### Bug Fixes

 - <csr-id-661e994cb452242a1d7c831ab88b9a66a244faff/> avoid change name by mistake when Join
 - <csr-id-58bd78e8a4fc34e7f76a9f449301b9216cb6a1d4/> complete the relocation flow

### New Features (BREAKING)

 - <csr-id-565e57619557e2b63f028eb214b59fc69b77fc37/> register op batching

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 10 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
 - 10 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.48.0 ([`9eff035`](https://github.com/maidsafe/safe_network/commit/9eff03598ba09aa339180d7ecd57b50174180095))
    - minor improvement to client log msgs related to configured timeouts ([`58632a2`](https://github.com/maidsafe/safe_network/commit/58632a27d271140fc4d777f25a76b0daea582426))
    - remove Generic client error ([`0d4343c`](https://github.com/maidsafe/safe_network/commit/0d4343c8fa56749d3ec9390e298d1d6384573a67))
    - avoid change name by mistake when Join ([`661e994`](https://github.com/maidsafe/safe_network/commit/661e994cb452242a1d7c831ab88b9a66a244faff))
    - safe_network-0.47.0 ([`8570965`](https://github.com/maidsafe/safe_network/commit/85709655b0ce38246515658b956aa9b8f67cb55a))
    - use Vec of DataCmd instead of wrapper struct ([`fd31bd2`](https://github.com/maidsafe/safe_network/commit/fd31bd2ef5ccc9149f8f0a2844c52af60bff3840))
    - upgrade batch to write ahead log with index ([`84ebec0`](https://github.com/maidsafe/safe_network/commit/84ebec0e1b2b3e1107d673734125aadbdb108472))
    - register op batching ([`565e576`](https://github.com/maidsafe/safe_network/commit/565e57619557e2b63f028eb214b59fc69b77fc37))
    - checking Relocation during CI ([`7eda276`](https://github.com/maidsafe/safe_network/commit/7eda2760da82b3079d5eee2f97e2d15ac8da0d57))
    - complete the relocation flow ([`58bd78e`](https://github.com/maidsafe/safe_network/commit/58bd78e8a4fc34e7f76a9f449301b9216cb6a1d4))
</details>

## v0.47.0 (2021-12-08)

### New Features

 - <csr-id-fd31bd2ef5ccc9149f8f0a2844c52af60bff3840/> use Vec of DataCmd instead of wrapper struct
 - <csr-id-84ebec0e1b2b3e1107d673734125aadbdb108472/> upgrade batch to write ahead log with index

### Bug Fixes

 - <csr-id-58bd78e8a4fc34e7f76a9f449301b9216cb6a1d4/> complete the relocation flow

### New Features (BREAKING)

 - <csr-id-565e57619557e2b63f028eb214b59fc69b77fc37/> register op batching

## v0.46.6 (2021-12-07)

### Bug Fixes

 - <csr-id-ddde0d6767f5230eca7a760423538fa5e4f640a2/> order of target length in ElderConn error corrected
 - <csr-id-1ea33e0c63fead5097b57496b2fd997201a2c531/> keep trying even with connection error w/ always-joinable flag
 - <csr-id-8e480e5eb5cd38f9bfb3b723aa1d0dd5e4be3122/> aovid overwrite pervious session by  accident
 - <csr-id-fdc4ba6bb36dff85126c8273cdfc67f9b07e2175/> multiple fixes for DKG-AE
   - discard DKG session info for older sessions

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 1 calendar day.
 - 5 days passed between releases.
 - 5 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.46.6 ([`b5e9dcc`](https://github.com/maidsafe/safe_network/commit/b5e9dcc5b13b1eda711d4760d9feb8dc929a0c43))
    - order of target length in ElderConn error corrected ([`ddde0d6`](https://github.com/maidsafe/safe_network/commit/ddde0d6767f5230eca7a760423538fa5e4f640a2))
    - keep trying even with connection error w/ always-joinable flag ([`1ea33e0`](https://github.com/maidsafe/safe_network/commit/1ea33e0c63fead5097b57496b2fd997201a2c531))
    - aovid overwrite pervious session by  accident ([`8e480e5`](https://github.com/maidsafe/safe_network/commit/8e480e5eb5cd38f9bfb3b723aa1d0dd5e4be3122))
    - clippy tidyup for rust 1.57 ([`05f6d98`](https://github.com/maidsafe/safe_network/commit/05f6d98cf21f0158f4b5161484c7c15a0561b6f4))
</details>

## v0.46.5 (2021-12-02)

### Bug Fixes

<csr-id-01a5c961fc02f9ca8d6f60286306d5efba460e4e/>

 - <csr-id-fdc4ba6bb36dff85126c8273cdfc67f9b07e2175/> multiple fixes for DKG-AE
   - discard DKG session info for older sessions

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.46.5 ([`8657784`](https://github.com/maidsafe/safe_network/commit/86577846e845c110c49e15c95c6bd5595db51773))
    - multiple fixes for DKG-AE ([`fdc4ba6`](https://github.com/maidsafe/safe_network/commit/fdc4ba6bb36dff85126c8273cdfc67f9b07e2175))
</details>

## v0.46.4 (2021-12-02)

### Bug Fixes

 - <csr-id-01a5c961fc02f9ca8d6f60286306d5efba460e4e/> avoid network_knowledge loopup when sending DkgRetry or DkgSessionInfo
 - <csr-id-27868bc51ba2dd12cc584396c53a158706d0c07b/> avoid network_knowledge lookup when sending DKGNotReady or DkgSessionUnknown

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.46.4 ([`de3051e`](https://github.com/maidsafe/safe_network/commit/de3051e7e809a8f75507c54f3cf053a4244fdf19))
    - avoid network_knowledge loopup when sending DkgRetry or DkgSessionInfo ([`01a5c96`](https://github.com/maidsafe/safe_network/commit/01a5c961fc02f9ca8d6f60286306d5efba460e4e))
</details>

## v0.46.3 (2021-12-02)

### New Features

 - <csr-id-5b003745ce3421b0554f35a6198d58cf67a1f4c6/> aggregate joins per section key
   Previously we only did one aggregation. But errors could arise if we had to resend and scrap aggregation.
   
   This means we can aggregate any number of churning section keys.

### Bug Fixes

 - <csr-id-27868bc51ba2dd12cc584396c53a158706d0c07b/> avoid network_knowledge lookup when sending DKGNotReady or DkgSessionUnknown

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.46.3 ([`69e9be2`](https://github.com/maidsafe/safe_network/commit/69e9be2a1567bfa211af7e9d7595381d9a0a3b38))
    - avoid network_knowledge lookup when sending DKGNotReady or DkgSessionUnknown ([`27868bc`](https://github.com/maidsafe/safe_network/commit/27868bc51ba2dd12cc584396c53a158706d0c07b))
</details>

## v0.46.2 (2021-12-01)

### New Features

 - <csr-id-5b003745ce3421b0554f35a6198d58cf67a1f4c6/> aggregate joins per section key
   Previously we only did one aggregation. But errors could arise if we had to resend and scrap aggregation.
   
   This means we can aggregate any number of churning section keys.

### Bug Fixes

 - <csr-id-8de5ecf50008793546760621d906f23e8fe9791f/> make has_split.sh detect SplitSuccess correctly
 - <csr-id-8cfc5a4adf8a31bf6916273597cf4c18aa5adc46/> reduce backoff time; avoid start DKG shrink elders

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 6 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.46.2 ([`260eaab`](https://github.com/maidsafe/safe_network/commit/260eaabd2d1b0c26dec9febc963929e65d7ec912))
    - remove sig check on join ApprovalShare receipt ([`b4f0306`](https://github.com/maidsafe/safe_network/commit/b4f0306fb945cce096de7f68c3cf6ece6905786d))
    - remove Joinrejection DkgUnderway ([`204f927`](https://github.com/maidsafe/safe_network/commit/204f927220c8bd1829ac89feaed1c48a8034e80b))
    - leave longer wait on testnet startup ([`2ef8e45`](https://github.com/maidsafe/safe_network/commit/2ef8e45f53ff6925e56321ed0ebc922d2d4dd9b9))
    - aggregate joins per section key ([`5b00374`](https://github.com/maidsafe/safe_network/commit/5b003745ce3421b0554f35a6198d58cf67a1f4c6))
    - dont send dkg underway ([`1eea844`](https://github.com/maidsafe/safe_network/commit/1eea84476294c1dbfbfe72fe0e9acdb997762595))
</details>

## v0.46.1 (2021-12-01)

### Bug Fixes

 - <csr-id-8de5ecf50008793546760621d906f23e8fe9791f/> make has_split.sh detect SplitSuccess correctly
 - <csr-id-8cfc5a4adf8a31bf6916273597cf4c18aa5adc46/> reduce backoff time; avoid start DKG shrink elders

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 3 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.46.1 ([`bf55f9b`](https://github.com/maidsafe/safe_network/commit/bf55f9b7e3b96319de4423e19333bf3b16fd1c78))
    - make has_split.sh detect SplitSuccess correctly ([`8de5ecf`](https://github.com/maidsafe/safe_network/commit/8de5ecf50008793546760621d906f23e8fe9791f))
    - reduce backoff time; avoid start DKG shrink elders ([`8cfc5a4`](https://github.com/maidsafe/safe_network/commit/8cfc5a4adf8a31bf6916273597cf4c18aa5adc46))
</details>

## v0.46.0 (2021-12-01)

<csr-id-f3d3ab2b059040ff08b6239c8a6583c64eac160e/>

### New Features

 - <csr-id-24f1aa0208e3e474862c18b21ea9f048cb6abf25/> expose API for calculate Blob/Spot address without a network connection

### chore (BREAKING)

 - <csr-id-f3d3ab2b059040ff08b6239c8a6583c64eac160e/> include remote addrs in listerners threads log entries


### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 5 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.46.0 ([`8ea9498`](https://github.com/maidsafe/safe_network/commit/8ea94983b37b1d559358a62d6ca075b97c193f0d))
    - unify skip port forwarding arguments ([`3f77429`](https://github.com/maidsafe/safe_network/commit/3f77429e8bd659a5b2e7aa377437fac1b3d709c0))
    - safe_network-0.45.0 ([`a7e0585`](https://github.com/maidsafe/safe_network/commit/a7e058536ae6ae27228bd2254ea6465c5eface35))
    - expose API for calculate Blob/Spot address without a network connection ([`24f1aa0`](https://github.com/maidsafe/safe_network/commit/24f1aa0208e3e474862c18b21ea9f048cb6abf25))
    - include remote addrs in listerners threads log entries ([`f3d3ab2`](https://github.com/maidsafe/safe_network/commit/f3d3ab2b059040ff08b6239c8a6583c64eac160e))
</details>

## v0.45.0 (2021-11-30)

<csr-id-f3d3ab2b059040ff08b6239c8a6583c64eac160e/>

### New Features

 - <csr-id-24f1aa0208e3e474862c18b21ea9f048cb6abf25/> expose API for calculate Blob/Spot address without a network connection

### chore (BREAKING)

 - <csr-id-f3d3ab2b059040ff08b6239c8a6583c64eac160e/> include remote addrs in listerners threads log entries


## v0.44.5 (2021-11-30)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 4 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.44.5 ([`aaab10b`](https://github.com/maidsafe/safe_network/commit/aaab10b3a5a44d9ec844757c71ac091016f51fd1))
    - only check split count for health check for now ([`d364a7e`](https://github.com/maidsafe/safe_network/commit/d364a7e364827b2d71f196a9f897b7d613bbab94))
    - log desired elder count during health check ([`0da3c99`](https://github.com/maidsafe/safe_network/commit/0da3c99785e026075214b7cfa7933f64420aa00f))
    - add SN_AE_WAIT env var ([`9deb9e7`](https://github.com/maidsafe/safe_network/commit/9deb9e7e2ae5d909eeb745e9021cf7660ed55dc3))
</details>

## v0.44.4 (2021-11-30)

### New Features

 - <csr-id-d2c352198c0e421cf4b7327a17c4343dc693b2dd/> start a fresh round of joins when aggregation is erroring.
   Aggregation may error, perhaps due to section churn. If we half or more bad signaturs that would prevent aggregation. We clean the slate and start a fresh round of aggregation and join messaging.

### Bug Fixes

 - <csr-id-758661d87edfef0fee6676993f1e96b3fd2d26bb/> don't crash node on failed signature aggregation on join
 - <csr-id-b79e8d6dbd18a4a0787d326a6b10146fa6b90f30/> retry joins on section key mismatch.
   We could sometimes get a key sent from nodes with a newer section_pk
   than we were expecting.
   
   We should then retry

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.44.4 ([`984c5f8`](https://github.com/maidsafe/safe_network/commit/984c5f83e3f4d889dc4e0583b09571e540357cf9))
    - spawn writing prefix_map to disk as separate thread ([`e841ae0`](https://github.com/maidsafe/safe_network/commit/e841ae0cf3caf852e8a48b559dcffb64b7fcecad))
</details>

## v0.44.3 (2021-11-30)

### New Features

 - <csr-id-d2c352198c0e421cf4b7327a17c4343dc693b2dd/> start a fresh round of joins when aggregation is erroring.
   Aggregation may error, perhaps due to section churn. If we half or more bad signaturs that would prevent aggregation. We clean the slate and start a fresh round of aggregation and join messaging.

### Bug Fixes

 - <csr-id-758661d87edfef0fee6676993f1e96b3fd2d26bb/> don't crash node on failed signature aggregation on join
 - <csr-id-b79e8d6dbd18a4a0787d326a6b10146fa6b90f30/> retry joins on section key mismatch.
   We could sometimes get a key sent from nodes with a newer section_pk
   than we were expecting.
   
   We should then retry
 - <csr-id-5523d0fcca7bbbbd05b6d125692f5fbd1a8f50d7/> avoid use outdated keyshare status

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 6 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.44.3 ([`ec3dd49`](https://github.com/maidsafe/safe_network/commit/ec3dd4991535bb22235e2d1d413dd93489b8aedf))
    - start a fresh round of joins when aggregation is erroring. ([`d2c3521`](https://github.com/maidsafe/safe_network/commit/d2c352198c0e421cf4b7327a17c4343dc693b2dd))
    - increase join node aggregatin expiration. ([`9885536`](https://github.com/maidsafe/safe_network/commit/9885536afe337bf109e3ab18eec054100bf3fd82))
    - improve node logging ([`cf67a37`](https://github.com/maidsafe/safe_network/commit/cf67a374ae5aa5a9f95d39357fdbd937eb5e1a1a))
    - don't crash node on failed signature aggregation on join ([`758661d`](https://github.com/maidsafe/safe_network/commit/758661d87edfef0fee6676993f1e96b3fd2d26bb))
    - retry joins on section key mismatch. ([`b79e8d6`](https://github.com/maidsafe/safe_network/commit/b79e8d6dbd18a4a0787d326a6b10146fa6b90f30))
</details>

## v0.44.2 (2021-11-30)

### Bug Fixes

 - <csr-id-5523d0fcca7bbbbd05b6d125692f5fbd1a8f50d7/> avoid use outdated keyshare status

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 3 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.44.2 ([`51b0f00`](https://github.com/maidsafe/safe_network/commit/51b0f0068c9a279da9a1edf45509cf80a90e663d))
    - move key_share check into network_knowledge_update ([`a3c5ef6`](https://github.com/maidsafe/safe_network/commit/a3c5ef63f37ec0d2f98d45176e27da7de31baabc))
    - avoid use outdated keyshare status ([`5523d0f`](https://github.com/maidsafe/safe_network/commit/5523d0fcca7bbbbd05b6d125692f5fbd1a8f50d7))
</details>

## v0.44.1 (2021-11-29)

### Bug Fixes

 - <csr-id-0e505be2f57ab427cd3ed8c9564fd8b84909f6f3/> restore `ServiceMsg` authority check
   The `AuthorityProof` struct is designed to be a proof of valid
   authority, by ensuring all possible constructors either generate or
   validate a signature. This can only be guaranteed if the field remains
   module-private. At some point it seems the field was made `pub(crate)`,
   which meant we were missing an authority check for some `ServiceMsg`s,

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 days passed between releases.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.44.1 ([`14c84c9`](https://github.com/maidsafe/safe_network/commit/14c84c9db23557626e4889eff0ff403a574dccad))
    - remove Core::is_dkg_underway flag which is not necessary ([`10e135b`](https://github.com/maidsafe/safe_network/commit/10e135b4f77dcd30de99eb7bc370ba1e15bbd148))
</details>

## v0.44.0 (2021-11-26)

### New Features

 - <csr-id-cc256bdf3f493f8841be07b9d7634c486e21a1cf/> avoid broadcasting DKG messages
 - <csr-id-60e6a5b1c1db4011c6bcdb473be3dbfea8858d6a/> revamp joins/rejoins to follow BRB

### Bug Fixes

 - <csr-id-0e505be2f57ab427cd3ed8c9564fd8b84909f6f3/> restore `ServiceMsg` authority check
   The `AuthorityProof` struct is designed to be a proof of valid
   authority, by ensuring all possible constructors either generate or
   validate a signature. This can only be guaranteed if the field remains
   module-private. At some point it seems the field was made `pub(crate)`,
   which meant we were missing an authority check for some `ServiceMsg`s,
 - <csr-id-23bd9b23243da1d20cee8e7f06adc90f0894c2e7/> fix node joining age for genesis section Adults
 - <csr-id-5dd0961c5172ed890b2453eb33332380e4757ad3/> refactor join flows to use the new Peer type
 - <csr-id-1822857e35b888df75bea652a2858cfd34b8c2fc/> remove redundant send_node_approval

### Bug Fixes (BREAKING)

 - <csr-id-94c91e16135e524202608b2dc6312102890eab94/> include the original message when requesting DkgSessionInfo
   - also early-returns when signature verification fails / the key is
   outdated

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.44.0 ([`75a4b53`](https://github.com/maidsafe/safe_network/commit/75a4b537573d4e5e8767e38fa7d1b1126dffe148))
    - restore `ServiceMsg` authority check ([`0e505be`](https://github.com/maidsafe/safe_network/commit/0e505be2f57ab427cd3ed8c9564fd8b84909f6f3))
</details>

## v0.43.0 (2021-11-26)

### New Features

 - <csr-id-cc256bdf3f493f8841be07b9d7634c486e21a1cf/> avoid broadcasting DKG messages
 - <csr-id-60e6a5b1c1db4011c6bcdb473be3dbfea8858d6a/> revamp joins/rejoins to follow BRB

### Bug Fixes

 - <csr-id-23bd9b23243da1d20cee8e7f06adc90f0894c2e7/> fix node joining age for genesis section Adults
 - <csr-id-5dd0961c5172ed890b2453eb33332380e4757ad3/> refactor join flows to use the new Peer type
 - <csr-id-1822857e35b888df75bea652a2858cfd34b8c2fc/> remove redundant send_node_approval

### New Features (BREAKING)

 - <csr-id-3fe9d7a6624fe5503f80395f6ed11426b131d3b1/> move Url to sn_api

### Bug Fixes (BREAKING)

 - <csr-id-94c91e16135e524202608b2dc6312102890eab94/> include the original message when requesting DkgSessionInfo
   - also early-returns when signature verification fails / the key is
   outdated

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 26 commits contributed to the release.
 - 26 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.43.0 ([`c78f470`](https://github.com/maidsafe/safe_network/commit/c78f4703a970e8b7466b091ad331d0f2233aa9a3))
    - prefer existing connection when merging `Peer`s ([`0f27864`](https://github.com/maidsafe/safe_network/commit/0f2786483016adb2f44199cd4e5bf55e8c54adc3))
    - prevent concurrent sends from opening many connections ([`ee6431e`](https://github.com/maidsafe/safe_network/commit/ee6431e9509dd5b02ec2eca4c10ea17c7dddcfc9))
    - only recreate failed connections in `Comm::send` ([`d73deeb`](https://github.com/maidsafe/safe_network/commit/d73deebbf94e65898d29455d6d4ed951ca59b814))
    - simplify `Comm::send` error handling ([`3fa2957`](https://github.com/maidsafe/safe_network/commit/3fa2957fe4f4a1f14b9001e101270dec4e386c14))
    - replace `Comm::send` closure with normal `fn` ([`612f843`](https://github.com/maidsafe/safe_network/commit/612f8437429d4d9852f54fa0e297b059b8e86827))
    - fix node joining age for genesis section Adults ([`23bd9b2`](https://github.com/maidsafe/safe_network/commit/23bd9b23243da1d20cee8e7f06adc90f0894c2e7))
    - use bls-dkg 0.9.0 ([`2003e34`](https://github.com/maidsafe/safe_network/commit/2003e34cc26c450486151fa1df74ffea61c8c9ae))
    - reuse system message sender connections ([`0ab3c35`](https://github.com/maidsafe/safe_network/commit/0ab3c35e424e60ffbed0edf8c1f71da48daa0978))
    - merge sender connection into SAP when joining ([`c7b9d74`](https://github.com/maidsafe/safe_network/commit/c7b9d748b12e6d1e23d2a2972f2553af50343b22))
    - remove connection pooling ([`d85d896`](https://github.com/maidsafe/safe_network/commit/d85d896ceaa68785797b1c525ae8653ef26f2708))
    - reuse more `Peer`s in DKG sessions ([`1c5929b`](https://github.com/maidsafe/safe_network/commit/1c5929bcc0638e8ff05dd495b3cedfdeff7540aa))
    - reduce the testnet node numbers and make name deduce more ([`c051e02`](https://github.com/maidsafe/safe_network/commit/c051e02afdcbd2c21deec19b1baebfb8293d515c))
    - fixes to node joining and DKG flows ([`016b7d4`](https://github.com/maidsafe/safe_network/commit/016b7d47f708308490c1883db4ba239fc2add59b))
    - avoid broadcasting DKG messages ([`cc256bd`](https://github.com/maidsafe/safe_network/commit/cc256bdf3f493f8841be07b9d7634c486e21a1cf))
    - get section name from prefix and get section key from our ([`e6df2ec`](https://github.com/maidsafe/safe_network/commit/e6df2ec1d2c8ab02bdaa95ed4c3288cfc0e532cc))
    - include the original message when requesting DkgSessionInfo ([`94c91e1`](https://github.com/maidsafe/safe_network/commit/94c91e16135e524202608b2dc6312102890eab94))
    - replace dkg message backlog with message exchange ([`ddd3726`](https://github.com/maidsafe/safe_network/commit/ddd3726a0a2a131ae7c05a4190edb8bdc7fa3665))
    - add comments and use backoff for DKGUnderway on joining ([`580f2e2`](https://github.com/maidsafe/safe_network/commit/580f2e26baf787bdee64c093a03ae9470668619d))
    - refactor join flows to use the new Peer type ([`5dd0961`](https://github.com/maidsafe/safe_network/commit/5dd0961c5172ed890b2453eb33332380e4757ad3))
    - send a retry join message if DKG is underway ([`52a39b4`](https://github.com/maidsafe/safe_network/commit/52a39b4c41b09ec33c60bc09c55fc63484e524cf))
    - adults join with age lesser than the youngest elder in genesis section ([`f69c0e3`](https://github.com/maidsafe/safe_network/commit/f69c0e319b639b678b46b73724a77ed7172cfec3))
    - backoff BRB join response ([`da39113`](https://github.com/maidsafe/safe_network/commit/da39113a7f97f6881643e66a48e19f2e06368e8d))
    - fixes node join/rejoin tests to support the BRB join process ([`7ae741f`](https://github.com/maidsafe/safe_network/commit/7ae741f93f39c437618bde150458eedd7663b512))
    - remove redundant send_node_approval ([`1822857`](https://github.com/maidsafe/safe_network/commit/1822857e35b888df75bea652a2858cfd34b8c2fc))
    - revamp joins/rejoins to follow BRB ([`60e6a5b`](https://github.com/maidsafe/safe_network/commit/60e6a5b1c1db4011c6bcdb473be3dbfea8858d6a))
</details>

## v0.42.0 (2021-11-25)

### New Features

 - <csr-id-a8c0e645d9bf11834557049045cca95f8d715a77/> deduce expected_prefix from expected_age

### New Features (BREAKING)

 - <csr-id-3fe9d7a6624fe5503f80395f6ed11426b131d3b1/> move Url to sn_api

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 3 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.42.0/sn_api-0.43.0 ([`ca21d1e`](https://github.com/maidsafe/safe_network/commit/ca21d1e97fcd28ca351887636affffff78e3aeb3))
    - remove url deps from sn ([`bad0833`](https://github.com/maidsafe/safe_network/commit/bad083332fdcc3a0bc3c3f13628c00315f1b2519))
    - move Url to sn_api ([`3fe9d7a`](https://github.com/maidsafe/safe_network/commit/3fe9d7a6624fe5503f80395f6ed11426b131d3b1))
</details>

## v0.41.4 (2021-11-25)

### New Features

 - <csr-id-a8c0e645d9bf11834557049045cca95f8d715a77/> deduce expected_prefix from expected_age
 - <csr-id-b96034796d479352340caaf3d6e6b6d7e6e425ad/> adapt registers to hold vec u8 instead of Url in sn

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.41.4/sn_api-0.42.0 ([`8b8a361`](https://github.com/maidsafe/safe_network/commit/8b8a3616673405005d77868dc397bd7542ab3ea7))
    - deduce expected_prefix from expected_age ([`a8c0e64`](https://github.com/maidsafe/safe_network/commit/a8c0e645d9bf11834557049045cca95f8d715a77))
</details>

## v0.41.3 (2021-11-24)

### New Features

 - <csr-id-b96034796d479352340caaf3d6e6b6d7e6e425ad/> adapt registers to hold vec u8 instead of Url in sn

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 4 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.41.3 ([`4b72bfc`](https://github.com/maidsafe/safe_network/commit/4b72bfc9a6c3a0db4821e7ebf1f4b5daa7cc56d1))
    - sn_api-0.41.0 ([`df25e49`](https://github.com/maidsafe/safe_network/commit/df25e4920c570771f6813ca03da02f6dfc8e59fb))
    - add comment about vec size ([`9d65724`](https://github.com/maidsafe/safe_network/commit/9d657249f3d0391e7c3bbeae6f81909993802cc7))
    - adapt registers to hold vec u8 instead of Url in sn ([`b960347`](https://github.com/maidsafe/safe_network/commit/b96034796d479352340caaf3d6e6b6d7e6e425ad))
</details>

## v0.41.2 (2021-11-24)

### New Features

 - <csr-id-b96034796d479352340caaf3d6e6b6d7e6e425ad/> adapt registers to hold vec u8 instead of Url in sn

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 4 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.41.2/sn_api-0.40.1 ([`a973039`](https://github.com/maidsafe/safe_network/commit/a973039178af33b859d421cf36571de49cceff17))
    - simplify `Proposal::as_signable_bytes` ([`bf0488d`](https://github.com/maidsafe/safe_network/commit/bf0488d239fc52ce03c1f380ae0986810d753007))
    - replace `ProposalUtils` with inherent impl ([`aca0fb7`](https://github.com/maidsafe/safe_network/commit/aca0fb7a451ffc25c6e34479cc9201fef42796be))
    - duplicate `Proposal` in `routing` ([`c34c28d`](https://github.com/maidsafe/safe_network/commit/c34c28dd8776196a6c1c475b7f3ec3be709c0b6d))
</details>

## v0.41.1 (2021-11-23)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 11 commits contributed to the release.
 - 10 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.41.1 ([`62aa668`](https://github.com/maidsafe/safe_network/commit/62aa668d5777058ae617f8952cfcb62be002abf3))
    - revert "chore(release):" ([`ad1617f`](https://github.com/maidsafe/safe_network/commit/ad1617f96954a810898484e5b00b5b8b12495f4e))
    - revert "chore(release): safe_network-0.42.0/sn_api-0.41.0" ([`d8ec5a8`](https://github.com/maidsafe/safe_network/commit/d8ec5a81ae566e8d7068592e01cff4e808b1cad1))
    - chore(release): ([`d794bc6`](https://github.com/maidsafe/safe_network/commit/d794bc6862692004432699b8deae1a52a1ae1207))
    - renaming some variables and functions for better readability ([`b456f2f`](https://github.com/maidsafe/safe_network/commit/b456f2f610ea57e5d8a4811fbca5a26175434645))
    - safe_network-0.42.0/sn_api-0.41.0 ([`63432eb`](https://github.com/maidsafe/safe_network/commit/63432eb2e528401ae67da8eea0c82837ab42fc18))
    - reuse connections during join flow ([`950a4ee`](https://github.com/maidsafe/safe_network/commit/950a4eece1a11f70b2aef71cc11404603bb2ec5c))
    - add 'connectedness' to `Peer`'s `Display` ([`7a875a8`](https://github.com/maidsafe/safe_network/commit/7a875a88c911b5a9db59a55b81c94b995e2b95ae))
    - inherit span on `Comm::send` incoming message listener ([`5514d9c`](https://github.com/maidsafe/safe_network/commit/5514d9c06ee400e983a258ba2eb37bff1abf0dd0))
    - add a feature for increased `WireMsg` debug info ([`1848189`](https://github.com/maidsafe/safe_network/commit/1848189aa780f2f6aabddb3564b816c72f9bee6e))
    - replace `Sender` with `UnknownPeer` ([`f73364e`](https://github.com/maidsafe/safe_network/commit/f73364efa66718b92e04f24d4546c1e248198ce8))
</details>

## v0.41.0 (2021-11-23)

### New Features

 - <csr-id-f29c94d7dfab2e82b9db70fcfeddc4a71d987abb/> use AE to progress various intra-DKG phases
   sometimes a DKG session might receive DKG messages from a further phase
   that it has not reached yet. eg. it might receive Proposal messages
   while in the Initialization phase. When this happens, the DKG can
   respond with a DkgNotReady message to the message source which will sent
   a list of 'DKG Messages' that can be applied to an existing DKG process
   so it can progress to the next phase.
   
   note that this does not solve the case where the DKG session has not yet
   started. that will need to be handled separately. they are currently
   pushed to the backlog and handled later
 - <csr-id-46ea542731fa3e2cede4ce9357783d3681434643/> allow ELDER_COUNT to be overridden by env var SN_ELDER_COUNT
 - <csr-id-f6a3f78156b9cc0f934c19e5d4c0004238a593e4/> verify and use SAP received in AE-Redirect to update client's network knowledge
 - <csr-id-ae5fc40e29161652306b2e42b92d2a80fc746708/> verify and use SAP received in AE-Redirect to update our network knowledge
 - <csr-id-20bda03e07985114b8a54c866755be8a240c2504/> retry client queires if elder conns fail

### Bug Fixes

<csr-id-d3b88f749ca6ee53f65e200105aeeea691581e83/>
<csr-id-35a8fdb9dca60ca268d536958a9d32ce0a876792/>
<csr-id-c27d7e997c5a7812f995f113f31edf30c2c21272/>
<csr-id-55eb0f259a83faff470dbfdeb9365d314ed6a697/>
<csr-id-42d90b763606e2d324c5ce1235fc801105c07acb/>
<csr-id-302ce4e605d72a0925509bfe3220c2b1ddac677d/>
<csr-id-c78513903457a701096b5c542f15012e71d33c46/>

 - <csr-id-9a82649f0ca01c6d2eae57f260d2f98246724556/> multiples fixes for unit tests
   - error instead of panicking if logger is already initialized

### New Features (BREAKING)

 - <csr-id-3a59ee3b532bbc26388780ddc2f5b51ddae61d4c/> include section chain in AE-Redirect messages

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 63 commits contributed to the release over the course of 7 calendar days.
 - 8 days passed between releases.
 - 63 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network-0.41.0 ([`14fdaa6`](https://github.com/maidsafe/safe_network/commit/14fdaa6537619483e94424ead5751d5ab41c8a01))
    - Remove always sending a message to ourself when we can handle it directly ([`dbdda13`](https://github.com/maidsafe/safe_network/commit/dbdda1392325e8a08a6a28988208769552d5e543))
    - multiples fixes for unit tests ([`9a82649`](https://github.com/maidsafe/safe_network/commit/9a82649f0ca01c6d2eae57f260d2f98246724556))
    - send DkgRetry for DkgError::MissingPart as well ([`d3b88f7`](https://github.com/maidsafe/safe_network/commit/d3b88f749ca6ee53f65e200105aeeea691581e83))
    - use AE to progress various intra-DKG phases ([`f29c94d`](https://github.com/maidsafe/safe_network/commit/f29c94d7dfab2e82b9db70fcfeddc4a71d987abb))
    - ensure client chunk count is 3 w/ 7 elders ([`35a8fdb`](https://github.com/maidsafe/safe_network/commit/35a8fdb9dca60ca268d536958a9d32ce0a876792))
    - allow ELDER_COUNT to be overridden by env var SN_ELDER_COUNT ([`46ea542`](https://github.com/maidsafe/safe_network/commit/46ea542731fa3e2cede4ce9357783d3681434643))
    - move ELDER_COUNT to root of sn ([`e8e44fb`](https://github.com/maidsafe/safe_network/commit/e8e44fb69dae7fe1cae3bbf39f7b03a90df44dd7))
    - depend on ELDER_SIZE where it should be used ([`86c83be`](https://github.com/maidsafe/safe_network/commit/86c83bedf061b285b0d4f3effb54cc89a9fbd84d))
    - set node count to be 33 ([`14328b3`](https://github.com/maidsafe/safe_network/commit/14328b3d5b2579e4f038624c2353ec36c45fa9ed))
    - increase join timeout when always-joinable set ([`590bfea`](https://github.com/maidsafe/safe_network/commit/590bfea538bb78fd20bb9574c3ea9ed6dcceced0))
    - increase join timeout before waiting to retry at bin/node ([`85f2b5f`](https://github.com/maidsafe/safe_network/commit/85f2b5fd2adbf1f57e184c9e687fd14929b911e7))
    - fix a compilation error introduced by merge ([`c27d7e9`](https://github.com/maidsafe/safe_network/commit/c27d7e997c5a7812f995f113f31edf30c2c21272))
    - store `Peer`s in `SectionAuthorityProvider` ([`b1297f2`](https://github.com/maidsafe/safe_network/commit/b1297f2e42cdd7f7945c9fd5b4012086f8298b85))
    - reuse known `Peer`s in DKG sessions ([`680f943`](https://github.com/maidsafe/safe_network/commit/680f9431bc75efea455dd7c6985e41c11740dc3e))
    - clarify `connection` in `Peer`'s `Debug` output ([`eb02910`](https://github.com/maidsafe/safe_network/commit/eb0291082e49d1114c853b214f55b8a25e18d1e1))
    - store a `Peer` in `NodeState` ([`93bffbf`](https://github.com/maidsafe/safe_network/commit/93bffbf1343621a8d5187649d1bb6d2a81cba793))
    - set `Peer::connection` on send ([`1268fb6`](https://github.com/maidsafe/safe_network/commit/1268fb6e46e4ba061ba7f724917371d8e0db0092))
    - bye bye so much olde node code ([`14c6ab9`](https://github.com/maidsafe/safe_network/commit/14c6ab95da62a144a798665ade6da626fb5ee6f2))
    - readd routing member joined event for tests ([`30d220c`](https://github.com/maidsafe/safe_network/commit/30d220c180c7586de56282fafe52b54c8b619860))
    - log node age on relocation in routing ([`6c02091`](https://github.com/maidsafe/safe_network/commit/6c0209187797bb27fa1529dd2f0ee8116975a02d))
    - set joins allowed in routing code ([`a328d70`](https://github.com/maidsafe/safe_network/commit/a328d70720a795b62ac05a68454abf1034e97b9d))
    - burn down more of the /node dir ([`a98cc81`](https://github.com/maidsafe/safe_network/commit/a98cc810409a4ccc78e3c7a36b38b5f00cfcb23a))
    - add StillElderAfterSplit LogMarker ([`da1fe8d`](https://github.com/maidsafe/safe_network/commit/da1fe8dda6325e05b4d976d2b77d084c06d98cb0))
    - remove AdultsChanged event and ahndle everything inside routing ([`078f451`](https://github.com/maidsafe/safe_network/commit/078f45148af2da8d1690490720ef5f95555d42cd))
    - joining node age to genesis section shall be less than or equal to suggested age ([`55eb0f2`](https://github.com/maidsafe/safe_network/commit/55eb0f259a83faff470dbfdeb9365d314ed6a697))
    - update client listener error handling ([`4c9b20e`](https://github.com/maidsafe/safe_network/commit/4c9b20ee58be9ca3579c7d1f4edd09452d59d38c))
    - cleanup test clippy ([`49d3a29`](https://github.com/maidsafe/safe_network/commit/49d3a298be19a46dcb6232ee8087c9bf0381db23))
    - retry client queires if elder conns fail ([`20bda03`](https://github.com/maidsafe/safe_network/commit/20bda03e07985114b8a54c866755be8a240c2504))
    - dont always exit on client listener loop errors ([`cdc31cb`](https://github.com/maidsafe/safe_network/commit/cdc31cb44af8b6dc2393a0f15c98c0be364db6ff))
    - fail query if we cannot send to more than 1 elder ([`bc71707`](https://github.com/maidsafe/safe_network/commit/bc717073fde5fef59cbbe670ab6b7a9d66253d3b))
    - fix routing tests failing after we no longer emit MemerLeft event ([`189aeb6`](https://github.com/maidsafe/safe_network/commit/189aeb6941d277f94e47ade0e851696206aaa146))
    - simplify msg handling for NodeCmds ([`9d5baf8`](https://github.com/maidsafe/safe_network/commit/9d5baf83c630e597407e5c5d4a47fc7f34805696))
    - more node plumbing cleanup ([`9aaccb8`](https://github.com/maidsafe/safe_network/commit/9aaccb8784afd03454cd69f599dc2b7822ca130c))
    - handle set storage level in routing directly ([`8f1cc32`](https://github.com/maidsafe/safe_network/commit/8f1cc32b87a9e333f32399020339dd41e2d0b049))
    - handle member left directly in routing code ([`1fbc9b2`](https://github.com/maidsafe/safe_network/commit/1fbc9b24d14460c6a88a064297c3a327ca9182aa))
    - handle receiving dataExchange packet in routing. ([`4e7da0a`](https://github.com/maidsafe/safe_network/commit/4e7da0aeddba7287c1556ca318d9cdd8459000d2))
    - remove node plumbing ([`456c022`](https://github.com/maidsafe/safe_network/commit/456c022ddf66af5f991e6aca2240d2ab73f8241d))
    - perform data exchange on split in routing ([`ccb7efd`](https://github.com/maidsafe/safe_network/commit/ccb7efda1ee0c6303ac537ca9edd4b6c5cfcc5f2))
    - correlate client connections without `ConnectedPeers` ([`b70a105`](https://github.com/maidsafe/safe_network/commit/b70a1053adc267780215636dd80a759c45d533d5))
    - use `qp2p::Connection` for connected senders ([`1d62fce`](https://github.com/maidsafe/safe_network/commit/1d62fcefd5d44ef0df84b8126ba88de1127bd2bd))
    - represent `HandleMessage::sender` as an enum ([`20c1d7b`](https://github.com/maidsafe/safe_network/commit/20c1d7bf22907a44be2cc9585dd2ac55dd2985bf))
    - add an `Option<qp2p::Connection>` to `Peer` ([`182c4db`](https://github.com/maidsafe/safe_network/commit/182c4db4531c77fa6c67b8267cd7895b49f26ae1))
    - add a log marker for reusing a connection ([`02f1325`](https://github.com/maidsafe/safe_network/commit/02f1325c18779ff02e6bb7d903fda4519ca87231))
    - add a feature to disable connection pooling ([`1bf1766`](https://github.com/maidsafe/safe_network/commit/1bf17662ecae4e1f3e9969ac03a8f543f57f2cd0))
    - on split retain adult members only ([`d55978b`](https://github.com/maidsafe/safe_network/commit/d55978b38c9f69c0e31b335909ca650758c522c1))
    - fix demotion sendmessage test ([`9a14911`](https://github.com/maidsafe/safe_network/commit/9a149113d6ffa4d4e5564ad8ba274117a4522685))
    - only fire SplitSuccess if elder ([`f8de28a`](https://github.com/maidsafe/safe_network/commit/f8de28a8e040439fcf83f217261facb934cacd51))
    - send data updates to new elders only ([`231bde0`](https://github.com/maidsafe/safe_network/commit/231bde07064d6597867b5668a1e5ff319de3a202))
    - dont AE-update sibling elders on split. ([`3b7091b`](https://github.com/maidsafe/safe_network/commit/3b7091bd8b45fc5bbfa6f1714ec56e123d76c647))
    - remove node plumbing ([`3b4e2e3`](https://github.com/maidsafe/safe_network/commit/3b4e2e33285f4b1a53bd52b147dc85c570ed2f6e))
    - perform data exchange on split in routing ([`345033a`](https://github.com/maidsafe/safe_network/commit/345033abe74a86c212e3b38fc0f6b655216b63b0))
    - stop sending unnecessary AE-Update msgs to to-be-promoted candidates ([`4a1349c`](https://github.com/maidsafe/safe_network/commit/4a1349c5e99fdc45c038afdd2dac24486ee625ea))
    - verify and use SAP received in AE-Redirect to update client's network knowledge ([`f6a3f78`](https://github.com/maidsafe/safe_network/commit/f6a3f78156b9cc0f934c19e5d4c0004238a593e4))
    - verify and use SAP received in AE-Redirect to update our network knowledge ([`ae5fc40`](https://github.com/maidsafe/safe_network/commit/ae5fc40e29161652306b2e42b92d2a80fc746708))
    - log an error when failing to add latest sibling key to proof chain ([`d673283`](https://github.com/maidsafe/safe_network/commit/d6732833db0bb13d8414f99758e8767ab8ab2bdd))
    - increase timeout for network forming ([`4f5e84d`](https://github.com/maidsafe/safe_network/commit/4f5e84da96d7277dfc4e385ff03edf6c1d84991e))
    - update launch tool and secured linked list deps ([`d75e5c6`](https://github.com/maidsafe/safe_network/commit/d75e5c65d04c60dca477cca4e704308198cfd6cf))
    - use correct dst section key for AE-Update to siblings after split ([`42d90b7`](https://github.com/maidsafe/safe_network/commit/42d90b763606e2d324c5ce1235fc801105c07acb))
    - include section chain in AE-Redirect messages ([`3a59ee3`](https://github.com/maidsafe/safe_network/commit/3a59ee3b532bbc26388780ddc2f5b51ddae61d4c))
    - use a DAG to keep track of all sections chains within our NetworkKnowledge ([`90f2979`](https://github.com/maidsafe/safe_network/commit/90f2979a5bcb7f8e1786b5bdd868793d2fe924b4))
    - raise SectionSplit event whenever prefix changed ([`302ce4e`](https://github.com/maidsafe/safe_network/commit/302ce4e605d72a0925509bfe3220c2b1ddac677d))
    - during bootstrap, handling the case prefix_map was loaded ([`c785139`](https://github.com/maidsafe/safe_network/commit/c78513903457a701096b5c542f15012e71d33c46))
</details>

## v0.40.0 (2021-11-15)

### New Features

 - <csr-id-b8b0097ac86645f0c7a7352f2bf220279068228c/> support `--json-logs` in `testnet`
   This makes it easier to enable JSON logs for local testnets.
 - <csr-id-044fd61950be76e3207694094dcec81313937403/> allow to change the default interval for testnet nodes

### Bug Fixes

 - <csr-id-ad633e1b6882db1aac0cb1a530300d9e4d666fd8/> avoid holding write lock on ae_backoff_cache during sleep
   This could inadvertently be slowing down other joins if we're holding the write lock on the cache while we backoff
 - <csr-id-366ad9a42034fe450c30908d372fede0ff92f655/> draining backlog before process DKG message
 - <csr-id-c4a42fb5965c79aaae2a0ef2ae62fcb987c37525/> less DKG progression interval; switch SAP on DKGcompletion when OurElder already received
 - <csr-id-adf9feac964fb7f690bd43aeef3270c82fde419c/> fix `Eq` for `SectionPeers`
   This would have always returned true, since it was comparing with
   itself...

### New Features (BREAKING)

 - <csr-id-9b8ddfde0287e47b6f18a77a8e8847d80ee84bcd/> provide the SAP proof chain in JoinResponse::Retry msgs
   - Joining node now makes use of the NetworkPrefixMap to validate and
   accept new SAPs using the proof chain provided in JoinResponse::Retry.

### refactor (BREAKING)

 - <csr-id-ee165d41ca40be378423394b6422570d1d47727c/> duplicate `SectionAuthorityProvider` in `routing`
   This is part of the move towards passing connection handles through
   network knowledge. Whilst it would be possible to put a connection
   handle in the `messaging` struct, and just skip it when (de)serialising,
   this seems less clean than having a separate struct.
 - <csr-id-00acf0c0d8a65bdd5355ba909d73e74729a27044/> move `SectionPeers` from `messaging` to `routing`
   `SectionPeers` was only used in the `SystemMsg::AntiEntropyUpdate`
   message. Rather than simply inlining the `DashMap`, it's been replaced
   by `BTreeSet<SectionAuth<NodeState>>` since the map keys are redundant
   with the `name` field of `NodeState` (and `BTreeSet` specifically for
   deterministic ordering).
   
   With the move, it's also become `pub(crate)`, so from the perspective of
   the public API this type has been removed.
 - <csr-id-7d5d5e11fef39a6dc1b89c972e42772db807374c/> move `ElderCandidates` to `routing`
   The `ElderCandidate` type was in `messaging`, and was only used in the
   `SystemMsg::DkStart` message. It was more commonly used as network
   state, so homing it in `routing::network_knowledge` makes more sense.
   This also means the type no longer needs to be public, and doesn't need
   to be (de)serialisable, which opens up the possibility of putting more
   stuff in it in future (e.g. connection handles...).
   
   As a first step down that road, the `elders` field now contains `Peer`s
   rather than merely `SocketAddr`s. The majority of reads of the field
   ultimately want either name-only or `Peer`s, so this seems reasonable
   regardless.
 - <csr-id-855b8ff87217e92a5f7d55fb78ab73c9d81f75a2/> make `SectionAuthorityProviderUtils` `pub(crate)`
   This shouldn't really be part of the public API, and having it such
   transitively grows the visibility of its collaborators (e.g.
   `ElderCandidates`) which also don't need to be in the public API.
 - <csr-id-23b8a08e97fa415b9216caac5da18cb97ede980f/> remove `Copy` from `Peer`
   Eventually we may want to put a `Connection` into `Peer`, which is not
   `Copy`. As such, `Peer` itself will be unable to derive `Copy`. Although
   this might not happen for a while, removing dependence on `Copy` now
   will make it smoother when the time comes.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
 - 3 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network v0.40.0/sn_api v0.39.0 ([`7001573`](https://github.com/maidsafe/safe_network/commit/70015730c3e08881f803e9ce59be7ca16185ae11))
    - retry on connection loss of reused connection ([`1389ffa`](https://github.com/maidsafe/safe_network/commit/1389ffa00762e126047a206abc475599c277930c))
    - avoid holding write lock on ae_backoff_cache during sleep ([`ad633e1`](https://github.com/maidsafe/safe_network/commit/ad633e1b6882db1aac0cb1a530300d9e4d666fd8))
</details>

## v0.39.0 (2021-11-12)

### New Features

 - <csr-id-b8b0097ac86645f0c7a7352f2bf220279068228c/> support `--json-logs` in `testnet`
   This makes it easier to enable JSON logs for local testnets.
 - <csr-id-044fd61950be76e3207694094dcec81313937403/> allow to change the default interval for testnet nodes

### Bug Fixes

 - <csr-id-366ad9a42034fe450c30908d372fede0ff92f655/> draining backlog before process DKG message
 - <csr-id-c4a42fb5965c79aaae2a0ef2ae62fcb987c37525/> less DKG progression interval; switch SAP on DKGcompletion when OurElder already received
 - <csr-id-adf9feac964fb7f690bd43aeef3270c82fde419c/> fix `Eq` for `SectionPeers`
   This would have always returned true, since it was comparing with
   itself...

### New Features (BREAKING)

 - <csr-id-9b8ddfde0287e47b6f18a77a8e8847d80ee84bcd/> provide the SAP proof chain in JoinResponse::Retry msgs
   - Joining node now makes use of the NetworkPrefixMap to validate and
   accept new SAPs using the proof chain provided in JoinResponse::Retry.

### refactor (BREAKING)

 - <csr-id-ee165d41ca40be378423394b6422570d1d47727c/> duplicate `SectionAuthorityProvider` in `routing`
   This is part of the move towards passing connection handles through
   network knowledge. Whilst it would be possible to put a connection
   handle in the `messaging` struct, and just skip it when (de)serialising,
   this seems less clean than having a separate struct.
 - <csr-id-00acf0c0d8a65bdd5355ba909d73e74729a27044/> move `SectionPeers` from `messaging` to `routing`
   `SectionPeers` was only used in the `SystemMsg::AntiEntropyUpdate`
   message. Rather than simply inlining the `DashMap`, it's been replaced
   by `BTreeSet<SectionAuth<NodeState>>` since the map keys are redundant
   with the `name` field of `NodeState` (and `BTreeSet` specifically for
   deterministic ordering).
   
   With the move, it's also become `pub(crate)`, so from the perspective of
   the public API this type has been removed.
 - <csr-id-7d5d5e11fef39a6dc1b89c972e42772db807374c/> move `ElderCandidates` to `routing`
   The `ElderCandidate` type was in `messaging`, and was only used in the
   `SystemMsg::DkStart` message. It was more commonly used as network
   state, so homing it in `routing::network_knowledge` makes more sense.
   This also means the type no longer needs to be public, and doesn't need
   to be (de)serialisable, which opens up the possibility of putting more
   stuff in it in future (e.g. connection handles...).
   
   As a first step down that road, the `elders` field now contains `Peer`s
   rather than merely `SocketAddr`s. The majority of reads of the field
   ultimately want either name-only or `Peer`s, so this seems reasonable
   regardless.
 - <csr-id-855b8ff87217e92a5f7d55fb78ab73c9d81f75a2/> make `SectionAuthorityProviderUtils` `pub(crate)`
   This shouldn't really be part of the public API, and having it such
   transitively grows the visibility of its collaborators (e.g.
   `ElderCandidates`) which also don't need to be in the public API.
 - <csr-id-23b8a08e97fa415b9216caac5da18cb97ede980f/> remove `Copy` from `Peer`
   Eventually we may want to put a `Connection` into `Peer`, which is not
   `Copy`. As such, `Peer` itself will be unable to derive `Copy`. Although
   this might not happen for a while, removing dependence on `Copy` now
   will make it smoother when the time comes.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 19 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 19 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network v0.39.0 ([`ab00cf0`](https://github.com/maidsafe/safe_network/commit/ab00cf08d217654c57449437348b73576a65e89f))
    - remove unnecessary peer lagging check ([`5a3b70e`](https://github.com/maidsafe/safe_network/commit/5a3b70e9721fcdfdd809d2a6bd85968446b4e9a3))
    - make `NodeState` not `Copy` ([`0a5027c`](https://github.com/maidsafe/safe_network/commit/0a5027cd9b2c62833ccf70e2bcca5ab22625a840))
    - duplicate `SectionAuthorityProvider` in `routing` ([`ee165d4`](https://github.com/maidsafe/safe_network/commit/ee165d41ca40be378423394b6422570d1d47727c))
    - duplicate `NodeState` in `routing` ([`645047b`](https://github.com/maidsafe/safe_network/commit/645047b231eaf69f1299dee22ff2079feb7f5a95))
    - move `SectionPeers` from `messaging` to `routing` ([`00acf0c`](https://github.com/maidsafe/safe_network/commit/00acf0c0d8a65bdd5355ba909d73e74729a27044))
    - update testnet default interval ([`08629e5`](https://github.com/maidsafe/safe_network/commit/08629e583a03852d9f02b0e7ff66a829e3056d9b))
    - rename forced to skip section info agreement, tidy assignment ([`5a81d87`](https://github.com/maidsafe/safe_network/commit/5a81d874cf8040ff81a602bc8c5707a4ba6c64ff))
    - update bls_dkg to 0.7.1 ([`8aa27b0`](https://github.com/maidsafe/safe_network/commit/8aa27b01f8043e98953971d1623aaf8b07d1596e))
    - draining backlog before process DKG message ([`366ad9a`](https://github.com/maidsafe/safe_network/commit/366ad9a42034fe450c30908d372fede0ff92f655))
    - less DKG progression interval; switch SAP on DKGcompletion when OurElder already received ([`c4a42fb`](https://github.com/maidsafe/safe_network/commit/c4a42fb5965c79aaae2a0ef2ae62fcb987c37525))
    - stepped age with long interval ([`48a7d00`](https://github.com/maidsafe/safe_network/commit/48a7d0062f259b362eb745e1bd59e77df514f1b8))
    - move `ElderCandidates` to `routing` ([`7d5d5e1`](https://github.com/maidsafe/safe_network/commit/7d5d5e11fef39a6dc1b89c972e42772db807374c))
    - make `SectionAuthorityProviderUtils` `pub(crate)` ([`855b8ff`](https://github.com/maidsafe/safe_network/commit/855b8ff87217e92a5f7d55fb78ab73c9d81f75a2))
    - move `routing::Peer` into `network_knowledge` ([`0e2b01b`](https://github.com/maidsafe/safe_network/commit/0e2b01bb7e9c32f0bc5c1c6fd616acfcc5c45020))
    - remove `Copy` from `Peer` ([`23b8a08`](https://github.com/maidsafe/safe_network/commit/23b8a08e97fa415b9216caac5da18cb97ede980f))
    - add more tracing around connections ([`008f363`](https://github.com/maidsafe/safe_network/commit/008f36317f440579fe952325ccb97c9b5a547165))
    - support `--json-logs` in `testnet` ([`b8b0097`](https://github.com/maidsafe/safe_network/commit/b8b0097ac86645f0c7a7352f2bf220279068228c))
    - update bls_dkg and blsttc to 0.7 and 0.3.4 respectively ([`213cb39`](https://github.com/maidsafe/safe_network/commit/213cb39be8fbfdf614f3eb6248b14fe161927a14))
</details>

## v0.38.0 (2021-11-10)

### New Features

 - <csr-id-044fd61950be76e3207694094dcec81313937403/> allow to change the default interval for testnet nodes

### Bug Fixes

 - <csr-id-adf9feac964fb7f690bd43aeef3270c82fde419c/> fix `Eq` for `SectionPeers`
   This would have always returned true, since it was comparing with
   itself...

### New Features (BREAKING)

 - <csr-id-9b8ddfde0287e47b6f18a77a8e8847d80ee84bcd/> provide the SAP proof chain in JoinResponse::Retry msgs
   - Joining node now makes use of the NetworkPrefixMap to validate and
   accept new SAPs using the proof chain provided in JoinResponse::Retry.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 1 day passed between releases.
 - 6 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - safe_network v0.38.0 ([`cbdf023`](https://github.com/maidsafe/safe_network/commit/cbdf0236f7cd241e7addb1afe75ed5e5cfac00ab))
    - fix `Eq` for `SectionPeers` ([`adf9fea`](https://github.com/maidsafe/safe_network/commit/adf9feac964fb7f690bd43aeef3270c82fde419c))
    - reuse join permits for relocated nodes too. ([`a7968dd`](https://github.com/maidsafe/safe_network/commit/a7968dd17927e346b3d32bb5971ed6457aea6606))
    - insert a connected peer for outgoing connections ([`1568adb`](https://github.com/maidsafe/safe_network/commit/1568adb28a2a6a3cdf8a9737e098a5ea7bb2c419))
    - allow to change the default interval for testnet nodes ([`044fd61`](https://github.com/maidsafe/safe_network/commit/044fd61950be76e3207694094dcec81313937403))
    - provide the SAP proof chain in JoinResponse::Retry msgs ([`9b8ddfd`](https://github.com/maidsafe/safe_network/commit/9b8ddfde0287e47b6f18a77a8e8847d80ee84bcd))
</details>

## v0.37.0 (2021-11-09)

### New Features

<csr-id-1e92fa5a2ae4931f6265d82af121125495f58655/>
<csr-id-56f3b514fceccbc1cc47256410b4f2119bb8affd/>
<csr-id-ddad0b8ce37d3537a9e9ed66da18758b6b3ace68/>
<csr-id-cfaed1ece5120d60e9f352b4e9ef70448e2ed3f2/>
<csr-id-60239655dba08940bd293b3c9243ac732923acfe/>
<csr-id-4cafe78e3fdb60d144e8cf810788116ce01de025/>
<csr-id-d1ecf96a6d965928d434810ccc9c89d1bc7fac4e/>
<csr-id-958e38ecd3b4e4dc908913192a1d43b83e082d08/>

 - <csr-id-a3552ae2dd0f727a71505d832c1ed2520283e8c8/> add network health check script to easily wait until we're ready+healthy
 - <csr-id-ba5f28475048bfaebcc37c660bec65644e4e52fe/> cache prefix_map for clients
   - also refactors methods for writing data to disk

### Bug Fixes

 - <csr-id-e406a9a61bb313dcd445d639e82fa8ae1ff99964/> refactor the Join process regarding used_recipients
 - <csr-id-db76c5ee5b2efc35214d12df0a2aa4e137231fa6/> if attempting to send a msg fails, continue with the rest of cmd processing instead of bailing out
 - <csr-id-5b596a444d24f7021db9d3c322bafa33d49dcfae/> client config test timeout/keepalive needed updating
 - <csr-id-d44fd8d5888c554bf1aa0d56e08471cfe90bd988/> routing test tweaks
 - <csr-id-a927d9e27d831a48348eab6d41e0f4231b0e62c7/> make health check tolerante of possible demotions
 - <csr-id-42a3123b0aa4f01daedc2faebd3fa3430dbc1618/> tesnet grep is_log_file check
 - <csr-id-9214dff1541b28577c44d8cbbebeec0b80598653/> node tests after removing blocking cmd layer
 - <csr-id-bc47eed7e327a8e02fff1d385648c752ad33b8f1/> fix windows grep lock error: dont search non log files
 - <csr-id-27e8d895983d236a0949354219c4adbe3f1e22a0/> readd keep alive for client
 - <csr-id-e45dd10ac770ed41431f0e8758a6228fc4dfbe3c/> update logs during AEUpdate
 - <csr-id-a0806aa624384ccb437bc8b4b2d108523ea5c068/> bring back elders len check during chain updates
 - <csr-id-57b736a8901ed76c402460fcf1799162cfdb3c37/> match on name
 - <csr-id-6eb389acfb3adcec0be07ee990106ed19a7f78f5/> make `KvStore::store` perform atomic immutable writes
   `KvStore` is the database abstraction we use to persist chunks. This is
   itself backed by `sled`. `sled` uses a log as one of the underlying
   storage primitives, which means that all inserts (even overwrites) will
   consume additional space (until the log is compacted, which may happen
   irregularly).
   
   However, chunks themselves are write-once which means that we would only
   ever need to write a value once. As such, we can use `compare_and_swap`
   to write the value only if it's not currently set. Since chunks are
   content-addressable, we furthermore don't need to do anything if the
   value did already exist and wasn't written, since there is only one
   possible value for a given key.
 - <csr-id-b53a86b9ba0bfb33ab9cccaecb383bcce0acf233/> fix test for `GetChunk` 'data not found' failures
   `QueryResponse::failed_with_data_not_found` is used to prevent sending
   'data not found' errors to clients. Clients, in turn, do not retry
   errors, only timeouts.
   
   Recently (c0b1770154) the error type returned when a chunk is not found
   was changed from `NoSuchData` to `ChunkNotFound`, but the check in
   `failed_with_data_not_found` was not updated. This could lead to
   spurious client errors when they attempt to read data before it is
   available (which would otherwise be covered by the retry on timeout,
   since nodes would not forward the data-not-found response).
 - <csr-id-5f985342239b74963f89e627e28031f8817c0d3d/> typo
 - <csr-id-a1910095d4b7a749186d614e04dad2b64a1eabff/> spot and blob management inconsistency add some comments
 - <csr-id-ab98ec2076c1f6c60899c505ed85aaefa5359278/> replace prefixmap check to ensure last proof chainkey is present on provided sap
 - <csr-id-96519e26ef9adf6360b6a8f79ca73ab4bc7b627b/> elders update uses correct key for prefixmap updates
 - <csr-id-3569939ff83978ba50039588eb87d4e6da4fedd2/> updated routing untrusted ae test for new error
 - <csr-id-f615aacdb4cd972545ba88c927cfb5a9b357fb9a/> provide valid proof chain to update prefix map during elders agreement handling
 - <csr-id-51ea358eff0edee0b27c5e21af4f38a7ee09422c/> keep section in sync on ae retry recevial at nodes
 - <csr-id-0df6615df2fdb88d7fd89f9751c6b88e1b7ebb5f/> ae blob tests dont have standard wait
 - <csr-id-b0319628d587a2caeb1aa5be52505cbb6ede40d3/> use client query in routing tests instead of bytes
 - <csr-id-20a69ccb6c6b1f466b519318dd85ef11e7700027/> don't bail out when handling ElderAgreement if prefix-map is not updated
 - <csr-id-0ea37af54f761b2a1a5137463637736c6be25206/> enough having enough elders before split attempt
 - <csr-id-0d3fe1a6e02f6d83115e9098c706a98cb688d41d/> allow to set the genesis key for Core and Section structs of first node
 - <csr-id-c360dc61bd7d4ce1745ba7bfbe9d032fedac067a/> prevent deadlock by dropping write locks in relocation
 - <csr-id-084926e045952670295eb666c82f1d77ff88f1be/> fix a possible panic when creating client
   `client::connections::Session::make_contact_with_nodes` would slice the
   known initial contacts, but this would panic if fewer than
   `NODES_TO_CONTACT_PER_STARTUP_BATCH` (currently: 3) nodes are known.
   It's not clear that this is a precondition or requirement, so for now we
   simply take as many as we can up to
   `NODES_TO_CONTACT_PER_STARTUP_BATCH`. If it *is* a requirement, we
   should return an `Err` rather than panic.
 - <csr-id-3a19e42770a12885659d653b98511378dad8015f/> improve join req handling for elders
 - <csr-id-2d807cb4b75bfd52d9dde7b91214bd3dc5a6d992/> maintain lexicographical order
 - <csr-id-c765828018158bda663f937a73bcc47ab358884c/> write to correct source
 - <csr-id-ba7c4c609150839825a4992c08e3bdd00b698269/> get more splits (at the cost of more join msgs)
 - <csr-id-104ed366dbaafd98fd3ef67899e27540894a959c/> dont wait before first join request
 - <csr-id-0033fc65db266d92623f53628f9f5b6e6069920d/> fix elders agreement routing tests using wrong command type
 - <csr-id-ec3f16a8c33e8afbdfffb392466ba422216d3f68/> missing post rebase async_trait dep
 - <csr-id-73c5baf94dd37346c6c9987aa51ca26f3a2fea1f/> only resend if we've had an update
 - <csr-id-6aa1130a7b380dc3d1ad12f3054cda3a390ff20d/> client config test
 - <csr-id-c88a1bc5fda40093bb129b4351eef73d2eb7c041/> resolution test except for the blob range tests
 - <csr-id-fd4513f054e282218797208dcac1de6903e94f2c/> build `sn_node` from inside `sn` directory
   Since we moved to a workspace layout, the `testnet` binary has been
   building more than necessary since it runs `cargo build` from the
   workspace root. Additionally, cargo's feature resolution may behave
   unexpectedly when executed from the workspace directory, so rather than
   using `-p safe_network` we rather run `cargo build` from the `sn`
   directory.
 - <csr-id-7ff7557850460d98b526646b21da635381a70e2a/> exit with error if `sn_node` build fails
   We would previously ignore the result of trying to build the `sn_node`
   binary, which would lead to either using the incorrect binary if one
   existed, or a later error because the binary doesn't exist.

### New Features (BREAKING)

 - <csr-id-06b57d587da4882bfce1b0acd09faf9129306ab2/> add log markers for connection open/close
   We can detect connection open/close easily in the connection listener
   tasks, since these are started as soon as a connection is opened, and
   finish when there are no more incoming connections (e.g. connection has
   closed).
 - <csr-id-20895dd4326341de4d44547861ac4a57ae8531cf/> the JoinResponse::Retry message now provides the expected age for the joining node

### refactor (BREAKING)

 - <csr-id-61dec0fd90b4df6b0695a7ba46da86999d199d4a/> remove `SectionAuthorityProviderUtils::elders`
   Since the `elders` field of `SectionAuthorityProvider` is public, this
   method is essentially equivalent to `sap.elders.clone()`. Furthermore,
   it was barely used (5 call sites).
 - <csr-id-49c76d13a91474038bd8cb005959a37a7d4c6603/> tweak `Peer` API
   `Peer`'s fields are no longer public, and the `name` and `addr` methods
   return owned (copied) values. There's no point in having both a public
   field and a getter, and the getters were used far more often. The
   getters have been changed to return owned values, since `XorName` and
   `SocketAddr` are both `Copy`, so it seems reasonable to avoid the
   indirection of a borrow.
 - <csr-id-c5d2e31a5f0cea381bb60dc1f896dbbda5038506/> remove `PeerUtils` trait
   This trait has been redundant since the `sn_messaging` and `sn_routing`
   crate were merged, but it's doubly so when the `Peer` struct is now
   itself in `routing`.
 - <csr-id-0cb790f5c3712e357f685bfb88cd237c5b5f76c5/> move `Peer` from `messaging` to `routing`
   The `Peer` struct no longer appears in any message definitions, and as
   such doesn't belong in `messaging`. It has been moved to `routing`,
   which is the only place it's now used.
   
   The derive for `Deserialize` and `Serialize` have also been removed from
   `Peer`, since they're no longer needed.
 - <csr-id-b3ce84012e6cdf4c87d6d4a3137ab6506264e949/> remove `Peer` from `NodeState`
   Syntactially, having a "peer" struct inside `NodeState` doesn't make a
   lot of sense, vs. including the name and address directly in the state.
   This also unblocks moving `Peer` out of `messaging`, which will remove
   the requirement for it to be serialisable etc.
   
   To make migration easier, an `age` method was added to the
   `NodeStateUtils` trait.
 - <csr-id-ecf0bc9a9736167edb15db7ff4e3cf5dc388dd22/> remove `reachable` from `messaging::system::Peer`
   Although the field is `false` on initialisation, and it is checked in a
   couple of places, in every case as far as I can tell it is set to `true`
   right after construction. As such, it's not really doing anything for us
   and we can remove it.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 140 commits contributed to the release over the course of 27 calendar days.
 - 130 commits where understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' where seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - (cargo-release) version 0.37.0 ([`ef42af4`](https://github.com/maidsafe/safe_network/commit/ef42af483725286dd8a0cef6f922dfd1739412d8))
    - refactor the Join process regarding used_recipients ([`e406a9a`](https://github.com/maidsafe/safe_network/commit/e406a9a61bb313dcd445d639e82fa8ae1ff99964))
    - if attempting to send a msg fails, continue with the rest of cmd processing instead of bailing out ([`db76c5e`](https://github.com/maidsafe/safe_network/commit/db76c5ee5b2efc35214d12df0a2aa4e137231fa6))
    - use the prefix map updating outcome to decide if current SAP and chain shall be updated ([`7d5212a`](https://github.com/maidsafe/safe_network/commit/7d5212a2b916a8e540403346e0770cee1a446884))
    - some variables renaming and improving some comments ([`b66444c`](https://github.com/maidsafe/safe_network/commit/b66444c7d7a10c9a26db30fe5ce8e91984031624))
    - drop JoinRetry responses for SAPs we've already resent join reqs to ([`109843c`](https://github.com/maidsafe/safe_network/commit/109843c67ffd4ac4675cdfe56d7fcdaf97664007))
    - avoid rebuild on windows for testnet ([`c5aae59`](https://github.com/maidsafe/safe_network/commit/c5aae59a4eac6c3285bc45845433d23fc96d155f))
    - client config test timeout/keepalive needed updating ([`5b596a4`](https://github.com/maidsafe/safe_network/commit/5b596a444d24f7021db9d3c322bafa33d49dcfae))
    - add more log markers. ([`723618d`](https://github.com/maidsafe/safe_network/commit/723618d0e3ad77eb26a3c698f9545490166b7ee0))
    - fix healthcheck 0/1 summation. ([`1b7f5d6`](https://github.com/maidsafe/safe_network/commit/1b7f5d6e198eb286034e12eed3fa4fc63b927e0f))
    - update log messages and test errors for clarity ([`80ce4ca`](https://github.com/maidsafe/safe_network/commit/80ce4ca9376b99037b9377425c477ea3a7493e54))
    - routing test tweaks ([`d44fd8d`](https://github.com/maidsafe/safe_network/commit/d44fd8d5888c554bf1aa0d56e08471cfe90bd988))
    - make health check tolerante of possible demotions ([`a927d9e`](https://github.com/maidsafe/safe_network/commit/a927d9e27d831a48348eab6d41e0f4231b0e62c7))
    - tesnet grep is_log_file check ([`42a3123`](https://github.com/maidsafe/safe_network/commit/42a3123b0aa4f01daedc2faebd3fa3430dbc1618))
    - node tests after removing blocking cmd layer ([`9214dff`](https://github.com/maidsafe/safe_network/commit/9214dff1541b28577c44d8cbbebeec0b80598653))
    - remove blocking/non blocking msg handling distinction ([`616c024`](https://github.com/maidsafe/safe_network/commit/616c024f2280fc99de063026756a9c938f78b885))
    - fix windows grep lock error: dont search non log files ([`bc47eed`](https://github.com/maidsafe/safe_network/commit/bc47eed7e327a8e02fff1d385648c752ad33b8f1))
    - clarify elder counts in health check test ([`cf5a041`](https://github.com/maidsafe/safe_network/commit/cf5a041742844b8b3d5e71c3e895018367b76013))
    - reduce join backoff times ([`ed6d2f0`](https://github.com/maidsafe/safe_network/commit/ed6d2f09c12b72446f0f55ecacb5fc4f35278575))
    - change client timeouts ([`9350f27`](https://github.com/maidsafe/safe_network/commit/9350f273a5afec88120fe79fe85ceaf8027d691e))
    - readd keep alive for client ([`27e8d89`](https://github.com/maidsafe/safe_network/commit/27e8d895983d236a0949354219c4adbe3f1e22a0))
    - reorder blob tst log initialisation to _after_ initial AE triggering msgs sent ([`a54c442`](https://github.com/maidsafe/safe_network/commit/a54c4426caccf71d28b2697094135892ec4a5e16))
    - update health check to test for Prefix(1/0), use this in CI ([`8eedb66`](https://github.com/maidsafe/safe_network/commit/8eedb660461b13b236545643fa21868eb6613826))
    - add env var for client query timeout ([`abfe737`](https://github.com/maidsafe/safe_network/commit/abfe7378604a74119accd7b9f86bef5682b0784a))
    - update logs during AEUpdate ([`e45dd10`](https://github.com/maidsafe/safe_network/commit/e45dd10ac770ed41431f0e8758a6228fc4dfbe3c))
    - reduce clone and lock during message handling ([`0399a9e`](https://github.com/maidsafe/safe_network/commit/0399a9e76547a03f1e3902aec30ecbd57ed437c7))
    - thread a `Peer` through more places ([`6bb5e64`](https://github.com/maidsafe/safe_network/commit/6bb5e64b3b0eb518719581278551272ae8f2b2ed))
    - simplify some iterator chains ([`e47e9ec`](https://github.com/maidsafe/safe_network/commit/e47e9ecb0c7f23209a9c1eb58c248fdf2facfd4a))
    - use `peers()` instead of `Peer::new` when available ([`3b728b6`](https://github.com/maidsafe/safe_network/commit/3b728b65a07a33019e06dd6f3da9fd334e6da9e1))
    - remove `SectionAuthorityProviderUtils::elders` ([`61dec0f`](https://github.com/maidsafe/safe_network/commit/61dec0fd90b4df6b0695a7ba46da86999d199d4a))
    - replace `(XorName, SocketAddr)` with `Peer` ([`87c62d3`](https://github.com/maidsafe/safe_network/commit/87c62d39d240afc01118135bc18d22fe23fc421c))
    - Fix crates.io badge ([`e764941`](https://github.com/maidsafe/safe_network/commit/e76494135d3c8f50b60d146d5c4dadc55248ba39))
    - tweak `Peer` API ([`49c76d1`](https://github.com/maidsafe/safe_network/commit/49c76d13a91474038bd8cb005959a37a7d4c6603))
    - remove `PeerUtils` trait ([`c5d2e31`](https://github.com/maidsafe/safe_network/commit/c5d2e31a5f0cea381bb60dc1f896dbbda5038506))
    - move `Peer` from `messaging` to `routing` ([`0cb790f`](https://github.com/maidsafe/safe_network/commit/0cb790f5c3712e357f685bfb88cd237c5b5f76c5))
    - remove `Peer` from `NodeState` ([`b3ce840`](https://github.com/maidsafe/safe_network/commit/b3ce84012e6cdf4c87d6d4a3137ab6506264e949))
    - remove `reachable` from `messaging::system::Peer` ([`ecf0bc9`](https://github.com/maidsafe/safe_network/commit/ecf0bc9a9736167edb15db7ff4e3cf5dc388dd22))
    - implement `Deref` for `SectionAuth` ([`23f53a3`](https://github.com/maidsafe/safe_network/commit/23f53a339a5fe0d2f2e5d415bfc01e646a81a5c8))
    - add network health check script to easily wait until we're ready+healthy ([`a3552ae`](https://github.com/maidsafe/safe_network/commit/a3552ae2dd0f727a71505d832c1ed2520283e8c8))
    - bring back elders len check during chain updates ([`a0806aa`](https://github.com/maidsafe/safe_network/commit/a0806aa624384ccb437bc8b4b2d108523ea5c068))
    - adapt routing unit tests to new network knowledge handling logic ([`9581fcf`](https://github.com/maidsafe/safe_network/commit/9581fcf27765752f6b556d632e8396e93d0f15e0))
    - renaming section mod to network_knowledge ([`5232bf5`](https://github.com/maidsafe/safe_network/commit/5232bf51c738197339ac70ee4f46adee4fa87179))
    - storing all sections chains in routing network knowledge ([`d7972da`](https://github.com/maidsafe/safe_network/commit/d7972da6abd3001e75019bf72aae6a98919ed1db))
    - refactoring NetworkKnowledge private API ([`49e2d14`](https://github.com/maidsafe/safe_network/commit/49e2d14a1c5f8fd83aa4b9a5abe67e23fca9f966))
    - moving to a unified network knowledge for updating SAPs and section chain ([`5326de5`](https://github.com/maidsafe/safe_network/commit/5326de5f658dfe75d1f5c44224d8623123848b08))
    - Section->NetworkKnowledge name change ([`f9cd39b`](https://github.com/maidsafe/safe_network/commit/f9cd39b6d3b1877fa0f3fa73bd6e3a796cae3b08))
    - match on name ([`57b736a`](https://github.com/maidsafe/safe_network/commit/57b736a8901ed76c402460fcf1799162cfdb3c37))
    - reduce the report check interval ([`208abdd`](https://github.com/maidsafe/safe_network/commit/208abdd108654ab83e8bee763793201e6f7b5eb2))
    - bump rust edition ([`fc10d03`](https://github.com/maidsafe/safe_network/commit/fc10d037d64efc86796f1b1c6f255a4c7f91d3e1))
    - make clients read prefix_map from disk ([`9a3cffc`](https://github.com/maidsafe/safe_network/commit/9a3cffc52589e8adf6dac75ae6aab4c184118648))
    - cache prefix_map for clients ([`ba5f284`](https://github.com/maidsafe/safe_network/commit/ba5f28475048bfaebcc37c660bec65644e4e52fe))
    - add `KvStore::flush` to avoid waiting in tests ([`c2df9db`](https://github.com/maidsafe/safe_network/commit/c2df9db2fe9e99450cc16060ff034289ab683783))
    - remove unnecessary `allow(unused)` ([`12c673e`](https://github.com/maidsafe/safe_network/commit/12c673e7f52ed1c4cbe15c14fe6eb4e68b986e18))
    - remove unused `KvStore::store_batch` ([`9d27749`](https://github.com/maidsafe/safe_network/commit/9d27749bc7d59ee499980044f57eab86d2e63d04))
    - make `KvStore::store` perform atomic immutable writes ([`6eb389a`](https://github.com/maidsafe/safe_network/commit/6eb389acfb3adcec0be07ee990106ed19a7f78f5))
    - remove redundant `K` type parameter from `KvStore` ([`2fc2f0e`](https://github.com/maidsafe/safe_network/commit/2fc2f0e6fbb5f7b05e61281e90992053ef5f0f5d))
    - update elder count check to be general one split health check ([`a03d2ce`](https://github.com/maidsafe/safe_network/commit/a03d2cef55665759ddfeb40972676c87b17ccfa8))
    - fix test for `GetChunk` 'data not found' failures ([`b53a86b`](https://github.com/maidsafe/safe_network/commit/b53a86b9ba0bfb33ab9cccaecb383bcce0acf233))
    - fix!(sn/node): disable keep-alive by default ([`eb90bb9`](https://github.com/maidsafe/safe_network/commit/eb90bb90db977ae6e368047e8a61efd6caba25bd))
    - add log markers for connection open/close ([`06b57d5`](https://github.com/maidsafe/safe_network/commit/06b57d587da4882bfce1b0acd09faf9129306ab2))
    - update `qp2p`, which removes `ConnectionPool` ([`432e36d`](https://github.com/maidsafe/safe_network/commit/432e36de012695c6c5e20bd704dc184db9c5c4d6))
    - typo ([`5f98534`](https://github.com/maidsafe/safe_network/commit/5f985342239b74963f89e627e28031f8817c0d3d))
    - spot and blob management inconsistency add some comments ([`a191009`](https://github.com/maidsafe/safe_network/commit/a1910095d4b7a749186d614e04dad2b64a1eabff))
    - unignore routing demote test ([`aaa0af4`](https://github.com/maidsafe/safe_network/commit/aaa0af4d1685c65a3c166070a590c10e9fd54765))
    - replace prefixmap check to ensure last proof chainkey is present on provided sap ([`ab98ec2`](https://github.com/maidsafe/safe_network/commit/ab98ec2076c1f6c60899c505ed85aaefa5359278))
    - Revert "feat(messages): add more prioritiy leves for different types of messages" ([`a0fd091`](https://github.com/maidsafe/safe_network/commit/a0fd09155c885dbfd6858a68805a8d4391284eb0))
    - Revert "chore: use constants for message priority" ([`6d1cdc6`](https://github.com/maidsafe/safe_network/commit/6d1cdc64078de06a43281d924f58d01b615e9268))
    - ignore demotion test for now ([`bde3cd5`](https://github.com/maidsafe/safe_network/commit/bde3cd5eac75cc41a3a9ffefb091584273575f68))
    - elders update uses correct key for prefixmap updates ([`96519e2`](https://github.com/maidsafe/safe_network/commit/96519e26ef9adf6360b6a8f79ca73ab4bc7b627b))
    - tweak demotion test ([`73b405d`](https://github.com/maidsafe/safe_network/commit/73b405d1228244a2b984e1294d5e8542f8691cef))
    - use prefixmap update as opposed to verify against redundant same chain ([`0c9c1f2`](https://github.com/maidsafe/safe_network/commit/0c9c1f2edd9e872b9ba1642ac50f59a63f68b488))
    - trust any valid key for chain updates ([`9bdd68f`](https://github.com/maidsafe/safe_network/commit/9bdd68f313b3b7881cb39db02026184bbab0bfb0))
    - move known prefix log to verify and udpate ([`8b7b664`](https://github.com/maidsafe/safe_network/commit/8b7b66450349b669e023539e92c77f9a3b830948))
    - updated routing untrusted ae test for new error ([`3569939`](https://github.com/maidsafe/safe_network/commit/3569939ff83978ba50039588eb87d4e6da4fedd2))
    - add check to received sap on ae update ([`7fc29f8`](https://github.com/maidsafe/safe_network/commit/7fc29f8187a29c2eeff8bbb5e09f068414bb8b93))
    - provide valid proof chain to update prefix map during elders agreement handling ([`f615aac`](https://github.com/maidsafe/safe_network/commit/f615aacdb4cd972545ba88c927cfb5a9b357fb9a))
    - keep section in sync on ae retry recevial at nodes ([`51ea358`](https://github.com/maidsafe/safe_network/commit/51ea358eff0edee0b27c5e21af4f38a7ee09422c))
    - ae blob tests dont have standard wait ([`0df6615`](https://github.com/maidsafe/safe_network/commit/0df6615df2fdb88d7fd89f9751c6b88e1b7ebb5f))
    - use constants for message priority ([`4415c9b`](https://github.com/maidsafe/safe_network/commit/4415c9b1d166f7e53032a0100d829e8581255a1e))
    - use client query in routing tests instead of bytes ([`b031962`](https://github.com/maidsafe/safe_network/commit/b0319628d587a2caeb1aa5be52505cbb6ede40d3))
    - add more prioritiy leves for different types of messages ([`1e92fa5`](https://github.com/maidsafe/safe_network/commit/1e92fa5a2ae4931f6265d82af121125495f58655))
    - remove unused data exchange structs ([`8eb1877`](https://github.com/maidsafe/safe_network/commit/8eb1877effb8cf0bfc8986c23d49d727500087dd))
    - the JoinResponse::Retry message now provides the expected age for the joining node ([`20895dd`](https://github.com/maidsafe/safe_network/commit/20895dd4326341de4d44547861ac4a57ae8531cf))
    - don't bail out when handling ElderAgreement if prefix-map is not updated ([`20a69cc`](https://github.com/maidsafe/safe_network/commit/20a69ccb6c6b1f466b519318dd85ef11e7700027))
    - stepped fixed age during first section ([`56f3b51`](https://github.com/maidsafe/safe_network/commit/56f3b514fceccbc1cc47256410b4f2119bb8affd))
    - minor refactor to prefix_map reading ([`0b84139`](https://github.com/maidsafe/safe_network/commit/0b8413998f018d7d577f2248e36e21f6c2744116))
    - read prefix_map from disk if available ([`ddad0b8`](https://github.com/maidsafe/safe_network/commit/ddad0b8ce37d3537a9e9ed66da18758b6b3ace68))
    - try to update Section before updating Node info when relocating ([`aaa6903`](https://github.com/maidsafe/safe_network/commit/aaa6903e612178ce59481b2e81fe3bd0d1cc2617))
    - moving Proposal utilities into routing:Core ([`9cc1629`](https://github.com/maidsafe/safe_network/commit/9cc16296db7241819e17dd2673c7b3cb9fe2ead8))
    - simplifying key shares cache ([`c49f9a1`](https://github.com/maidsafe/safe_network/commit/c49f9a16c9e0912bf581a2afef22ac4806898ade))
    - allow to set the genesis key for Core and Section structs of first node ([`0d3fe1a`](https://github.com/maidsafe/safe_network/commit/0d3fe1a6e02f6d83115e9098c706a98cb688d41d))
    - enough having enough elders before split attempt ([`0ea37af`](https://github.com/maidsafe/safe_network/commit/0ea37af54f761b2a1a5137463637736c6be25206))
    - prevent deadlock by dropping write locks in relocation ([`c360dc6`](https://github.com/maidsafe/safe_network/commit/c360dc61bd7d4ce1745ba7bfbe9d032fedac067a))
    - increase node resource proof difficulty ([`2651423`](https://github.com/maidsafe/safe_network/commit/2651423c61b160841557d279c9b706abdaab4cdf))
    - tweak join backoff ([`20461a8`](https://github.com/maidsafe/safe_network/commit/20461a84dbc0fc373b184a3982a79affad0544f6))
    - remove core RwLock ([`6a8f5a1`](https://github.com/maidsafe/safe_network/commit/6a8f5a1f41e5a0f4c0cce6914d4b330b68f5e5d8))
    - build `sn_node` from inside `sn` directory ([`fd4513f`](https://github.com/maidsafe/safe_network/commit/fd4513f054e282218797208dcac1de6903e94f2c))
    - exit with error if `sn_node` build fails ([`7ff7557`](https://github.com/maidsafe/safe_network/commit/7ff7557850460d98b526646b21da635381a70e2a))
    - merge github.com:maidsafe/sn_cli into safe_network ([`414aca2`](https://github.com/maidsafe/safe_network/commit/414aca284b35f1bcb27e5d0cca2bfe451b69e27b))
    - get more splits (at the cost of more join msgs) ([`ba7c4c6`](https://github.com/maidsafe/safe_network/commit/ba7c4c609150839825a4992c08e3bdd00b698269))
    - upgrade `sn_launch_tool` ([`da70738`](https://github.com/maidsafe/safe_network/commit/da70738ff0e24827b749a970f466f3983b70442c))
    - tweak join retry backoff timing ([`7ddf2f8`](https://github.com/maidsafe/safe_network/commit/7ddf2f850441e6596133ab7a596eb766380008c3))
    - upgrade `tracing-appender` and `tracing-subscriber` ([`0387123`](https://github.com/maidsafe/safe_network/commit/0387123114ff6ae42920577706497319c8a888cb))
    - update actions workflows for workspace refactor ([`3703819`](https://github.com/maidsafe/safe_network/commit/3703819c7f0da220c8ff21169ca1e8161a20157b))
    - tweak join retry backoff timing ([`aa17309`](https://github.com/maidsafe/safe_network/commit/aa17309092211e8d1ba36d4aaf50c4677a461594))
    - encapsulate Section info to reduce SAP and chain cloning ([`9cd2d37`](https://github.com/maidsafe/safe_network/commit/9cd2d37dafb95a2765d5c7801a7bb0c58286c47c))
    - move safe_network code into sn directory ([`2254329`](https://github.com/maidsafe/safe_network/commit/225432908839359800d301d9e5aa8274e4652ee1))
    - dont wait before first join request ([`104ed36`](https://github.com/maidsafe/safe_network/commit/104ed366dbaafd98fd3ef67899e27540894a959c))
    - don't backoff when sending join resource challenge responses ([`7b430a5`](https://github.com/maidsafe/safe_network/commit/7b430a54f50846a8475cec804bc24552043558b7))
    - fix elders agreement routing tests using wrong command type ([`0033fc6`](https://github.com/maidsafe/safe_network/commit/0033fc65db266d92623f53628f9f5b6e6069920d))
    - use tokio::semaphore for limiting concurrent joins ([`cfaed1e`](https://github.com/maidsafe/safe_network/commit/cfaed1ece5120d60e9f352b4e9ef70448e2ed3f2))
    - add network elder count test, using testnet grep ([`e24763e`](https://github.com/maidsafe/safe_network/commit/e24763e604e6a25ee1b211af6cf3cdd388ec0978))
    - remove extraneous comment ([`fe62ad9`](https://github.com/maidsafe/safe_network/commit/fe62ad9f590f151dd20a6832dfab81d34fc9c020))
    - cleanup comments ([`9e3cb4e`](https://github.com/maidsafe/safe_network/commit/9e3cb4e7e4dc3d1b8fa23455151de60d5ea03d4d))
    - fix a possible panic when creating client ([`084926e`](https://github.com/maidsafe/safe_network/commit/084926e045952670295eb666c82f1d77ff88f1be))
    - some clippy cleanup ([`37aec09`](https://github.com/maidsafe/safe_network/commit/37aec09a555d428c730cdd982d06cf5cb58b60b1))
    - improve join req handling for elders ([`3a19e42`](https://github.com/maidsafe/safe_network/commit/3a19e42770a12885659d653b98511378dad8015f))
    - remove unused debug ([`810fe07`](https://github.com/maidsafe/safe_network/commit/810fe0797a4f23a6bb2586d901ccac9272a9beb2))
    - add some DKG related markers ([`ca074b9`](https://github.com/maidsafe/safe_network/commit/ca074b92b31add1ad6d0db50f2ba3b3d1ae25d5a))
    - add AE backoff before resending messages to a node ([`6023965`](https://github.com/maidsafe/safe_network/commit/60239655dba08940bd293b3c9243ac732923acfe))
    - moving Section definition our of messaging onto routing ([`b719b74`](https://github.com/maidsafe/safe_network/commit/b719b74abdd1cd84b3813ec7046f4fdf99cde6a2))
    - missing post rebase async_trait dep ([`ec3f16a`](https://github.com/maidsafe/safe_network/commit/ec3f16a8c33e8afbdfffb392466ba422216d3f68))
    - maintain lexicographical order ([`2d807cb`](https://github.com/maidsafe/safe_network/commit/2d807cb4b75bfd52d9dde7b91214bd3dc5a6d992))
    - backoff during join request looping ([`4cafe78`](https://github.com/maidsafe/safe_network/commit/4cafe78e3fdb60d144e8cf810788116ce01de025))
    - write to correct source ([`c765828`](https://github.com/maidsafe/safe_network/commit/c765828018158bda663f937a73bcc47ab358884c))
    - make subcommand optional, to get stats quickly ([`d1ecf96`](https://github.com/maidsafe/safe_network/commit/d1ecf96a6d965928d434810ccc9c89d1bc7fac4e))
    - move AE updat resend after retry to only happen for validated saps ([`993d564`](https://github.com/maidsafe/safe_network/commit/993d564f0ad5d8cac8d9a32b8c6c8d1dd00c3fd9))
    - only resend if we've had an update ([`73c5baf`](https://github.com/maidsafe/safe_network/commit/73c5baf94dd37346c6c9987aa51ca26f3a2fea1f))
    - add LogMarker for all different send msg types ([`fd8e8f3`](https://github.com/maidsafe/safe_network/commit/fd8e8f3fb22f8592a45db33e75f237ef22cd1f5b))
    - make elder agreemeent its own blocking command ([`c1c2bca`](https://github.com/maidsafe/safe_network/commit/c1c2bcac021dda4bf18a4d80ad2f86370d56efa7))
    - flesh out far too many 'let _ = ' ([`e2727b9`](https://github.com/maidsafe/safe_network/commit/e2727b91c836619dadf2e464ee7c57b338427f22))
    - enable pleasant span viewing for node logs ([`958e38e`](https://github.com/maidsafe/safe_network/commit/958e38ecd3b4e4dc908913192a1d43b83e082d08))
    - client config test ([`6aa1130`](https://github.com/maidsafe/safe_network/commit/6aa1130a7b380dc3d1ad12f3054cda3a390ff20d))
    - make section peers concurrent by using Arc<DashMap> ([`148140f`](https://github.com/maidsafe/safe_network/commit/148140f1d932e6c4b30122ebcca3450ab6c84544))
    - Merge remote-tracking branch 'upstream/main' into merge_sn_api_into_workspace ([`8ed5aff`](https://github.com/maidsafe/safe_network/commit/8ed5aff8b30ce798f71eac22d66eb3aa9b0bdcdd))
    - Merge remote-tracking branch 'upstream/main' into merge_sn_api_into_workspace ([`3a2817a`](https://github.com/maidsafe/safe_network/commit/3a2817a4c802d74b57d475d88d7bc23223994147))
    - Merge remote-tracking branch 'upstream/main' into merge_sn_api_into_workspace ([`50f48ae`](https://github.com/maidsafe/safe_network/commit/50f48aefcba272345df7d4cd45a59071a5844932))
    - Merge branch 'merge_sn_api_into_workspace' into nrs_resolver_refactor ([`a273d97`](https://github.com/maidsafe/safe_network/commit/a273d9733b8d50b94b0ea3faec1d9e721d86aa27))
    - resolution test except for the blob range tests ([`c88a1bc`](https://github.com/maidsafe/safe_network/commit/c88a1bc5fda40093bb129b4351eef73d2eb7c041))
    - update actions workflows for workspace refactor ([`d0134e8`](https://github.com/maidsafe/safe_network/commit/d0134e870bb097e095e1c8a33e607cf7994e6491))
</details>

