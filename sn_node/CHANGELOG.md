# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.73.4 (2023-02-03)

### New Features

 - <csr-id-41fc522226945051a14b455ca45f637175b9143f/> only submit traces if environment var is set
   We will only configure the tracing infrastructure if the `otlp` feature is enabled *and* the
   `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable is set.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Merge #2064 ([`eb993b7`](https://github.com/maidsafe/safe_network/commit/eb993b7f5d131f4a34d94920b1e7b0733eecc139))
    - only submit traces if environment var is set ([`41fc522`](https://github.com/maidsafe/safe_network/commit/41fc522226945051a14b455ca45f637175b9143f))
</details>

## v0.73.3 (2023-02-02)

<csr-id-e706848522d6c52d6ed5eddf638376996cc947a9/>
<csr-id-1540f6e00a5cd6a3803054d3e8386927a0962a1f/>

### Chore

 - <csr-id-e706848522d6c52d6ed5eddf638376996cc947a9/> add clippy check for unused async

### Chore

 - <csr-id-3831dae3e34623ef252298645a43cbafcc923a13/> sn_interface-0.17.1/sn_fault_detection-0.15.3/sn_comms-0.2.1/sn_client-0.78.2/sn_node-0.73.3/sn_api-0.76.1

### Refactor

 - <csr-id-1540f6e00a5cd6a3803054d3e8386927a0962a1f/> unused async removal

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 1 day passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.17.1/sn_fault_detection-0.15.3/sn_comms-0.2.1/sn_client-0.78.2/sn_node-0.73.3/sn_api-0.76.1 ([`3831dae`](https://github.com/maidsafe/safe_network/commit/3831dae3e34623ef252298645a43cbafcc923a13))
    - Merge #2061 ([`bab8208`](https://github.com/maidsafe/safe_network/commit/bab82087260ac4f1f44e688db2e67ca2387a7175))
    - add clippy check for unused async ([`e706848`](https://github.com/maidsafe/safe_network/commit/e706848522d6c52d6ed5eddf638376996cc947a9))
    - unused async removal ([`1540f6e`](https://github.com/maidsafe/safe_network/commit/1540f6e00a5cd6a3803054d3e8386927a0962a1f))
</details>

## v0.73.2 (2023-02-01)

<csr-id-2f1c6edded1f40761b55add3af53dc1849ac36a5/>

### Chore

 - <csr-id-2f1c6edded1f40761b55add3af53dc1849ac36a5/> sn_node-0.73.2

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.73.2 ([`2f1c6ed`](https://github.com/maidsafe/safe_network/commit/2f1c6edded1f40761b55add3af53dc1849ac36a5))
    - Merge #2052 ([`6ba5453`](https://github.com/maidsafe/safe_network/commit/6ba5453b572431d7dcbd15948de712458bc736dc))
    - refactor: moves serialize call to where needed - We were serializing before hitting the branch where it was needed. - Also aligns fn name withy doc. The cases when no cmd is generated are the unreachable and invalid msg cases. ([`98dc5e4`](https://github.com/maidsafe/safe_network/commit/98dc5e4efd110fb5d08ad2c58926dad8767619d8))
</details>

## v0.73.1 (2023-02-01)

<csr-id-817e20910874ad287ebb1b9bf5071ed452419414/>

### Chore

 - <csr-id-817e20910874ad287ebb1b9bf5071ed452419414/> sn_node-0.73.1

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.73.1 ([`817e209`](https://github.com/maidsafe/safe_network/commit/817e20910874ad287ebb1b9bf5071ed452419414))
    - Merge #2053 ([`3c7b043`](https://github.com/maidsafe/safe_network/commit/3c7b0434e784e5b7484d55d9d41b19898b941c1b))
</details>

## v0.73.0 (2023-02-01)

<csr-id-69f8ade1ea8bb3e77c169b17ae21a40370bfab58/>
<csr-id-839c540cf4c076de6d44a47daa4d139531fc6615/>
<csr-id-cee5c65a1a099606d5430452995d26edfd1f6bfc/>
<csr-id-f779144986a6b2b06f550d3a2a4cbc39c64af83d/>
<csr-id-47e0f87d5ccad33cfa82ef80f3648fe8270acaaa/>
<csr-id-9ef9a2f2c8711895b62b82d25cb9d208c464cad6/>

### Chore

 - <csr-id-69f8ade1ea8bb3e77c169b17ae21a40370bfab58/> sn_interface-0.17.0/sn_comms-0.2.0/sn_client-0.78.0/sn_node-0.73.0/sn_api-0.76.0/sn_cli-0.69.0
 - <csr-id-839c540cf4c076de6d44a47daa4d139531fc6615/> clean up comments [skip ci]

### Bug Fixes

 - <csr-id-8ca6e995817c6b8127e4874a6026bd1572b3991b/> config size

### Refactor

 - <csr-id-cee5c65a1a099606d5430452995d26edfd1f6bfc/> leave out reachability check for join
   This reachability check tries to reply/reach out to the node that could
   not connect, but we only save incoming connections, so this will not
   make it out to the node.
   
   Also, the reachability check was left out of the last release of qp2p,
   as the network should not only rely on these reachability checks, but
   the nodes should be unable to join the network anyway if they're not
   reachable in the first place.
 - <csr-id-f779144986a6b2b06f550d3a2a4cbc39c64af83d/> idle_timeout from 10s to 70s
   This was the timeout before this pull request. 70s was deduced to be a
   value that gave CI the time to pass many tests. Although this value is
   not optimal in a real world scenario, as routers might close the holes
   where the connections rely on, and thus the connections will actually
   timeout sooner in some cases. An optimal value was previously decided to
   be 18s, as some routers supposedly close holes after 20s.
 - <csr-id-47e0f87d5ccad33cfa82ef80f3648fe8270acaaa/> remove passing parameters to qp2p
   Rely on defaults in qp2p instead, which are sensible. This means the
   idle timeout from now on will be 10s which is currently the default in
   quinn too.

### Refactor (BREAKING)

 - <csr-id-9ef9a2f2c8711895b62b82d25cb9d208c464cad6/> implement new qp2p version
   This introduces quite some changes to the API that hopefully simplifies
   much of the internals of the node and client.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 8 commits contributed to the release.
 - 7 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.17.0/sn_comms-0.2.0/sn_client-0.78.0/sn_node-0.73.0/sn_api-0.76.0/sn_cli-0.69.0 ([`69f8ade`](https://github.com/maidsafe/safe_network/commit/69f8ade1ea8bb3e77c169b17ae21a40370bfab58))
    - Merge #1996 ([`bb7b2db`](https://github.com/maidsafe/safe_network/commit/bb7b2dbcae9c0a67fc0a23c279537df49d88a07a))
    - clean up comments [skip ci] ([`839c540`](https://github.com/maidsafe/safe_network/commit/839c540cf4c076de6d44a47daa4d139531fc6615))
    - leave out reachability check for join ([`cee5c65`](https://github.com/maidsafe/safe_network/commit/cee5c65a1a099606d5430452995d26edfd1f6bfc))
    - idle_timeout from 10s to 70s ([`f779144`](https://github.com/maidsafe/safe_network/commit/f779144986a6b2b06f550d3a2a4cbc39c64af83d))
    - config size ([`8ca6e99`](https://github.com/maidsafe/safe_network/commit/8ca6e995817c6b8127e4874a6026bd1572b3991b))
    - remove passing parameters to qp2p ([`47e0f87`](https://github.com/maidsafe/safe_network/commit/47e0f87d5ccad33cfa82ef80f3648fe8270acaaa))
    - implement new qp2p version ([`9ef9a2f`](https://github.com/maidsafe/safe_network/commit/9ef9a2f2c8711895b62b82d25cb9d208c464cad6))
</details>

## v0.72.42 (2023-02-01)

<csr-id-50c69469b488224c7f69de4e728a66594a31380e/>
<csr-id-9d17c6c99c48046a361fffe30749419a594715f5/>
<csr-id-58affbd6b7c6cf6450492796867e47a75456d0c0/>

### Chore

 - <csr-id-50c69469b488224c7f69de4e728a66594a31380e/> log more info when membership vote failed byzatine detect

### Chore

 - <csr-id-9d17c6c99c48046a361fffe30749419a594715f5/> sn_node-0.72.42
 - <csr-id-58affbd6b7c6cf6450492796867e47a75456d0c0/> set default values for env variable

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release over the course of 1 calendar day.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.42 ([`9d17c6c`](https://github.com/maidsafe/safe_network/commit/9d17c6c99c48046a361fffe30749419a594715f5))
    - Merge #2049 ([`a9389c1`](https://github.com/maidsafe/safe_network/commit/a9389c11db261b8bb9a082ef93cef9d766f0f516))
    - set default values for env variable ([`58affbd`](https://github.com/maidsafe/safe_network/commit/58affbd6b7c6cf6450492796867e47a75456d0c0))
    - log more info when membership vote failed byzatine detect ([`50c6946`](https://github.com/maidsafe/safe_network/commit/50c69469b488224c7f69de4e728a66594a31380e))
</details>

## v0.72.41 (2023-01-31)

<csr-id-fc34870f7d59344da39e38834e87e55ab6860376/>

### Chore

 - <csr-id-fc34870f7d59344da39e38834e87e55ab6860376/> sn_interface-0.16.20/sn_fault_detection-0.15.2/sn_node-0.72.41

### New Features

 - <csr-id-6153fcf52e02551443642d60a13e48de28e2ed3d/> perform ae before we deserialise msgs
   This removes unnecessary work

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 1 day passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.20/sn_fault_detection-0.15.2/sn_node-0.72.41 ([`fc34870`](https://github.com/maidsafe/safe_network/commit/fc34870f7d59344da39e38834e87e55ab6860376))
    - Merge #2039 ([`5b27450`](https://github.com/maidsafe/safe_network/commit/5b2745051d91eb3a4b6b8e575258b5c93ab81b04))
    - perform ae before we deserialise msgs ([`6153fcf`](https://github.com/maidsafe/safe_network/commit/6153fcf52e02551443642d60a13e48de28e2ed3d))
</details>

## v0.72.40 (2023-01-30)

<csr-id-89b6344e590a22894f88fc643ff8aa2c7aab2464/>
<csr-id-3377f4b4142e324bb769d4666f3ac127354bb107/>

### Chore

 - <csr-id-89b6344e590a22894f88fc643ff8aa2c7aab2464/> remove extra dkg trigger

### Chore

 - <csr-id-3377f4b4142e324bb769d4666f3ac127354bb107/> sn_node-0.72.40

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.40 ([`3377f4b`](https://github.com/maidsafe/safe_network/commit/3377f4b4142e324bb769d4666f3ac127354bb107))
    - Merge #2044 ([`716167c`](https://github.com/maidsafe/safe_network/commit/716167cf801b7036a6d91653b4254c20a9bfe1d2))
    - remove extra dkg trigger ([`89b6344`](https://github.com/maidsafe/safe_network/commit/89b6344e590a22894f88fc643ff8aa2c7aab2464))
</details>

## v0.72.39 (2023-01-29)

<csr-id-4ea2b420b5c6390bd894505e3c71cb5e673244b8/>

### Chore

 - <csr-id-4ea2b420b5c6390bd894505e3c71cb5e673244b8/> sn_interface-0.16.19/sn_node-0.72.39

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.19/sn_node-0.72.39 ([`4ea2b42`](https://github.com/maidsafe/safe_network/commit/4ea2b420b5c6390bd894505e3c71cb5e673244b8))
    - Merge #2041 ([`09fba13`](https://github.com/maidsafe/safe_network/commit/09fba13281fa7dadf810975c257e33fcc1f127f6))
    - fix(storage): make data exists not be an error - The data storage is idempotent, and it's not an error that data already exists. ([`ce6718c`](https://github.com/maidsafe/safe_network/commit/ce6718ca0bcfb9e1a6eaf2729559b9b8ef148d7c))
</details>

## v0.72.38 (2023-01-27)

<csr-id-4fa50e710c65dc4298f85f6eb01a3575155417d6/>
<csr-id-acfc8c88d5fbc15f46d76535c058c60b6d20433a/>

### Refactor

 - <csr-id-4fa50e710c65dc4298f85f6eb01a3575155417d6/> removing unnecessary SendStatus and SendWatcher

### Chore

 - <csr-id-acfc8c88d5fbc15f46d76535c058c60b6d20433a/> sn_comms-0.1.7/sn_node-0.72.38

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_comms-0.1.7/sn_node-0.72.38 ([`acfc8c8`](https://github.com/maidsafe/safe_network/commit/acfc8c88d5fbc15f46d76535c058c60b6d20433a))
    - Merge #2038 ([`8f1c443`](https://github.com/maidsafe/safe_network/commit/8f1c443a29f794da6a0412eab87672281c3a4d4b))
    - removing unnecessary SendStatus and SendWatcher ([`4fa50e7`](https://github.com/maidsafe/safe_network/commit/4fa50e710c65dc4298f85f6eb01a3575155417d6))
</details>

## v0.72.37 (2023-01-27)

<csr-id-c28ce487b26144187687083eac6edb1ddf030266/>
<csr-id-e990f883bec55e5e3c73a3b074428c42d2538785/>

### Chore

 - <csr-id-c28ce487b26144187687083eac6edb1ddf030266/> update cargo deps

### Chore

 - <csr-id-e990f883bec55e5e3c73a3b074428c42d2538785/> sn_comms-0.1.6/sn_node-0.72.37

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_comms-0.1.6/sn_node-0.72.37 ([`e990f88`](https://github.com/maidsafe/safe_network/commit/e990f883bec55e5e3c73a3b074428c42d2538785))
    - Merge #2013 #2029 ([`3456929`](https://github.com/maidsafe/safe_network/commit/3456929564e00303315da6b458d5fc4f97422301))
    - update cargo deps ([`c28ce48`](https://github.com/maidsafe/safe_network/commit/c28ce487b26144187687083eac6edb1ddf030266))
</details>

## v0.72.36 (2023-01-27)

<csr-id-12f9f764dc821d78b39073fe007a3a6ac32d70cb/>

### Chore

 - <csr-id-12f9f764dc821d78b39073fe007a3a6ac32d70cb/> sn_comms-0.1.5/sn_node-0.72.36

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_comms-0.1.5/sn_node-0.72.36 ([`12f9f76`](https://github.com/maidsafe/safe_network/commit/12f9f764dc821d78b39073fe007a3a6ac32d70cb))
    - Merge #2031 ([`96d8799`](https://github.com/maidsafe/safe_network/commit/96d8799cf10510c9d3514fdd9f6fdfc628568da3))
</details>

## v0.72.35 (2023-01-27)

<csr-id-03d9c561c7259351310ede6e4cfb6e78822d728a/>
<csr-id-81fe669f11343259bd167b75a8bfb004b4b83090/>

### Chore

 - <csr-id-03d9c561c7259351310ede6e4cfb6e78822d728a/> add mean to std dev to get threshold

### Chore

 - <csr-id-81fe669f11343259bd167b75a8bfb004b4b83090/> sn_fault_detection-0.15.1/sn_node-0.72.35

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_fault_detection-0.15.1/sn_node-0.72.35 ([`81fe669`](https://github.com/maidsafe/safe_network/commit/81fe669f11343259bd167b75a8bfb004b4b83090))
    - add mean to std dev to get threshold ([`03d9c56`](https://github.com/maidsafe/safe_network/commit/03d9c561c7259351310ede6e4cfb6e78822d728a))
</details>

## v0.72.34 (2023-01-27)

<csr-id-6b923515f0f0cd1b0d1a4ed51d3ca418e41d9916/>
<csr-id-01ff2ccf45dfc9d45c5ad540144d7a4a640830fc/>

### Chore

 - <csr-id-6b923515f0f0cd1b0d1a4ed51d3ca418e41d9916/> fix issues reported by new clippy

### Chore

 - <csr-id-01ff2ccf45dfc9d45c5ad540144d7a4a640830fc/> sn_interface-0.16.18/sn_comms-0.1.4/sn_client-0.77.9/sn_node-0.72.34/sn_api-0.75.5/sn_cli-0.68.6

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.18/sn_comms-0.1.4/sn_client-0.77.9/sn_node-0.72.34/sn_api-0.75.5/sn_cli-0.68.6 ([`01ff2cc`](https://github.com/maidsafe/safe_network/commit/01ff2ccf45dfc9d45c5ad540144d7a4a640830fc))
    - Merge branch 'main' into chore-comms-remove-unused-async ([`e92dd49`](https://github.com/maidsafe/safe_network/commit/e92dd49f38f9b56c7276e86ba79f7fd8f816af76))
    - Merge #2033 #2034 ([`06581d1`](https://github.com/maidsafe/safe_network/commit/06581d1ebbb23f432610d48bb3b773c742c0baaa))
    - Merge branch 'main' into RevertDkgCache ([`24ff625`](https://github.com/maidsafe/safe_network/commit/24ff6257f85922090cfaa5fa83044082d3ef8dab))
    - fix issues reported by new clippy ([`6b92351`](https://github.com/maidsafe/safe_network/commit/6b923515f0f0cd1b0d1a4ed51d3ca418e41d9916))
</details>

## v0.72.33 (2023-01-26)

<csr-id-d51dd695437dac1695447491d4f298334b7e0fd1/>
<csr-id-2e1061a08e591767eedc837369ada0843ce62701/>

### Chore

 - <csr-id-d51dd695437dac1695447491d4f298334b7e0fd1/> sn_comms-0.1.3/sn_node-0.72.33
 - <csr-id-2e1061a08e591767eedc837369ada0843ce62701/> remove a dkg vote clone only for logging
   Writes the debug to a var and uses that instead.
   Dkg vote clones are a low hanging mem fruit

### Bug Fixes

 - <csr-id-42f2c3709af96207b10b711878d03d42781bfdba/> send_out_bytes was not reporting send failures
   - sn_comms::Comm::send_out_bytes was spawning a task when sending a msg,
   now it's the caller's duty to do so if ever required.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_comms-0.1.3/sn_node-0.72.33 ([`d51dd69`](https://github.com/maidsafe/safe_network/commit/d51dd695437dac1695447491d4f298334b7e0fd1))
    - remove a dkg vote clone only for logging ([`2e1061a`](https://github.com/maidsafe/safe_network/commit/2e1061a08e591767eedc837369ada0843ce62701))
    - Revert "feat: reduce the amount of old DKG sessions we keep" ([`98d0a11`](https://github.com/maidsafe/safe_network/commit/98d0a11fca1db889c33b04a72c78d0a5b952e65b))
    - Merge #2025 ([`4baaae3`](https://github.com/maidsafe/safe_network/commit/4baaae3022d0295715e58f6f74bac3c6b2547be1))
    - send_out_bytes was not reporting send failures ([`42f2c37`](https://github.com/maidsafe/safe_network/commit/42f2c3709af96207b10b711878d03d42781bfdba))
</details>

## v0.72.32 (2023-01-26)

<csr-id-f31f3fcc09c503eeb8a580f73b126030da8e11a4/>
<csr-id-6ccbaa335378fd02a93447b67b9dec61c17ea1d0/>

### Chore

 - <csr-id-f31f3fcc09c503eeb8a580f73b126030da8e11a4/> further reduce membership clones

### Chore

 - <csr-id-6ccbaa335378fd02a93447b67b9dec61c17ea1d0/> sn_node-0.72.32

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.32 ([`6ccbaa3`](https://github.com/maidsafe/safe_network/commit/6ccbaa335378fd02a93447b67b9dec61c17ea1d0))
    - Merge #2032 ([`cb2ffd1`](https://github.com/maidsafe/safe_network/commit/cb2ffd1853e24d713fdec54cefde84028628d9b0))
    - chore(comm): remove unused async - Also shortens fn name and clarifies docs a bit. ([`3eced25`](https://github.com/maidsafe/safe_network/commit/3eced25805febe313d3d612756931fd52b0d67b0))
    - further reduce membership clones ([`f31f3fc`](https://github.com/maidsafe/safe_network/commit/f31f3fcc09c503eeb8a580f73b126030da8e11a4))
</details>

## v0.72.31 (2023-01-26)

<csr-id-d0d6d8fcf20da2d0a8fc6b63cd7cc78f17258baf/>

### Chore

 - <csr-id-d0d6d8fcf20da2d0a8fc6b63cd7cc78f17258baf/> sn_node-0.72.31

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.31 ([`d0d6d8f`](https://github.com/maidsafe/safe_network/commit/d0d6d8fcf20da2d0a8fc6b63cd7cc78f17258baf))
    - Merge #2004 ([`5883fb6`](https://github.com/maidsafe/safe_network/commit/5883fb65d1e9491cd0271457637968facd2a40d9))
</details>

## v0.72.30 (2023-01-25)

<csr-id-a4d295ccdddea3d4d11bca5eb0236a5447c75633/>
<csr-id-a24c1d2301cfc94b2c6456e0e8d3f9f86cec0cbf/>
<csr-id-6ba7b5a12ed8d15fb807524ee90dc250068c1004/>

### Chore

 - <csr-id-a4d295ccdddea3d4d11bca5eb0236a5447c75633/> sn_interface-0.16.17/sn_comms-0.1.2/sn_node-0.72.30
 - <csr-id-a24c1d2301cfc94b2c6456e0e8d3f9f86cec0cbf/> prevent unnecessary clones in periodics
   We check if anything has expired before we grab context

### New Features

 - <csr-id-1b23b6d4c233ed5e337a85f864fcc403c4f5e5b4/> set custom otlp service name

### Refactor

 - <csr-id-6ba7b5a12ed8d15fb807524ee90dc250068c1004/> removing Comm::members and unnecessary private types
   - We now use Comm::sessions as the list of members we keep sessions with,
   which is updated only when the user wants to update the set of known peers.
   - Removing unnecessary PeerSession's SessionStatus, disconnect fn, and SessionCmd.
   - Never remove sessions from Comm::sessions unless the set of known members is
   updated/changed by the user. Even if we failed to send using all peer session's connections,
   we keep the session since it's been set as a known and connectable peer.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 8 commits contributed to the release.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.17/sn_comms-0.1.2/sn_node-0.72.30 ([`a4d295c`](https://github.com/maidsafe/safe_network/commit/a4d295ccdddea3d4d11bca5eb0236a5447c75633))
    - Merge #2022 ([`3a99b2b`](https://github.com/maidsafe/safe_network/commit/3a99b2b616cfd3a90d271868e502d795790b2af0))
    - removing Comm::members and unnecessary private types ([`6ba7b5a`](https://github.com/maidsafe/safe_network/commit/6ba7b5a12ed8d15fb807524ee90dc250068c1004))
    - Merge #2027 ([`36fa5dd`](https://github.com/maidsafe/safe_network/commit/36fa5ddac4964e8b8f1ab85f90e2bdbffda7c132))
    - prevent unnecessary clones in periodics ([`a24c1d2`](https://github.com/maidsafe/safe_network/commit/a24c1d2301cfc94b2c6456e0e8d3f9f86cec0cbf))
    - Merge #2026 ([`abb98b7`](https://github.com/maidsafe/safe_network/commit/abb98b7b2dfecc2f76e027d3c7aae9ec22525bb9))
    - set custom otlp service name ([`1b23b6d`](https://github.com/maidsafe/safe_network/commit/1b23b6d4c233ed5e337a85f864fcc403c4f5e5b4))
    - Merge #2016 #2019 #2023 ([`c8e5746`](https://github.com/maidsafe/safe_network/commit/c8e574687ea74ed1adb69a722afe6bff734c19ad))
</details>

## v0.72.29 (2023-01-25)

<csr-id-8fae01400e9c0c4808860d1596d47c704f4656ed/>

### Chore

 - <csr-id-8fae01400e9c0c4808860d1596d47c704f4656ed/> sn_node-0.72.29

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.29 ([`8fae014`](https://github.com/maidsafe/safe_network/commit/8fae01400e9c0c4808860d1596d47c704f4656ed))
    - Merge #2017 ([`e477211`](https://github.com/maidsafe/safe_network/commit/e477211cf09db52b8eacbc3266bb43321525b0f1))
</details>

## v0.72.28 (2023-01-24)

<csr-id-6acbe4920d9d3a7db88e76a21e026bdee04e9584/>
<csr-id-2051fee1584ee2e4a8b7693ea96f18031c3a2a81/>

### Chore

 - <csr-id-6acbe4920d9d3a7db88e76a21e026bdee04e9584/> sn_interface-0.16.16/sn_node-0.72.28
 - <csr-id-2051fee1584ee2e4a8b7693ea96f18031c3a2a81/> remove unnecessary indirection

### New Features

 - <csr-id-908ee34d116e2a9e5250d3044f9dbe1c6d471ecc/> add retry for relocating node
 - <csr-id-5257295c18fd98d383bd70bbbe1fd3de1d0f9ea7/> reduce the amount of old DKG sessions we keep

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.16/sn_node-0.72.28 ([`6acbe49`](https://github.com/maidsafe/safe_network/commit/6acbe4920d9d3a7db88e76a21e026bdee04e9584))
    - Merge #2024 ([`fc0aa80`](https://github.com/maidsafe/safe_network/commit/fc0aa8062c0003a9ac3c263d4ea01111b5e6a8d3))
    - add retry for relocating node ([`908ee34`](https://github.com/maidsafe/safe_network/commit/908ee34d116e2a9e5250d3044f9dbe1c6d471ecc))
    - reduce the amount of old DKG sessions we keep ([`5257295`](https://github.com/maidsafe/safe_network/commit/5257295c18fd98d383bd70bbbe1fd3de1d0f9ea7))
    - remove unnecessary indirection ([`2051fee`](https://github.com/maidsafe/safe_network/commit/2051fee1584ee2e4a8b7693ea96f18031c3a2a81))
</details>

## v0.72.27 (2023-01-23)

<csr-id-2ce413fab1e5fff10593d7f5fcf7c9c41db1f9ff/>
<csr-id-e6ec500629844ad2d328d38fff7ebd0f52a8cb12/>
<csr-id-c94a953dddfcb20bf65d4bb34448dc2752a019c5/>

### Chore

 - <csr-id-2ce413fab1e5fff10593d7f5fcf7c9c41db1f9ff/> update readme

### Chore

 - <csr-id-c94a953dddfcb20bf65d4bb34448dc2752a019c5/> sn_interface-0.16.15/sn_node-0.72.27

### Refactor

 - <csr-id-e6ec500629844ad2d328d38fff7ebd0f52a8cb12/> use existing join flow

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.15/sn_node-0.72.27 ([`c94a953`](https://github.com/maidsafe/safe_network/commit/c94a953dddfcb20bf65d4bb34448dc2752a019c5))
    - Merge #2018 ([`1ee4f75`](https://github.com/maidsafe/safe_network/commit/1ee4f75af4dddb7b2bd18bb60317d3e977e356f7))
    - update readme ([`2ce413f`](https://github.com/maidsafe/safe_network/commit/2ce413fab1e5fff10593d7f5fcf7c9c41db1f9ff))
    - use existing join flow ([`e6ec500`](https://github.com/maidsafe/safe_network/commit/e6ec500629844ad2d328d38fff7ebd0f52a8cb12))
</details>

## v0.72.26 (2023-01-23)

<csr-id-36d818109e2d613221de3dc9f6ed061d04588d5b/>
<csr-id-0ab0c302dcc6ce32b0b71d696b0707a2c50cfa3a/>
<csr-id-203a8aace09111748e8b9913fa683e0c5ea6e69a/>
<csr-id-12a6620525a5767d906037a74caf0e38af3da596/>
<csr-id-40d91af58413368c79fb3d794cb1776bea44c4c4/>

### Chore

 - <csr-id-36d818109e2d613221de3dc9f6ed061d04588d5b/> only clone membership when needed
   this should vastily reduce allocations coming from membership.clone()
   
   (which accounts for the vast majority of alloc in a runnning node just now)

### Bug Fixes

 - <csr-id-0f1ac79146aac0d0cea11644cca75b68012fa23d/> as elder request missing data on any membership update

### Chore

 - <csr-id-203a8aace09111748e8b9913fa683e0c5ea6e69a/> small renaming tweaks for clarity
 - <csr-id-12a6620525a5767d906037a74caf0e38af3da596/> rename update_members to update_valid_comm_targets for clarity
 - <csr-id-40d91af58413368c79fb3d794cb1776bea44c4c4/> only clone membership when needed
   this should vastly reduce allocations coming from
   membership.clone()
   (which accounts for the vast majority of alloc in
   a runnning node just now)

### Chore

 - <csr-id-0ab0c302dcc6ce32b0b71d696b0707a2c50cfa3a/> sn_comms-0.1.1/sn_node-0.72.26

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release over the course of 1 calendar day.
 - 3 days passed between releases.
 - 6 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_comms-0.1.1/sn_node-0.72.26 ([`0ab0c30`](https://github.com/maidsafe/safe_network/commit/0ab0c302dcc6ce32b0b71d696b0707a2c50cfa3a))
    - Merge #2009 ([`83448f4`](https://github.com/maidsafe/safe_network/commit/83448f43dace53b3357796bf177edb98c3d5803d))
    - small renaming tweaks for clarity ([`203a8aa`](https://github.com/maidsafe/safe_network/commit/203a8aace09111748e8b9913fa683e0c5ea6e69a))
    - rename update_members to update_valid_comm_targets for clarity ([`12a6620`](https://github.com/maidsafe/safe_network/commit/12a6620525a5767d906037a74caf0e38af3da596))
    - as elder request missing data on any membership update ([`0f1ac79`](https://github.com/maidsafe/safe_network/commit/0f1ac79146aac0d0cea11644cca75b68012fa23d))
    - only clone membership when needed ([`36d8181`](https://github.com/maidsafe/safe_network/commit/36d818109e2d613221de3dc9f6ed061d04588d5b))
    - only clone membership when needed ([`40d91af`](https://github.com/maidsafe/safe_network/commit/40d91af58413368c79fb3d794cb1776bea44c4c4))
</details>

## v0.72.25 (2023-01-20)

<csr-id-cd9bf5fd5ccac42cd9de028cdaff8e0302498ed0/>
<csr-id-99a4c43e0131bfc2bf36ff07bef2e476b4e801b9/>
<csr-id-ad8cb2cfd5387a76b84682e018c02889579935c8/>
<csr-id-8d2ef1a0f298ef010f478fcd59c5b6c437b7b62f/>
<csr-id-87cb70eefdc63f80942a2c87ecc3790f76105b91/>
<csr-id-dc16323849e425e2ca2511f095caee5b0a4af1ab/>
<csr-id-38d85b391a72a3ee71f705d9b89d6dbc74c041e1/>
<csr-id-c48a6531cc5246319cf6453ddef4641fbf98ead5/>
<csr-id-fa65879d11d3e3dc3cd6127a2ac777b32f90afee/>
<csr-id-dbfa4ac0dd23e76060b8df44c4666a30bb9b317f/>
<csr-id-27fe2beaa76146b3bd235405f6b49c495214a336/>
<csr-id-a6c7e2f5860c752dd4ddda0768ec916eb6c25add/>
<csr-id-7674b9f5609384a35072043a777ac07b18c12bb3/>
<csr-id-b13f1d7e5e84e42ed076654a159418563a9a1a35/>
<csr-id-3b9304bab92f0715a134ce68afbfd08a7ad31e61/>
<csr-id-c0a8eaa0d564d04f856aef9d9e7b5c81a340e512/>
<csr-id-3b85f2087de40cea8d3e5406837c031a2af96203/>
<csr-id-fadaa1cd4ecb4ac920c80d7e014f840c5594077f/>
<csr-id-a578f25afbdd9b32935522df557bf175a792a6a5/>
<csr-id-a15c20e564600e77de28bad334fdaa4e6aa7fd92/>
<csr-id-21af053a5be2317be356e760c2b581c0f870a396/>
<csr-id-00e417037d7be7639e9ccc245d1808396b779ead/>
<csr-id-027b164f851209f0662e0a84ee839618d95af58d/>
<csr-id-5ae5a160f8cb29df62b8f0253e53e527129b4689/>
<csr-id-48a438c5973290c50d80128200812ee93999aabe/>
<csr-id-a33d28e4bc3f11779eaf0bf6cafd67800dbc4e0d/>
<csr-id-ea3caf4ff929590137f3c800f29dcdf39cbddc20/>
<csr-id-6c0f451646ea5840c79f112868637facdd08293c/>
<csr-id-a14deab2a4a2b127f8f2b32e0aa2c0ab98ead4d2/>
<csr-id-5c490793e6c434c3c68f276483a33bd330385524/>
<csr-id-7ead1c15859c0ca5553ab1df310607da8d526f90/>
<csr-id-542b627d4de792bd69f64f57b0da21c8775ce055/>
<csr-id-33a577f5e52029854757b9093e7b30535a7acabd/>
<csr-id-04525595bc5de39f85a128cfb691644b71a3fb79/>
<csr-id-35f3b27291905cd4c845e6005a07d23f76bc5449/>
<csr-id-f0b25be00ba4d97539bde19fb524edebc4d1aac5/>
<csr-id-ce13490ef261cde3e6888a0da4b84b9d0f2be3bc/>
<csr-id-96169a7fbab950b40dc87ff433a9e348709eae1a/>
<csr-id-9aaf91bfedd9fcf040f18e0762ff2cbbc03b4d6f/>
<csr-id-61834d8abf8e946768e2c958ca2159979497f782/>
<csr-id-b2edff4b5b63b5d8a7905428b2c78b1d26598f07/>
<csr-id-9f5b5ffb5975810e22c634f171984fcc803062aa/>
<csr-id-29562f26b3d9a10f72651c58df022db6b827b002/>
<csr-id-82ac545c7c3bbf1941fe9d9a80dcc2f99ff58a2f/>
<csr-id-b316262443ba5d38c4879ae9d2f583aba92e501a/>
<csr-id-efd7b9b9c775eb15b8beddee8286a1f788c2b940/>
<csr-id-2b6c4a8fcd2cd7b22e6a4b20f1218c859110be62/>
<csr-id-783d62461a65eb7c06b0d4f399b97216b6c75519/>

### Chore

 - <csr-id-cd9bf5fd5ccac42cd9de028cdaff8e0302498ed0/> remove readlock where we have context
 - <csr-id-99a4c43e0131bfc2bf36ff07bef2e476b4e801b9/> make joins bidi: set up send and await response Cmd
 - <csr-id-ad8cb2cfd5387a76b84682e018c02889579935c8/> rename SendMsgAwaitReponse to SendMsgAwaitResponseAndRespondToClient
 - <csr-id-8d2ef1a0f298ef010f478fcd59c5b6c437b7b62f/> clarify comments and ensure sorted output of faulty nodes
   other misc cleanup
 - <csr-id-87cb70eefdc63f80942a2c87ecc3790f76105b91/> clarify naming of elders/non elders
 - <csr-id-dc16323849e425e2ca2511f095caee5b0a4af1ab/> store elders in fault detection
 - <csr-id-38d85b391a72a3ee71f705d9b89d6dbc74c041e1/> rename Knowledge -> NetworkKnowledge for clarity
 - <csr-id-c48a6531cc5246319cf6453ddef4641fbf98ead5/> rename log_node_issue -> track_node_isse
 - <csr-id-fa65879d11d3e3dc3cd6127a2ac777b32f90afee/> do the mapping and remove unused
 - <csr-id-dbfa4ac0dd23e76060b8df44c4666a30bb9b317f/> replace node comms with sn_comms
 - <csr-id-27fe2beaa76146b3bd235405f6b49c495214a336/> add missing calls to update comm members
   More net knowledge situations can update membership
 - <csr-id-a6c7e2f5860c752dd4ddda0768ec916eb6c25add/> remove old superfluous separation
 - <csr-id-7674b9f5609384a35072043a777ac07b18c12bb3/> sort faulty nodes by fault level
 - <csr-id-b13f1d7e5e84e42ed076654a159418563a9a1a35/> remove OpId for failed request tracking
   We use bidi now, so we can report after any failure, no need for
   double accounting
 - <csr-id-3b9304bab92f0715a134ce68afbfd08a7ad31e61/> some optimisations during AE probe
 - <csr-id-c0a8eaa0d564d04f856aef9d9e7b5c81a340e512/> retry recoverable errors on node start
 - <csr-id-3b85f2087de40cea8d3e5406837c031a2af96203/> log current name on join attempt
 - <csr-id-fadaa1cd4ecb4ac920c80d7e014f840c5594077f/> ensure rt.shutdown_timer is run on node err
 - <csr-id-a578f25afbdd9b32935522df557bf175a792a6a5/> make a failure to respond a Comms issue
 - <csr-id-a15c20e564600e77de28bad334fdaa4e6aa7fd92/> improve conn log msg w/ msgid
 - <csr-id-21af053a5be2317be356e760c2b581c0f870a396/> happy new year 2023
 - <csr-id-00e417037d7be7639e9ccc245d1808396b779ead/> fix after rebase
 - <csr-id-027b164f851209f0662e0a84ee839618d95af58d/> improve logging and comments
 - <csr-id-5ae5a160f8cb29df62b8f0253e53e527129b4689/> adapt DKG docs
 - <csr-id-48a438c5973290c50d80128200812ee93999aabe/> moving functions to more logical files
 - <csr-id-a33d28e4bc3f11779eaf0bf6cafd67800dbc4e0d/> tidying up
 - <csr-id-ea3caf4ff929590137f3c800f29dcdf39cbddc20/> cleanup send msg log
 - <csr-id-6c0f451646ea5840c79f112868637facdd08293c/> refactor away WireMsgUtils + make NodeJoin MsgKind
 - <csr-id-a14deab2a4a2b127f8f2b32e0aa2c0ab98ead4d2/> rename ListenerEvent -> ConnectionEvent
 - <csr-id-5c490793e6c434c3c68f276483a33bd330385524/> request missing data from the entire section
 - <csr-id-7ead1c15859c0ca5553ab1df310607da8d526f90/> cleanup commented code
 - <csr-id-542b627d4de792bd69f64f57b0da21c8775ce055/> some minor fixes and cleanup
 - <csr-id-33a577f5e52029854757b9093e7b30535a7acabd/> lighten the CouldNotStoreData flow
   Dont return the full data, just the address. No
   need to fill up other nodes as removing this one will trigger
   that flow
 - <csr-id-04525595bc5de39f85a128cfb691644b71a3fb79/> disabling keep-alive msgs from client to nodes
   - Setting sn_node idle-timeout to 70secs (to match ADULT_RESPONSE_TIMEOUT),
   which allows the node to keep client connections a bit longer since it may
   need more time (when under stress) to send back a response before closing them.
   - Setting sn_client default idle_timeout to match query/cmd timeout values.
 - <csr-id-35f3b27291905cd4c845e6005a07d23f76bc5449/> remove outdated comment
 - <csr-id-f0b25be00ba4d97539bde19fb524edebc4d1aac5/> remove unused argument

### Chore

 - <csr-id-783d62461a65eb7c06b0d4f399b97216b6c75519/> sn_interface-0.16.14/sn_client-0.77.8/sn_node-0.72.25/sn_api-0.75.4/sn_cli-0.68.5

### New Features

<csr-id-5f8a446a1c8a2798f708b4c792c2fe3553c4d135/>
<csr-id-d095ae1096560b26a218224863e7abf48218b543/>
<csr-id-c51029ed2e46f8a89f5162bde99c8852e8b7cfd7/>
<csr-id-a4cad278abd1504011678e8cc176f1c7df77493e/>
<csr-id-3cb993982488dfae10ff20bd73db72edec1d8cd3/>
<csr-id-4e9826c656e8763e888825ee2511e806a6e34928/>

 - <csr-id-74b5e7794a213b70db7231c31b68cee340976119/> check elder scores against elders only, non elders against non elders
 - <csr-id-ee90905abcf228d3a9d468ff0bb89a598cb0290d/> extra relocation criterias when relocate elder related node
   1, not to relocate elder to self section
   2, not counter join_as_relocated node as elder candidate immediately
 - <csr-id-f072aae9155cf833ce3a0f304496f43f6862dff4/> add ElderVoting issue type and track on section proposal votes outgoing
 - <csr-id-64cda9fab0016b91fa0cba4650c28b47f9ee6e93/> track faults when we vote, untrack when they vote
 - <csr-id-addd2f806e81be1f04599fa556216b61ee5b8138/> enable removing knowledge issues
   This should allow us to track voted proposals more easily
   and weed out nodes that consistently dont vote
 - <csr-id-17877a2cd0d712e7a773f54e3df05698f2f201bc/> require total participation for handover requests
 - <csr-id-3d8070155bb88b7403ae97730b33510b8c3af685/> introducing Cmd::SendNodeMsgResponse for NodeMsg responses to nodes over streams
   - Having this internal sn_node::Cmd to handle sending msg responses to nodes over
   a response bi-stream allows us to decouple such logic from the rest, but it also
   allows us to have unit tests within sn_node which verify the outcome of processing
   Cmds without sending any msg over the wire.

### Bug Fixes

 - <csr-id-28cdebb4b05c5d64dcbe8dfb39a72c88fd2c28bd/> update comm members on AE msg in
 - <csr-id-ca01afa62d455aa35a3cf63defa891f4ea025f54/> fix a deadlock during relocation
 - <csr-id-5091fe18f4c0b22075f924efa2c2a3d368b186d4/> skip the propose NodeIsOffline flow for relocations
 - <csr-id-00ae9b1181be4e5753da8c8854e864e90b80606b/> expect! macro doesn't support named parameters in the msg string
 - <csr-id-16526095eba5520325ff0fb4fcda5ff620ffbb49/> resolve issues cause the relocation failure
 - <csr-id-0b305a8bcf3cf08e0f54a48652d9aacbc7b5ce85/> unit test where wrong part was serialized
 - <csr-id-006b51e35435d61bac417674230e83d040814fac/> avoid reverifying for partial dag creation
   AE back and forth and other messaging can mean we make a good amount of
   partial_dags. Before this, we were pulling from a verified section_dag
   and verifying _every_ insert into our fresh partial dag.
   
   This was costly. So now we avoid it.
 - <csr-id-cf5fc48730da38aa276b42602dd83a9c879b31d1/> adapt logging to be less confusing
 - <csr-id-233d0bbfdd31873dd26401e916805f937fa0e7c0/> add tiny delay to bidi retry
   This should allow DashMap to be in sync and prevent a harsh loop
   killing us here (as it has done)
 - <csr-id-cfe3a8b6c6bd437a4ef1505f2a906041da7ca632/> always run the DKG checks on membership decision

### Refactor

 - <csr-id-ce13490ef261cde3e6888a0da4b84b9d0f2be3bc/> removing unnecessary internal helper data type
 - <csr-id-96169a7fbab950b40dc87ff433a9e348709eae1a/> pass NodeContext within Cmd::UpdateNetworkAndHandleValidClientMsg
 - <csr-id-9aaf91bfedd9fcf040f18e0762ff2cbbc03b4d6f/> forward client data cmds/queries to holders through Cmd::SendMsgAndAwaitResponse
   - Unifying and simplifying logic to send client data cmds and queries to holders so in both
   cases the sn_node `Cmd::SendMsgAndAwaitResponse` is used.
   - Renaming `sn_comms::Error::CmdSendError` to `SendError` since it's not specific for
   cmds but for any msg.
   - Some internal sn_node helper functions were moved to different files/mods so they are closer
   to the logic making use of them.
 - <csr-id-61834d8abf8e946768e2c958ca2159979497f782/> removing unnecessary send_stream field from Cmd::SendLockingJoinMsg
 - <csr-id-b2edff4b5b63b5d8a7905428b2c78b1d26598f07/> replace nodejoin type with flag
 - <csr-id-9f5b5ffb5975810e22c634f171984fcc803062aa/> proposal into multiple distinct messages
 - <csr-id-29562f26b3d9a10f72651c58df022db6b827b002/> removing redundant node messaging logic
 - <csr-id-82ac545c7c3bbf1941fe9d9a80dcc2f99ff58a2f/> removing Mutex we hold around SendStream
   - Each SendStream is now moved into either `Cmd`s or functions
   instead of being shared using a Mutex around it.
 - <csr-id-b316262443ba5d38c4879ae9d2f583aba92e501a/> making sn_node::Cmd non-clonable

### Test

 - <csr-id-efd7b9b9c775eb15b8beddee8286a1f788c2b940/> fixing and re-enabling Spentbook spend msg handlig tests
 - <csr-id-2b6c4a8fcd2cd7b22e6a4b20f1218c859110be62/> add ElderVoting to the startup msg count
   Adds ElderVoting as a msg that can fail to aid detection of bad elders.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 136 commits contributed to the release over the course of 23 calendar days.
 - 24 days passed between releases.
 - 71 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.14/sn_client-0.77.8/sn_node-0.72.25/sn_api-0.75.4/sn_cli-0.68.5 ([`783d624`](https://github.com/maidsafe/safe_network/commit/783d62461a65eb7c06b0d4f399b97216b6c75519))
    - Merge #2008 ([`ffac6c6`](https://github.com/maidsafe/safe_network/commit/ffac6c68dc0612a41aa74c533231a63006c22b22))
    - update comm members on AE msg in ([`28cdebb`](https://github.com/maidsafe/safe_network/commit/28cdebb4b05c5d64dcbe8dfb39a72c88fd2c28bd))
    - always run the DKG checks on membership decision ([`cfe3a8b`](https://github.com/maidsafe/safe_network/commit/cfe3a8b6c6bd437a4ef1505f2a906041da7ca632))
    - Merge #1997 #1998 #2002 ([`0c968ad`](https://github.com/maidsafe/safe_network/commit/0c968ad50d9e9dada3f5f5488bd1708fddadef72))
    - Merge #1994 ([`0ef2344`](https://github.com/maidsafe/safe_network/commit/0ef2344d9e57b46d1a364d2cc56f85edd945f762))
    - remove readlock where we have context ([`cd9bf5f`](https://github.com/maidsafe/safe_network/commit/cd9bf5fd5ccac42cd9de028cdaff8e0302498ed0))
    - fix a deadlock during relocation ([`ca01afa`](https://github.com/maidsafe/safe_network/commit/ca01afa62d455aa35a3cf63defa891f4ea025f54))
    - make joins bidi: set up send and await response Cmd ([`99a4c43`](https://github.com/maidsafe/safe_network/commit/99a4c43e0131bfc2bf36ff07bef2e476b4e801b9))
    - rename SendMsgAwaitReponse to SendMsgAwaitResponseAndRespondToClient ([`ad8cb2c`](https://github.com/maidsafe/safe_network/commit/ad8cb2cfd5387a76b84682e018c02889579935c8))
    - Merge #1991 ([`0ee56bd`](https://github.com/maidsafe/safe_network/commit/0ee56bd4d504349275fcb5d32dee57b9f36c418a))
    - removing unnecessary internal helper data type ([`ce13490`](https://github.com/maidsafe/safe_network/commit/ce13490ef261cde3e6888a0da4b84b9d0f2be3bc))
    - Merge #1940 #1982 ([`3bddfdb`](https://github.com/maidsafe/safe_network/commit/3bddfdb6241116144e1e8869c192d20b89ae5534))
    - fixing and re-enabling Spentbook spend msg handlig tests ([`efd7b9b`](https://github.com/maidsafe/safe_network/commit/efd7b9b9c775eb15b8beddee8286a1f788c2b940))
    - Merge #1981 ([`85da86c`](https://github.com/maidsafe/safe_network/commit/85da86cd0d4914489fc74125bb7a2655136f3508))
    - Merge branch 'main' into include-join-in-node ([`7a9110e`](https://github.com/maidsafe/safe_network/commit/7a9110eb2b458b9955eec872fafbe29af61d6674))
    - refactor(join): use existing msg-flow - Ae checks are made on TryJoin msg. - Elders drop msgs with invalid/unreachable parameters. - Unit tests more of `unit`-style than `e2e`. ([`8f596e9`](https://github.com/maidsafe/safe_network/commit/8f596e914f841839dffe89c67aa090f29bc03109))
    - Merge #1986 #1990 ([`ebb4c4a`](https://github.com/maidsafe/safe_network/commit/ebb4c4a52d602b75929ba16736a5c5781122ce00))
    - Merge #1988 ([`424e490`](https://github.com/maidsafe/safe_network/commit/424e4903838f3f89d84443278a8a553b8deebb13))
    - Merge #1987 ([`1bf3c65`](https://github.com/maidsafe/safe_network/commit/1bf3c65dda02489297e98fb27ce3cf4a241ebf48))
    - skip the propose NodeIsOffline flow for relocations ([`5091fe1`](https://github.com/maidsafe/safe_network/commit/5091fe18f4c0b22075f924efa2c2a3d368b186d4))
    - clarify comments and ensure sorted output of faulty nodes ([`8d2ef1a`](https://github.com/maidsafe/safe_network/commit/8d2ef1a0f298ef010f478fcd59c5b6c437b7b62f))
    - check elder scores against elders only, non elders against non elders ([`74b5e77`](https://github.com/maidsafe/safe_network/commit/74b5e7794a213b70db7231c31b68cee340976119))
    - clarify naming of elders/non elders ([`87cb70e`](https://github.com/maidsafe/safe_network/commit/87cb70eefdc63f80942a2c87ecc3790f76105b91))
    - store elders in fault detection ([`dc16323`](https://github.com/maidsafe/safe_network/commit/dc16323849e425e2ca2511f095caee5b0a4af1ab))
    - Merge #1985 ([`adedabe`](https://github.com/maidsafe/safe_network/commit/adedabe9b2fb06ae7f5cfaa773c04cde7fea2084))
    - pass NodeContext within Cmd::UpdateNetworkAndHandleValidClientMsg ([`96169a7`](https://github.com/maidsafe/safe_network/commit/96169a7fbab950b40dc87ff433a9e348709eae1a))
    - expect! macro doesn't support named parameters in the msg string ([`00ae9b1`](https://github.com/maidsafe/safe_network/commit/00ae9b1181be4e5753da8c8854e864e90b80606b))
    - extra relocation criterias when relocate elder related node ([`ee90905`](https://github.com/maidsafe/safe_network/commit/ee90905abcf228d3a9d468ff0bb89a598cb0290d))
    - Merge #1984 ([`dd07ad0`](https://github.com/maidsafe/safe_network/commit/dd07ad03a6112504c65c52a39aba0379b19c886c))
    - add ElderVoting to the startup msg count ([`2b6c4a8`](https://github.com/maidsafe/safe_network/commit/2b6c4a8fcd2cd7b22e6a4b20f1218c859110be62))
    - add ElderVoting issue type and track on section proposal votes outgoing ([`f072aae`](https://github.com/maidsafe/safe_network/commit/f072aae9155cf833ce3a0f304496f43f6862dff4))
    - rename Knowledge -> NetworkKnowledge for clarity ([`38d85b3`](https://github.com/maidsafe/safe_network/commit/38d85b391a72a3ee71f705d9b89d6dbc74c041e1))
    - rename log_node_issue -> track_node_isse ([`c48a653`](https://github.com/maidsafe/safe_network/commit/c48a6531cc5246319cf6453ddef4641fbf98ead5))
    - track faults when we vote, untrack when they vote ([`64cda9f`](https://github.com/maidsafe/safe_network/commit/64cda9fab0016b91fa0cba4650c28b47f9ee6e93))
    - enable removing knowledge issues ([`addd2f8`](https://github.com/maidsafe/safe_network/commit/addd2f806e81be1f04599fa556216b61ee5b8138))
    - Merge #1978 ([`fde6710`](https://github.com/maidsafe/safe_network/commit/fde67106242ad3d47f04ce99261a1e6299e94047))
    - forward client data cmds/queries to holders through Cmd::SendMsgAndAwaitResponse ([`9aaf91b`](https://github.com/maidsafe/safe_network/commit/9aaf91bfedd9fcf040f18e0762ff2cbbc03b4d6f))
    - Merge #1979 ([`6b8e25c`](https://github.com/maidsafe/safe_network/commit/6b8e25c6195c59d50ed61351e21570a594d209e8))
    - require total participation for handover requests ([`17877a2`](https://github.com/maidsafe/safe_network/commit/17877a2cd0d712e7a773f54e3df05698f2f201bc))
    - Merge #1974 ([`5afb1d0`](https://github.com/maidsafe/safe_network/commit/5afb1d064737daad6961ad290c2ff7c3ff2f1e38))
    - Merge #1975 ([`635a1b2`](https://github.com/maidsafe/safe_network/commit/635a1b29c9f8be3f708c6670de51ce68c0d34663))
    - introducing Cmd::SendNodeMsgResponse for NodeMsg responses to nodes over streams ([`3d80701`](https://github.com/maidsafe/safe_network/commit/3d8070155bb88b7403ae97730b33510b8c3af685))
    - resolve issues cause the relocation failure ([`1652609`](https://github.com/maidsafe/safe_network/commit/16526095eba5520325ff0fb4fcda5ff620ffbb49))
    - Merge #1973 ([`f308b44`](https://github.com/maidsafe/safe_network/commit/f308b44fbc8cb0b669ed129727e638285ba65f1d))
    - fix(tests): add feat flag to call test fn from ext - As we test comms in sn_node, now when in another crate, cfg(test) is not detected, and we solve that by adding the dev-dep with a feat flag. ([`76b5e75`](https://github.com/maidsafe/safe_network/commit/76b5e75af26e4a25dcc7f8e0b58e842350339b02))
    - do the mapping and remove unused ([`fa65879`](https://github.com/maidsafe/safe_network/commit/fa65879d11d3e3dc3cd6127a2ac777b32f90afee))
    - replace node comms with sn_comms ([`dbfa4ac`](https://github.com/maidsafe/safe_network/commit/dbfa4ac0dd23e76060b8df44c4666a30bb9b317f))
    - Merge #1961 ([`7da114b`](https://github.com/maidsafe/safe_network/commit/7da114b75cdb2a919506b0800ece860cb3e6df3e))
    - Merge #1966 ([`888bf7d`](https://github.com/maidsafe/safe_network/commit/888bf7d5b4b09996e98da442fae78251f2f41951))
    - add missing calls to update comm members ([`27fe2be`](https://github.com/maidsafe/safe_network/commit/27fe2beaa76146b3bd235405f6b49c495214a336))
    - make comm members list thread safe ([`5f8a446`](https://github.com/maidsafe/safe_network/commit/5f8a446a1c8a2798f708b4c792c2fe3553c4d135))
    - Merge #1962 ([`61f7d98`](https://github.com/maidsafe/safe_network/commit/61f7d98c84df9d465d9e54c06e3d5569ceff097c))
    - Merge #1967 ([`600534f`](https://github.com/maidsafe/safe_network/commit/600534f77f8e0bbc11d0dfdce6212dd4b7916118))
    - removing unnecessary send_stream field from Cmd::SendLockingJoinMsg ([`61834d8`](https://github.com/maidsafe/safe_network/commit/61834d8abf8e946768e2c958ca2159979497f782))
    - feat(comm): only cache conns of members - Updates comms on member changes. - Only adds connections to local cache if from a member. - Clears connections when members are lost. ([`fcdcde5`](https://github.com/maidsafe/safe_network/commit/fcdcde5e517bd8c91ba278ea73b934416d2ce857))
    - remove old superfluous separation ([`a6c7e2f`](https://github.com/maidsafe/safe_network/commit/a6c7e2f5860c752dd4ddda0768ec916eb6c25add))
    - replace nodejoin type with flag ([`b2edff4`](https://github.com/maidsafe/safe_network/commit/b2edff4b5b63b5d8a7905428b2c78b1d26598f07))
    - sort faulty nodes by fault level ([`7674b9f`](https://github.com/maidsafe/safe_network/commit/7674b9f5609384a35072043a777ac07b18c12bb3))
    - Merge #1959 ([`e13ca56`](https://github.com/maidsafe/safe_network/commit/e13ca5638ff96f0da7259a64cf0f2d6019fbc3da))
    - Merge #1958 ([`d3355bc`](https://github.com/maidsafe/safe_network/commit/d3355bc3c47e3f68517dfc62c01f647571bd1f73))
    - remove OpId for failed request tracking ([`b13f1d7`](https://github.com/maidsafe/safe_network/commit/b13f1d7e5e84e42ed076654a159418563a9a1a35))
    - Merge #1954 ([`0a12fa1`](https://github.com/maidsafe/safe_network/commit/0a12fa192492364a6abe94dd003aa936c4364a64))
    - adding SendClientResponse, SendNodeResponse, and SendMsgAndAwaitResponse cmds ([`d095ae1`](https://github.com/maidsafe/safe_network/commit/d095ae1096560b26a218224863e7abf48218b543))
    - Merge #1956 ([`d005784`](https://github.com/maidsafe/safe_network/commit/d005784be478c93a3e801e090f37ccf17a4acc19))
    - some optimisations during AE probe ([`3b9304b`](https://github.com/maidsafe/safe_network/commit/3b9304bab92f0715a134ce68afbfd08a7ad31e61))
    - retry recoverable errors on node start ([`c0a8eaa`](https://github.com/maidsafe/safe_network/commit/c0a8eaa0d564d04f856aef9d9e7b5c81a340e512))
    - Merge #1957 ([`d089750`](https://github.com/maidsafe/safe_network/commit/d08975010df21847a0ae830f52275861ab5011d9))
    - log current name on join attempt ([`3b85f20`](https://github.com/maidsafe/safe_network/commit/3b85f2087de40cea8d3e5406837c031a2af96203))
    - Merge #1955 ([`cd4cf9f`](https://github.com/maidsafe/safe_network/commit/cd4cf9f204b59aa847a0c952719c9aefd9e68454))
    - ensure rt.shutdown_timer is run on node err ([`fadaa1c`](https://github.com/maidsafe/safe_network/commit/fadaa1cd4ecb4ac920c80d7e014f840c5594077f))
    - make a failure to respond a Comms issue ([`a578f25`](https://github.com/maidsafe/safe_network/commit/a578f25afbdd9b32935522df557bf175a792a6a5))
    - improve conn log msg w/ msgid ([`a15c20e`](https://github.com/maidsafe/safe_network/commit/a15c20e564600e77de28bad334fdaa4e6aa7fd92))
    - Merge #1944 ([`9b110e8`](https://github.com/maidsafe/safe_network/commit/9b110e819e9838f16622b7a3b410eedb087be687))
    - Merge #1946 #1950 ([`8313977`](https://github.com/maidsafe/safe_network/commit/8313977ac69ebb157a37409ec4c084db29744f71))
    - Merge #1951 ([`24ca31f`](https://github.com/maidsafe/safe_network/commit/24ca31fd53c570c7c97849b74ded850c05273353))
    - happy new year 2023 ([`21af053`](https://github.com/maidsafe/safe_network/commit/21af053a5be2317be356e760c2b581c0f870a396))
    - Merge #1941 ([`a8227e8`](https://github.com/maidsafe/safe_network/commit/a8227e8b3bda5f51d1de8bd39e9d7bba5705a93a))
    - fix after rebase ([`00e4170`](https://github.com/maidsafe/safe_network/commit/00e417037d7be7639e9ccc245d1808396b779ead))
    - improve logging and comments ([`027b164`](https://github.com/maidsafe/safe_network/commit/027b164f851209f0662e0a84ee839618d95af58d))
    - adapt DKG docs ([`5ae5a16`](https://github.com/maidsafe/safe_network/commit/5ae5a160f8cb29df62b8f0253e53e527129b4689))
    - unit test where wrong part was serialized ([`0b305a8`](https://github.com/maidsafe/safe_network/commit/0b305a8bcf3cf08e0f54a48652d9aacbc7b5ce85))
    - moving functions to more logical files ([`48a438c`](https://github.com/maidsafe/safe_network/commit/48a438c5973290c50d80128200812ee93999aabe))
    - tidying up ([`a33d28e`](https://github.com/maidsafe/safe_network/commit/a33d28e4bc3f11779eaf0bf6cafd67800dbc4e0d))
    - proposal into multiple distinct messages ([`9f5b5ff`](https://github.com/maidsafe/safe_network/commit/9f5b5ffb5975810e22c634f171984fcc803062aa))
    - removing redundant node messaging logic ([`29562f2`](https://github.com/maidsafe/safe_network/commit/29562f26b3d9a10f72651c58df022db6b827b002))
    - Merge #1948 ([`bc2d0c1`](https://github.com/maidsafe/safe_network/commit/bc2d0c1d6672b86c05be2dd08567531149ad7355))
    - avoid reverifying for partial dag creation ([`006b51e`](https://github.com/maidsafe/safe_network/commit/006b51e35435d61bac417674230e83d040814fac))
    - Merge #1945 ([`b4fa062`](https://github.com/maidsafe/safe_network/commit/b4fa062f39a6617d0998efbd6dace72e6ae265bf))
    - feat(ae): target 1 rand elder in 3 rand sections - Maintains the global network knowledge of all sections. ([`bf17cd2`](https://github.com/maidsafe/safe_network/commit/bf17cd21ca9dce28025a583c2bd6b8dcda477b2d))
    - disable unnecessary checks ([`c51029e`](https://github.com/maidsafe/safe_network/commit/c51029ed2e46f8a89f5162bde99c8852e8b7cfd7))
    - Merge #1942 ([`28d5b96`](https://github.com/maidsafe/safe_network/commit/28d5b967404b1c28406328a18d88bd4c85f7a335))
    - adapt logging to be less confusing ([`cf5fc48`](https://github.com/maidsafe/safe_network/commit/cf5fc48730da38aa276b42602dd83a9c879b31d1))
    - cleanup send msg log ([`ea3caf4`](https://github.com/maidsafe/safe_network/commit/ea3caf4ff929590137f3c800f29dcdf39cbddc20))
    - remove PeerSession on ConnectionClosed ([`a4cad27`](https://github.com/maidsafe/safe_network/commit/a4cad278abd1504011678e8cc176f1c7df77493e))
    - dont cache joining node peer sessions ([`3cb9939`](https://github.com/maidsafe/safe_network/commit/3cb993982488dfae10ff20bd73db72edec1d8cd3))
    - refactor away WireMsgUtils + make NodeJoin MsgKind ([`6c0f451`](https://github.com/maidsafe/safe_network/commit/6c0f451646ea5840c79f112868637facdd08293c))
    - rename ListenerEvent -> ConnectionEvent ([`a14deab`](https://github.com/maidsafe/safe_network/commit/a14deab2a4a2b127f8f2b32e0aa2c0ab98ead4d2))
    - Merge #1939 ([`68af821`](https://github.com/maidsafe/safe_network/commit/68af821a6f34924a1d80777bd71a8deb5a04c30e))
    - fix(storage): replace modulo op with div - The operator was a remnant from previous version, that had not yet been replaced. - Also updates some comments. ([`63eeb96`](https://github.com/maidsafe/safe_network/commit/63eeb9664a2e858e53bd33e0747a51ac2821fa79))
    - Merge #1937 ([`3d3fb26`](https://github.com/maidsafe/safe_network/commit/3d3fb26b2f55cd4fb820674cfe91753aa8aa1fcd))
    - Merge #1891 ([`716717c`](https://github.com/maidsafe/safe_network/commit/716717c1b3db9a881858bf8d2570f7fb9f4979f0))
    - removing Mutex we hold around SendStream ([`82ac545`](https://github.com/maidsafe/safe_network/commit/82ac545c7c3bbf1941fe9d9a80dcc2f99ff58a2f))
    - Merge #1938 ([`cac9e5b`](https://github.com/maidsafe/safe_network/commit/cac9e5b03abb67652dfed44016059acfe5da95de))
    - fix(join): do not override startup join allowed - Also sets joins allowed if we are still at min capacity after a data cleanup. ([`d9d32e1`](https://github.com/maidsafe/safe_network/commit/d9d32e1caf4d4e0c32300c73136298a3733531a4))
    - feat(storage): remove data outside of our range - On splits and (for now) node joins, we remove data that is no longer under our responsibility to hold. ([`30584ca`](https://github.com/maidsafe/safe_network/commit/30584caff6699644f577536f2a408658c1ea0600))
    - request missing data from the entire section ([`5c49079`](https://github.com/maidsafe/safe_network/commit/5c490793e6c434c3c68f276483a33bd330385524))
    - Merge #1934 ([`0a0d760`](https://github.com/maidsafe/safe_network/commit/0a0d760bc2a9513aefb033749952160c10350b74))
    - feat(churn): vote node offline when premature full - Otherwise we only log the issue, for consensused accumulation. ([`61f6ea9`](https://github.com/maidsafe/safe_network/commit/61f6ea96678a57e11f31220eb0855a1c185d8406))
    - feat(section): add nodes as used space grows - Used space levels of n%-point increments are defined, where n is derived from recommended_section_size. - Every time we pass such a level for the first time, we add a node. ([`d0a5f62`](https://github.com/maidsafe/safe_network/commit/d0a5f623f8ba111dddc5720384409c9742d4e2b7))
    - feat(data): trigger joins on min capacity reached - Allows Elders to monitor data usage and increase section size based on estimated (assuming uniform distr.) usage across the section. - This also makes the `max_capacity` config flag a convenience setting for node operators, allowing them to not exceed a certain level of actual space usage. ([`41454ce`](https://github.com/maidsafe/safe_network/commit/41454ce7deb1d63c0ad171634c21b7c1352fd312))
    - Merge #1926 #1936 ([`acc88c5`](https://github.com/maidsafe/safe_network/commit/acc88c5d94900c840cb6c3111ef92fc24b0f3a3d))
    - Merge branch 'main' into proposal_refactor ([`0bc7f94`](https://github.com/maidsafe/safe_network/commit/0bc7f94c72c374d667a9b455c4f4f1830366e4a4))
    - cleanup commented code ([`7ead1c1`](https://github.com/maidsafe/safe_network/commit/7ead1c15859c0ca5553ab1df310607da8d526f90))
    - making sn_node::Cmd non-clonable ([`b316262`](https://github.com/maidsafe/safe_network/commit/b316262443ba5d38c4879ae9d2f583aba92e501a))
    - fix(faults): do not kick out when couldnt store - Instead, just log an issue, and let the problem accumulate before we kick out. ([`5755a9d`](https://github.com/maidsafe/safe_network/commit/5755a9d81243e77e98bc0495d27b43e4309b322a))
    - some minor fixes and cleanup ([`542b627`](https://github.com/maidsafe/safe_network/commit/542b627d4de792bd69f64f57b0da21c8775ce055))
    - refactor(storage): let elders send wiremsg to self - If the Elder is holding the requested data, we allow it to query itself via comms (like we already do with store cmd). This reduces code duplication. We can optimize it later if we find that necessary. ([`c72d1aa`](https://github.com/maidsafe/safe_network/commit/c72d1aa580107584df87b31d1140fb7864060ef1))
    - feat(storage): use nodes where adults were used - This continues the move over to also using elders for storage. ([`250da72`](https://github.com/maidsafe/safe_network/commit/250da72ea38b82037ae928ac0eeb8c4b91568448))
    - feat(storage): use members where adults were used - This continues the move over to also using elders for storage. ([`63185dd`](https://github.com/maidsafe/safe_network/commit/63185dd5f693a121e02682bdd16aafaf8e5e5df5))
    - remove accounting of storage levels ([`4e9826c`](https://github.com/maidsafe/safe_network/commit/4e9826c656e8763e888825ee2511e806a6e34928))
    - refactor(storage): do some renaming - Renames some methods, variables and struct members. ([`7076ce9`](https://github.com/maidsafe/safe_network/commit/7076ce9befd790cf2aecd1aa67b7cfc3e78c7f60))
    - refactor(cmd): remove disambiguation - Renames `ReplicateOneData` to `StoreData`, which is the cmd used when a client stores data. - This leaves `ReplicateDataBatch` as the unambiguous cmd, already exclusively used for data replication between nodes. ([`29ee1d5`](https://github.com/maidsafe/safe_network/commit/29ee1d5205c1f4d079160f133e27e1cd1039b406))
    - feat(storage): report threshold reached only - This removes the book keeping of storage level on Elders. - Makes Adults report only when threshold reached. - Makes Elders allow joins until split when majority of Adults full. ([`a216003`](https://github.com/maidsafe/safe_network/commit/a216003b6275d36f1b419ad3cc2be30adb72700d))
    - Merge #1873 ([`8be1563`](https://github.com/maidsafe/safe_network/commit/8be1563fcddde2323ae2f892687dc76f253f3fb2))
    - chore(naming): rename dysfunction - Uses the more common vocabulary in fault tolerance area. ([`f68073f`](https://github.com/maidsafe/safe_network/commit/f68073f2897894375f5a09b870e2bfe4e03c3b10))
    - Merge #1933 ([`b408f59`](https://github.com/maidsafe/safe_network/commit/b408f597cbb7f5ea4737af2b06f2fd1dbe3f1786))
    - lighten the CouldNotStoreData flow ([`33a577f`](https://github.com/maidsafe/safe_network/commit/33a577f5e52029854757b9093e7b30535a7acabd))
    - Merge #1927 ([`8f7f2a4`](https://github.com/maidsafe/safe_network/commit/8f7f2a4fc2e1d6cabb4f4849510234df4e1255be))
    - disabling keep-alive msgs from client to nodes ([`0452559`](https://github.com/maidsafe/safe_network/commit/04525595bc5de39f85a128cfb691644b71a3fb79))
    - Merge #1931 ([`964a1bf`](https://github.com/maidsafe/safe_network/commit/964a1bfde64969a2335b1f6f4558d0ea917474b2))
    - remove outdated comment ([`35f3b27`](https://github.com/maidsafe/safe_network/commit/35f3b27291905cd4c845e6005a07d23f76bc5449))
    - fix(name): rename from interrogative to imperative - The naming was confusing as to what the method actually did. ([`f2327d7`](https://github.com/maidsafe/safe_network/commit/f2327d7072612d02218ba3a55b181539f78cdf6b))
    - remove unused argument ([`f0b25be`](https://github.com/maidsafe/safe_network/commit/f0b25be00ba4d97539bde19fb524edebc4d1aac5))
    - Merge #1929 ([`a6a2bdd`](https://github.com/maidsafe/safe_network/commit/a6a2bdd2ce6569dae2a6fe5927fd2a94bcaa4927))
    - add tiny delay to bidi retry ([`233d0bb`](https://github.com/maidsafe/safe_network/commit/233d0bbfdd31873dd26401e916805f937fa0e7c0))
</details>

## v0.72.24 (2022-12-27)

<csr-id-a38cd49958df82fd65d0a3f13670693f40a1e6b2/>

### Chore

 - <csr-id-a38cd49958df82fd65d0a3f13670693f40a1e6b2/> sn_interface-0.16.13/sn_client-0.77.7/sn_node-0.72.24

### Bug Fixes

 - <csr-id-220fd52ab3e1bac776ba74793d5042de220bb315/> set default keep-alive interval to be 1/2 of idle_timeout value set
   - By default the sn_client keep_alive msgs interval will now be set to 1/2 the
   value set for the idle_timeout value.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.13/sn_client-0.77.7/sn_node-0.72.24 ([`a38cd49`](https://github.com/maidsafe/safe_network/commit/a38cd49958df82fd65d0a3f13670693f40a1e6b2))
    - Merge #1924 ([`be2cded`](https://github.com/maidsafe/safe_network/commit/be2cdedb19154adf324782d7178f0e25018cd16c))
    - set default keep-alive interval to be 1/2 of idle_timeout value set ([`220fd52`](https://github.com/maidsafe/safe_network/commit/220fd52ab3e1bac776ba74793d5042de220bb315))
</details>

## v0.72.23 (2022-12-26)

<csr-id-6230dd6d001cea9c80cd0eaed5dece1d696335b6/>
<csr-id-dfff22988ebb99da5bd84c927be283d8d92b8fce/>

### Refactor

 - <csr-id-6230dd6d001cea9c80cd0eaed5dece1d696335b6/> simplify logic retrieve qp2p config from sn_node::Config
   - We used to keep an instance of qp2p::Config within sn_node::Config which
   required to keep in sync when modifying/reading network related config, we
   now simply build a qp2p::Config just when is queried from sn_node::Config.

### Chore

 - <csr-id-dfff22988ebb99da5bd84c927be283d8d92b8fce/> sn_node-0.72.23

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 4 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.23 ([`dfff229`](https://github.com/maidsafe/safe_network/commit/dfff22988ebb99da5bd84c927be283d8d92b8fce))
    - Merge #1925 ([`ec4dde8`](https://github.com/maidsafe/safe_network/commit/ec4dde8a583b5c6d4c7451e76977a80a840f9764))
    - simplify logic retrieve qp2p config from sn_node::Config ([`6230dd6`](https://github.com/maidsafe/safe_network/commit/6230dd6d001cea9c80cd0eaed5dece1d696335b6))
</details>

## v0.72.22 (2022-12-22)

<csr-id-4ddc75277726d5d752ff5340c5d885622d76b990/>

### Chore

 - <csr-id-4ddc75277726d5d752ff5340c5d885622d76b990/> sn_node-0.72.22/sn_cli-0.68.4

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.22/sn_cli-0.68.4 ([`4ddc752`](https://github.com/maidsafe/safe_network/commit/4ddc75277726d5d752ff5340c5d885622d76b990))
    - Merge #1915 ([`a41a2bc`](https://github.com/maidsafe/safe_network/commit/a41a2bcd7d0be64d20efc913d643091718ce743e))
</details>

## v0.72.21 (2022-12-22)

<csr-id-ff4a6aea4edc722f0aef23cea8100d7c09d3100a/>
<csr-id-c6ff5c120048c526788fd415c2db075f4be94090/>

### Chore

 - <csr-id-ff4a6aea4edc722f0aef23cea8100d7c09d3100a/> remove unused event formatting option
   - the `.event_format()` overrides the `.with_thread_names()` option,
     hence remove it

### Chore

 - <csr-id-c6ff5c120048c526788fd415c2db075f4be94090/> sn_interface-0.16.12/sn_node-0.72.21

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.12/sn_node-0.72.21 ([`c6ff5c1`](https://github.com/maidsafe/safe_network/commit/c6ff5c120048c526788fd415c2db075f4be94090))
    - Merge #1916 ([`24e9b56`](https://github.com/maidsafe/safe_network/commit/24e9b561f2396b944d7b02d2da453c7d9998d55d))
    - remove unused event formatting option ([`ff4a6ae`](https://github.com/maidsafe/safe_network/commit/ff4a6aea4edc722f0aef23cea8100d7c09d3100a))
</details>

## v0.72.20 (2022-12-22)

<csr-id-c6ac3e58159a30d4efa1ee1f35c787532d685ca5/>

### Bug Fixes

<csr-id-2dce913547af13c31ee1785160a7e86be82c8ac9/>

 - <csr-id-c1b517f99b4688ccd65eb91615b8fb531f95e853/> prevent panic when we have multiple tracing subscribers
   - When we have the `otlp` feature enabled, we effectively have
   multiple subscribers consuming the logs.

### Chore

 - <csr-id-c6ac3e58159a30d4efa1ee1f35c787532d685ca5/> sn_node-0.72.20

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.20 ([`c6ac3e5`](https://github.com/maidsafe/safe_network/commit/c6ac3e58159a30d4efa1ee1f35c787532d685ca5))
    - prevent panic when we have multiple tracing subscribers ([`c1b517f`](https://github.com/maidsafe/safe_network/commit/c1b517f99b4688ccd65eb91615b8fb531f95e853))
    - use vector of `Layers` to build the `Subscriber` ([`2dce913`](https://github.com/maidsafe/safe_network/commit/2dce913547af13c31ee1785160a7e86be82c8ac9))
    - Merge #1922 ([`cc39e3f`](https://github.com/maidsafe/safe_network/commit/cc39e3fb7a95e2d14cd2550932c7d263df74a9ed))
</details>

## v0.72.19 (2022-12-22)

<csr-id-54180f6c075d7f15f18aecf6068748e18f29a1b5/>
<csr-id-ea500663afffca0a083a75f3e9b5972ebd89a5bd/>
<csr-id-1f69f70b3784e3b8ab7ca56c2b60815e989b03ba/>
<csr-id-c9c30abbb232ae7ee173fcecf36b608e15cb92fd/>
<csr-id-3456e8c5bbe06b6ebfb92df5b05a94a3c0d1336d/>
<csr-id-3f094260e46e52e7293315cd772000617233d53e/>
<csr-id-0436915e88c1422d487d370ad718fda4c6c578a2/>
<csr-id-c70703f6a23ab7ab28a5f838366aa7b303e06e98/>
<csr-id-00cf71aad448cee3216b0d3e3cc1b3bc6159d14a/>
<csr-id-6bef36cadd09bba0bff9171a352813e3e860ee2c/>

### Chore

 - <csr-id-54180f6c075d7f15f18aecf6068748e18f29a1b5/> improve error on failed send + track dysf
 - <csr-id-ea500663afffca0a083a75f3e9b5972ebd89a5bd/> only track full nodes as/when joins are not already allowed
 - <csr-id-1f69f70b3784e3b8ab7ca56c2b60815e989b03ba/> use 1gb, and increase threshold for warning
 - <csr-id-c9c30abbb232ae7ee173fcecf36b608e15cb92fd/> refactor out replicate_data_batch code
 - <csr-id-3456e8c5bbe06b6ebfb92df5b05a94a3c0d1336d/> reduce MAX_MISSED_DATA_TO_REPLICATE to 25
 - <csr-id-3f094260e46e52e7293315cd772000617233d53e/> rename to ReplicateDataBatch
   to be more clearly distinct from the single replication flow
 - <csr-id-0436915e88c1422d487d370ad718fda4c6c578a2/> remove replication data bundling
   Now that we have a limit per msg and batches determined by sender
 - <csr-id-c70703f6a23ab7ab28a5f838366aa7b303e06e98/> dont limit network size by default
 - <csr-id-00cf71aad448cee3216b0d3e3cc1b3bc6159d14a/> make default node size 10gb

### Chore

 - <csr-id-6bef36cadd09bba0bff9171a352813e3e860ee2c/> sn_interface-0.16.11/sn_client-0.77.6/sn_node-0.72.19

### New Features

 - <csr-id-de6770aae3aeeccc4689047c0a07667e4b392be3/> separate data replication into rounds

### Bug Fixes

 - <csr-id-c4b47f1fa7b3d814a0de236f8a50b2c9f89750f2/> dont bail on join if sap update errors
 - <csr-id-2128c1bb2d364aa4d303831f146807d1bc7addea/> move data batch send out of loop
   We were duplicate sending data as we pushed each datum to the batch.
   
   :facepalm:
 - <csr-id-386bf375395ace0acf140ae6a8ea42df2457daa4/> remove async call and LogCtx
   The readlock in here could have been causing a deadlock
 - <csr-id-952cc5999c68ae6b97aaaa9744ba3c635490cbe7/> initialize logging just once
   - opentelemetry tracing requires a tokio runtime to be present, hence
   leave a separate rt running if `otlp` feature is enabled

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 16 commits contributed to the release.
 - 15 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.11/sn_client-0.77.6/sn_node-0.72.19 ([`6bef36c`](https://github.com/maidsafe/safe_network/commit/6bef36cadd09bba0bff9171a352813e3e860ee2c))
    - initialize logging just once ([`952cc59`](https://github.com/maidsafe/safe_network/commit/952cc5999c68ae6b97aaaa9744ba3c635490cbe7))
    - Merge #1917 ([`94fecdf`](https://github.com/maidsafe/safe_network/commit/94fecdff1270a7f215095f7419cfa1bb649213ce))
    - dont bail on join if sap update errors ([`c4b47f1`](https://github.com/maidsafe/safe_network/commit/c4b47f1fa7b3d814a0de236f8a50b2c9f89750f2))
    - improve error on failed send + track dysf ([`54180f6`](https://github.com/maidsafe/safe_network/commit/54180f6c075d7f15f18aecf6068748e18f29a1b5))
    - only track full nodes as/when joins are not already allowed ([`ea50066`](https://github.com/maidsafe/safe_network/commit/ea500663afffca0a083a75f3e9b5972ebd89a5bd))
    - use 1gb, and increase threshold for warning ([`1f69f70`](https://github.com/maidsafe/safe_network/commit/1f69f70b3784e3b8ab7ca56c2b60815e989b03ba))
    - move data batch send out of loop ([`2128c1b`](https://github.com/maidsafe/safe_network/commit/2128c1bb2d364aa4d303831f146807d1bc7addea))
    - remove async call and LogCtx ([`386bf37`](https://github.com/maidsafe/safe_network/commit/386bf375395ace0acf140ae6a8ea42df2457daa4))
    - refactor out replicate_data_batch code ([`c9c30ab`](https://github.com/maidsafe/safe_network/commit/c9c30abbb232ae7ee173fcecf36b608e15cb92fd))
    - reduce MAX_MISSED_DATA_TO_REPLICATE to 25 ([`3456e8c`](https://github.com/maidsafe/safe_network/commit/3456e8c5bbe06b6ebfb92df5b05a94a3c0d1336d))
    - rename to ReplicateDataBatch ([`3f09426`](https://github.com/maidsafe/safe_network/commit/3f094260e46e52e7293315cd772000617233d53e))
    - remove replication data bundling ([`0436915`](https://github.com/maidsafe/safe_network/commit/0436915e88c1422d487d370ad718fda4c6c578a2))
    - dont limit network size by default ([`c70703f`](https://github.com/maidsafe/safe_network/commit/c70703f6a23ab7ab28a5f838366aa7b303e06e98))
    - make default node size 10gb ([`00cf71a`](https://github.com/maidsafe/safe_network/commit/00cf71aad448cee3216b0d3e3cc1b3bc6159d14a))
    - separate data replication into rounds ([`de6770a`](https://github.com/maidsafe/safe_network/commit/de6770aae3aeeccc4689047c0a07667e4b392be3))
</details>

## v0.72.18 (2022-12-21)

<csr-id-bf159dc0477417bfd35b0f778822dbdeb3dd0023/>
<csr-id-5ca4e906c3ff3a55cdedcff1203df57f9f5d4767/>

### Refactor

 - <csr-id-bf159dc0477417bfd35b0f778822dbdeb3dd0023/> serialise the msg header only once when replicating data to holders
   - Also removing some minor but unnecessary WireMsg caching, and some objs cloning.

### Chore

 - <csr-id-5ca4e906c3ff3a55cdedcff1203df57f9f5d4767/> sn_interface-0.16.10/sn_node-0.72.18

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.10/sn_node-0.72.18 ([`5ca4e90`](https://github.com/maidsafe/safe_network/commit/5ca4e906c3ff3a55cdedcff1203df57f9f5d4767))
    - Merge #1921 ([`c3b09c5`](https://github.com/maidsafe/safe_network/commit/c3b09c5a851ce23ae4628455c7c7f3f17b58ed8c))
    - serialise the msg header only once when replicating data to holders ([`bf159dc`](https://github.com/maidsafe/safe_network/commit/bf159dc0477417bfd35b0f778822dbdeb3dd0023))
</details>

## v0.72.17 (2022-12-21)

<csr-id-a1f1ac9401edfb18cb9d209ba866b89a622aeaf2/>
<csr-id-046224649bbbbd2f160cc69b8320a1b127284600/>

### Chore

 - <csr-id-a1f1ac9401edfb18cb9d209ba866b89a622aeaf2/> sn_node-0.72.17

### Chore

 - <csr-id-046224649bbbbd2f160cc69b8320a1b127284600/> remove references to IGD

### Bug Fixes

 - <csr-id-21705c1458e6c57f3db428758a6a7767e0cfb251/> persist log guard
   So we keep logging after init

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.17 ([`a1f1ac9`](https://github.com/maidsafe/safe_network/commit/a1f1ac9401edfb18cb9d209ba866b89a622aeaf2))
    - Merge #1914 ([`a5da8fc`](https://github.com/maidsafe/safe_network/commit/a5da8fcb3687fe34acbafed369a3fa0f2f20a4cf))
    - remove references to IGD ([`0462246`](https://github.com/maidsafe/safe_network/commit/046224649bbbbd2f160cc69b8320a1b127284600))
    - persist log guard ([`21705c1`](https://github.com/maidsafe/safe_network/commit/21705c1458e6c57f3db428758a6a7767e0cfb251))
</details>

## v0.72.16 (2022-12-20)

<csr-id-c75b41ab4b2dead66ce37487255939205f771aa6/>

### Chore

 - <csr-id-c75b41ab4b2dead66ce37487255939205f771aa6/> sn_node-0.72.16

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.16 ([`c75b41a`](https://github.com/maidsafe/safe_network/commit/c75b41ab4b2dead66ce37487255939205f771aa6))
    - Merge #1862 ([`aed6549`](https://github.com/maidsafe/safe_network/commit/aed65493fb5dd3cb6c39f32559e0bb20bff157c9))
</details>

## v0.72.15 (2022-12-20)

<csr-id-aed73cfa0eb0dc3271defa7de2a90a96c790bc8d/>

### Chore

 - <csr-id-aed73cfa0eb0dc3271defa7de2a90a96c790bc8d/> sn_interface-0.16.9/sn_client-0.77.4/sn_node-0.72.15

### New Features

 - <csr-id-96e8c7c5315090462e1269c48027cdba1bfea23a/> retry sending msg to peer cleaning up all cached bad connections
   - When sending a msg to a peer, if it fails with an existing cached connection,
   it will keep retrying till it either finds another cached connection which it
   succeeds with, or it cleans them all up from the cache creating a new connection
   to the peer as last attempt.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.9/sn_client-0.77.4/sn_node-0.72.15 ([`aed73cf`](https://github.com/maidsafe/safe_network/commit/aed73cfa0eb0dc3271defa7de2a90a96c790bc8d))
    - Merge #1899 ([`d88b5dd`](https://github.com/maidsafe/safe_network/commit/d88b5dd5c8c5799c6896b19a9c4de094943b377f))
    - retry sending msg to peer cleaning up all cached bad connections ([`96e8c7c`](https://github.com/maidsafe/safe_network/commit/96e8c7c5315090462e1269c48027cdba1bfea23a))
</details>

## v0.72.14 (2022-12-20)

<csr-id-fbf081a85626d8e65598e786f60cbcfe477419f8/>

### Chore

 - <csr-id-fbf081a85626d8e65598e786f60cbcfe477419f8/> sn_node-0.72.14

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.14 ([`fbf081a`](https://github.com/maidsafe/safe_network/commit/fbf081a85626d8e65598e786f60cbcfe477419f8))
    - Merge #1903 ([`aefe4b2`](https://github.com/maidsafe/safe_network/commit/aefe4b2d132f9330d274beff766d0015c71e22a6))
</details>

## v0.72.13 (2022-12-20)

<csr-id-98b25549f85b5d885ae6ee5825e7262c7d29e38b/>

### Chore

 - <csr-id-98b25549f85b5d885ae6ee5825e7262c7d29e38b/> sn_node-0.72.13

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.13 ([`98b2554`](https://github.com/maidsafe/safe_network/commit/98b25549f85b5d885ae6ee5825e7262c7d29e38b))
    - Merge #1905 ([`9c1565f`](https://github.com/maidsafe/safe_network/commit/9c1565f188b31110151cd2d4ac0c3fa58aa83edd))
</details>

## v0.72.12 (2022-12-20)

<csr-id-bb11b8369c36d20eb926d11fd7fbaa41ff37f011/>

### Bug Fixes

 - <csr-id-3f3d6400d58f8ec9f68dbbaf7814c962559404fe/> init logging in loop
   Ensures logging functions correctly

### Chore

 - <csr-id-bb11b8369c36d20eb926d11fd7fbaa41ff37f011/> sn_interface-0.16.8/sn_node-0.72.12

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.8/sn_node-0.72.12 ([`bb11b83`](https://github.com/maidsafe/safe_network/commit/bb11b8369c36d20eb926d11fd7fbaa41ff37f011))
    - init logging in loop ([`3f3d640`](https://github.com/maidsafe/safe_network/commit/3f3d6400d58f8ec9f68dbbaf7814c962559404fe))
    - Merge #1910 ([`f9cd9d6`](https://github.com/maidsafe/safe_network/commit/f9cd9d61a7b9229c14ea284c8aa9bf10a9f78bbd))
    - Revert "feat(join): prevent joins from nodes behind NAT" ([`c46bb99`](https://github.com/maidsafe/safe_network/commit/c46bb9934d7c12881dcac887ae55fe796027525d))
</details>

## v0.72.11 (2022-12-20)

<csr-id-43a3680784029da46fd549f7d06e2aff786a98d0/>
<csr-id-4d16bbedc35e470200126bb8a2554d8d96b8faa5/>
<csr-id-5dfa24c9982c13fb321006f13b5ff417153191f3/>
<csr-id-a6addd1dde96833d6629e75b418ac2a244ab31f3/>

### Chore

 - <csr-id-43a3680784029da46fd549f7d06e2aff786a98d0/> pr suggestions

### Chore

 - <csr-id-a6addd1dde96833d6629e75b418ac2a244ab31f3/> sn_interface-0.16.7/sn_client-0.77.3/sn_node-0.72.11/sn_api-0.75.3/sn_cli-0.68.3

### Bug Fixes

 - <csr-id-22402ca6acb0215ecfe9b1fdbf306c0f9cb87d95/> genesis_sap is required to create the `SectionTree`
   - The fields of the tree are assumed to be in sync. But it is not the
   case for a newly created tree.

### Refactor

 - <csr-id-4d16bbedc35e470200126bb8a2554d8d96b8faa5/> relocate based on our new name
   - Pass in our current name to get relocated to the correct section
   - Avoids creating a new `NetworkKnowledge` instance
 - <csr-id-5dfa24c9982c13fb321006f13b5ff417153191f3/> rework constructor
   - Create the `NetworkKnowledge` struct by passing in a `SectionTree`
     and a `Prefix`. The current signed SAP is retrieved from the tree
     using the provided prefix.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.7/sn_client-0.77.3/sn_node-0.72.11/sn_api-0.75.3/sn_cli-0.68.3 ([`a6addd1`](https://github.com/maidsafe/safe_network/commit/a6addd1dde96833d6629e75b418ac2a244ab31f3))
    - Merge #1848 ([`ddaf857`](https://github.com/maidsafe/safe_network/commit/ddaf8571749c142e9960407cfd9cfa94231a36ad))
    - pr suggestions ([`43a3680`](https://github.com/maidsafe/safe_network/commit/43a3680784029da46fd549f7d06e2aff786a98d0))
    - relocate based on our new name ([`4d16bbe`](https://github.com/maidsafe/safe_network/commit/4d16bbedc35e470200126bb8a2554d8d96b8faa5))
    - rework constructor ([`5dfa24c`](https://github.com/maidsafe/safe_network/commit/5dfa24c9982c13fb321006f13b5ff417153191f3))
    - genesis_sap is required to create the `SectionTree` ([`22402ca`](https://github.com/maidsafe/safe_network/commit/22402ca6acb0215ecfe9b1fdbf306c0f9cb87d95))
</details>

## v0.72.10 (2022-12-19)

<csr-id-8fcbf73821b9cbde8ed2d87910842134e179fdbf/>

### Chore

 - <csr-id-8fcbf73821b9cbde8ed2d87910842134e179fdbf/> sn_interface-0.16.6/sn_node-0.72.10

### Bug Fixes

 - <csr-id-4c79cbc641b2395afd7b600a1511a64709cc5309/> ensure we use a fresh runtime each startup
   Previously the runtime was only refreshed if we
   errored out, now we refresh if were attempting to rejoin too
 - <csr-id-a893f6a3ac27fde2294904299fd08e29f93db0b3/> ues a fresh runtime every node_run call
   This should close all existing endpoints and other spawned tasks
   and get us a proper fresh ndoe instance
 - <csr-id-936454fd9d087c29a54a84e3b4672d0d60f81dbd/> make initial get/connect blocking
   This avoids many many connection attempts to the same node if one is in progress already.

### New Features

 - <csr-id-6fa35bc5b094583b728d8d068d9ae21df12d40b9/> bundle messages according to size and number

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.6/sn_node-0.72.10 ([`8fcbf73`](https://github.com/maidsafe/safe_network/commit/8fcbf73821b9cbde8ed2d87910842134e179fdbf))
    - Merge #1907 ([`8ebd0a6`](https://github.com/maidsafe/safe_network/commit/8ebd0a67d548169fc4cbf716f0c940425096264f))
    - bundle messages according to size and number ([`6fa35bc`](https://github.com/maidsafe/safe_network/commit/6fa35bc5b094583b728d8d068d9ae21df12d40b9))
    - ensure we use a fresh runtime each startup ([`4c79cbc`](https://github.com/maidsafe/safe_network/commit/4c79cbc641b2395afd7b600a1511a64709cc5309))
    - ues a fresh runtime every node_run call ([`a893f6a`](https://github.com/maidsafe/safe_network/commit/a893f6a3ac27fde2294904299fd08e29f93db0b3))
    - make initial get/connect blocking ([`936454f`](https://github.com/maidsafe/safe_network/commit/936454fd9d087c29a54a84e3b4672d0d60f81dbd))
</details>

## v0.72.9 (2022-12-19)

<csr-id-27d14ffb98d5ce86c61c3c43b2bf33055f21c32d/>

### Chore

 - <csr-id-27d14ffb98d5ce86c61c3c43b2bf33055f21c32d/> sn_node-0.72.9

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.9 ([`27d14ff`](https://github.com/maidsafe/safe_network/commit/27d14ffb98d5ce86c61c3c43b2bf33055f21c32d))
    - Merge #1880 #1898 ([`aea33e3`](https://github.com/maidsafe/safe_network/commit/aea33e3292f815d9b60ed02c14dfa907dc7e6984))
</details>

## v0.72.8 (2022-12-19)

<csr-id-b0199a21705a622dbfdc5bc3f6326fd5979ac345/>
<csr-id-781459fe3e92c91f503953fc5aa6a2241f1c587f/>

### Chore

 - <csr-id-b0199a21705a622dbfdc5bc3f6326fd5979ac345/> sn_node-0.72.8

### Refactor

 - <csr-id-781459fe3e92c91f503953fc5aa6a2241f1c587f/> pass `NodeMsgs` via the `Comm` module
   - The dkg tests bypassed the Comm module and used a queue to pass
     along the `NodeMsgs` for testing. This was due to the fact that the
     msgs were sent asynchronously, preventing any control of the flow.
   - Hence using the test-only synchronous msg sender allows us to do the
     above without using any extra queues.

### Bug Fixes

 - <csr-id-b1cc6f56f16b715cec2013ec07ef83dcc0df03d0/> fix edge case while calculating max_prefixes

### New Features

 - <csr-id-a371c6a593451c4190818b5140576a25755871d4/> synchronously send `NodeMsg` to other nodes
   - This allows us to control the flow of msgs in certain tests

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 2 days passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.8 ([`b0199a2`](https://github.com/maidsafe/safe_network/commit/b0199a21705a622dbfdc5bc3f6326fd5979ac345))
    - pass `NodeMsgs` via the `Comm` module ([`781459f`](https://github.com/maidsafe/safe_network/commit/781459fe3e92c91f503953fc5aa6a2241f1c587f))
    - synchronously send `NodeMsg` to other nodes ([`a371c6a`](https://github.com/maidsafe/safe_network/commit/a371c6a593451c4190818b5140576a25755871d4))
    - fix edge case while calculating max_prefixes ([`b1cc6f5`](https://github.com/maidsafe/safe_network/commit/b1cc6f56f16b715cec2013ec07ef83dcc0df03d0))
    - Merge #1901 ([`7d88182`](https://github.com/maidsafe/safe_network/commit/7d881828c14db3aed471c2591919144c01c64301))
</details>

## v0.72.7 (2022-12-17)

<csr-id-a8d7efe0b55280756811c571525b2947ca268bfc/>

### Chore

 - <csr-id-a8d7efe0b55280756811c571525b2947ca268bfc/> sn_interface-0.16.5/sn_node-0.72.7

### New Features

 - <csr-id-8aa694171b1dd5c2505259e67d6e3434ee94d213/> prevent joins from nodes behind NAT

### Bug Fixes

 - <csr-id-3683215412906f58736e5a5e77c140b7b82d35a9/> relax pattern matching on JoinRequest
 - <csr-id-40d72d1f2915c322e8f9b181c3af2c05f6f9a075/> remove closed incoming connections from Link store

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.5/sn_node-0.72.7 ([`a8d7efe`](https://github.com/maidsafe/safe_network/commit/a8d7efe0b55280756811c571525b2947ca268bfc))
    - Merge #1900 ([`9650289`](https://github.com/maidsafe/safe_network/commit/96502896245fc41a3ef619d3959f4938413e938c))
    - remove closed incoming connections from Link store ([`40d72d1`](https://github.com/maidsafe/safe_network/commit/40d72d1f2915c322e8f9b181c3af2c05f6f9a075))
    - relax pattern matching on JoinRequest ([`3683215`](https://github.com/maidsafe/safe_network/commit/3683215412906f58736e5a5e77c140b7b82d35a9))
    - prevent joins from nodes behind NAT ([`8aa6941`](https://github.com/maidsafe/safe_network/commit/8aa694171b1dd5c2505259e67d6e3434ee94d213))
</details>

## v0.72.6 (2022-12-16)

<csr-id-4ce57cc7c349c209d2fa60d876706ad15dd07a04/>

### Chore

 - <csr-id-4ce57cc7c349c209d2fa60d876706ad15dd07a04/> sn_node-0.72.6/sn_cli-0.68.2

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.6/sn_cli-0.68.2 ([`4ce57cc`](https://github.com/maidsafe/safe_network/commit/4ce57cc7c349c209d2fa60d876706ad15dd07a04))
    - Merge #1896 #1897 ([`b4f5700`](https://github.com/maidsafe/safe_network/commit/b4f57007619856a368f635aac5a0e865d3f35bc5))
</details>

## v0.72.5 (2022-12-16)

<csr-id-244349ee077775910cf386a82ca7ff22dbf7ee2f/>
<csr-id-aedc93e5a809c110bb40740df5ca5d688b26e8d4/>
<csr-id-7950fc96036e925820910bbd7438968db9f14862/>

### Chore

 - <csr-id-244349ee077775910cf386a82ca7ff22dbf7ee2f/> sn_node-0.72.5
 - <csr-id-aedc93e5a809c110bb40740df5ca5d688b26e8d4/> remove the looping system load log
 - <csr-id-7950fc96036e925820910bbd7438968db9f14862/> change limit-section to still allow joins when needed

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.5 ([`244349e`](https://github.com/maidsafe/safe_network/commit/244349ee077775910cf386a82ca7ff22dbf7ee2f))
    - remove the looping system load log ([`aedc93e`](https://github.com/maidsafe/safe_network/commit/aedc93e5a809c110bb40740df5ca5d688b26e8d4))
    - Merge branch 'main' into AlternateNetworkLimitation ([`5354f5e`](https://github.com/maidsafe/safe_network/commit/5354f5e9a0c0ac2145c5c5063b28d48f7bc3a30d))
    - Merge #1892 ([`5ebdfb8`](https://github.com/maidsafe/safe_network/commit/5ebdfb8fd659f2589d4b5de704c61b4a6975efe9))
    - change limit-section to still allow joins when needed ([`7950fc9`](https://github.com/maidsafe/safe_network/commit/7950fc96036e925820910bbd7438968db9f14862))
</details>

## v0.72.4 (2022-12-16)

<csr-id-e0f052e46dcfb2beda4edc414fa7f560726fcd73/>
<csr-id-837c70af642b904f42121aa0a08f697eba551826/>
<csr-id-e764fd015664dc565bc5ea2168a0879f718e3e08/>
<csr-id-37651b8659a641c18775f151e77e5d5ee4903f51/>
<csr-id-56d905fb67135d891e1ce44955ef4744212645b7/>
<csr-id-e9c58e34deaad1e7448399e5ae0de81926f4445e/>

### Chore

 - <csr-id-e0f052e46dcfb2beda4edc414fa7f560726fcd73/> revert change split detection instead of size
   This reverts commit 38ebca089ed7134a63d9fefbf69f4f791b5858fb.

### Chore

 - <csr-id-837c70af642b904f42121aa0a08f697eba551826/> sn_interface-0.16.4/sn_node-0.72.4
 - <csr-id-e764fd015664dc565bc5ea2168a0879f718e3e08/> dont block replication channel
   Move the actual data replication off thread to unblock the channel
 - <csr-id-37651b8659a641c18775f151e77e5d5ee4903f51/> increase data replication batch size
 - <csr-id-56d905fb67135d891e1ce44955ef4744212645b7/> sort data for replication before we start batching
   This means we should send closest data first
 - <csr-id-e9c58e34deaad1e7448399e5ae0de81926f4445e/> refactor flow_ctrl / replication sender

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release.
 - 6 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.4/sn_node-0.72.4 ([`837c70a`](https://github.com/maidsafe/safe_network/commit/837c70af642b904f42121aa0a08f697eba551826))
    - dont block replication channel ([`e764fd0`](https://github.com/maidsafe/safe_network/commit/e764fd015664dc565bc5ea2168a0879f718e3e08))
    - increase data replication batch size ([`37651b8`](https://github.com/maidsafe/safe_network/commit/37651b8659a641c18775f151e77e5d5ee4903f51))
    - sort data for replication before we start batching ([`56d905f`](https://github.com/maidsafe/safe_network/commit/56d905fb67135d891e1ce44955ef4744212645b7))
    - refactor flow_ctrl / replication sender ([`e9c58e3`](https://github.com/maidsafe/safe_network/commit/e9c58e34deaad1e7448399e5ae0de81926f4445e))
    - Merge #1895 ([`266a11a`](https://github.com/maidsafe/safe_network/commit/266a11aba08c7a7a0673499cf94144273dd48111))
    - revert change split detection instead of size ([`e0f052e`](https://github.com/maidsafe/safe_network/commit/e0f052e46dcfb2beda4edc414fa7f560726fcd73))
</details>

## v0.72.3 (2022-12-15)

<csr-id-f1b929344db992ace9e05aeffc96cd81e72b1ae0/>
<csr-id-3c46851572dfdd0125a48d4e04774eb3bf2e9969/>

### Chore

 - <csr-id-f1b929344db992ace9e05aeffc96cd81e72b1ae0/> change wording of lib.rs docs

### Chore

 - <csr-id-3c46851572dfdd0125a48d4e04774eb3bf2e9969/> sn_node-0.72.3

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.72.3 ([`3c46851`](https://github.com/maidsafe/safe_network/commit/3c46851572dfdd0125a48d4e04774eb3bf2e9969))
    - change wording of lib.rs docs ([`f1b9293`](https://github.com/maidsafe/safe_network/commit/f1b929344db992ace9e05aeffc96cd81e72b1ae0))
</details>

## v0.72.2 (2022-12-15)

<csr-id-c42f6361cd6366c91d2e0c232abf0c070ab27ab7/>

### Chore

 - <csr-id-c42f6361cd6366c91d2e0c232abf0c070ab27ab7/> sn_interface-0.16.2/sn_node-0.72.2

### Bug Fixes

 - <csr-id-fc90ff09ae51eda433e28536289176104cb62872/> send all data batches, not just the first
 - <csr-id-093b97cf39e9b251055426c0c6bc050ba6135885/> update network knowledge AFTER sent join request

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.2/sn_node-0.72.2 ([`c42f636`](https://github.com/maidsafe/safe_network/commit/c42f6361cd6366c91d2e0c232abf0c070ab27ab7))
    - update network knowledge AFTER sent join request ([`093b97c`](https://github.com/maidsafe/safe_network/commit/093b97cf39e9b251055426c0c6bc050ba6135885))
    - Merge #1889 ([`d0ab3b0`](https://github.com/maidsafe/safe_network/commit/d0ab3b05a0200d266b55cdb9477cef35b071f186))
    - send all data batches, not just the first ([`fc90ff0`](https://github.com/maidsafe/safe_network/commit/fc90ff09ae51eda433e28536289176104cb62872))
    - Merge #1888 ([`fc0be25`](https://github.com/maidsafe/safe_network/commit/fc0be25da404d64a33b1addb6499033883f5035a))
</details>

## v0.72.1 (2022-12-15)

<csr-id-841a004786767c53ab9d60d4a310299d535b86bc/>
<csr-id-89e1e40ed9100b28a1ad5ed196620a6d6415706e/>
<csr-id-52be4b12a07f3851474b713f66c821defd7a29f5/>
<csr-id-6e4fce574b8f1e916ea3bd941ed7ecaec3a62931/>
<csr-id-82c0cf683f8052374eafbb859176c69d52956c72/>
<csr-id-6e84b0aa25bfd6ecff45812cc32e189245b8ec3a/>

### Chore

 - <csr-id-841a004786767c53ab9d60d4a310299d535b86bc/> make stream.finish non blocking where we can
 - <csr-id-89e1e40ed9100b28a1ad5ed196620a6d6415706e/> ignore qp2p::SendStream::finish errors
   They dont mean a msg was not sent.

### Refactor

 - <csr-id-6e4fce574b8f1e916ea3bd941ed7ecaec3a62931/> minor refactor within comm mod

### Chore

 - <csr-id-82c0cf683f8052374eafbb859176c69d52956c72/> sn_interface-0.16.1/sn_client-0.77.1/sn_node-0.72.1/sn_api-0.75.1
 - <csr-id-6e84b0aa25bfd6ecff45812cc32e189245b8ec3a/> removing unused payload_debug field from msgs

### New Features

 - <csr-id-38ebca089ed7134a63d9fefbf69f4f791b5858fb/> change split detection instead of size

### Bug Fixes

 - <csr-id-b67adb74f03e4e8784ec4d391032d9a1eacb847d/> write all Register cmds to disk even if one or more failed
   - When writting Register cmds log to disk, we log and return the error for
   any of them failing, but we don't prevent the rest to be written to disk.

### Other

 - <csr-id-52be4b12a07f3851474b713f66c821defd7a29f5/> allow client tests to be multithreaded

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 10 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 8 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.1/sn_client-0.77.1/sn_node-0.72.1/sn_api-0.75.1 ([`82c0cf6`](https://github.com/maidsafe/safe_network/commit/82c0cf683f8052374eafbb859176c69d52956c72))
    - minor refactor within comm mod ([`6e4fce5`](https://github.com/maidsafe/safe_network/commit/6e4fce574b8f1e916ea3bd941ed7ecaec3a62931))
    - removing unused payload_debug field from msgs ([`6e84b0a`](https://github.com/maidsafe/safe_network/commit/6e84b0aa25bfd6ecff45812cc32e189245b8ec3a))
    - Merge #1887 ([`2b66221`](https://github.com/maidsafe/safe_network/commit/2b6622144178d6a67db1392dfd4929232cb4ca62))
    - write all Register cmds to disk even if one or more failed ([`b67adb7`](https://github.com/maidsafe/safe_network/commit/b67adb74f03e4e8784ec4d391032d9a1eacb847d))
    - Merge #1884 ([`3f3175e`](https://github.com/maidsafe/safe_network/commit/3f3175ed7e006d68176670b31ddded2cef024b15))
    - make stream.finish non blocking where we can ([`841a004`](https://github.com/maidsafe/safe_network/commit/841a004786767c53ab9d60d4a310299d535b86bc))
    - allow client tests to be multithreaded ([`52be4b1`](https://github.com/maidsafe/safe_network/commit/52be4b12a07f3851474b713f66c821defd7a29f5))
    - ignore qp2p::SendStream::finish errors ([`89e1e40`](https://github.com/maidsafe/safe_network/commit/89e1e40ed9100b28a1ad5ed196620a6d6415706e))
    - change split detection instead of size ([`38ebca0`](https://github.com/maidsafe/safe_network/commit/38ebca089ed7134a63d9fefbf69f4f791b5858fb))
</details>

## v0.72.0 (2022-12-13)

<csr-id-cc9b9e0f09ceed4af7ca6d0a0913cfda1e184eef/>
<csr-id-06041ee529171e51581499a585effac67f037d17/>
<csr-id-ede33cc7a590e356232acd5feff6b44ff647517f/>
<csr-id-3dc20bc9424c295bd9038b2847fe01f639b83407/>
<csr-id-c0b9b274b60b8c6ecf905c6294e5afb86c69e16f/>
<csr-id-51bc2ca02dd8fe9634c9a68889c5eedc1e52c31e/>
<csr-id-f63d1896e554b72d6a5af463b7b6e7992a1aa8ed/>
<csr-id-8bd83c64ca8cccc78dfe4641e522b4a02f03cbb8/>
<csr-id-b4164cf0782c3083ba2a4e0f9b3f12445747730e/>
<csr-id-93ef0c1b78d9ec656aa8df85d0a741a26d09f780/>
<csr-id-f3fbf83dc15eb791ccc134d780042484a51ab90e/>
<csr-id-64b6c35105168b9fa4b0fb9d626ed9552fd0bed3/>
<csr-id-57662862a8f59b70b9ca41515ff775003ea803fa/>
<csr-id-c84f844873208af49e1743199ca75c015d0e14c7/>
<csr-id-e5b0dda1315a5299131cacd135b1d1ab66ed7073/>
<csr-id-0ddfb0c2ceffbf69fca172ec555abe1495be1980/>
<csr-id-f06b3e75ce97e7c749d2969276ad6533369806bb/>
<csr-id-986fa81fb00daaa37a17e55b459d66efacfff650/>
<csr-id-4392fd265faec0b8e6c637342bd71119322d53b4/>
<csr-id-78371980ebf3f1b65ec62c49c35d8a6c015c5537/>
<csr-id-7ac1e8abc3d792d5ea69e1e77fc7bdee63268f26/>
<csr-id-762170be83e21659cb8170987a5c2cdd9f2c14e6/>
<csr-id-98d0d4cf79919fb56854d6f4492af6740df3587d/>
<csr-id-5e81ac4fb8a2312eb546a4b86e71be05df7c4e26/>
<csr-id-6cf816f4e3ce1e81af614ba84de83ccf13e8e402/>
<csr-id-c4cf647221cdf0ffaeca4d4a8da82d7dc7b21ca6/>
<csr-id-f33602e0959a0ea86e4802376bd2f073cd8ea57c/>
<csr-id-8f072f2bcfd48e5c4047e32332c29e231632efae/>
<csr-id-200cf34c1cfc26b219ee12b9e8b0478fc5415745/>
<csr-id-c436078af2afca7468bb6d8a579f457dfb662bf3/>
<csr-id-3c8ac507a85e7c11219fdab8a61eadde89f8582f/>
<csr-id-18c0444be06d84c386b18031e59f2162fef81d89/>
<csr-id-e37180bd65d26c6f4737dabeae8a210674a82bdf/>
<csr-id-197bb663e088309150a6f85e5c86877e5e0ad2d8/>
<csr-id-bd2baa03f889fa94fab68ecf9f1acffb71aa993d/>
<csr-id-7bc55f7b1a819c4609a85176d99f287b54a9ad44/>
<csr-id-7d79e3e53ce49dc664d4a9215e5cc02839540879/>
<csr-id-4df40864b5ee030edf16bad0becaa08a07c15fd9/>
<csr-id-38b8f55121d8b7c461efa6dd0c0407c4fae93418/>
<csr-id-a55b74b4c8f9bede3c91a9426d4687df01138257/>
<csr-id-d43bac5ce07b0f08766858eadc4b8f98f9bcfc12/>
<csr-id-667009dc02e6bb17bfaa60e2374d5ab7b75a7be5/>
<csr-id-af9d3abc665a7490492417aebe974cb5ef839d53/>
<csr-id-860f326a9baf7e62d191eec13359fa5313e6956d/>
<csr-id-ee824e7785b8da770b5aa6bba3415a274a4e0d68/>
<csr-id-b223bd532bba649c6dd09cf26166758d6ff56893/>
<csr-id-2e7fde627f41dfff7b5b8cd049f26ee127269bfe/>
<csr-id-c71fcd1fd61a4a2f9075d3d3e5f922b6d06644e6/>
<csr-id-2b9268c2aff5aae5eb1584d2698f282c8beae73e/>
<csr-id-62d472354d65f1cd9001f2df00cbf0c82734b969/>
<csr-id-58d656949c09dc3d6445d899725fe2c41d46c216/>
<csr-id-d9bfaef416bc357bdc32632eb0ba263b6995d613/>
<csr-id-0ef5a2cfd82de58f02a23a96a305b172f27c33c8/>
<csr-id-c3aa9414b4232dbaf356cec3fb71460f4d916b4f/>
<csr-id-f2dff3636d3bf1446af53790a42a46473079698f/>
<csr-id-1bf23ff2d00b66232267403f94d3fa133416fdd3/>
<csr-id-d19c0b204c368a967a2f6de9d68e1db0caebb71a/>
<csr-id-ebb7b793b1e7b4ba439f3f93b5d7ac32e9acc2c2/>
<csr-id-0bd38c524b5f6685e3a8ad21b3ae793d097c6b6d/>
<csr-id-745ae58408997504dd04663a1c8bea8b688ded66/>
<csr-id-36686c794eccb84573e741fdbe6f9af2eb18c8c9/>
<csr-id-5b0b8589bc9c90fac6285f626a46907b2dd0e819/>
<csr-id-3b1548dd8b7f77538f47d6bca6338239899d18a3/>
<csr-id-07e0fc8c7776efdabed61b451258709e4dc8f3d0/>
<csr-id-560d1850783439b7e5affdc5d86a61cbf619fca0/>
<csr-id-8b8f9be3e18b1ae2cf94b9e2dbd17f925b7580f6/>
<csr-id-a05e6a28d890da5103e12cfae4ee54ffc0870be3/>
<csr-id-bebd96de67906ab0d49a4a42edb9b62a2f1d88f2/>
<csr-id-f65d4d457ef8b55fc87f6669f6d8380b371366a1/>
<csr-id-27cbeb647b90e7105e5b650b436944d3cdd813c5/>
<csr-id-865b459ab0cfcfbee91298787b5c2c4425e5b6a0/>
<csr-id-8b50947fe3f2eeb6bfe6350f754303ebba9c09dd/>
<csr-id-68929bda1832d85c1a9f43d904f1c687a3bc9dc4/>
<csr-id-a05d7ca9b7d2319085a2dea9119735e3e44f50c1/>
<csr-id-98abbbe7af8c870faa22d62819691054e07df718/>
<csr-id-6185599884f49a71343c67e625a1a9dd9a75393d/>
<csr-id-a9e2c6962b6646cc6b4ac24bf9da50dbf48d3f60/>
<csr-id-7ed94ba0599325900246743334b4b821331cba86/>
<csr-id-999bfaa633e9fcd2a9bc47b90b128f0e3946a951/>
<csr-id-f856d853c0166f35bb2b98b24e3f3c8c09783b2d/>
<csr-id-a1263ecef879fbdba932588cc37ea63959a0435b/>
<csr-id-914b816921fc8f4da99bbf77a9fdd91d896411a4/>
<csr-id-9a6f4a3cf20852f4b5604bf08a04aba592dca0fa/>
<csr-id-e57d83235f60a16bd7e1ee801f35a599113dc71a/>
<csr-id-0f11b2ed16c765bee3e25f2f25b374e3f06f8e8f/>
<csr-id-5a539a74d0dcca7a8671910d45bbb08ca4382671/>
<csr-id-411ea371660b5f76a5c3f887f78331e58f8b6961/>
<csr-id-177407d1a0e817ea5fff0a98f74cd358f25ac727/>
<csr-id-014b132923c5affebe2c485aa3e791a22a6b90c5/>
<csr-id-3c05e616e54f423b66d35114668dac35ab5eae14/>
<csr-id-ec88642644b76c0751db25c9b02b068cd77318d1/>
<csr-id-37785c72943ad126ef8fc94f4a6c2139ae478d69/>
<csr-id-fc7670e6cc4284d7cf5614185e8e93ffb9f0ba37/>
<csr-id-51425951e8a66a8fd938a8dd2378b583cc80fb94/>
<csr-id-6276e931291afd518648a47bc10374640b462cad/>
<csr-id-8955514b2d08d2f7fbb4ebbf48d9807a9d5127ac/>
<csr-id-e0803687a1b3b374efdf040cac0ecd5c6b4fc60a/>
<csr-id-1601cde194cb1f3bab2f6b54cc0ca784adb912b8/>
<csr-id-151a22f63a0aaaf94070f3bd0e1f6bd2f9239856/>
<csr-id-70d848a43b6df02812195845434849b98f409367/>
<csr-id-a7f017cc55da5cba53e8f10063afefc61ea5635a/>
<csr-id-a0b2df5a0b12c70872dfc854d660afd0cf8b21aa/>
<csr-id-09c48916a7f2145c1d3cd091d6219fb7150fe1c2/>
<csr-id-7654f58dbde0c44fd799da16b2a3c4e3a73217df/>
<csr-id-9a1cdf6f0135ce53f43a48c4346aff9023ccad33/>
<csr-id-cbf5ed4b1065d5d4471bc27291c054f48b64678e/>
<csr-id-3d72e4b71c079f7ddd8be08642165e53ebf987f6/>
<csr-id-e1e161ba54ed8e0af298814f0da5b953ff053c93/>
<csr-id-9992d9701ecadff2b7682e47387014b9d11dba63/>
<csr-id-f4c3808f582cf06d58303383296af4ab7a13f0df/>
<csr-id-2e937145c39039ee55505f00637cf484943f4471/>
<csr-id-80446f5d9df88d5915dcf1d3ea2c213c22e40c14/>
<csr-id-3f52833a8ce977aa79268ecaac61070f01e9c374/>
<csr-id-77cb17c41bbf258c3f1b16934c4c71b5e5ad2456/>
<csr-id-100e2ae70d21e141e1ebbc324f8b06e3d3f1a01c/>
<csr-id-e973eee96c9065ce87a1fa65ae45d9be8d6f940c/>
<csr-id-03da7f67fff1fa5bb06d60a66dfdb531506dec4c/>
<csr-id-859fc30fa70ce41ceb910e0352c71dda5c5501ce/>
<csr-id-b550ed00dd3e3af878758aea1a2efb6eba0e8d66/>
<csr-id-b2b661d3e891403bf747228985930b301c9ad28f/>
<csr-id-05846c1741318f51394090c66e7c0ddf911e31ee/>
<csr-id-30bedf882f1f642592703b92be0966035ef01068/>
<csr-id-b6474691ea6af5ee441b02f6cb9c3cf2b8f97459/>
<csr-id-c7de08209c659ec93557d6ea10e0bcd8c3b74d8b/>
<csr-id-230a6ed7f1f4193fa36b2fbb83bea072f4944c1d/>
<csr-id-2b5119423248ce95021a0b1ba826b426d79d7e61/>
<csr-id-e263a66e7db336cf5868b33dde507abbbc25f81c/>
<csr-id-46068295e8e1c760ebab68f4338b14cf11588605/>
<csr-id-994b1ef49ffee7c1b02b6361297bacd190e9b5e2/>
<csr-id-f0ae5773669937b3c824e98557ecff5994cb1df7/>
<csr-id-69d0687c4afc8d8f6e088663dfa4482c4d72ade0/>
<csr-id-8d79f6fa586758fd75e5e66be95d9d240d9d6551/>
<csr-id-4c6ae07d217cbed09d34adda1f3859191eb581c0/>
<csr-id-636b38889c33b34ffdd391d479605938664bf731/>
<csr-id-006a4f801f585594142e5ac4a9b19d218676c8b3/>
<csr-id-2c8f9331ab0f7f63f7d14f7113ecf9ad5e6c4618/>
<csr-id-aa53ee414631dd4faff600101e909fa98f6885fe/>
<csr-id-298b5d86e4ea331f1c4c7213a724f275e01a06d1/>
<csr-id-f66e02ebbcc0298692cfc3d4d4faf69ba2ba1f8f/>
<csr-id-5179cf2dec47295f9673212efa6e23e9531e5ea3/>
<csr-id-6ca7f4377308a0dd47dbd17a3d01b07321d9b8a9/>
<csr-id-e8ab025a3454005890418b10a50560b3c65fd68f/>
<csr-id-9bee893e381375b6de65d77357e13f1897b9d757/>
<csr-id-3dc0bb1f0d8d04c8a92a75eab73e10721b105a10/>
<csr-id-d22ad7c46753c3e5d3f3c50da4546c80e302dee9/>
<csr-id-bc2c4ee21335b627e3e998dd56209f72f20aac90/>
<csr-id-9414beed24795db97277eb0c15fe24910f4220d7/>
<csr-id-4a466e5a14b61f0dcf5467298d11d831a9a8d7e2/>
<csr-id-0e73ec1ce28c5e2206eb84a6a24d996a23affd2f/>
<csr-id-9f539e9a8dd9c22e7440539114b2fbdaaeb34515/>
<csr-id-3353ab617b438ca12cdac68a1f7956e3050eefcf/>
<csr-id-093ea5bfc200f940662c5c0e458c38c5c77294a9/>
<csr-id-4b6569ab2a9face420385d29d7baab31d8ca4d1e/>
<csr-id-9f8ecf90470ac18de31a956c1eee5f9f2d4c77a7/>
<csr-id-30670403d5466f7a052d753136e72e2720d2954d/>
<csr-id-93c7a054d87df7054224664c4a03c4507bcfbae6/>
<csr-id-9fad752ce1849763ae16cdb42159b9dccf1a13d0/>
<csr-id-633dfc836c10eafc54dedefc53b2cbc9526970bb/>
<csr-id-1bfa58e5487b3c8cafdef1593c2037b331c30dd1/>
<csr-id-ab22c6989f55994065f0d859b91e141f7489a722/>
<csr-id-32744bcf6c94d9a7cda81813646560b70be53eb2/>
<csr-id-ba78a41f509a3015a5319e09e1e748ac91952b70/>
<csr-id-72abbfbc583b5b0dc99a0f7d90cb4d7eb72bd8c4/>
<csr-id-dcb76019971c2765c54ba04e22c1a7d2d1ad2d47/>
<csr-id-8f355749c85e5d117e304bd499da1144b00e6809/>
<csr-id-85c30ffa90488f36c32516ca97cea3b246cffbcb/>
<csr-id-80c9f7a74a5c7f80a898f97729a813b6d2445f73/>
<csr-id-3302aee7a41abd58b6deba18cc690c5e74aabff4/>
<csr-id-702a03fae21fa517b02a1c0271f0617ca5c4f85c/>
<csr-id-3215110b021aaa7d3b755b7e80432aeed1e0b436/>
<csr-id-acaa90a13d598915bafc3584c70826f233d89881/>
<csr-id-d87bbddfd92ecd802b27531cbcbc13c7271c6a10/>
<csr-id-07d0991fed28d49c9be85d44a3343b66fac076d9/>
<csr-id-28c7a9f7b2ce43d698288c12e35eb6a7026a4163/>
<csr-id-f289de53f3894a58a6e4db51ce81aaf34f276490/>
<csr-id-452ef9c5778ad88270f4e251adc49ccbc9b3cb09/>
<csr-id-85f4d00e81ac5bf67b6be89d7ff51b7bb1060ed6/>
<csr-id-322721635724f9dfa9351dd31e6883a32a330fe4/>
<csr-id-dd45c8f42b4f8674eeaea90aa27a465bd3bae0a2/>
<csr-id-04605882c324057deef5bec05bdae90e15b5a940/>
<csr-id-f88d542b64c3bde832968c99dcfe38e99d85b4f5/>
<csr-id-de2479e5fe56b3ebf526215b4860ce9f64c7f20c/>
<csr-id-0a85816f4168024b2892fd77760580b1f8d2d9e9/>
<csr-id-072c5d4c5de7810a0837144853435e2ff2d091d0/>
<csr-id-610880711da814c7717c665e9cb34a729bda5797/>
<csr-id-1152b2764e955edd80fb33921a8d8fe52654a896/>
<csr-id-60e333d4ced688f3382cde513300d38790613692/>
<csr-id-6343b6fd21fe3bf81412d922da5e14b2c8b6f3c5/>
<csr-id-0176a56a311dd8450f7cd845bc37cc28a7b11c0d/>
<csr-id-8bf032fff0ba48816311d6ea6967e3c300aedccf/>
<csr-id-73f5531790ef8817ed3551fd9e4bcbcc7fc6f4f9/>
<csr-id-4efda958c765e41b39444f397c500db95716ffa2/>
<csr-id-058acaa701792ee58913b2d4524c759367fb65fc/>
<csr-id-ffbb607835a87683280e4f16b0e2e1b5ca2fd0a1/>
<csr-id-774c908b6c44b702259782cefdef5d4cdd228385/>
<csr-id-42c4008aeadac297c212a65cde7109a055f61cec/>
<csr-id-f53337e6e0c7c4f804489f0d370d4cc97331597f/>
<csr-id-edea3fb90697837317f7f050913fefd534938bfd/>
<csr-id-5dfeab95bb67454cc257028185dbbf7e1f98d351/>
<csr-id-ac9d1b257db48ab336d2c80e4ff573208cbd4c6c/>
<csr-id-6be0ea16b0ffe2c153c6a13f36916a91fb58cd05/>
<csr-id-fc0c7512144c0c42184b6ae1b5a64e4d894d0eab/>
<csr-id-80917f19125222ce6892e45487f2abe098fefd7a/>
<csr-id-bdf50e7ad1214ef4bb48c0a12db8a7700193bb2a/>
<csr-id-a973b62a8ef48acc92af8735e7e7bcac94e0092f/>
<csr-id-d550b553acbd70d4adb830a0600f7da7b833ee18/>
<csr-id-66a15497201ef63c52721a6ba8ce4840393f03bc/>
<csr-id-dcf40cba6ae0f73476d3095a01aca5c3cade031c/>
<csr-id-ea1d0490f0b67a9f39bd98b2bd5830a0f63fbf6e/>
<csr-id-e3bb817e20843f68ee21e9a5dd7e52c8a6e92b88/>

### Chore

 - <csr-id-cc9b9e0f09ceed4af7ca6d0a0913cfda1e184eef/> expand some message handling log
 - <csr-id-06041ee529171e51581499a585effac67f037d17/> warn if dysf channels are closed
 - <csr-id-ede33cc7a590e356232acd5feff6b44ff647517f/> simplify
 - <csr-id-3dc20bc9424c295bd9038b2847fe01f639b83407/> add feat to limit section size
   Allows for one less variable during testnets
 - <csr-id-c0b9b274b60b8c6ecf905c6294e5afb86c69e16f/> increase node to 50gb
   This should allow for longer data retention tests for now (while avoiding more nodes joining
 - <csr-id-51bc2ca02dd8fe9634c9a68889c5eedc1e52c31e/> standalone methods to get secret keys
 - <csr-id-f63d1896e554b72d6a5af463b7b6e7992a1aa8ed/> update comments
 - <csr-id-8bd83c64ca8cccc78dfe4641e522b4a02f03cbb8/> remove `network_utils` module
 - <csr-id-b4164cf0782c3083ba2a4e0f9b3f12445747730e/> spawn new thread for dysf channel calls
   This should avoid any awaiting due to full channel during
   dkg/churn events
 - <csr-id-93ef0c1b78d9ec656aa8df85d0a741a26d09f780/> use STANDARD_CHANNEL_SIZE in node. increase to 100_000
   Channels could be an uniwitting blocker as we await for capacity.
   
   If we expect channels to be cleared in a timely fashion,
   we should aim not to have an artifical bottleneck.
   
   100_000 chosen as vast majority of channelsdon't require a lot of data.
   And anything large waiting, will just be in a channel elsewhere taking up memory,
   so this way we avoid slowing down node processing
 - <csr-id-f3fbf83dc15eb791ccc134d780042484a51ab90e/> don't store client peer sessions
 - <csr-id-64b6c35105168b9fa4b0fb9d626ed9552fd0bed3/> pass in context to NodeMsg handling
   Thus reducing read lock calls
 - <csr-id-57662862a8f59b70b9ca41515ff775003ea803fa/> remove more periodic logs
 - <csr-id-c84f844873208af49e1743199ca75c015d0e14c7/> minor improvements to sn_node bin error messages
 - <csr-id-e5b0dda1315a5299131cacd135b1d1ab66ed7073/> minor logging improvements to help debug msgs arriving/processing on client and nodes
 - <csr-id-0ddfb0c2ceffbf69fca172ec555abe1495be1980/> rebase changes
 - <csr-id-f06b3e75ce97e7c749d2969276ad6533369806bb/> upgrading qp2p to version 0.31.0
 - <csr-id-986fa81fb00daaa37a17e55b459d66efacfff650/> another log removed
 - <csr-id-4392fd265faec0b8e6c637342bd71119322d53b4/> less logs
 - <csr-id-78371980ebf3f1b65ec62c49c35d8a6c015c5537/> rename methods, update comments
 - <csr-id-7ac1e8abc3d792d5ea69e1e77fc7bdee63268f26/> cleanup some overzealous log msgs
 - <csr-id-762170be83e21659cb8170987a5c2cdd9f2c14e6/> remove duplicate context in handle_ae_msg
 - <csr-id-98d0d4cf79919fb56854d6f4492af6740df3587d/> pr suggestions
 - <csr-id-5e81ac4fb8a2312eb546a4b86e71be05df7c4e26/> remove unusued `Error` module
 - <csr-id-6cf816f4e3ce1e81af614ba84de83ccf13e8e402/> remove unusued `Result` from methods
 - <csr-id-c4cf647221cdf0ffaeca4d4a8da82d7dc7b21ca6/> remove Membership from NodeContext.
   Heaptracking has shown this to be ~80% of mem used, and moost of the time membership in the context is unused. Instead we now manually clone membership as/when we need it.
 - <csr-id-f33602e0959a0ea86e4802376bd2f073cd8ea57c/> consume context where makes sense
 - <csr-id-8f072f2bcfd48e5c4047e32332c29e231632efae/> reduce AE node msg handling locking
 - <csr-id-200cf34c1cfc26b219ee12b9e8b0478fc5415745/> import organisation
 - <csr-id-c436078af2afca7468bb6d8a579f457dfb662bf3/> Remove locks in data replication flows.
   Moves the replication flow to be outwith of node, using channels to gather data and send it in another thread entirely. This should remove another cause of serious node.write().lock usage
 - <csr-id-3c8ac507a85e7c11219fdab8a61eadde89f8582f/> remove is_not_elder from NodeContext
 - <csr-id-18c0444be06d84c386b18031e59f2162fef81d89/> rename MyNodeSnapshot -> NodeContext
 - <csr-id-e37180bd65d26c6f4737dabeae8a210674a82bdf/> cleanup unneeded code block
 - <csr-id-197bb663e088309150a6f85e5c86877e5e0ad2d8/> cleanup commented code
 - <csr-id-bd2baa03f889fa94fab68ecf9f1acffb71aa993d/> move (and rename) matching_section to MyNodeSnapshot
 - <csr-id-7bc55f7b1a819c4609a85176d99f287b54a9ad44/> node.get_snapshot -> snapshot
 - <csr-id-7d79e3e53ce49dc664d4a9215e5cc02839540879/> retry join with new name if we get a completely new SAP
 - <csr-id-4df40864b5ee030edf16bad0becaa08a07c15fd9/> add in SetStorageLevel cmd to remove mut on DataStorage for normal replication ops
 - <csr-id-38b8f55121d8b7c461efa6dd0c0407c4fae93418/> Pass around MyNodeState to avoid holding locks
   For longer running message handling, we now pass around the inital
   MyNodeState. This avoids a tonnnn of read locks and therefore hopefully
   prevents holding up write and reads needlessly.
 - <csr-id-a55b74b4c8f9bede3c91a9426d4687df01138257/> replace `TestSAP` with `TestSapBuilder`
 - <csr-id-d43bac5ce07b0f08766858eadc4b8f98f9bcfc12/> update self_update to 0.32
 - <csr-id-667009dc02e6bb17bfaa60e2374d5ab7b75a7be5/> remove duplicate strum/strum_macros/heck deps
 - <csr-id-af9d3abc665a7490492417aebe974cb5ef839d53/> bump tokio-util 0.6 to 0.7
 - <csr-id-860f326a9baf7e62d191eec13359fa5313e6956d/> criterion 0.3 -> 0.4, tracing-subscriber 0.2 -> 0.3
 - <csr-id-ee824e7785b8da770b5aa6bba3415a274a4e0d68/> bump blsttc to 8.0.0
 - <csr-id-b223bd532bba649c6dd09cf26166758d6ff56893/> remove outgoingmsg::client variant
 - <csr-id-2e7fde627f41dfff7b5b8cd049f26ee127269bfe/> send client error response if storage fails
 - <csr-id-c71fcd1fd61a4a2f9075d3d3e5f922b6d06644e6/> Send AE retries to all elders in updated section
   This should avoid issues where we always send to the same node
   regardless of initial target elder (as "closest" node in a new prefix
   will be the same regardless of the initial elder)
 - <csr-id-2b9268c2aff5aae5eb1584d2698f282c8beae73e/> retry loop for bidi initialisation
 - <csr-id-62d472354d65f1cd9001f2df00cbf0c82734b969/> elders wait for all storage reqs to be successful
 - <csr-id-58d656949c09dc3d6445d899725fe2c41d46c216/> remove adult healthcheck for now
   Refactor for separate 'replicate' and 'replicate and respond to client'
   flows.
 - <csr-id-d9bfaef416bc357bdc32632eb0ba263b6995d613/> reset keep_alive flag to be optional
 - <csr-id-0ef5a2cfd82de58f02a23a96a305b172f27c33c8/> add in error msg in the event of missing stream during adult query
 - <csr-id-c3aa9414b4232dbaf356cec3fb71460f4d916b4f/> add mising sleep before retrying connection.
   Dont mark connections as retried unless link has no more candidates
 - <csr-id-f2dff3636d3bf1446af53790a42a46473079698f/> tweaks to reduce use of async
 - <csr-id-1bf23ff2d00b66232267403f94d3fa133416fdd3/> set nodes join interval to 30secs for testnet in sn-api tests job
   - Upgrading qp2p to v0.30.1.
   - Include bi-stream id in logs both on client and node sides.
   - Removing unused sn_api and sn_client test helpers.
   - Adding a 1sec delay in sn-api tests before querying uploaded data.
 - <csr-id-d19c0b204c368a967a2f6de9d68e1db0caebb71a/> move conn acquisition off thread too.
   This was currently potentially a blocker if there'd been connection issues.
   And might prevent other messages being sent
 - <csr-id-ebb7b793b1e7b4ba439f3f93b5d7ac32e9acc2c2/> Comm is clone. Dont close endpoint on drop.
   We are currently closing our endpoint when we dropp a comms instance.
   This could lead to closed connections and other unintended consequences
   if we drop a clone while another instance persists
 - <csr-id-0bd38c524b5f6685e3a8ad21b3ae793d097c6b6d/> remove reachability check from inital join
 - <csr-id-745ae58408997504dd04663a1c8bea8b688ded66/> don't hold read lock over reachability check
 - <csr-id-36686c794eccb84573e741fdbe6f9af2eb18c8c9/> reducing the amount of threads used in CI for client tests
   - Run sn_client tests in single-threaded mode.
   - Nodes to wait 30secs before retrying to joining the network when rejected.
   - Testnet to launch nodes with an interval of 30secs.
 - <csr-id-5b0b8589bc9c90fac6285f626a46907b2dd0e819/> cleanup unused deps
 - <csr-id-3b1548dd8b7f77538f47d6bca6338239899d18a3/> refactor statemap invocation avoid node.read().await
   Just directly call the funcs, pass in a stable node_identifier so we don't need to lock on each and every cmd process
 - <csr-id-07e0fc8c7776efdabed61b451258709e4dc8f3d0/> extra log for membership generation during invalid proposal
 - <csr-id-560d1850783439b7e5affdc5d86a61cbf619fca0/> drop write lock during udpate knowledge handler faster
 - <csr-id-8b8f9be3e18b1ae2cf94b9e2dbd17f925b7580f6/> log msgid with msg on SendToNodes
 - <csr-id-a05e6a28d890da5103e12cfae4ee54ffc0870be3/> additional membership logs
 - <csr-id-bebd96de67906ab0d49a4a42edb9b62a2f1d88f2/> remove ConnectionCleanup.
   This should all just be hanled by qp2p/quinn and standard connection timeouts
 - <csr-id-f65d4d457ef8b55fc87f6669f6d8380b371366a1/> reduce logging for periodic loop
 - <csr-id-27cbeb647b90e7105e5b650b436944d3cdd813c5/> log MsgId in InsufficentAck error
 - <csr-id-865b459ab0cfcfbee91298787b5c2c4425e5b6a0/> logs around elder periodics
 - <csr-id-8b50947fe3f2eeb6bfe6350f754303ebba9c09dd/> increase event channel size
 - <csr-id-68929bda1832d85c1a9f43d904f1c687a3bc9dc4/> refactor client ACK receipt
   moves to write to a map on ack receipt, regardless of when it come sin.
   Previously we could miss ACKs if they were in before our DashMap was updated
 - <csr-id-a05d7ca9b7d2319085a2dea9119735e3e44f50c1/> only enqueue msgs at start, thereafter process all enqueued
 - <csr-id-98abbbe7af8c870faa22d62819691054e07df718/> remove ExpiringConnection struct
   dont disconnect link when sutting down channel
   
   Allow dropping of link to do all cleanup
 - <csr-id-6185599884f49a71343c67e625a1a9dd9a75393d/> dont have increase conn retries for existing conn retries
   Just keep retrying as long as there are more connections to try.
   
   (As we dont attempt to connect to a client, we can just use up everythign we have... any retry limit might cause a fail before we hit the live connection)
 - <csr-id-a9e2c6962b6646cc6b4ac24bf9da50dbf48d3f60/> more logs around conn closing
 - <csr-id-7ed94ba0599325900246743334b4b821331cba86/> only cleanup on local close if there's no more connections available
 - <csr-id-999bfaa633e9fcd2a9bc47b90b128f0e3946a951/> wait before reqnqueing job so Link connection can be cleaned up...
 - <csr-id-f856d853c0166f35bb2b98b24e3f3c8c09783b2d/> allow too many args
 - <csr-id-a1263ecef879fbdba932588cc37ea63959a0435b/> only perform periodic checks when cmd queue is empty
 - <csr-id-914b816921fc8f4da99bbf77a9fdd91d896411a4/> increase data query timeout
 - <csr-id-9a6f4a3cf20852f4b5604bf08a04aba592dca0fa/> dont manually clean up client peers
   further responses for same opId may come in and succeed. Fastest response would naturally be NotFound... So this would skew results
 - <csr-id-e57d83235f60a16bd7e1ee801f35a599113dc71a/> write query responses as they come in
   as opposed to requiring a channel to be in place.
 - <csr-id-0f11b2ed16c765bee3e25f2f25b374e3f06f8e8f/> cleanup dropped peers
   and bubble up send channel errors to be handled
 - <csr-id-5a539a74d0dcca7a8671910d45bbb08ca4382671/> refactor client ACK receipt
   moves to write to a map on ack receipt, regardless of when it come sin.
   Previously we could miss ACKs if they were in before our DashMap was updated
 - <csr-id-411ea371660b5f76a5c3f887f78331e58f8b6961/> dont try and establish connections for ServiceMsgs
 - <csr-id-177407d1a0e817ea5fff0a98f74cd358f25ac727/> removing done TODOs
 - <csr-id-014b132923c5affebe2c485aa3e791a22a6b90c5/> further node join write lock reduction
 - <csr-id-3c05e616e54f423b66d35114668dac35ab5eae14/> reduce locks during join req handling
 - <csr-id-ec88642644b76c0751db25c9b02b068cd77318d1/> reduce node locking now we can have the lock inside valid msg handling
 - <csr-id-37785c72943ad126ef8fc94f4a6c2139ae478d69/> explicit errors in join
 - <csr-id-fc7670e6cc4284d7cf5614185e8e93ffb9f0ba37/> refactor statemap invocation avoid node.read().await
   Just directly call the funcs, pass in a stable node_identifier so we don't need to lock on each and every cmd process
 - <csr-id-51425951e8a66a8fd938a8dd2378b583cc80fb94/> use gen_section_tree_update test utility
 - <csr-id-6276e931291afd518648a47bc10374640b462cad/> adapt DKG docs to sigshare change
 - <csr-id-8955514b2d08d2f7fbb4ebbf48d9807a9d5127ac/> remove commented out code
 - <csr-id-e0803687a1b3b374efdf040cac0ecd5c6b4fc60a/> adapt DKG test
 - <csr-id-1601cde194cb1f3bab2f6b54cc0ca784adb912b8/> increase channel sizes.
   Packed channels may give us inintentional waits
 - <csr-id-151a22f63a0aaaf94070f3bd0e1f6bd2f9239856/> rename a helper func
 - <csr-id-70d848a43b6df02812195845434849b98f409367/> rename SectionAuth to SectionSigned
 - <csr-id-a7f017cc55da5cba53e8f10063afefc61ea5635a/> remove some code duplication
 - <csr-id-a0b2df5a0b12c70872dfc854d660afd0cf8b21aa/> improve namings
 - <csr-id-09c48916a7f2145c1d3cd091d6219fb7150fe1c2/> use latest sdkg
 - <csr-id-7654f58dbde0c44fd799da16b2a3c4e3a73217df/> update cargo sn_sdkg dep
 - <csr-id-9a1cdf6f0135ce53f43a48c4346aff9023ccad33/> compile after rebase
 - <csr-id-cbf5ed4b1065d5d4471bc27291c054f48b64678e/> use sn_sdkg github dependency and clippy
 - <csr-id-3d72e4b71c079f7ddd8be08642165e53ebf987f6/> github sn_dkg dependency
 - <csr-id-e1e161ba54ed8e0af298814f0da5b953ff053c93/> clippy
 - <csr-id-9992d9701ecadff2b7682e47387014b9d11dba63/> compile after rebase
 - <csr-id-f4c3808f582cf06d58303383296af4ab7a13f0df/> logging improvement
 - <csr-id-2e937145c39039ee55505f00637cf484943f4471/> add nightly fixes
 - <csr-id-80446f5d9df88d5915dcf1d3ea2c213c22e40c14/> remove unused rs files
 - <csr-id-3f52833a8ce977aa79268ecaac61070f01e9c374/> remove unused rs files
 - <csr-id-77cb17c41bbf258c3f1b16934c4c71b5e5ad2456/> add nightly fixes
 - <csr-id-100e2ae70d21e141e1ebbc324f8b06e3d3f1a01c/> remove spend retry on client
   The spend retry depends on providing new network knowledge. We will be using another mechanism to
   obtain this knowledge, which is not available at the moment. Once it's available, we'll add the
   retry again.
   
   For now we decided it's best to remove it and only merge the node-side changes.
   
   This also fixes up various changes after the merge of the new SectionsDAG that replaced the
   SecuredLinkedList.
 - <csr-id-e973eee96c9065ce87a1fa65ae45d9be8d6f940c/> remove redundant genesis_key argument in `NetworkKnowledge` constructor
 - <csr-id-03da7f67fff1fa5bb06d60a66dfdb531506dec4c/> optimizations and code cleanup
 - <csr-id-859fc30fa70ce41ceb910e0352c71dda5c5501ce/> enable `SectionTree` proptest

### Test

 - <csr-id-66a15497201ef63c52721a6ba8ce4840393f03bc/> add reg edit bench

### Refactor

 - <csr-id-dcf40cba6ae0f73476d3095a01aca5c3cade031c/> update qp2p (quinn)

### Chore

 - <csr-id-ea1d0490f0b67a9f39bd98b2bd5830a0f63fbf6e/> upgrade sn_dbc and blsttc
   Upgrade both of these crates to resolve a publishing issue regarding a crate that had been yanked
   being pulled in to the dependency graph.
 - <csr-id-e3bb817e20843f68ee21e9a5dd7e52c8a6e92b88/> sn_interface-0.16.0/sn_dysfunction-0.15.0/sn_client-0.77.0/sn_node-0.72.0/sn_api-0.75.0/sn_cli-0.68.0

### New Features

<csr-id-fc9e4feab2a504168221fe2bd893d9327a45ae6f/>
<csr-id-e5ae9d7174ebb7abee2a4643f1c046f13cf15c01/>
<csr-id-2542643330dcc35ff49b54e88fd038a19d0f5d18/>
<csr-id-bcdb4fc8035c108f2e24c14983af30ddfb54b8fd/>
<csr-id-5a39a843c5570993b0e27780a1c2887bbf7a3212/>
<csr-id-87bc680733b8a24fdcb9f6dedb5ef5ef61becfe8/>
<csr-id-17167b84b910631e1c847c657b88a2e0b422b1cd/>
<csr-id-815d8034d26fb0a7dec22ceca4ad4e31653041c4/>
<csr-id-8368c402bf5b305279b44d8c28cbb497c4bec333/>
<csr-id-3fd0a00bad2f9ca266a56de2086b54088459e153/>
<csr-id-95436a1f722bfd02a735dc3cf2f171a8b70de552/>
<csr-id-e945ea97717ff2b63fd553afba82421c128166c4/>
<csr-id-b27f15cbea63b1994fdf9b576a064273afe8f166/>
<csr-id-83922d007af49f2e63cc1c81020db9cde905b66d/>
<csr-id-a641d9f359e0dbc12bc55c62f126a04efa5e78f3/>
<csr-id-e9dec49535c22c924af7d2144f4a298cae930eee/>
<csr-id-a5bf211daa0272597f1a2d852a17592258a2115a/>
<csr-id-14ba1a8a034e488728762df09398347f8b909d65/>
<csr-id-8c348b2286925edf319daede9a064399b41f6ec1/>
<csr-id-5019dd3896d278227878ffd91ce14d0cecb2b9dd/>
<csr-id-a60eef81e20bee599f9b861822c8e8c3424073af/>
<csr-id-50e877d9ac75b62dfa9e851564b6fd6b60167ca3/>
<csr-id-266f3120b574133fccc39405d3a5a02d05806dfc/>
<csr-id-2bcc1a0735d9cd727f71c93b58de85cd01c8a603/>
<csr-id-59942675bc99d11ae4fcc6a5909aeba4b0f17ec9/>
<csr-id-8f2c41406165351796ca8df36d2ae3457e000ab9/>
<csr-id-b30459ffc565bb38ce7975c443b8df8139b77752/>
<csr-id-b9d39b09e39b7d91fd556abeb385310f50a0eee0/>
<csr-id-a65a5093e55b233dc4fa1293217d6bcd55a9d731/>
<csr-id-45072a674021efe9efd1eeaf5f372b8a8bfba4c8/>
<csr-id-01c8f5d799fce7bf3b8600042587626900940d01/>
<csr-id-f4b5d6b207a0874621d777582ca5906e69196e06/>
<csr-id-45d194d4d5927450a91a73c776c68871e857d48c/>
<csr-id-3ed0e0167d5bec04d6c57d94ce1a63d1f043a1a0/>
<csr-id-40fa4d9c0308c1104aaff60890bd48e08c8508b2/>
<csr-id-69dce88a25254dd84a98ea9571ffccef17208744/>
<csr-id-ba859ae5f064d6dc15aa563ee956a26e85df1d45/>
<csr-id-f5d53c188a03150a06bdee97fb585f7900b7c251/>
<csr-id-5c8b1f50d1bf346d45bd2a9faf92bbf33cb448da/>
<csr-id-057ce1ce1e174102e23d96cfcd2ab1d090a6f1dc/>
<csr-id-2020ef1a91c8520abc4bb74d3de6385b8cd283b4/>
<csr-id-864c023e26697a609a9ad230c04e7aef7416650c/>
<csr-id-0cd47ad56e0d93e3e99feb0dfcea8094f871ff6f/>

 - <csr-id-707627f8915a6032390b035786e3e39d1f7bac8d/> allow to change the Elder-to-Adult query responses timeout by env var
   - feat(node): allow to change the timeout for Elder-to-Adult query responses
   by setting `SN_ADULT_RESPONSE_TIMEOUT` env var
* Debugging message to indicate a spend request being processed correctly, which proved useful when
     trying to get the automated test working.
* Remove the current section key from the unknown section key error. It's not necessary to include
     this.
* When running the baby fleming network with the Makefile, include log messages from `sn_interface`.
* Fix up git-based references to `sn_dbc` crate.

### Bug Fixes

<csr-id-80edaf1bfbbd247ed92eb91b6de011264224f654/>
<csr-id-37dc7b76a3f6c7f9d6b8f832562dc79032910412/>
<csr-id-4c0a0c1b1419b44a8ef48a43f7f5bbd666eb1202/>
<csr-id-6cafdc75fc808a66b9d88d67b91adc67a7ef5b99/>
<csr-id-186c606d8eaf4e1b95162ae970369be26a56fb9e/>
<csr-id-45c5031a965166dc1e2bf8862a8ace0d25b668e1/>
<csr-id-b55ab9bfc16fd11d755cc88625b73936ce231bfc/>
<csr-id-51c86dfcd2aa1ed3b5a868077d35f230245b5345/>
<csr-id-3a13e6773a8faf22dc071e22e5965778ef8e54ad/>
<csr-id-a935167f2549a87344153d354421ed8b8d408576/>
<csr-id-e71eaa8467b244f875726b40e09ea255b3811c40/>
<csr-id-1b2d350b358c34b4bd90c6fd3b5def0515e87e1c/>
<csr-id-cd820b2c9ed03be82bba01368a046f8137fdbba5/>
<csr-id-93fdda0505671426119f300a0444f7c6e51756a8/>
<csr-id-851f7ad85197e2a474c72d5716c4b7858e840442/>
<csr-id-0021a073e37306cadf0b56a1bdac8cb37734dae6/>
<csr-id-27e97a75f72f8de9801c57ef062ba7f9a9b73432/>
<csr-id-9acc41d5884ce4e6f647937fe56df906a7f86452/>
<csr-id-d2d69e10ad6ca5b55945ff15834a8c262b56b3d8/>
<csr-id-894458c0ba15225754d3c75642926f6ee40db34b/>
<csr-id-aad74a94886df2a95b57859fa53591a8813c7860/>
<csr-id-be99942b65306640f057f11b9b8f7345b9752ac5/>
<csr-id-f1e967ad26b06f06f82815c884563df1729219e7/>
<csr-id-9a014135a5fe9dc031847e91ed0cb0e52815cec1/>
<csr-id-c98cc98674552794960e5953c5dbf405d961c333/>
<csr-id-8f887c0f3f128f5d59304a0b47f6105cb52f3155/>
<csr-id-96f275d2ff609fc27292ee8f33b188aa0d67ec4a/>
<csr-id-d3923967e5886373bbf77696e4a813e700156f2a/>
<csr-id-35fcfb6ee2331f2bc34e7680145d0d105e1354ba/>
<csr-id-cdb3a474db1a51ac78234e1458e36ed2e70cc5dc/>
<csr-id-7f5cbbe83d37fba9255078b893a02ab639eeb739/>
<csr-id-cb052a3db16366977526d5d2a7f9c75a2990cf34/>
<csr-id-7b850af7bd4579f1de60629aa88df3c81a35e546/>
<csr-id-9454c727cbfb334ab68800a5022cf2b687d4f97d/>
<csr-id-25448dc71fbe7877560654a4d5b5d69857c10ac9/>
<csr-id-4a6df2948384bbfd3970982fde203778f950bff3/>
<csr-id-ffa2cf3c9b49aaa3f5f64b88e0800a4047948378/>
<csr-id-ce7d1863e523dbe1761ca60e1cebcf1a802cf83b/>
<csr-id-18b393f7c7c5b9d38bf2bc17751b9dbbf08604fa/>
<csr-id-54222aeea627209c53a1f595578436deb2763ef0/>
<csr-id-1c277731397527b26b8e5edb16d9a5ffda7243d7/>
<csr-id-85d3361b3c4b9f66a456a26671406660e641e41f/>
<csr-id-269245f3289c8d4f482e101aad809a325d67ed7c/>
<csr-id-5280460bb43a013d8273e5c30742c71596e4799a/>
<csr-id-30fa400d2fa1d5029f5e0190c506cc92b0bcdf09/>
<csr-id-22a0de0e3bd8478a10729112ec1b3bce9ba5cb90/>
<csr-id-4884c511d302522aa408ebf9350a7ff6cefeecb7/>
<csr-id-1f08fe59ceeccbf724d28f3bebc855e2999492d7/>
<csr-id-26b9f5b5ef39bab12466a7820de713c60b593faf/>

 - <csr-id-7a5a3d31e0668a7beb64742593181c0a30af05f4/> add comments on idempotency checks in elder state init
 - <csr-id-74eea7619948a11a8ddc87b25a61ef510cd10506/> add a log for when an elder is missing their key share
 - <csr-id-64ca74861bc7b80e6c6f3dba412940b86f338821/> relax lagging dkg test on which cmds are ok
 - <csr-id-a5f5deca04a21ddc7ae691cd1da3ca598dae05b0/> adapting tests to work with the signed-sap logic changes
 - <csr-id-e1953cf95f77053ee243478fb1053e257b197e67/> wait before retrying creation on node instance error
 - <csr-id-de8ed40b9f1ad353c9a8ded58db5de76acee21e1/> reconnect upon any LinkError::Connection(_) error when sending a msg on a bi-stream
   - Upgrading qp2p to v0.32.0.

### Other

 - <csr-id-b550ed00dd3e3af878758aea1a2efb6eba0e8d66/> ignore large clippy variants for now
 - <csr-id-b2b661d3e891403bf747228985930b301c9ad28f/> fix for recent node comm changes
 - <csr-id-05846c1741318f51394090c66e7c0ddf911e31ee/> reduce wait for all ndoes to join
 - <csr-id-30bedf882f1f642592703b92be0966035ef01068/> drop nextest, use cargo test
   this should hopefully avoid recompilaitons that are happening
   as well as occasional no-cache hits for nextest itself (which
   means more compiling
 - <csr-id-b6474691ea6af5ee441b02f6cb9c3cf2b8f97459/> sn_dkg integration
 - <csr-id-c7de08209c659ec93557d6ea10e0bcd8c3b74d8b/> minor refactoring and fixing issue reported by new clippy version
 - <csr-id-230a6ed7f1f4193fa36b2fbb83bea072f4944c1d/> spend with updated network knowledge
   Previously I had a placeholder in for this case, but now have something working.
   
   The test requires having two network sections and one of the input DBCs for a transaction being
   signed by the other section key.
   
   The `TestNodeBuilder` was extended with a function that creates a section without a creating a node,
   and this included being able to provide a section chain and tree.

### Refactor

 - <csr-id-2b5119423248ce95021a0b1ba826b426d79d7e61/> inline closure that was only used once
 - <csr-id-e263a66e7db336cf5868b33dde507abbbc25f81c/> remove some verbosity in the dkg test mocks
 - <csr-id-46068295e8e1c760ebab68f4338b14cf11588605/> inline network knowledge update functions
 - <csr-id-994b1ef49ffee7c1b02b6361297bacd190e9b5e2/> remove extra logic around SAP update
 - <csr-id-f0ae5773669937b3c824e98557ecff5994cb1df7/> modify dkg test cases
 - <csr-id-69d0687c4afc8d8f6e088663dfa4482c4d72ade0/> modify `anti_entropy` test cases
   - Modifies the test cases under `messaging/ae` to make use of `TestNetwork`
   - Simulate a network with two sections, where "our section" has gone through
     3 churn events.
   - The `Env::other_signed_sap` refers to the SAP from prefix1
   - The `Env::proof_chain` refers to the section chain of prefix1.
 - <csr-id-8d79f6fa586758fd75e5e66be95d9d240d9d6551/> spentbook_spend_with_updated_network_knowledge_should_update_the_node
   - The test is modified to make use of `TestNetwork` utility.
   - Creates a Network with 2 sections.
   - The `dispatcher` is obtained from the genesis prefix
 - <csr-id-4c6ae07d217cbed09d34adda1f3859191eb581c0/> use `TestNetwork` to simulate elder change
   - Modifies the test cases under `flow_ctrl/tests` to make use of `TestNetwork`
   - The previous envs had a section go through an elder change resulting
     in two SAPs.
   - An elder node was then created such that it had the `sk_share` for
     the new SAP, but not the complete SAP; indicating that its handover
     is still in progress.
   - The same scenario can be simulated by calling `sap_with_members()`
     which creates a random SAP but with the provided set of members.
 - <csr-id-636b38889c33b34ffdd391d479605938664bf731/> handle_agreement_on_online_of_elder_candidate
   - The test is modified to make use of `TestNetwork` utility.
   - The previous env created a custom `NetworkKnowledge` with varying ages
     for the elders. A node was then created with this custom `NetworkKnowledge`
   - The `elder_age_pattern` can be directly passed into
     `TestNetworkBuilder::sap()` to achieve the same result.
 - <csr-id-006a4f801f585594142e5ac4a9b19d218676c8b3/> simple usage of `TestNetwork`
   - Modifies the test cases under `flow_ctrl/tests` to make use of `TestNetwork`
   - A network is built with a couple of sections. Then a node is retrieved
     from one of the sections to be used in the tests.
   - The `peer` from `TestNodeBuilder::build()` refers to
     `node.info().peer()` where `node` is from the prefix that was passed
     into `TestNodeBuilder::new()`
 - <csr-id-2c8f9331ab0f7f63f7d14f7113ecf9ad5e6c4618/> remove `EventSender` and `Event`
 - <csr-id-aa53ee414631dd4faff600101e909fa98f6885fe/> replace main thread loop with sleep
 - <csr-id-298b5d86e4ea331f1c4c7213a724f275e01a06d1/> remove events from tests
 - <csr-id-f66e02ebbcc0298692cfc3d4d4faf69ba2ba1f8f/> convert events to logs
 - <csr-id-5179cf2dec47295f9673212efa6e23e9531e5ea3/> move to sn_interfaces
 - <csr-id-6ca7f4377308a0dd47dbd17a3d01b07321d9b8a9/> mark redirect code with TODO to replace w/ retry + AEProbe
 - <csr-id-e8ab025a3454005890418b10a50560b3c65fd68f/> remove unnecessary Box around JoinResponse
 - <csr-id-9bee893e381375b6de65d77357e13f1897b9d757/> general cleanup of comments, unused fields etc.
 - <csr-id-3dc0bb1f0d8d04c8a92a75eab73e10721b105a10/> remove section_tree_updates from within join messages
 - <csr-id-d22ad7c46753c3e5d3f3c50da4546c80e302dee9/> use recv_node_msg across all join tests
 - <csr-id-bc2c4ee21335b627e3e998dd56209f72f20aac90/> use send_join_response helper where it makes sense
 - <csr-id-9414beed24795db97277eb0c15fe24910f4220d7/> working on fixing test failures
 - <csr-id-4a466e5a14b61f0dcf5467298d11d831a9a8d7e2/> make Proposal saner and add docs
 - <csr-id-0e73ec1ce28c5e2206eb84a6a24d996a23affd2f/> removing internal unnecessary struct
 - <csr-id-9f539e9a8dd9c22e7440539114b2fbdaaeb34515/> provide age pattern to generate `NodeInfo`
 - <csr-id-3353ab617b438ca12cdac68a1f7956e3050eefcf/> organize more network knowledge utils
 - <csr-id-093ea5bfc200f940662c5c0e458c38c5c77294a9/> organize key related test utilites
 - <csr-id-4b6569ab2a9face420385d29d7baab31d8ca4d1e/> organize network_knowledge test utilites
 - <csr-id-9f8ecf90470ac18de31a956c1eee5f9f2d4c77a7/> remove redundant `bls::SecretKeySet` wrapper
 - <csr-id-30670403d5466f7a052d753136e72e2720d2954d/> remove from sn_node
 - <csr-id-93c7a054d87df7054224664c4a03c4507bcfbae6/> use one channel per Cmd/Query sent to await for responses
 - <csr-id-9fad752ce1849763ae16cdb42159b9dccf1a13d0/> remove some ? noise in tests
 - <csr-id-633dfc836c10eafc54dedefc53b2cbc9526970bb/> AuthKind into MsgKind without node sig
 - <csr-id-1bfa58e5487b3c8cafdef1593c2037b331c30dd1/> remove msg aggregators
 - <csr-id-ab22c6989f55994065f0d859b91e141f7489a722/> assert_lists asserts instead of returning Result
 - <csr-id-32744bcf6c94d9a7cda81813646560b70be53eb2/> remove `SectionAuthorityProvider`, `SectionTreeUpdate` messages
 - <csr-id-ba78a41f509a3015a5319e09e1e748ac91952b70/> move `MembershipState`, `RelocateDetails` out from messaging
 - <csr-id-72abbfbc583b5b0dc99a0f7d90cb4d7eb72bd8c4/> remove `NodeState` message
 - <csr-id-dcb76019971c2765c54ba04e22c1a7d2d1ad2d47/> rename MsgEvent to MsgFromPeer and convert it to struct
 - <csr-id-8f355749c85e5d117e304bd499da1144b00e6809/> Joiner::send takes NodeMsg instead of JoinRequest
 - <csr-id-85c30ffa90488f36c32516ca97cea3b246cffbcb/> move timeout out of receive_join_response
 - <csr-id-80c9f7a74a5c7f80a898f97729a813b6d2445f73/> rename network_contacts to section_tree
 - <csr-id-3302aee7a41abd58b6deba18cc690c5e74aabff4/> move elders sig trust within DkgStart instead of deep within messaging
 - <csr-id-702a03fae21fa517b02a1c0271f0617ca5c4f85c/> Remove resource proof
   We have dysfunction
 - <csr-id-3215110b021aaa7d3b755b7e80432aeed1e0b436/> fmt
 - <csr-id-acaa90a13d598915bafc3584c70826f233d89881/> Remove resource proof
   We have dysfunction
 - <csr-id-d87bbddfd92ecd802b27531cbcbc13c7271c6a10/> defining a private type to hold periodic checks timestamps within the FlowCtrl instance
 - <csr-id-07d0991fed28d49c9be85d44a3343b66fac076d9/> adapt confusing auth related code
 - <csr-id-28c7a9f7b2ce43d698288c12e35eb6a7026a4163/> one more renaming
 - <csr-id-f289de53f3894a58a6e4db51ce81aaf34f276490/> various more renamings
 - <csr-id-452ef9c5778ad88270f4e251adc49ccbc9b3cb09/> rename a bunch of auth to sig
 - <csr-id-85f4d00e81ac5bf67b6be89d7ff51b7bb1060ed6/> simplify section sig
 - <csr-id-322721635724f9dfa9351dd31e6883a32a330fe4/> pull out bootstrap functions for normal and genesis nodes
 - <csr-id-dd45c8f42b4f8674eeaea90aa27a465bd3bae0a2/> Looking at change to NodeSig
 - <csr-id-04605882c324057deef5bec05bdae90e15b5a940/> merge Comm::{first_node, bootstrap} into Comm::new
 - <csr-id-f88d542b64c3bde832968c99dcfe38e99d85b4f5/> join process no longer takes a bootstrap node arg
 - <csr-id-de2479e5fe56b3ebf526215b4860ce9f64c7f20c/> move building of join targets into the join method
 - <csr-id-0a85816f4168024b2892fd77760580b1f8d2d9e9/> rebase
 - <csr-id-072c5d4c5de7810a0837144853435e2ff2d091d0/> move test_utils module
   Previously this module was located specifically under the `section_authority_provider` module, but
   now it moves to its own module and various DBC-related testing utilities are moved from `sn_node`
   to this location. Again, this test setup is needed by both `sn_node` and `sn_client`.
 - <csr-id-610880711da814c7717c665e9cb34a729bda5797/> move build_spent_proof_share to sn_interface
   We move this function from `sn_node` for the same reason we moved `get_public_commitments`.
   
   The location for it is not perfect, but it may as well sit alongside the section keys provider, as
   the function uses that to generate the share.
 - <csr-id-1152b2764e955edd80fb33921a8d8fe52654a896/> get public commitments from sn_dbc
   The code for retrieving and validating the public commitments was moved out to the sn_dbc crate.
   
   It's needed for both the spend request and test setup code which is going to be referenced in both
   `sn_node` and `sn_client`.
   
   Also fixed a clippy error in `SectionTree::get_signed_by_key`.
 - <csr-id-60e333d4ced688f3382cde513300d38790613692/> bundle proof chain, SAP into `SectionTreeUpdate`
   The `SectionTree` always requires a Proof chain and a SAP to update it,
   hence bundle it together to make it cleaner
    - A `proof chain` is a chain of keys where each key is signed by the previous one
    - A `section chain` is a proof chain to our current section key
 - <csr-id-6343b6fd21fe3bf81412d922da5e14b2c8b6f3c5/> pull out will-be-elder check into node
 - <csr-id-0176a56a311dd8450f7cd845bc37cc28a7b11c0d/> add node helper for updating net_know
 - <csr-id-8bf032fff0ba48816311d6ea6967e3c300aedccf/> remove unused genesis parameter

### Style

 - <csr-id-73f5531790ef8817ed3551fd9e4bcbcc7fc6f4f9/> applied clippy nightly
   Mainly lints `iter_kv_map` (e.g. use `keys()`, instead of `iter()`, which
   iterates over both keys and values where the value is not used) and `needless_borrow`.

### Test

 - <csr-id-4efda958c765e41b39444f397c500db95716ffa2/> fix ae test by providing updated snapshot
 - <csr-id-058acaa701792ee58913b2d4524c759367fb65fc/> comment out old OutgoingMsg::Client code in ignored tests
 - <csr-id-ffbb607835a87683280e4f16b0e2e1b5ca2fd0a1/> tidying for clippy
 - <csr-id-774c908b6c44b702259782cefdef5d4cdd228385/> fix node config keep alive test time
 - <csr-id-42c4008aeadac297c212a65cde7109a055f61cec/> ignore client cmd tests that require response stream for now
 - <csr-id-f53337e6e0c7c4f804489f0d370d4cc97331597f/> ignore spentbook test that tries to handle client w/o a stream etc
 - <csr-id-edea3fb90697837317f7f050913fefd534938bfd/> remove two spend tests that ar eno longer valid in this flow
 - <csr-id-5dfeab95bb67454cc257028185dbbf7e1f98d351/> ignore failed send until we have feedback event channel
 - <csr-id-ac9d1b257db48ab336d2c80e4ff573208cbd4c6c/> fix ae tests for send_stream addition

### Chore (BREAKING)

 - <csr-id-6be0ea16b0ffe2c153c6a13f36916a91fb58cd05/> attempt to reduce allocations

 - <csr-id-fc0c7512144c0c42184b6ae1b5a64e4d894d0eab/> removing unnecessary error types, plus some sn_node log msg improvements

### New Features (BREAKING)

<csr-id-7106b7533e119dc94bbf19fa304f3eb1f8dc9425/>

 - <csr-id-f225a2d84ad3422b4f466fa2bf713c3a767588dc/> adding more context info to some node Error types
   - Initialising logger in sn_client spentbook API tests.

### Refactor (BREAKING)

 - <csr-id-80917f19125222ce6892e45487f2abe098fefd7a/> breaking up client msg type separating requests from responses
   - A new messaging type `ClientMsgResponse` is introduced explicitly for client msg responses.
   - With new msg type, a new msg kind `MsgKind::ClientMsgResponse` is introduced which removes
   the need of providing a fake client authority in each of the responses sent by nodes to clients.
 - <csr-id-bdf50e7ad1214ef4bb48c0a12db8a7700193bb2a/> removing unused Error types and adding context info to a couple of them
 - <csr-id-a973b62a8ef48acc92af8735e7e7bcac94e0092f/> removing op id from query response
   - Use the query msg id to generate the operation id to track the response from Adults
   - Remove peers from pending data queries when response was obtained from Adults
   - Removing correlation id from SystemMsg node query/response
   - Redefine system::NodeQueryResponse type just as an alias to data::QueryResponse
 - <csr-id-d550b553acbd70d4adb830a0600f7da7b833ee18/> removing dst_address fn from ClientMsg as it doesn't always contain that info


### Commit Statistics

<csr-read-only-do-not-edit/>

 - 401 commits contributed to the release over the course of 84 calendar days.
 - 85 days passed between releases.
 - 311 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Merge #1882 ([`16e82d1`](https://github.com/maidsafe/safe_network/commit/16e82d13cfeee993c85c04f1c6f90e4305c90487))
    - upgrade sn_dbc and blsttc ([`ea1d049`](https://github.com/maidsafe/safe_network/commit/ea1d0490f0b67a9f39bd98b2bd5830a0f63fbf6e))
    - Merge #1858 ([`0911a17`](https://github.com/maidsafe/safe_network/commit/0911a17edd50763fb1deb85f25c51a1263f13a7f))
    - change of config size test ([`1f08fe5`](https://github.com/maidsafe/safe_network/commit/1f08fe59ceeccbf724d28f3bebc855e2999492d7))
    - Merge #1875 ([`381dff4`](https://github.com/maidsafe/safe_network/commit/381dff4d6e3ed97d210ecf3e7b662ae17cd3d1a9))
    - update qp2p (quinn) ([`dcf40cb`](https://github.com/maidsafe/safe_network/commit/dcf40cba6ae0f73476d3095a01aca5c3cade031c))
    - add reg edit bench ([`66a1549`](https://github.com/maidsafe/safe_network/commit/66a15497201ef63c52721a6ba8ce4840393f03bc))
    - Merge #1851 ([`b90d652`](https://github.com/maidsafe/safe_network/commit/b90d6524cfeafeba7de7b2b65255fadc98e33eea))
    - sn_interface-0.16.0/sn_dysfunction-0.15.0/sn_client-0.77.0/sn_node-0.72.0/sn_api-0.75.0/sn_cli-0.68.0 ([`e3bb817`](https://github.com/maidsafe/safe_network/commit/e3bb817e20843f68ee21e9a5dd7e52c8a6e92b88))
    - remove full adults from data storage algos ([`26b9f5b`](https://github.com/maidsafe/safe_network/commit/26b9f5b5ef39bab12466a7820de713c60b593faf))
    - Merge #1846 ([`56df839`](https://github.com/maidsafe/safe_network/commit/56df8392897e5d1641570942a3852644e4cce427))
    - add comments on idempotency checks in elder state init ([`7a5a3d3`](https://github.com/maidsafe/safe_network/commit/7a5a3d31e0668a7beb64742593181c0a30af05f4))
    - add a log for when an elder is missing their key share ([`74eea76`](https://github.com/maidsafe/safe_network/commit/74eea7619948a11a8ddc87b25a61ef510cd10506))
    - inline closure that was only used once ([`2b51194`](https://github.com/maidsafe/safe_network/commit/2b5119423248ce95021a0b1ba826b426d79d7e61))
    - remove some verbosity in the dkg test mocks ([`e263a66`](https://github.com/maidsafe/safe_network/commit/e263a66e7db336cf5868b33dde507abbbc25f81c))
    - relax lagging dkg test on which cmds are ok ([`64ca748`](https://github.com/maidsafe/safe_network/commit/64ca74861bc7b80e6c6f3dba412940b86f338821))
    - adapting tests to work with the signed-sap logic changes ([`a5f5dec`](https://github.com/maidsafe/safe_network/commit/a5f5deca04a21ddc7ae691cd1da3ca598dae05b0))
    - inline network knowledge update functions ([`4606829`](https://github.com/maidsafe/safe_network/commit/46068295e8e1c760ebab68f4338b14cf11588605))
    - remove extra logic around SAP update ([`994b1ef`](https://github.com/maidsafe/safe_network/commit/994b1ef49ffee7c1b02b6361297bacd190e9b5e2))
    - Merge #1865 ([`3b3f2f3`](https://github.com/maidsafe/safe_network/commit/3b3f2f32a52caef299a1174a24f76981667a2b2b))
    - Merge branch 'main' into message_handling ([`80e4030`](https://github.com/maidsafe/safe_network/commit/80e4030820b1380450b86fa6e8c57ee41344a0ed))
    - Merge #1863 ([`fc3cd7b`](https://github.com/maidsafe/safe_network/commit/fc3cd7b6f7bd8af9b3e1c942819e4537ee5aac1e))
    - Merge branch 'main' into message_handling ([`6d92148`](https://github.com/maidsafe/safe_network/commit/6d92148dc42bf5f53a21f98131523502133fd169))
    - Merge branch 'main' into Fix-PreventHardNodeLoopOnError ([`ddcc8b2`](https://github.com/maidsafe/safe_network/commit/ddcc8b2e8cd76b63b8a434b4b8b4178747636dfb))
    - Merge branch 'main' into Fix-PreventHardNodeLoopOnError ([`1d14544`](https://github.com/maidsafe/safe_network/commit/1d14544e015d02348dac04f4fa20de36316ce616))
    - Merge #1815 ([`ea487fc`](https://github.com/maidsafe/safe_network/commit/ea487fc3f734830997851326d66121e55a7ee9fb))
    - allow to change the Elder-to-Adult query responses timeout by env var ([`707627f`](https://github.com/maidsafe/safe_network/commit/707627f8915a6032390b035786e3e39d1f7bac8d))
    - expand some message handling log ([`cc9b9e0`](https://github.com/maidsafe/safe_network/commit/cc9b9e0f09ceed4af7ca6d0a0913cfda1e184eef))
    - Merge #1844 ([`308814b`](https://github.com/maidsafe/safe_network/commit/308814b95a023f3f6a4b51140b0a65a3c7e013a8))
    - wait before retrying creation on node instance error ([`e1953cf`](https://github.com/maidsafe/safe_network/commit/e1953cf95f77053ee243478fb1053e257b197e67))
    - warn if dysf channels are closed ([`06041ee`](https://github.com/maidsafe/safe_network/commit/06041ee529171e51581499a585effac67f037d17))
    - Merge #1855 ([`233fc5f`](https://github.com/maidsafe/safe_network/commit/233fc5f7ed26623c1bec7442503cb1fee81179ef))
    - Merge #1845 ([`9d0f958`](https://github.com/maidsafe/safe_network/commit/9d0f958a0d2bceb9aad7b93b51aa17acf3394b30))
    - reconnect upon any LinkError::Connection(_) error when sending a msg on a bi-stream ([`de8ed40`](https://github.com/maidsafe/safe_network/commit/de8ed40b9f1ad353c9a8ded58db5de76acee21e1))
    - simplify ([`ede33cc`](https://github.com/maidsafe/safe_network/commit/ede33cc7a590e356232acd5feff6b44ff647517f))
    - fix(tests): include promoted node in section - The test was generating the promoted node with random name instead of within the section it was going to. This lead to random test failures, as the node either went into the correct or the wrong section. ([`fab18e0`](https://github.com/maidsafe/safe_network/commit/fab18e0d86946ac4e6a229978d2c93a740a68c3d))
    - chore(logs): address pr review comments - Fixes log lines. - Tests: Nicer error's with stack traces when with panic instead of bail. ([`f6efbeb`](https://github.com/maidsafe/safe_network/commit/f6efbeb4d36165ebaa7484ccc6e831833264311a))
    - refactor(proposal): use HandoverCompleted - This replaces both NewElders and NewSections. Also - Adds/updates verify serialize for signing test case. - Updates split tests, giving that HandleNewSectionsAgreement cmd flow is now also tested. ([`d020ce2`](https://github.com/maidsafe/safe_network/commit/d020ce28926011e7135c51e1e9395ef0c4084fc2))
    - update sibling on split ([`fc9e4fe`](https://github.com/maidsafe/safe_network/commit/fc9e4feab2a504168221fe2bd893d9327a45ae6f))
    - Merge #1853 ([`0e0a9dc`](https://github.com/maidsafe/safe_network/commit/0e0a9dcd68a46cd35ff2d7002d4c9b12c70874f7))
    - add feat to limit section size ([`3dc20bc`](https://github.com/maidsafe/safe_network/commit/3dc20bc9424c295bd9038b2847fe01f639b83407))
    - increase node to 50gb ([`c0b9b27`](https://github.com/maidsafe/safe_network/commit/c0b9b274b60b8c6ecf905c6294e5afb86c69e16f))
    - Merge #1797 ([`d16ebf1`](https://github.com/maidsafe/safe_network/commit/d16ebf1efd9fb7199891e46e2114b40fa7cc8687))
    - standalone methods to get secret keys ([`51bc2ca`](https://github.com/maidsafe/safe_network/commit/51bc2ca02dd8fe9634c9a68889c5eedc1e52c31e))
    - update comments ([`f63d189`](https://github.com/maidsafe/safe_network/commit/f63d1896e554b72d6a5af463b7b6e7992a1aa8ed))
    - modify dkg test cases ([`f0ae577`](https://github.com/maidsafe/safe_network/commit/f0ae5773669937b3c824e98557ecff5994cb1df7))
    - check member type before modifying node ([`80edaf1`](https://github.com/maidsafe/safe_network/commit/80edaf1bfbbd247ed92eb91b6de011264224f654))
    - modify `anti_entropy` test cases ([`69d0687`](https://github.com/maidsafe/safe_network/commit/69d0687c4afc8d8f6e088663dfa4482c4d72ade0))
    - spentbook_spend_with_updated_network_knowledge_should_update_the_node ([`8d79f6f`](https://github.com/maidsafe/safe_network/commit/8d79f6fa586758fd75e5e66be95d9d240d9d6551))
    - use `TestNetwork` to simulate elder change ([`4c6ae07`](https://github.com/maidsafe/safe_network/commit/4c6ae07d217cbed09d34adda1f3859191eb581c0))
    - handle_agreement_on_online_of_elder_candidate ([`636b388`](https://github.com/maidsafe/safe_network/commit/636b38889c33b34ffdd391d479605938664bf731))
    - simple usage of `TestNetwork` ([`006a4f8`](https://github.com/maidsafe/safe_network/commit/006a4f801f585594142e5ac4a9b19d218676c8b3))
    - remove `network_utils` module ([`8bd83c6`](https://github.com/maidsafe/safe_network/commit/8bd83c64ca8cccc78dfe4641e522b4a02f03cbb8))
    - Merge #1836 ([`22bc0ce`](https://github.com/maidsafe/safe_network/commit/22bc0ceef9dae773657aa111e35fd64069658581))
    - report internal error upon failing to send or fnish a response bi-stream ([`37dc7b7`](https://github.com/maidsafe/safe_network/commit/37dc7b76a3f6c7f9d6b8f832562dc79032910412))
    - Merge #1825 ([`ffdb4d7`](https://github.com/maidsafe/safe_network/commit/ffdb4d7244839532e754bc509918e46cd8645357))
    - Merge #1831 ([`5a01ea2`](https://github.com/maidsafe/safe_network/commit/5a01ea24bd220af7b901061af41900b5df3ba79c))
    - spawn new thread for dysf channel calls ([`b4164cf`](https://github.com/maidsafe/safe_network/commit/b4164cf0782c3083ba2a4e0f9b3f12445747730e))
    - use STANDARD_CHANNEL_SIZE in node. increase to 100_000 ([`93ef0c1`](https://github.com/maidsafe/safe_network/commit/93ef0c1b78d9ec656aa8df85d0a741a26d09f780))
    - Merge #1807 ([`1514a15`](https://github.com/maidsafe/safe_network/commit/1514a15b4cf4e82af8038b720d0bdf32cce4e182))
    - don't store client peer sessions ([`f3fbf83`](https://github.com/maidsafe/safe_network/commit/f3fbf83dc15eb791ccc134d780042484a51ab90e))
    - Merge #1827 ([`4ec86e9`](https://github.com/maidsafe/safe_network/commit/4ec86e9f398eb905285a5d378278fff2fb122671))
    - log cleanup ([`4c0a0c1`](https://github.com/maidsafe/safe_network/commit/4c0a0c1b1419b44a8ef48a43f7f5bbd666eb1202))
    - try to rejoin network if removed ([`e5ae9d7`](https://github.com/maidsafe/safe_network/commit/e5ae9d7174ebb7abee2a4643f1c046f13cf15c01))
    - remove `EventSender` and `Event` ([`2c8f933`](https://github.com/maidsafe/safe_network/commit/2c8f9331ab0f7f63f7d14f7113ecf9ad5e6c4618))
    - replace main thread loop with sleep ([`aa53ee4`](https://github.com/maidsafe/safe_network/commit/aa53ee414631dd4faff600101e909fa98f6885fe))
    - remove events from tests ([`298b5d8`](https://github.com/maidsafe/safe_network/commit/298b5d86e4ea331f1c4c7213a724f275e01a06d1))
    - convert events to logs ([`f66e02e`](https://github.com/maidsafe/safe_network/commit/f66e02ebbcc0298692cfc3d4d4faf69ba2ba1f8f))
    - Merge #1824 ([`9494582`](https://github.com/maidsafe/safe_network/commit/949458280b567aa6dce387b276c06c2cb55d7ca4))
    - applied clippy nightly ([`73f5531`](https://github.com/maidsafe/safe_network/commit/73f5531790ef8817ed3551fd9e4bcbcc7fc6f4f9))
    - Merge #1818 ([`e95ebba`](https://github.com/maidsafe/safe_network/commit/e95ebba6e50879e9110b21afb5933685a591b85a))
    - Merge #1817 ([`7fd2bb0`](https://github.com/maidsafe/safe_network/commit/7fd2bb09faf12c65712ee25dab3fd08841cf2d4c))
    - adding more context info to some node Error types ([`f225a2d`](https://github.com/maidsafe/safe_network/commit/f225a2d84ad3422b4f466fa2bf713c3a767588dc))
    - pass in context to NodeMsg handling ([`64b6c35`](https://github.com/maidsafe/safe_network/commit/64b6c35105168b9fa4b0fb9d626ed9552fd0bed3))
    - Merge #1796 ([`e15180b`](https://github.com/maidsafe/safe_network/commit/e15180b53d1daaec76b7eba4637ffc16076c80af))
    - Merge #1809 ([`9042bd2`](https://github.com/maidsafe/safe_network/commit/9042bd2cba7466b2a21592488d5765e27d05eda5))
    - test(spentbook): remove old and commented tests - Cleanup of tests that will not be implemented there. ([`ee15a1d`](https://github.com/maidsafe/safe_network/commit/ee15a1de648f1192137c32b00b32d90d7cd84f4a))
    - move to sn_interfaces ([`5179cf2`](https://github.com/maidsafe/safe_network/commit/5179cf2dec47295f9673212efa6e23e9531e5ea3))
    - refactor(msgs): remove one layer of indirection - ValidateMsg can be replaced with HandleMsg. ([`fcc72d9`](https://github.com/maidsafe/safe_network/commit/fcc72d9cf97d9a9ed3529ab0193aafef65540c70))
    - Merge #1793 ([`c5ab10f`](https://github.com/maidsafe/safe_network/commit/c5ab10f2831cc1f6978dfa518293649f08033e03))
    - attempt to reduce allocations ([`6be0ea1`](https://github.com/maidsafe/safe_network/commit/6be0ea16b0ffe2c153c6a13f36916a91fb58cd05))
    - remove more periodic logs ([`5766286`](https://github.com/maidsafe/safe_network/commit/57662862a8f59b70b9ca41515ff775003ea803fa))
    - Merge #1747 ([`24c019c`](https://github.com/maidsafe/safe_network/commit/24c019cf6996bbe05caead1639a3f34083246015))
    - Merge #1651 ([`aaaa01b`](https://github.com/maidsafe/safe_network/commit/aaaa01b32f40c4a5ca7618cd7f820efb14551440))
    - Merge #1800 ([`42b6834`](https://github.com/maidsafe/safe_network/commit/42b68341e18e372b01d8b4e92babd35a40c3a133))
    - wait for threshold + 1 section tree update responses ([`6cafdc7`](https://github.com/maidsafe/safe_network/commit/6cafdc75fc808a66b9d88d67b91adc67a7ef5b99))
    - minor improvements to sn_node bin error messages ([`c84f844`](https://github.com/maidsafe/safe_network/commit/c84f844873208af49e1743199ca75c015d0e14c7))
    - Merge #1744 #1792 ([`ea83392`](https://github.com/maidsafe/safe_network/commit/ea83392ccc9cbb79b175c29ba77c4a7e27a5398f))
    - minor logging improvements to help debug msgs arriving/processing on client and nodes ([`e5b0dda`](https://github.com/maidsafe/safe_network/commit/e5b0dda1315a5299131cacd135b1d1ab66ed7073))
    - mark redirect code with TODO to replace w/ retry + AEProbe ([`6ca7f43`](https://github.com/maidsafe/safe_network/commit/6ca7f4377308a0dd47dbd17a3d01b07321d9b8a9))
    - remove unnecessary Box around JoinResponse ([`e8ab025`](https://github.com/maidsafe/safe_network/commit/e8ab025a3454005890418b10a50560b3c65fd68f))
    - better log messsages for discarded messages ([`186c606`](https://github.com/maidsafe/safe_network/commit/186c606d8eaf4e1b95162ae970369be26a56fb9e))
    - general cleanup of comments, unused fields etc. ([`9bee893`](https://github.com/maidsafe/safe_network/commit/9bee893e381375b6de65d77357e13f1897b9d757))
    - chore(clippy) ([`fec604d`](https://github.com/maidsafe/safe_network/commit/fec604d3c534d36fc05b420a23fb5e513611e4a7))
    - don't check senders of section updates ([`45c5031`](https://github.com/maidsafe/safe_network/commit/45c5031a965166dc1e2bf8862a8ace0d25b668e1))
    - check that section tree update came from elders ([`b55ab9b`](https://github.com/maidsafe/safe_network/commit/b55ab9bfc16fd11d755cc88625b73936ce231bfc))
    - better log msg when discarding non-bootstrap message ([`51c86df`](https://github.com/maidsafe/safe_network/commit/51c86dfcd2aa1ed3b5a868077d35f230245b5345))
    - adapt tests to work with the new JoinResponses without updates ([`3a13e67`](https://github.com/maidsafe/safe_network/commit/3a13e6773a8faf22dc071e22e5965778ef8e54ad))
    - remove section_tree_updates from within join messages ([`3dc0bb1`](https://github.com/maidsafe/safe_network/commit/3dc0bb1f0d8d04c8a92a75eab73e10721b105a10))
    - adapt remaining join tests to use the new AE Flow ([`a935167`](https://github.com/maidsafe/safe_network/commit/a935167f2549a87344153d354421ed8b8d408576))
    - use recv_node_msg across all join tests ([`d22ad7c`](https://github.com/maidsafe/safe_network/commit/d22ad7c46753c3e5d3f3c50da4546c80e302dee9))
    - adapt the join disallowed test work with the new flow ([`e71eaa8`](https://github.com/maidsafe/safe_network/commit/e71eaa8467b244f875726b40e09ea255b3811c40))
    - remove unused Result return from join_as_adult test ([`1b2d350`](https://github.com/maidsafe/safe_network/commit/1b2d350b358c34b4bd90c6fd3b5def0515e87e1c))
    - get join_as_adult test working with the new join flow ([`cd820b2`](https://github.com/maidsafe/safe_network/commit/cd820b2c9ed03be82bba01368a046f8137fdbba5))
    - use send_join_response helper where it makes sense ([`bc2c4ee`](https://github.com/maidsafe/safe_network/commit/bc2c4ee21335b627e3e998dd56209f72f20aac90))
    - working on fixing test failures ([`9414bee`](https://github.com/maidsafe/safe_network/commit/9414beed24795db97277eb0c15fe24910f4220d7))
    - bootstrap the section tree before starting join process ([`2542643`](https://github.com/maidsafe/safe_network/commit/2542643330dcc35ff49b54e88fd038a19d0f5d18))
    - rebase changes ([`0ddfb0c`](https://github.com/maidsafe/safe_network/commit/0ddfb0c2ceffbf69fca172ec555abe1495be1980))
    - tests(dkg): use gossip to catchup on DKG votes Simulate nodes dropping some `NodeMsgs` directed at them. This will cause the DKG to stall since they missed some votes. Send DKG gossip from a random node to the participants whenever the DKG is stalled. This should allow the participants to catchup and terminate. ([`b10e194`](https://github.com/maidsafe/safe_network/commit/b10e19437b9b19975c51f5b9296c494fa92c9ccd))
    - tests(dkg): total participation of nodes is required DKG requires all the votes from the participants to progress ([`63e70e1`](https://github.com/maidsafe/safe_network/commit/63e70e13857180feae4327a3e67aba73cf0ca362))
    - tests(dkg): lagging node should not propose `SectionInfo` After DKG termination, each node proposes a new `SectionInfo` to denote the change in the `SectionKey`. But if a lagging node (DKG in-progress) has received the `SectionAuthorityProvider` from a terminated node through AE, then the lagging node should not propose `SectionInfo`. ([`9889f7b`](https://github.com/maidsafe/safe_network/commit/9889f7bdb0191a1bc5ff864be5434769c7785c21))
    - tests(dkg): simulate a `DKG` round Simulate a DKG round from start till termination and verify the newly generated `SecretKeyShare` for each node. The test bypasses the comm module and passes the `NodeMsgs` directly to the corresponding peers. ([`19f6111`](https://github.com/maidsafe/safe_network/commit/19f6111053ba825fe2eddcca66755c60d92d7c0e))
    - upgrading qp2p to version 0.31.0 ([`f06b3e7`](https://github.com/maidsafe/safe_network/commit/f06b3e75ce97e7c749d2969276ad6533369806bb))
    - Merge branch 'main' into remove-dataaddress-from-ack ([`ac2548b`](https://github.com/maidsafe/safe_network/commit/ac2548b1890935eb94e8802902d8bb1df0aae8fd))
    - Merge #1790 ([`aac4f97`](https://github.com/maidsafe/safe_network/commit/aac4f9737483904e6dd10bebf002fc1bf8899b29))
    - another log removed ([`986fa81`](https://github.com/maidsafe/safe_network/commit/986fa81fb00daaa37a17e55b459d66efacfff650))
    - Merge #1785 ([`018eaab`](https://github.com/maidsafe/safe_network/commit/018eaab8bef81f4105318c34dc05c4d58412a60d))
    - less logs ([`4392fd2`](https://github.com/maidsafe/safe_network/commit/4392fd265faec0b8e6c637342bd71119322d53b4))
    - Merge #1763 ([`fe082f9`](https://github.com/maidsafe/safe_network/commit/fe082f9a19f9838256f3bf6a16e74bb6b2e32a29))
    - fix(network_builder): construct a complete Network The builder now creates a complete Network (complete tree of prefixes), thus we take the max_prefix that the user has provided and create all the sibling (and its ancestor) prefixes. ([`7ea9048`](https://github.com/maidsafe/safe_network/commit/7ea9048c05a3b82a9fc4169fd694ce2accc18f11))
    - fix(network_builder): remove `TestNetwork::sk_shares` The sk_shares of a node can be queried, hence we don't have to store it. ([`3e9cdfa`](https://github.com/maidsafe/safe_network/commit/3e9cdfa8bdb1658704b0033d7ab755a686247a27))
    - rename methods, update comments ([`7837198`](https://github.com/maidsafe/safe_network/commit/78371980ebf3f1b65ec62c49c35d8a6c015c5537))
    - feat(network_builder): build a working `NetworkKnowledge` The `NetworkKnowledge` can be retrieved from any section and it will contain the information from genesis section till the current one. Also the the `SectionPeers` is updated using the current section members. ([`5d31e27`](https://github.com/maidsafe/safe_network/commit/5d31e2720d8691444c1b8ebbe59bf811fb5cdba8))
    - feat(network_builder): build a single `MyNode` instance A single node could've been an elder in some older sections. Hence we retrieve all the `SectionKeyShare` from all the old sections. Currently we ignore the sibling `prefixes` for which it could've been an elder. ([`b335502`](https://github.com/maidsafe/safe_network/commit/b3355029b615aa913b464c02eb7dc0dcd1b20cc8))
    - feat(network_builder): retrieve nodes from a section The `MyNodeInfo` along with the `Comm` structs are needed to construct a single node instance. We also return the `SectionKeyShare` if the node is an Elder. We can also retrieve the `mspc::Receiver` used inside the `Comm` module. ([`b0731e3`](https://github.com/maidsafe/safe_network/commit/b0731e3be909706161091e64534b5ec44924eb20))
    - feat(network_builder): build a working `SectionTree` The `max_prefixes` can be considered as the leaves of a tree and hence inserting `max_prefix.ancestors()` will effectively insert a single branch from the root till the leaf of the tree. A single prefix can also contain multiple SAPs and these are inserted in the order in which they were provided. ([`d16da92`](https://github.com/maidsafe/safe_network/commit/d16da92d0e866a42cba39c1cd42a055872e47a6b))
    - feat(network_builder): build `TestNetwork` utility Calling the `build()` method will process the SAPs that the caller has provided and also generate SAPs for the missing prefixes. These are prefixes for which the SAPs are not provided by the caller, but are still required to generate a functional `SectionTree`. ([`03cb393`](https://github.com/maidsafe/safe_network/commit/03cb393f49f3349261db4f608c5ad4bf07dc6349))
    - feat(network_builder): define `TestNetwork` and `TestNetworkBuilder` The `TestNetwork` utility will facilitate setting up the environment for tests that make use of `MyNode` instances. We can tell the builder to generate a network with a specific SAP in a prefix and it will create a functional network with all the gaps filled in. It also allows us to retrieve a `MyNode` instance from any SAP with valid `Comm`, `NetworkKnowledge`, `SectionKeyProvider` etc. ([`09f1c69`](https://github.com/maidsafe/safe_network/commit/09f1c6943957e9572139a213ef1b1df6f9f4ff7a))
    - when failed to store data the Adult was returning a success response to Elder ([`93fdda0`](https://github.com/maidsafe/safe_network/commit/93fdda0505671426119f300a0444f7c6e51756a8))
    - Merge #1780 ([`5a35817`](https://github.com/maidsafe/safe_network/commit/5a35817c9f928fe66a745ac645d9560964e05e8b))
    - make Proposal saner and add docs ([`4a466e5`](https://github.com/maidsafe/safe_network/commit/4a466e5a14b61f0dcf5467298d11d831a9a8d7e2))
    - Merge #1776 ([`bb65746`](https://github.com/maidsafe/safe_network/commit/bb657464f8217aa1a41501c4025ceb5dc6d0aca7))
    - cleanup some overzealous log msgs ([`7ac1e8a`](https://github.com/maidsafe/safe_network/commit/7ac1e8abc3d792d5ea69e1e77fc7bdee63268f26))
    - remove duplicate context in handle_ae_msg ([`762170b`](https://github.com/maidsafe/safe_network/commit/762170be83e21659cb8170987a5c2cdd9f2c14e6))
    - Merge #1777 ([`f24e622`](https://github.com/maidsafe/safe_network/commit/f24e622dcb53362623300bdc02d70779590964a5))
    - refactor(sn_dysfunction): rename `IssueType` variants The `IssueType` is now used to track as well as untrack dysfunctional nodes. Hence modify the names to be more generic. ([`489deb5`](https://github.com/maidsafe/safe_network/commit/489deb59bd53337d889aac2c73b49fa51440f148))
    - pr suggestions ([`98d0d4c`](https://github.com/maidsafe/safe_network/commit/98d0d4cf79919fb56854d6f4492af6740df3587d))
    - refactor(dysfunction): move `DysfunctionDetection` to a new task Decouple the Dysfunction module from `MyNode` by moving it to a separate `tokio::task`. Use `mpsc` channels to communicate between the tasks. ([`535d5cf`](https://github.com/maidsafe/safe_network/commit/535d5cfc0c0f0226fbbc051de595564a5628fe1c))
    - remove unusued `Error` module ([`5e81ac4`](https://github.com/maidsafe/safe_network/commit/5e81ac4fb8a2312eb546a4b86e71be05df7c4e26))
    - remove unusued `Result` from methods ([`6cf816f`](https://github.com/maidsafe/safe_network/commit/6cf816f4e3ce1e81af614ba84de83ccf13e8e402))
    - Merge #1779 ([`be3973b`](https://github.com/maidsafe/safe_network/commit/be3973b8496f9920c5059173f48e0237fe3385ba))
    - remove Membership from NodeContext. ([`c4cf647`](https://github.com/maidsafe/safe_network/commit/c4cf647221cdf0ffaeca4d4a8da82d7dc7b21ca6))
    - Merge #1765 ([`90a870e`](https://github.com/maidsafe/safe_network/commit/90a870ebe1ce5110b4b264e8e317acc30152ceb1))
    - making AE msg for clients to be a variant of client response msg type ([`7106b75`](https://github.com/maidsafe/safe_network/commit/7106b7533e119dc94bbf19fa304f3eb1f8dc9425))
    - consume context where makes sense ([`f33602e`](https://github.com/maidsafe/safe_network/commit/f33602e0959a0ea86e4802376bd2f073cd8ea57c))
    - reduce AE node msg handling locking ([`8f072f2`](https://github.com/maidsafe/safe_network/commit/8f072f2bcfd48e5c4047e32332c29e231632efae))
    - Merge #1769 ([`cb8acf7`](https://github.com/maidsafe/safe_network/commit/cb8acf761cd6e39b48d6afab5c9337af3f2fb5aa))
    - import organisation ([`200cf34`](https://github.com/maidsafe/safe_network/commit/200cf34c1cfc26b219ee12b9e8b0478fc5415745))
    - Remove locks in data replication flows. ([`c436078`](https://github.com/maidsafe/safe_network/commit/c436078af2afca7468bb6d8a579f457dfb662bf3))
    - Merge #1766 ([`19ffd04`](https://github.com/maidsafe/safe_network/commit/19ffd04ac02fe98c72c0c4d497c29bdf961e9201))
    - refactor(responses): return correct cmd response - Returns the ack corresponding to the cmd. - Renames `ClientMsgResponse` to `ClientDataResponse`. - Makes `NodeDataResponse` be handled like `ClientDataResponse`. - Moves data write acks to `NodeDataReponse`. - Makes `NodeEvent` only be Adult to Elder notifications. ([`bd3b46e`](https://github.com/maidsafe/safe_network/commit/bd3b46e686a6f47cc006ce1f5da2f3041a614b2d))
    - Merge #1768 ([`56e0927`](https://github.com/maidsafe/safe_network/commit/56e0927ef2950771b786d5a3692718c07d3ace32))
    - ae_msg_from_the_future_is_handled unit test was failing due to wrong assumption of test utility ([`851f7ad`](https://github.com/maidsafe/safe_network/commit/851f7ad85197e2a474c72d5716c4b7858e840442))
    - Merge #1761 ([`aa6b24a`](https://github.com/maidsafe/safe_network/commit/aa6b24adb1790bc97eca1ffdc4d265247ec4953e))
    - removing unnecessary error types, plus some sn_node log msg improvements ([`fc0c751`](https://github.com/maidsafe/safe_network/commit/fc0c7512144c0c42184b6ae1b5a64e4d894d0eab))
    - Merge #1749 ([`ad2574c`](https://github.com/maidsafe/safe_network/commit/ad2574cb7fad692c2f9924fd87130f0b0bb9e2c2))
    - remove is_not_elder from NodeContext ([`3c8ac50`](https://github.com/maidsafe/safe_network/commit/3c8ac507a85e7c11219fdab8a61eadde89f8582f))
    - rename MyNodeSnapshot -> NodeContext ([`18c0444`](https://github.com/maidsafe/safe_network/commit/18c0444be06d84c386b18031e59f2162fef81d89))
    - cleanup unneeded code block ([`e37180b`](https://github.com/maidsafe/safe_network/commit/e37180bd65d26c6f4737dabeae8a210674a82bdf))
    - cleanup commented code ([`197bb66`](https://github.com/maidsafe/safe_network/commit/197bb663e088309150a6f85e5c86877e5e0ad2d8))
    - move (and rename) matching_section to MyNodeSnapshot ([`bd2baa0`](https://github.com/maidsafe/safe_network/commit/bd2baa03f889fa94fab68ecf9f1acffb71aa993d))
    - node.get_snapshot -> snapshot ([`7bc55f7`](https://github.com/maidsafe/safe_network/commit/7bc55f7b1a819c4609a85176d99f287b54a9ad44))
    - fix ae test by providing updated snapshot ([`4efda95`](https://github.com/maidsafe/safe_network/commit/4efda958c765e41b39444f397c500db95716ffa2))
    - retry join with new name if we get a completely new SAP ([`7d79e3e`](https://github.com/maidsafe/safe_network/commit/7d79e3e53ce49dc664d4a9215e5cc02839540879))
    - add in SetStorageLevel cmd to remove mut on DataStorage for normal replication ops ([`4df4086`](https://github.com/maidsafe/safe_network/commit/4df40864b5ee030edf16bad0becaa08a07c15fd9))
    - only regen join name once per sap ([`0021a07`](https://github.com/maidsafe/safe_network/commit/0021a073e37306cadf0b56a1bdac8cb37734dae6))
    - Pass around MyNodeState to avoid holding locks ([`38b8f55`](https://github.com/maidsafe/safe_network/commit/38b8f55121d8b7c461efa6dd0c0407c4fae93418))
    - Merge #1762 ([`ec0f45f`](https://github.com/maidsafe/safe_network/commit/ec0f45fec37a434d5a0ab252b3a8c61e78041c61))
    - replace `TestSAP` with `TestSapBuilder` ([`a55b74b`](https://github.com/maidsafe/safe_network/commit/a55b74b4c8f9bede3c91a9426d4687df01138257))
    - only proposal when reached handover consensus first time ([`27e97a7`](https://github.com/maidsafe/safe_network/commit/27e97a75f72f8de9801c57ef062ba7f9a9b73432))
    - removing internal unnecessary struct ([`0e73ec1`](https://github.com/maidsafe/safe_network/commit/0e73ec1ce28c5e2206eb84a6a24d996a23affd2f))
    - Merge #1637 ([`45903a9`](https://github.com/maidsafe/safe_network/commit/45903a9988528f543b09afbb56a89d21effbb929))
    - Merge branch 'main' into refactor-client-msg-response ([`f9c0efc`](https://github.com/maidsafe/safe_network/commit/f9c0efc0db6b07c7bee6073b07aaae3e62ad780d))
    - refactor(tests): remove `tokio::LocalSet` The `Comm` module does not use `spawn_local` anymore, hence we do not need to use a local task set for the tests ([`15f3218`](https://github.com/maidsafe/safe_network/commit/15f3218140a2475ef545d641c26170a361d0dd57))
    - breaking up client msg type separating requests from responses ([`80917f1`](https://github.com/maidsafe/safe_network/commit/80917f19125222ce6892e45487f2abe098fefd7a))
    - remove AE hold back ([`bcdb4fc`](https://github.com/maidsafe/safe_network/commit/bcdb4fc8035c108f2e24c14983af30ddfb54b8fd))
    - small changes ([`9acc41d`](https://github.com/maidsafe/safe_network/commit/9acc41d5884ce4e6f647937fe56df906a7f86452))
    - provide age pattern to generate `NodeInfo` ([`9f539e9`](https://github.com/maidsafe/safe_network/commit/9f539e9a8dd9c22e7440539114b2fbdaaeb34515))
    - organize more network knowledge utils ([`3353ab6`](https://github.com/maidsafe/safe_network/commit/3353ab617b438ca12cdac68a1f7956e3050eefcf))
    - organize key related test utilites ([`093ea5b`](https://github.com/maidsafe/safe_network/commit/093ea5bfc200f940662c5c0e458c38c5c77294a9))
    - organize network_knowledge test utilites ([`4b6569a`](https://github.com/maidsafe/safe_network/commit/4b6569ab2a9face420385d29d7baab31d8ca4d1e))
    - remove redundant `bls::SecretKeySet` wrapper ([`9f8ecf9`](https://github.com/maidsafe/safe_network/commit/9f8ecf90470ac18de31a956c1eee5f9f2d4c77a7))
    - remove from sn_node ([`3067040`](https://github.com/maidsafe/safe_network/commit/30670403d5466f7a052d753136e72e2720d2954d))
    - Merge #1724 ([`ef69747`](https://github.com/maidsafe/safe_network/commit/ef697470545ac8b3c359f721bb30b0f8b7854b65))
    - create new connection to peer if a SendStream cannot be finished when sending a msg ([`d2d69e1`](https://github.com/maidsafe/safe_network/commit/d2d69e10ad6ca5b55945ff15834a8c262b56b3d8))
    - update self_update to 0.32 ([`d43bac5`](https://github.com/maidsafe/safe_network/commit/d43bac5ce07b0f08766858eadc4b8f98f9bcfc12))
    - remove duplicate strum/strum_macros/heck deps ([`667009d`](https://github.com/maidsafe/safe_network/commit/667009dc02e6bb17bfaa60e2374d5ab7b75a7be5))
    - bump tokio-util 0.6 to 0.7 ([`af9d3ab`](https://github.com/maidsafe/safe_network/commit/af9d3abc665a7490492417aebe974cb5ef839d53))
    - criterion 0.3 -> 0.4, tracing-subscriber 0.2 -> 0.3 ([`860f326`](https://github.com/maidsafe/safe_network/commit/860f326a9baf7e62d191eec13359fa5313e6956d))
    - bump blsttc to 8.0.0 ([`ee824e7`](https://github.com/maidsafe/safe_network/commit/ee824e7785b8da770b5aa6bba3415a274a4e0d68))
    - add missing client ae response w/ stream ([`894458c`](https://github.com/maidsafe/safe_network/commit/894458c0ba15225754d3c75642926f6ee40db34b))
    - comment out old OutgoingMsg::Client code in ignored tests ([`058acaa`](https://github.com/maidsafe/safe_network/commit/058acaa701792ee58913b2d4524c759367fb65fc))
    - remove outgoingmsg::client variant ([`b223bd5`](https://github.com/maidsafe/safe_network/commit/b223bd532bba649c6dd09cf26166758d6ff56893))
    - send client error response if storage fails ([`2e7fde6`](https://github.com/maidsafe/safe_network/commit/2e7fde627f41dfff7b5b8cd049f26ee127269bfe))
    - tidying for clippy ([`ffbb607`](https://github.com/maidsafe/safe_network/commit/ffbb607835a87683280e4f16b0e2e1b5ca2fd0a1))
    - fix node config keep alive test time ([`774c908`](https://github.com/maidsafe/safe_network/commit/774c908b6c44b702259782cefdef5d4cdd228385))
    - ignore client cmd tests that require response stream for now ([`42c4008`](https://github.com/maidsafe/safe_network/commit/42c4008aeadac297c212a65cde7109a055f61cec))
    - ignore large clippy variants for now ([`b550ed0`](https://github.com/maidsafe/safe_network/commit/b550ed00dd3e3af878758aea1a2efb6eba0e8d66))
    - Send AE retries to all elders in updated section ([`c71fcd1`](https://github.com/maidsafe/safe_network/commit/c71fcd1fd61a4a2f9075d3d3e5f922b6d06644e6))
    - retry loop for bidi initialisation ([`2b9268c`](https://github.com/maidsafe/safe_network/commit/2b9268c2aff5aae5eb1584d2698f282c8beae73e))
    - elders wait for all storage reqs to be successful ([`62d4723`](https://github.com/maidsafe/safe_network/commit/62d472354d65f1cd9001f2df00cbf0c82734b969))
    - remove adult healthcheck for now ([`58d6569`](https://github.com/maidsafe/safe_network/commit/58d656949c09dc3d6445d899725fe2c41d46c216))
    - chore: split out replication flows depending on iff we need client responses ([`bf826cb`](https://github.com/maidsafe/safe_network/commit/bf826cb9d0f572465a2738b104e161576a077ab7))
    - reset keep_alive flag to be optional ([`d9bfaef`](https://github.com/maidsafe/safe_network/commit/d9bfaef416bc357bdc32632eb0ba263b6995d613))
    - don't report status sent for failed bidi stream send ([`aad74a9`](https://github.com/maidsafe/safe_network/commit/aad74a94886df2a95b57859fa53591a8813c7860))
    - ignore spentbook test that tries to handle client w/o a stream etc ([`f53337e`](https://github.com/maidsafe/safe_network/commit/f53337e6e0c7c4f804489f0d370d4cc97331597f))
    - add in error msg in the event of missing stream during adult query ([`0ef5a2c`](https://github.com/maidsafe/safe_network/commit/0ef5a2cfd82de58f02a23a96a305b172f27c33c8))
    - remove two spend tests that ar eno longer valid in this flow ([`edea3fb`](https://github.com/maidsafe/safe_network/commit/edea3fb90697837317f7f050913fefd534938bfd))
    - cmd responses sent from adults over stream ([`5a39a84`](https://github.com/maidsafe/safe_network/commit/5a39a843c5570993b0e27780a1c2887bbf7a3212))
    - use bi/di send streams for elder-> adults queries ([`87bc680`](https://github.com/maidsafe/safe_network/commit/87bc680733b8a24fdcb9f6dedb5ef5ef61becfe8))
    - enable cmd ids and child ids again ([`17167b8`](https://github.com/maidsafe/safe_network/commit/17167b84b910631e1c847c657b88a2e0b422b1cd))
    - removing unused Error types and adding context info to a couple of them ([`bdf50e7`](https://github.com/maidsafe/safe_network/commit/bdf50e7ad1214ef4bb48c0a12db8a7700193bb2a))
    - add mising sleep before retrying connection. ([`c3aa941`](https://github.com/maidsafe/safe_network/commit/c3aa9414b4232dbaf356cec3fb71460f4d916b4f))
    - tweaks to reduce use of async ([`f2dff36`](https://github.com/maidsafe/safe_network/commit/f2dff3636d3bf1446af53790a42a46473079698f))
    - remove ae_backoff_cache ([`815d803`](https://github.com/maidsafe/safe_network/commit/815d8034d26fb0a7dec22ceca4ad4e31653041c4))
    - set nodes join interval to 30secs for testnet in sn-api tests job ([`1bf23ff`](https://github.com/maidsafe/safe_network/commit/1bf23ff2d00b66232267403f94d3fa133416fdd3))
    - move conn acquisition off thread too. ([`d19c0b2`](https://github.com/maidsafe/safe_network/commit/d19c0b204c368a967a2f6de9d68e1db0caebb71a))
    - Comm is clone. Dont close endpoint on drop. ([`ebb7b79`](https://github.com/maidsafe/safe_network/commit/ebb7b793b1e7b4ba439f3f93b5d7ac32e9acc2c2))
    - remove reachability check from inital join ([`0bd38c5`](https://github.com/maidsafe/safe_network/commit/0bd38c524b5f6685e3a8ad21b3ae793d097c6b6d))
    - reset ae_backoff_cache ([`be99942`](https://github.com/maidsafe/safe_network/commit/be99942b65306640f057f11b9b8f7345b9752ac5))
    - don't hold read lock over reachability check ([`745ae58`](https://github.com/maidsafe/safe_network/commit/745ae58408997504dd04663a1c8bea8b688ded66))
    - refactor(cmds): replace ack+error with response BREAKING CHANGE: ClientMsg enum variants changed. ([`df19b12`](https://github.com/maidsafe/safe_network/commit/df19b120bd769d0b375a27162f07a4a421f97ec0))
    - reducing the amount of threads used in CI for client tests ([`36686c7`](https://github.com/maidsafe/safe_network/commit/36686c794eccb84573e741fdbe6f9af2eb18c8c9))
    - ignore failed send until we have feedback event channel ([`5dfeab9`](https://github.com/maidsafe/safe_network/commit/5dfeab95bb67454cc257028185dbbf7e1f98d351))
    - cleanup unused deps ([`5b0b858`](https://github.com/maidsafe/safe_network/commit/5b0b8589bc9c90fac6285f626a46907b2dd0e819))
    - fix for recent node comm changes ([`b2b661d`](https://github.com/maidsafe/safe_network/commit/b2b661d3e891403bf747228985930b301c9ad28f))
    - fix ae response on stream for better prefix ([`f1e967a`](https://github.com/maidsafe/safe_network/commit/f1e967ad26b06f06f82815c884563df1729219e7))
    - refactor statemap invocation avoid node.read().await ([`3b1548d`](https://github.com/maidsafe/safe_network/commit/3b1548dd8b7f77538f47d6bca6338239899d18a3))
    - extra log for membership generation during invalid proposal ([`07e0fc8`](https://github.com/maidsafe/safe_network/commit/07e0fc8c7776efdabed61b451258709e4dc8f3d0))
    - drop write lock during udpate knowledge handler faster ([`560d185`](https://github.com/maidsafe/safe_network/commit/560d1850783439b7e5affdc5d86a61cbf619fca0))
    - reduce wait for all ndoes to join ([`05846c1`](https://github.com/maidsafe/safe_network/commit/05846c1741318f51394090c66e7c0ddf911e31ee))
    - log msgid with msg on SendToNodes ([`8b8f9be`](https://github.com/maidsafe/safe_network/commit/8b8f9be3e18b1ae2cf94b9e2dbd17f925b7580f6))
    - request membership AE when we have stalled votes ([`9a01413`](https://github.com/maidsafe/safe_network/commit/9a014135a5fe9dc031847e91ed0cb0e52815cec1))
    - drop nextest, use cargo test ([`30bedf8`](https://github.com/maidsafe/safe_network/commit/30bedf882f1f642592703b92be0966035ef01068))
    - fix ae tests for send_stream addition ([`ac9d1b2`](https://github.com/maidsafe/safe_network/commit/ac9d1b257db48ab336d2c80e4ff573208cbd4c6c))
    - additional membership logs ([`a05e6a2`](https://github.com/maidsafe/safe_network/commit/a05e6a28d890da5103e12cfae4ee54ffc0870be3))
    - remove ConnectionCleanup. ([`bebd96d`](https://github.com/maidsafe/safe_network/commit/bebd96de67906ab0d49a4a42edb9b62a2f1d88f2))
    - reduce logging for periodic loop ([`f65d4d4`](https://github.com/maidsafe/safe_network/commit/f65d4d457ef8b55fc87f6669f6d8380b371366a1))
    - use one channel per Cmd/Query sent to await for responses ([`93c7a05`](https://github.com/maidsafe/safe_network/commit/93c7a054d87df7054224664c4a03c4507bcfbae6))
    - use repsonse_stream for ae responses to client. ([`c98cc98`](https://github.com/maidsafe/safe_network/commit/c98cc98674552794960e5953c5dbf405d961c333))
    - spawn a task to read query/cmd responses from bi-stream ([`8f887c0`](https://github.com/maidsafe/safe_network/commit/8f887c0f3f128f5d59304a0b47f6105cb52f3155))
    - send client msg back on bidi stream ([`8368c40`](https://github.com/maidsafe/safe_network/commit/8368c402bf5b305279b44d8c28cbb497c4bec333))
    - use bi stream from client; process in Node ([`3fd0a00`](https://github.com/maidsafe/safe_network/commit/3fd0a00bad2f9ca266a56de2086b54088459e153))
    - move to event driven msg handling ([`95436a1`](https://github.com/maidsafe/safe_network/commit/95436a1f722bfd02a735dc3cf2f171a8b70de552))
    - log MsgId in InsufficentAck error ([`27cbeb6`](https://github.com/maidsafe/safe_network/commit/27cbeb647b90e7105e5b650b436944d3cdd813c5))
    - move DKG gossip check off thread ([`e945ea9`](https://github.com/maidsafe/safe_network/commit/e945ea97717ff2b63fd553afba82421c128166c4))
    - logs around elder periodics ([`865b459`](https://github.com/maidsafe/safe_network/commit/865b459ab0cfcfbee91298787b5c2c4425e5b6a0))
    - increase event channel size ([`8b50947`](https://github.com/maidsafe/safe_network/commit/8b50947fe3f2eeb6bfe6350f754303ebba9c09dd))
    - rebase issue w/ monitoring ([`96f275d`](https://github.com/maidsafe/safe_network/commit/96f275d2ff609fc27292ee8f33b188aa0d67ec4a))
    - refactor client ACK receipt ([`68929bd`](https://github.com/maidsafe/safe_network/commit/68929bda1832d85c1a9f43d904f1c687a3bc9dc4))
    - only enqueue msgs at start, thereafter process all enqueued ([`a05d7ca`](https://github.com/maidsafe/safe_network/commit/a05d7ca9b7d2319085a2dea9119735e3e44f50c1))
    - remove ExpiringConnection struct ([`98abbbe`](https://github.com/maidsafe/safe_network/commit/98abbbe7af8c870faa22d62819691054e07df718))
    - dont have increase conn retries for existing conn retries ([`6185599`](https://github.com/maidsafe/safe_network/commit/6185599884f49a71343c67e625a1a9dd9a75393d))
    - more logs around conn closing ([`a9e2c69`](https://github.com/maidsafe/safe_network/commit/a9e2c6962b6646cc6b4ac24bf9da50dbf48d3f60))
    - only cleanup on local close if there's no more connections available ([`7ed94ba`](https://github.com/maidsafe/safe_network/commit/7ed94ba0599325900246743334b4b821331cba86))
    - wait before reqnqueing job so Link connection can be cleaned up... ([`999bfaa`](https://github.com/maidsafe/safe_network/commit/999bfaa633e9fcd2a9bc47b90b128f0e3946a951))
    - allow too many args ([`f856d85`](https://github.com/maidsafe/safe_network/commit/f856d853c0166f35bb2b98b24e3f3c8c09783b2d))
    - enable multi-threaded runtime try 2 ([`b27f15c`](https://github.com/maidsafe/safe_network/commit/b27f15cbea63b1994fdf9b576a064273afe8f166))
    - spawn the wait/update for session in comms:is_sent ([`83922d0`](https://github.com/maidsafe/safe_network/commit/83922d007af49f2e63cc1c81020db9cde905b66d))
    - removing op id from query response ([`a973b62`](https://github.com/maidsafe/safe_network/commit/a973b62a8ef48acc92af8735e7e7bcac94e0092f))
    - only perform periodic checks when cmd queue is empty ([`a1263ec`](https://github.com/maidsafe/safe_network/commit/a1263ecef879fbdba932588cc37ea63959a0435b))
    - increase data query timeout ([`914b816`](https://github.com/maidsafe/safe_network/commit/914b816921fc8f4da99bbf77a9fdd91d896411a4))
    - make sessions return if they're empty, and if so, clean them up then ([`a641d9f`](https://github.com/maidsafe/safe_network/commit/a641d9f359e0dbc12bc55c62f126a04efa5e78f3))
    - dont manually clean up client peers ([`9a6f4a3`](https://github.com/maidsafe/safe_network/commit/9a6f4a3cf20852f4b5604bf08a04aba592dca0fa))
    - do no requeue existing validate msg cmds ([`e9dec49`](https://github.com/maidsafe/safe_network/commit/e9dec49535c22c924af7d2144f4a298cae930eee))
    - write query responses as they come in ([`e57d832`](https://github.com/maidsafe/safe_network/commit/e57d83235f60a16bd7e1ee801f35a599113dc71a))
    - cleanup dropped peers ([`0f11b2e`](https://github.com/maidsafe/safe_network/commit/0f11b2ed16c765bee3e25f2f25b374e3f06f8e8f))
    - refactor client ACK receipt ([`5a539a7`](https://github.com/maidsafe/safe_network/commit/5a539a74d0dcca7a8671910d45bbb08ca4382671))
    - force retries to use fresh connection ([`a5bf211`](https://github.com/maidsafe/safe_network/commit/a5bf211daa0272597f1a2d852a17592258a2115a))
    - dont try and establish connections for ServiceMsgs ([`411ea37`](https://github.com/maidsafe/safe_network/commit/411ea371660b5f76a5c3f887f78331e58f8b6961))
    - avoid mem size change across compiler version update ([`d392396`](https://github.com/maidsafe/safe_network/commit/d3923967e5886373bbf77696e4a813e700156f2a))
    - remove some ? noise in tests ([`9fad752`](https://github.com/maidsafe/safe_network/commit/9fad752ce1849763ae16cdb42159b9dccf1a13d0))
    - remove age stepping in genesis sections ([`14ba1a8`](https://github.com/maidsafe/safe_network/commit/14ba1a8a034e488728762df09398347f8b909d65))
    - Merge #1711 ([`c61d3c9`](https://github.com/maidsafe/safe_network/commit/c61d3c92531c04a188f724869d36c4492fb3d161))
    - removing done TODOs ([`177407d`](https://github.com/maidsafe/safe_network/commit/177407d1a0e817ea5fff0a98f74cd358f25ac727))
    - Merge #1703 ([`297004f`](https://github.com/maidsafe/safe_network/commit/297004fe04bba05765eb4d02394210024dfcf559))
    - AuthKind into MsgKind without node sig ([`633dfc8`](https://github.com/maidsafe/safe_network/commit/633dfc836c10eafc54dedefc53b2cbc9526970bb))
    - Add port 16685 ([`d673400`](https://github.com/maidsafe/safe_network/commit/d6734004a57557c67d25089957c8471f59644bbe))
    - Merge #1685 ([`992f917`](https://github.com/maidsafe/safe_network/commit/992f917830c6d7b10fbd4d1f03a81eb5e8a64fdc))
    - remove NodeMsgAuthority altogether ([`8c348b2`](https://github.com/maidsafe/safe_network/commit/8c348b2286925edf319daede9a064399b41f6ec1))
    - remove node auth ([`5019dd3`](https://github.com/maidsafe/safe_network/commit/5019dd3896d278227878ffd91ce14d0cecb2b9dd))
    - further node join write lock reduction ([`014b132`](https://github.com/maidsafe/safe_network/commit/014b132923c5affebe2c485aa3e791a22a6b90c5))
    - reduce locks during join req handling ([`3c05e61`](https://github.com/maidsafe/safe_network/commit/3c05e616e54f423b66d35114668dac35ab5eae14))
    - reduce node locking now we can have the lock inside valid msg handling ([`ec88642`](https://github.com/maidsafe/safe_network/commit/ec88642644b76c0751db25c9b02b068cd77318d1))
    - remove msg aggregators ([`1bfa58e`](https://github.com/maidsafe/safe_network/commit/1bfa58e5487b3c8cafdef1593c2037b331c30dd1))
    - get rid of InvalidState Error altogether ([`a60eef8`](https://github.com/maidsafe/safe_network/commit/a60eef81e20bee599f9b861822c8e8c3424073af))
    - explicit errors in join ([`37785c7`](https://github.com/maidsafe/safe_network/commit/37785c72943ad126ef8fc94f4a6c2139ae478d69))
    - refactor statemap invocation avoid node.read().await ([`fc7670e`](https://github.com/maidsafe/safe_network/commit/fc7670e6cc4284d7cf5614185e8e93ffb9f0ba37))
    - :chore(node): drop write lock during udpate knowledge handler faster ([`9900341`](https://github.com/maidsafe/safe_network/commit/99003419995f59cf03f840658eb1d780505625ff))
    - make membership ae >= from gen ([`35fcfb6`](https://github.com/maidsafe/safe_network/commit/35fcfb6ee2331f2bc34e7680145d0d105e1354ba))
    - section peers uses BTreeMaps instead of DashMap ([`50e877d`](https://github.com/maidsafe/safe_network/commit/50e877d9ac75b62dfa9e851564b6fd6b60167ca3))
    - assert_lists asserts instead of returning Result ([`ab22c69`](https://github.com/maidsafe/safe_network/commit/ab22c6989f55994065f0d859b91e141f7489a722))
    - Merge #1667 ([`00a6e1b`](https://github.com/maidsafe/safe_network/commit/00a6e1ba5edbeb647f2614161cb78d3a35420f27))
    - call to File::sync_data right after writing a Chunk or Register op to disk ([`cdb3a47`](https://github.com/maidsafe/safe_network/commit/cdb3a474db1a51ac78234e1458e36ed2e70cc5dc))
    - remove `SectionAuthorityProvider`, `SectionTreeUpdate` messages ([`32744bc`](https://github.com/maidsafe/safe_network/commit/32744bcf6c94d9a7cda81813646560b70be53eb2))
    - move `MembershipState`, `RelocateDetails` out from messaging ([`ba78a41`](https://github.com/maidsafe/safe_network/commit/ba78a41f509a3015a5319e09e1e748ac91952b70))
    - remove `NodeState` message ([`72abbfb`](https://github.com/maidsafe/safe_network/commit/72abbfbc583b5b0dc99a0f7d90cb4d7eb72bd8c4))
    - request membership AE when we have stalled votes ([`7f5cbbe`](https://github.com/maidsafe/safe_network/commit/7f5cbbe83d37fba9255078b893a02ab639eeb739))
    - remove section share auth ([`266f312`](https://github.com/maidsafe/safe_network/commit/266f3120b574133fccc39405d3a5a02d05806dfc))
    - universal aggregator without timers ([`2bcc1a0`](https://github.com/maidsafe/safe_network/commit/2bcc1a0735d9cd727f71c93b58de85cd01c8a603))
    - use gen_section_tree_update test utility ([`5142595`](https://github.com/maidsafe/safe_network/commit/51425951e8a66a8fd938a8dd2378b583cc80fb94))
    - adapt DKG docs to sigshare change ([`6276e93`](https://github.com/maidsafe/safe_network/commit/6276e931291afd518648a47bc10374640b462cad))
    - remove commented out code ([`8955514`](https://github.com/maidsafe/safe_network/commit/8955514b2d08d2f7fbb4ebbf48d9807a9d5127ac))
    - rename MsgEvent to MsgFromPeer and convert it to struct ([`dcb7601`](https://github.com/maidsafe/safe_network/commit/dcb76019971c2765c54ba04e22c1a7d2d1ad2d47))
    - Joiner::send takes NodeMsg instead of JoinRequest ([`8f35574`](https://github.com/maidsafe/safe_network/commit/8f355749c85e5d117e304bd499da1144b00e6809))
    - move timeout out of receive_join_response ([`85c30ff`](https://github.com/maidsafe/safe_network/commit/85c30ffa90488f36c32516ca97cea3b246cffbcb))
    - rename network_contacts to section_tree ([`80c9f7a`](https://github.com/maidsafe/safe_network/commit/80c9f7a74a5c7f80a898f97729a813b6d2445f73))
    - remove unused OutgoingMsg::Elder ([`5994267`](https://github.com/maidsafe/safe_network/commit/59942675bc99d11ae4fcc6a5909aeba4b0f17ec9))
    - adapt DKG test ([`e080368`](https://github.com/maidsafe/safe_network/commit/e0803687a1b3b374efdf040cac0ecd5c6b4fc60a))
    - move elders sig trust within DkgStart instead of deep within messaging ([`3302aee`](https://github.com/maidsafe/safe_network/commit/3302aee7a41abd58b6deba18cc690c5e74aabff4))
    - increase channel sizes. ([`1601cde`](https://github.com/maidsafe/safe_network/commit/1601cde194cb1f3bab2f6b54cc0ca784adb912b8))
    - Clippy ([`cb052a3`](https://github.com/maidsafe/safe_network/commit/cb052a3db16366977526d5d2a7f9c75a2990cf34))
    - Rebase clippy fix ([`7b850af`](https://github.com/maidsafe/safe_network/commit/7b850af7bd4579f1de60629aa88df3c81a35e546))
    - Remove resource proof ([`702a03f`](https://github.com/maidsafe/safe_network/commit/702a03fae21fa517b02a1c0271f0617ca5c4f85c))
    - fmt ([`3215110`](https://github.com/maidsafe/safe_network/commit/3215110b021aaa7d3b755b7e80432aeed1e0b436))
    - Remove resource proof ([`acaa90a`](https://github.com/maidsafe/safe_network/commit/acaa90a13d598915bafc3584c70826f233d89881))
    - defining a private type to hold periodic checks timestamps within the FlowCtrl instance ([`d87bbdd`](https://github.com/maidsafe/safe_network/commit/d87bbddfd92ecd802b27531cbcbc13c7271c6a10))
    - increase restart threshold during join ([`9454c72`](https://github.com/maidsafe/safe_network/commit/9454c727cbfb334ab68800a5022cf2b687d4f97d))
    - adapt confusing auth related code ([`07d0991`](https://github.com/maidsafe/safe_network/commit/07d0991fed28d49c9be85d44a3343b66fac076d9))
    - fmt ([`25448dc`](https://github.com/maidsafe/safe_network/commit/25448dc71fbe7877560654a4d5b5d69857c10ac9))
    - refactor: Node-> MyNode & MyNodeInfo ([`17baaf4`](https://github.com/maidsafe/safe_network/commit/17baaf4c27442273c238d09ebb240e65be85a582))
    - removing dst_address fn from ClientMsg as it doesn't always contain that info ([`d550b55`](https://github.com/maidsafe/safe_network/commit/d550b553acbd70d4adb830a0600f7da7b833ee18))
    - one more renaming ([`28c7a9f`](https://github.com/maidsafe/safe_network/commit/28c7a9f7b2ce43d698288c12e35eb6a7026a4163))
    - various more renamings ([`f289de5`](https://github.com/maidsafe/safe_network/commit/f289de53f3894a58a6e4db51ce81aaf34f276490))
    - rename a bunch of auth to sig ([`452ef9c`](https://github.com/maidsafe/safe_network/commit/452ef9c5778ad88270f4e251adc49ccbc9b3cb09))
    - rename a helper func ([`151a22f`](https://github.com/maidsafe/safe_network/commit/151a22f63a0aaaf94070f3bd0e1f6bd2f9239856))
    - simplify section sig ([`85f4d00`](https://github.com/maidsafe/safe_network/commit/85f4d00e81ac5bf67b6be89d7ff51b7bb1060ed6))
    - rename SectionAuth to SectionSigned ([`70d848a`](https://github.com/maidsafe/safe_network/commit/70d848a43b6df02812195845434849b98f409367))
    - pull out bootstrap functions for normal and genesis nodes ([`3227216`](https://github.com/maidsafe/safe_network/commit/322721635724f9dfa9351dd31e6883a32a330fe4))
    - Looking at change to NodeSig ([`dd45c8f`](https://github.com/maidsafe/safe_network/commit/dd45c8f42b4f8674eeaea90aa27a465bd3bae0a2))
    - refactor: NodeAuth -> NodeEvidence ([`cdc126b`](https://github.com/maidsafe/safe_network/commit/cdc126be934229198959eb3da317e5da92b16ac3))
    - adapt to empty vec change in sn_sdkg ([`8f2c414`](https://github.com/maidsafe/safe_network/commit/8f2c41406165351796ca8df36d2ae3457e000ab9))
    - integrate recursive sn_sdkg changes ([`b30459f`](https://github.com/maidsafe/safe_network/commit/b30459ffc565bb38ce7975c443b8df8139b77752))
    - restart after accumulated retry_response ([`4a6df29`](https://github.com/maidsafe/safe_network/commit/4a6df2948384bbfd3970982fde203778f950bff3))
    - join_invalid_retry_prefix test uses empty genesis prefix ([`ffa2cf3`](https://github.com/maidsafe/safe_network/commit/ffa2cf3c9b49aaa3f5f64b88e0800a4047948378))
    - revert 'is_new_sap' check removal from join process ([`ce7d186`](https://github.com/maidsafe/safe_network/commit/ce7d1863e523dbe1761ca60e1cebcf1a802cf83b))
    - merge Comm::{first_node, bootstrap} into Comm::new ([`0460588`](https://github.com/maidsafe/safe_network/commit/04605882c324057deef5bec05bdae90e15b5a940))
    - join process no longer takes a bootstrap node arg ([`f88d542`](https://github.com/maidsafe/safe_network/commit/f88d542b64c3bde832968c99dcfe38e99d85b4f5))
    - move building of join targets into the join method ([`de2479e`](https://github.com/maidsafe/safe_network/commit/de2479e5fe56b3ebf526215b4860ce9f64c7f20c))
    - remove unneeded err handling; bump join channel size ([`18b393f`](https://github.com/maidsafe/safe_network/commit/18b393f7c7c5b9d38bf2bc17751b9dbbf08604fa))
    - refactor ([`4beec97`](https://github.com/maidsafe/safe_network/commit/4beec978b1f2eae2198bcd85e3e0bf377d97575c))
    - refactor: ([`444ed16`](https://github.com/maidsafe/safe_network/commit/444ed16e55d8e962404c8c7b643b00f0685eed18))
    - rebase ([`0a85816`](https://github.com/maidsafe/safe_network/commit/0a85816f4168024b2892fd77760580b1f8d2d9e9))
    - refactor: rename files system_msgs -> node_msgs service_msgs -> client_msgs ([`10a23cd`](https://github.com/maidsafe/safe_network/commit/10a23cd020b3315172207eb498f7674a588d01eb))
    - Fix for rename in benches ([`54222ae`](https://github.com/maidsafe/safe_network/commit/54222aeea627209c53a1f595578436deb2763ef0))
    - refactor: ([`50d48bf`](https://github.com/maidsafe/safe_network/commit/50d48bfc4fcc54266125bc0f1a3369097376497c))
    - refactor: ServiceAuth -> ClientAuth Service -> Client NodeBlsShare -> SectionPart BlsShareAuth -> SectionAuthPart SystemMsg -> Node2NodeMsg OutgoingMsg::System -> OutgoingMsg::Node2Node + fmt / fix ([`0b9d08b`](https://github.com/maidsafe/safe_network/commit/0b9d08bf88b6892b53dabf82fa988674fdd9992a))
    - use separate genesis dbc and section keys ([`b9d39b0`](https://github.com/maidsafe/safe_network/commit/b9d39b09e39b7d91fd556abeb385310f50a0eee0))
    - Merge #1550 ([`c6f2e2f`](https://github.com/maidsafe/safe_network/commit/c6f2e2fb98e29911336f86f54c1d9b9605037b57))
    - add technical docs ([`a65a509`](https://github.com/maidsafe/safe_network/commit/a65a5093e55b233dc4fa1293217d6bcd55a9d731))
    - remove some code duplication ([`a7f017c`](https://github.com/maidsafe/safe_network/commit/a7f017cc55da5cba53e8f10063afefc61ea5635a))
    - improve namings ([`a0b2df5`](https://github.com/maidsafe/safe_network/commit/a0b2df5a0b12c70872dfc854d660afd0cf8b21aa))
    - use latest sdkg ([`09c4891`](https://github.com/maidsafe/safe_network/commit/09c48916a7f2145c1d3cd091d6219fb7150fe1c2))
    - update cargo sn_sdkg dep ([`7654f58`](https://github.com/maidsafe/safe_network/commit/7654f58dbde0c44fd799da16b2a3c4e3a73217df))
    - wait a safe 5 chain len before removing old dkgs ([`45072a6`](https://github.com/maidsafe/safe_network/commit/45072a674021efe9efd1eeaf5f372b8a8bfba4c8))
    - compile after rebase ([`9a1cdf6`](https://github.com/maidsafe/safe_network/commit/9a1cdf6f0135ce53f43a48c4346aff9023ccad33))
    - keep old DKG sessions for 2 more section churns ([`1c27773`](https://github.com/maidsafe/safe_network/commit/1c277731397527b26b8e5edb16d9a5ffda7243d7))
    - split barrier race condition ([`85d3361`](https://github.com/maidsafe/safe_network/commit/85d3361b3c4b9f66a456a26671406660e641e41f))
    - section info agreement using sap gen to check candidates ([`01c8f5d`](https://github.com/maidsafe/safe_network/commit/01c8f5d799fce7bf3b8600042587626900940d01))
    - cargo dep rev ([`269245f`](https://github.com/maidsafe/safe_network/commit/269245f3289c8d4f482e101aad809a325d67ed7c))
    - use sn_sdkg github dependency and clippy ([`cbf5ed4`](https://github.com/maidsafe/safe_network/commit/cbf5ed4b1065d5d4471bc27291c054f48b64678e))
    - github sn_dkg dependency ([`3d72e4b`](https://github.com/maidsafe/safe_network/commit/3d72e4b71c079f7ddd8be08642165e53ebf987f6))
    - gossip DKG termination to trigger handover ([`f4b5d6b`](https://github.com/maidsafe/safe_network/commit/f4b5d6b207a0874621d777582ca5906e69196e06))
    - selfish handover issue and limit known msg reprocessing ([`5280460`](https://github.com/maidsafe/safe_network/commit/5280460bb43a013d8273e5c30742c71596e4799a))
    - clippy ([`e1e161b`](https://github.com/maidsafe/safe_network/commit/e1e161ba54ed8e0af298814f0da5b953ff053c93))
    - compile after rebase ([`9992d97`](https://github.com/maidsafe/safe_network/commit/9992d9701ecadff2b7682e47387014b9d11dba63))
    - outdated gossip AE ([`45d194d`](https://github.com/maidsafe/safe_network/commit/45d194d4d5927450a91a73c776c68871e857d48c))
    - dkg start miss handling ([`3ed0e01`](https://github.com/maidsafe/safe_network/commit/3ed0e0167d5bec04d6c57d94ce1a63d1f043a1a0))
    - dkg gossip ([`40fa4d9`](https://github.com/maidsafe/safe_network/commit/40fa4d9c0308c1104aaff60890bd48e08c8508b2))
    - logging improvement ([`f4c3808`](https://github.com/maidsafe/safe_network/commit/f4c3808f582cf06d58303383296af4ab7a13f0df))
    - invalid signature during plit DKGs ([`30fa400`](https://github.com/maidsafe/safe_network/commit/30fa400d2fa1d5029f5e0190c506cc92b0bcdf09))
    - bls key upgrade issue, more logs ([`22a0de0`](https://github.com/maidsafe/safe_network/commit/22a0de0e3bd8478a10729112ec1b3bce9ba5cb90))
    - cleanup old DKG sessions when a Handover is complete ([`69dce88`](https://github.com/maidsafe/safe_network/commit/69dce88a25254dd84a98ea9571ffccef17208744))
    - some necessary cleanup ([`ba859ae`](https://github.com/maidsafe/safe_network/commit/ba859ae5f064d6dc15aa563ee956a26e85df1d45))
    - compiling sdkg integration ([`f5d53c1`](https://github.com/maidsafe/safe_network/commit/f5d53c188a03150a06bdee97fb585f7900b7c251))
    - sn_dkg integration ([`b647469`](https://github.com/maidsafe/safe_network/commit/b6474691ea6af5ee441b02f6cb9c3cf2b8f97459))
    - client retry spend on unknown section key ([`5c8b1f5`](https://github.com/maidsafe/safe_network/commit/5c8b1f50d1bf346d45bd2a9faf92bbf33cb448da))
    - move test_utils module ([`072c5d4`](https://github.com/maidsafe/safe_network/commit/072c5d4c5de7810a0837144853435e2ff2d091d0))
    - move build_spent_proof_share to sn_interface ([`6108807`](https://github.com/maidsafe/safe_network/commit/610880711da814c7717c665e9cb34a729bda5797))
    - get public commitments from sn_dbc ([`1152b27`](https://github.com/maidsafe/safe_network/commit/1152b2764e955edd80fb33921a8d8fe52654a896))
    - add nightly fixes ([`2e93714`](https://github.com/maidsafe/safe_network/commit/2e937145c39039ee55505f00637cf484943f4471))
    - remove unused rs files ([`80446f5`](https://github.com/maidsafe/safe_network/commit/80446f5d9df88d5915dcf1d3ea2c213c22e40c14))
    - remove unused rs files ([`3f52833`](https://github.com/maidsafe/safe_network/commit/3f52833a8ce977aa79268ecaac61070f01e9c374))
    - add nightly fixes ([`77cb17c`](https://github.com/maidsafe/safe_network/commit/77cb17c41bbf258c3f1b16934c4c71b5e5ad2456))
    - minor refactoring and fixing issue reported by new clippy version ([`c7de082`](https://github.com/maidsafe/safe_network/commit/c7de08209c659ec93557d6ea10e0bcd8c3b74d8b))
    - Merge #1557 ([`6cac22a`](https://github.com/maidsafe/safe_network/commit/6cac22af4994651719f64bc76391d729a3efb656))
    - remove spend retry on client ([`100e2ae`](https://github.com/maidsafe/safe_network/commit/100e2ae70d21e141e1ebbc324f8b06e3d3f1a01c))
    - spend with updated network knowledge ([`230a6ed`](https://github.com/maidsafe/safe_network/commit/230a6ed7f1f4193fa36b2fbb83bea072f4944c1d))
    - remove redundant genesis_key argument in `NetworkKnowledge` constructor ([`e973eee`](https://github.com/maidsafe/safe_network/commit/e973eee96c9065ce87a1fa65ae45d9be8d6f940c))
    - bundle proof chain, SAP into `SectionTreeUpdate` ([`60e333d`](https://github.com/maidsafe/safe_network/commit/60e333d4ced688f3382cde513300d38790613692))
    - retry dbc spend on unknown section key ([`057ce1c`](https://github.com/maidsafe/safe_network/commit/057ce1ce1e174102e23d96cfcd2ab1d090a6f1dc))
    - dbc spend can update network knowledge ([`2020ef1`](https://github.com/maidsafe/safe_network/commit/2020ef1a91c8520abc4bb74d3de6385b8cd283b4))
    - pull out will-be-elder check into node ([`6343b6f`](https://github.com/maidsafe/safe_network/commit/6343b6fd21fe3bf81412d922da5e14b2c8b6f3c5))
    - add node helper for updating net_know ([`0176a56`](https://github.com/maidsafe/safe_network/commit/0176a56a311dd8450f7cd845bc37cc28a7b11c0d))
    - remove unused genesis parameter ([`8bf032f`](https://github.com/maidsafe/safe_network/commit/8bf032fff0ba48816311d6ea6967e3c300aedccf))
    - Merge #1527 ([`1f06d6e`](https://github.com/maidsafe/safe_network/commit/1f06d6e90da6f889221f37cc8eac32b6933a94ba))
    - optimizations and code cleanup ([`03da7f6`](https://github.com/maidsafe/safe_network/commit/03da7f67fff1fa5bb06d60a66dfdb531506dec4c))
    - ignore update if we don't have KeyShare ([`4884c51`](https://github.com/maidsafe/safe_network/commit/4884c511d302522aa408ebf9350a7ff6cefeecb7))
    - custom Serializer, Deserializer for `SectionsDAG` ([`864c023`](https://github.com/maidsafe/safe_network/commit/864c023e26697a609a9ad230c04e7aef7416650c))
    - enable `SectionTree` proptest ([`859fc30`](https://github.com/maidsafe/safe_network/commit/859fc30fa70ce41ceb910e0352c71dda5c5501ce))
    - replace `SecuredLinkedList` with `SectionsDAG` ([`0cd47ad`](https://github.com/maidsafe/safe_network/commit/0cd47ad56e0d93e3e99feb0dfcea8094f871ff6f))
</details>

## v0.71.0 (2022-09-19)

<csr-id-a8a9fb90791b29496e8559090dca4161e04054da/>
<csr-id-a0bc2562df4f427752ec0f3ab85d9befe2d20050/>
<csr-id-84cedf30fff0cc298f9f658d2c58499990967fe4/>
<csr-id-2d1221999b959bf4d0879cf42050d5e1e3119445/>

### Chore

 - <csr-id-a8a9fb90791b29496e8559090dca4161e04054da/> sn_interface-0.15.0/sn_dysfunction-0.14.0/sn_client-0.76.0/sn_node-0.71.0/sn_api-0.74.0/sn_cli-0.67.0
 - <csr-id-a0bc2562df4f427752ec0f3ab85d9befe2d20050/> cleanup unused deps
 - <csr-id-84cedf30fff0cc298f9f658d2c58499990967fe4/> remove unused back-pressure code

### Bug Fixes

 - <csr-id-f1b7d17651fa105d2864059ce52a429f8b329af0/> pass id of parent to cmd

### Refactor (BREAKING)

 - <csr-id-2d1221999b959bf4d0879cf42050d5e1e3119445/> flattening up ServiceMsg::ServiceError and ServiceMsg::CmdError types

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 4 calendar days.
 - 9 days passed between releases.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.15.0/sn_dysfunction-0.14.0/sn_client-0.76.0/sn_node-0.71.0/sn_api-0.74.0/sn_cli-0.67.0 ([`a8a9fb9`](https://github.com/maidsafe/safe_network/commit/a8a9fb90791b29496e8559090dca4161e04054da))
    - flattening up ServiceMsg::ServiceError and ServiceMsg::CmdError types ([`2d12219`](https://github.com/maidsafe/safe_network/commit/2d1221999b959bf4d0879cf42050d5e1e3119445))
    - cleanup unused deps ([`a0bc256`](https://github.com/maidsafe/safe_network/commit/a0bc2562df4f427752ec0f3ab85d9befe2d20050))
    - remove unused back-pressure code ([`84cedf3`](https://github.com/maidsafe/safe_network/commit/84cedf30fff0cc298f9f658d2c58499990967fe4))
    - pass id of parent to cmd ([`f1b7d17`](https://github.com/maidsafe/safe_network/commit/f1b7d17651fa105d2864059ce52a429f8b329af0))
</details>

## v0.70.0 (2022-09-09)

<csr-id-448694176dd3b40a12bd8ecc16d9bb66fd171a37/>
<csr-id-5e70a7afff0d0969143d75f92ce82a02bc40a7b8/>
<csr-id-927931c9eb833df3e589d72affc4839ba57b5cc2/>

### Chore

 - <csr-id-448694176dd3b40a12bd8ecc16d9bb66fd171a37/> sn_interface-0.14.0/sn_dysfunction-0.13.0/sn_client-0.75.0/sn_node-0.70.0/sn_api-0.73.0/sn_cli-0.66.0

### New Features

 - <csr-id-7c8b022a53adbfb3abdd31a56f47bedc53031d1d/> set statemap as default in sn_node; docs in README
 - <csr-id-1601027ae1d88ef282a00cccdf2b01490c3e5224/> remove unnecessary double init for handover
 - <csr-id-ece2514375980140cf2adc5c263cd3878d6e1dc6/> don't HandlePeerFailedSend for ServiceMsgs

### Bug Fixes

 - <csr-id-829f9fbfefc001e73d1aa6a00c4c6ac5de4c87d3/> add missing back-pressure feature gate to code

### Refactor

 - <csr-id-5e70a7afff0d0969143d75f92ce82a02bc40a7b8/> avoiding unnecessary operation id generation upon query response at elder

### Chore (BREAKING)

 - <csr-id-927931c9eb833df3e589d72affc4839ba57b5cc2/> removing unused SystemMsg::NodeMsgError msg type

### New Features (BREAKING)

 - <csr-id-7bedb7bb7614a8af05f5892a28ff4732e87d4796/> return an error to the client when it cannot accept a query

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 10 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 8 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.14.0/sn_dysfunction-0.13.0/sn_client-0.75.0/sn_node-0.70.0/sn_api-0.73.0/sn_cli-0.66.0 ([`4486941`](https://github.com/maidsafe/safe_network/commit/448694176dd3b40a12bd8ecc16d9bb66fd171a37))
    - add missing back-pressure feature gate to code ([`829f9fb`](https://github.com/maidsafe/safe_network/commit/829f9fbfefc001e73d1aa6a00c4c6ac5de4c87d3))
    - avoiding unnecessary operation id generation upon query response at elder ([`5e70a7a`](https://github.com/maidsafe/safe_network/commit/5e70a7afff0d0969143d75f92ce82a02bc40a7b8))
    - removing unused SystemMsg::NodeMsgError msg type ([`927931c`](https://github.com/maidsafe/safe_network/commit/927931c9eb833df3e589d72affc4839ba57b5cc2))
    - return an error to the client when it cannot accept a query ([`7bedb7b`](https://github.com/maidsafe/safe_network/commit/7bedb7bb7614a8af05f5892a28ff4732e87d4796))
    - set statemap as default in sn_node; docs in README ([`7c8b022`](https://github.com/maidsafe/safe_network/commit/7c8b022a53adbfb3abdd31a56f47bedc53031d1d))
    - Merge #1559 ([`e762528`](https://github.com/maidsafe/safe_network/commit/e762528eabf076148b5c6767e61b16901681bffe))
    - remove unnecessary double init for handover ([`1601027`](https://github.com/maidsafe/safe_network/commit/1601027ae1d88ef282a00cccdf2b01490c3e5224))
    - Merge branch 'main' into Chore-ClientRetriesOnDataNotFound ([`bbca976`](https://github.com/maidsafe/safe_network/commit/bbca97680840e1069c88278fe14ddee153b97dbb))
    - don't HandlePeerFailedSend for ServiceMsgs ([`ece2514`](https://github.com/maidsafe/safe_network/commit/ece2514375980140cf2adc5c263cd3878d6e1dc6))
</details>

## v0.69.0 (2022-09-07)

<csr-id-fe659c5685289fe0071b54298dcac394e83c0dce/>
<csr-id-84bfdaaf5b0df86912fef806dcb04f353e828b69/>
<csr-id-638bcdfea4cbc713d8a4faecec7ed8538317fa29/>
<csr-id-0c49daf5dbfad2593ccf13cb114841045688ffed/>

### Chore

 - <csr-id-fe659c5685289fe0071b54298dcac394e83c0dce/> sn_interface-0.13.0/sn_dysfunction-0.12.0/sn_client-0.74.0/sn_node-0.69.0/sn_api-0.72.0/sn_cli-0.65.0
 - <csr-id-84bfdaaf5b0df86912fef806dcb04f353e828b69/> pass by reference instead of by value

### Bug Fixes

 - <csr-id-4dcbd154698c5f7302502490724150e8eefe32af/> avoid using timeout to handle ContentNotFound
   Instead of waiting for another adult to respond, return the error.
   
   The requeueing is ineffective since the node will not respond again and
   the operation will linger in the cache until it expires.

### Refactor

 - <csr-id-638bcdfea4cbc713d8a4faecec7ed8538317fa29/> minor refactor to Capacity functions impl, plus removing unused fns

### New Features (BREAKING)

 - <csr-id-d671f4ee4c76b42187d266aee99351114acf6cd7/> report any error occurred when handling a service msg back to the client
   - Removing several unused sn_node::Error types.

### Refactor (BREAKING)

 - <csr-id-0c49daf5dbfad2593ccf13cb114841045688ffed/> removing unused Error types
   - Minor refactor to how we convert sn_node modules Error types to sn_interface::Error types.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 6 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.13.0/sn_dysfunction-0.12.0/sn_client-0.74.0/sn_node-0.69.0/sn_api-0.72.0/sn_cli-0.65.0 ([`fe659c5`](https://github.com/maidsafe/safe_network/commit/fe659c5685289fe0071b54298dcac394e83c0dce))
    - minor refactor to Capacity functions impl, plus removing unused fns ([`638bcdf`](https://github.com/maidsafe/safe_network/commit/638bcdfea4cbc713d8a4faecec7ed8538317fa29))
    - avoid using timeout to handle ContentNotFound ([`4dcbd15`](https://github.com/maidsafe/safe_network/commit/4dcbd154698c5f7302502490724150e8eefe32af))
    - pass by reference instead of by value ([`84bfdaa`](https://github.com/maidsafe/safe_network/commit/84bfdaaf5b0df86912fef806dcb04f353e828b69))
    - removing unused Error types ([`0c49daf`](https://github.com/maidsafe/safe_network/commit/0c49daf5dbfad2593ccf13cb114841045688ffed))
    - report any error occurred when handling a service msg back to the client ([`d671f4e`](https://github.com/maidsafe/safe_network/commit/d671f4ee4c76b42187d266aee99351114acf6cd7))
</details>

## v0.68.0 (2022-09-06)

<csr-id-d28fdf3ddd0a39f7bbc6426e1e71d990319b0ec7/>
<csr-id-bcbca889993268429636b003c5ae50ed6cbda527/>
<csr-id-dd89cac97da96ffe26ae78c4b7b62aa952ec53fc/>
<csr-id-921438659ccaf65b2ea8cc00efb61d8146ef71ef/>
<csr-id-933f4282fce2e28a1956fd1b50cc8061a68e1515/>
<csr-id-b040ea14e53247094838de6f1fa9af2830b051fa/>
<csr-id-3a718d8c0957957a75250b044c9d1ad1b5874ab0/>
<csr-id-906625895228de811515e7b71d5f55d067964d24/>
<csr-id-7d8956a8d7ac2bf0961b90b19c48e89dda6cfb21/>
<csr-id-39dd5a043c75492e416bb9371015a1365b06fa01/>
<csr-id-183d7f83985a36deeb5933ae9b1880df21da2866/>
<csr-id-070f7d8902c3bbc2a88b3be1a8f44de3c2726df6/>
<csr-id-4a9d4c4bdc8f81d11ea78f888368f64d76754d8e/>
<csr-id-63958a8629c9fbca8e6604edb17d9b61ca92a4ee/>
<csr-id-9f9a95614991197b240b3c1a363eb4e5946d3fae/>
<csr-id-62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2/>
<csr-id-277d925aa3ed5ff01eab8c1d2488f653cfe6effc/>
<csr-id-a6685348578fe546576bd13405e6485d984b3487/>
<csr-id-ed9f627d0e2c42ab1b7386888cced751ae28f98a/>
<csr-id-5b73b33b683991be9e9f6440c3d8d568edab3ad6/>
<csr-id-b7530feb40987f433ff12c5176cfdbc375359dc6/>
<csr-id-1b9e0a6564e9564201ef3a3e04adb0bfbef6ac14/>

### Chore

 - <csr-id-d28fdf3ddd0a39f7bbc6426e1e71d990319b0ec7/> sn_interface-0.11.0/sn_dysfunction-0.10.0/sn_client-0.72.0/sn_node-0.67.0/sn_api-0.70.0/sn_cli-0.63.0
 - <csr-id-bcbca889993268429636b003c5ae50ed6cbda527/> nightly question_mark lint applied
 - <csr-id-dd89cac97da96ffe26ae78c4b7b62aa952ec53fc/> replace implicit clones with clone
 - <csr-id-921438659ccaf65b2ea8cc00efb61d8146ef71ef/> unneeded iter methods removal
 - <csr-id-933f4282fce2e28a1956fd1b50cc8061a68e1515/> makefile targets for easier local network use
   There are now two targets for running a local network, one that does a clean build and one that
   doesn't. The nodes have debug logging set and also use a debug build for speed.
   
   Also added some additional logging output to the command processing code to indicate the return of
   an error to the client.
   
   This also fixes up a mistake made while resolving a merge conflict.
 - <csr-id-b040ea14e53247094838de6f1fa9af2830b051fa/> sn_interface lints and fixes
   Apply lints used in other crates, as far as they can easily be applied.
   The `unused_results` lint has been left out, as that is too much
   cleaning up to do, just like adding documentation to all the public
   interfaces.
 - <csr-id-3a718d8c0957957a75250b044c9d1ad1b5874ab0/> switch on clippy::unwrap_used as a warning


### Chore

 - <csr-id-1b9e0a6564e9564201ef3a3e04adb0bfbef6ac14/> sn_interface-0.12.0/sn_dysfunction-0.11.0/sn_client-0.73.0/sn_node-0.68.0/sn_api-0.71.0/sn_cli-0.64.0

### New Features

 - <csr-id-08a0a8eb75a0ca9d51fa321686d17dbcf97fc04e/> fix time alignment; more states; mv to sn_interface
 - <csr-id-5b43e28eba3cb81d3122fe7d62737a2f51e174d6/> use err result to send errors back to client
   Previously the mechanism for handling errors in `handle_valid_service_msg` was to return an `Ok`
   result with an empty list of commands, or if you wanted to send an error response back to the
   client, to include a command in that `Ok` result.
   
   A new `CmdProcessingClientRespondError` variant is added to `Error` to enable returning an `Err`
   result when we want to return an error back to the client. When the command processing code
   encounters this error variant, it will process the commands included in the error, which will be the
   information to send back to the client.
   
   The intention is also to return `Err` results from `handle_valid_service_msg` when any errors are
   encountered, not just for the limited few we want to send back to the client.
 - <csr-id-b1a5590e84eb7a0f4872e42de61c3c5d33ae9fdf/> move statemap logs behind feature flag
 - <csr-id-5b9bb3bd70741fa7bd9cf0db3dbdf7284d62f343/> breakdown states by cmd type
 - <csr-id-1abeb803bfb7effbbb23d1fad75b341d2f3c9402/> better colors
 - <csr-id-8badef313d6f78e0183402983120f2184c130407/> some minimal infrastructure for generating statemaps

### Bug Fixes

 - <csr-id-7c207e407533c09108ae6668d51b398caf896f0f/> always resend query to adults
 - <csr-id-6bdc82295dfdcaa617c7c1e36d2b72f085e50042/> update qp2p for unique ids
   Latest qp2p should provide global unique connection id
   
   previously duplication of ids could have been breaking
   connection management
 - <csr-id-d20f4e0aed02f99d44d5d1407bb7a42b67baf878/> send adult -> elder ae probes before split
 - <csr-id-fec85dbc3b857458de5c09a02490eab7c1227b40/> avoid testing data collision during bench test

### Other

 - <csr-id-906625895228de811515e7b71d5f55d067964d24/> fix missing block on in data_store bench
 - <csr-id-7d8956a8d7ac2bf0961b90b19c48e89dda6cfb21/> fix missing await in data_storage bench

### Refactor

 - <csr-id-39dd5a043c75492e416bb9371015a1365b06fa01/> small tweaks; clippy::equatable_if_let
 - <csr-id-183d7f83985a36deeb5933ae9b1880df21da2866/> skip spentbook register creation if it already exists
 - <csr-id-070f7d8902c3bbc2a88b3be1a8f44de3c2726df6/> optimise the way we apply and save replicated Register cmds on local storage
 - <csr-id-4a9d4c4bdc8f81d11ea78f888368f64d76754d8e/> remove stopped state from CmdCtrl
 - <csr-id-63958a8629c9fbca8e6604edb17d9b61ca92a4ee/> move probe creation to network knowledge
 - <csr-id-9f9a95614991197b240b3c1a363eb4e5946d3fae/> err result from gen spent proof share
   The spent proof share generation function now returns `SpentProofShare` directly rather than an
   `Option`, and we return `Err` results rather than `Ok(None)`. The unit tests were then updated to
   check for specific kinds of errors. The calling code is also a bit cleaner since it can just use the
   `?` operator.
   
   Note the use of an `allow` attribute to suppress a Clippy warning about this function having too
   many arguments. The additional arguments are just required for the composition of the error that
   gets sent back to the client, which I consider justified. Otherwise this error will need to be
   handled in a special way outside the function, which is an unnecessary complication.
 - <csr-id-62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2/> separating internal chunk from register store implementation layer

### Test

 - <csr-id-277d925aa3ed5ff01eab8c1d2488f653cfe6effc/> unit tests for register storage key internal helper functions

### New Features (BREAKING)

 - <csr-id-f5361d91f8215585651229eb6dc2535f2ecb631c/> update qp2p to use UsrMsgBytes and avoid reserializing bytes
   This makes use of udpate qp2p to avoid having to reserialise the
   WireMsgheader for every message when we're just updating the Dst.
   
   This in turn avoids the neccesity to clone the msg payload when
   serilizing; allowing us to to use the shared data struct Bytes for all
   parts, reducing both compute and memory use.

### Refactor (BREAKING)

 - <csr-id-a6685348578fe546576bd13405e6485d984b3487/> improving internal helpers in register storage mod to reuse some logic/code
   - Removing some storage Error types, while adding more context information to others.
   - Allowing the Register storage to store 'Register edit' cmds even when the 'Register create' cmd
   is not found in the local replica/store yet.
 - <csr-id-ed9f627d0e2c42ab1b7386888cced751ae28f98a/> removing unnecessary ReplicatedDataAddress type
 - <csr-id-5b73b33b683991be9e9f6440c3d8d568edab3ad6/> removing unnecessary types
 - <csr-id-b7530feb40987f433ff12c5176cfdbc375359dc6/> moving encoding/decoding utilities of data addresses types to storage impl

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 41 commits contributed to the release over the course of 8 calendar days.
 - 8 days passed between releases.
 - 33 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.12.0/sn_dysfunction-0.11.0/sn_client-0.73.0/sn_node-0.68.0/sn_api-0.71.0/sn_cli-0.64.0 ([`1b9e0a6`](https://github.com/maidsafe/safe_network/commit/1b9e0a6564e9564201ef3a3e04adb0bfbef6ac14))
    - update qp2p to use UsrMsgBytes and avoid reserializing bytes ([`f5361d9`](https://github.com/maidsafe/safe_network/commit/f5361d91f8215585651229eb6dc2535f2ecb631c))
    - Merge #1545 ([`b62c056`](https://github.com/maidsafe/safe_network/commit/b62c056b0b28f67a40d9e036b2d64b36fd5380bd))
    - Merge #1544 ([`e8202a6`](https://github.com/maidsafe/safe_network/commit/e8202a6ea8c07f8ae0a04273b2cda350758352ab))
    - always resend query to adults ([`7c207e4`](https://github.com/maidsafe/safe_network/commit/7c207e407533c09108ae6668d51b398caf896f0f))
    - update qp2p for unique ids ([`6bdc822`](https://github.com/maidsafe/safe_network/commit/6bdc82295dfdcaa617c7c1e36d2b72f085e50042))
    - sn_interface-0.11.0/sn_dysfunction-0.10.0/sn_client-0.72.0/sn_node-0.67.0/sn_api-0.70.0/sn_cli-0.63.0 ([`d28fdf3`](https://github.com/maidsafe/safe_network/commit/d28fdf3ddd0a39f7bbc6426e1e71d990319b0ec7))
    - fix missing block on in data_store bench ([`9066258`](https://github.com/maidsafe/safe_network/commit/906625895228de811515e7b71d5f55d067964d24))
    - fix missing await in data_storage bench ([`7d8956a`](https://github.com/maidsafe/safe_network/commit/7d8956a8d7ac2bf0961b90b19c48e89dda6cfb21))
    - nightly question_mark lint applied ([`bcbca88`](https://github.com/maidsafe/safe_network/commit/bcbca889993268429636b003c5ae50ed6cbda527))
    - replace implicit clones with clone ([`dd89cac`](https://github.com/maidsafe/safe_network/commit/dd89cac97da96ffe26ae78c4b7b62aa952ec53fc))
    - unneeded iter methods removal ([`9214386`](https://github.com/maidsafe/safe_network/commit/921438659ccaf65b2ea8cc00efb61d8146ef71ef))
    - small tweaks; clippy::equatable_if_let ([`39dd5a0`](https://github.com/maidsafe/safe_network/commit/39dd5a043c75492e416bb9371015a1365b06fa01))
    - Merge #1525 ([`6884257`](https://github.com/maidsafe/safe_network/commit/6884257ae51616949b0dfaefaa47fcdd090a7d54))
    - unit tests for register storage key internal helper functions ([`277d925`](https://github.com/maidsafe/safe_network/commit/277d925aa3ed5ff01eab8c1d2488f653cfe6effc))
    - skip spentbook register creation if it already exists ([`183d7f8`](https://github.com/maidsafe/safe_network/commit/183d7f83985a36deeb5933ae9b1880df21da2866))
    - improving internal helpers in register storage mod to reuse some logic/code ([`a668534`](https://github.com/maidsafe/safe_network/commit/a6685348578fe546576bd13405e6485d984b3487))
    - optimise the way we apply and save replicated Register cmds on local storage ([`070f7d8`](https://github.com/maidsafe/safe_network/commit/070f7d8902c3bbc2a88b3be1a8f44de3c2726df6))
    - Merge #1535 ([`7327112`](https://github.com/maidsafe/safe_network/commit/7327112da76871d52b5039546419ab18e41982f8))
    - fix time alignment; more states; mv to sn_interface ([`08a0a8e`](https://github.com/maidsafe/safe_network/commit/08a0a8eb75a0ca9d51fa321686d17dbcf97fc04e))
    - Merge #1536 ([`5194123`](https://github.com/maidsafe/safe_network/commit/519412319c9b7504c97cbeae6e398a210226d14e))
    - send adult -> elder ae probes before split ([`d20f4e0`](https://github.com/maidsafe/safe_network/commit/d20f4e0aed02f99d44d5d1407bb7a42b67baf878))
    - remove stopped state from CmdCtrl ([`4a9d4c4`](https://github.com/maidsafe/safe_network/commit/4a9d4c4bdc8f81d11ea78f888368f64d76754d8e))
    - move probe creation to network knowledge ([`63958a8`](https://github.com/maidsafe/safe_network/commit/63958a8629c9fbca8e6604edb17d9b61ca92a4ee))
    - makefile targets for easier local network use ([`933f428`](https://github.com/maidsafe/safe_network/commit/933f4282fce2e28a1956fd1b50cc8061a68e1515))
    - err result from gen spent proof share ([`9f9a956`](https://github.com/maidsafe/safe_network/commit/9f9a95614991197b240b3c1a363eb4e5946d3fae))
    - use err result to send errors back to client ([`5b43e28`](https://github.com/maidsafe/safe_network/commit/5b43e28eba3cb81d3122fe7d62737a2f51e174d6))
    - chore(clippy) ([`611fc23`](https://github.com/maidsafe/safe_network/commit/611fc23f9174aabf85c7adea7159677c1508d388))
    - move statemap logs behind feature flag ([`b1a5590`](https://github.com/maidsafe/safe_network/commit/b1a5590e84eb7a0f4872e42de61c3c5d33ae9fdf))
    - breakdown states by cmd type ([`5b9bb3b`](https://github.com/maidsafe/safe_network/commit/5b9bb3bd70741fa7bd9cf0db3dbdf7284d62f343))
    - better colors ([`1abeb80`](https://github.com/maidsafe/safe_network/commit/1abeb803bfb7effbbb23d1fad75b341d2f3c9402))
    - some minimal infrastructure for generating statemaps ([`8badef3`](https://github.com/maidsafe/safe_network/commit/8badef313d6f78e0183402983120f2184c130407))
    - sn_interface lints and fixes ([`b040ea1`](https://github.com/maidsafe/safe_network/commit/b040ea14e53247094838de6f1fa9af2830b051fa))
    - Merge #1521 ([`755c9c1`](https://github.com/maidsafe/safe_network/commit/755c9c1653dd7b700fa1f7d25269cf1352f18f3c))
    - Merge branch 'main' into avoid_testing_data_collision ([`60c368b`](https://github.com/maidsafe/safe_network/commit/60c368b8494eaeb219572c2304bf787a168cfee0))
    - avoid testing data collision during bench test ([`fec85db`](https://github.com/maidsafe/safe_network/commit/fec85dbc3b857458de5c09a02490eab7c1227b40))
    - switch on clippy::unwrap_used as a warning ([`3a718d8`](https://github.com/maidsafe/safe_network/commit/3a718d8c0957957a75250b044c9d1ad1b5874ab0))
    - separating internal chunk from register store implementation layer ([`62bc8d6`](https://github.com/maidsafe/safe_network/commit/62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2))
    - removing unnecessary ReplicatedDataAddress type ([`ed9f627`](https://github.com/maidsafe/safe_network/commit/ed9f627d0e2c42ab1b7386888cced751ae28f98a))
    - removing unnecessary types ([`5b73b33`](https://github.com/maidsafe/safe_network/commit/5b73b33b683991be9e9f6440c3d8d568edab3ad6))
    - moving encoding/decoding utilities of data addresses types to storage impl ([`b7530fe`](https://github.com/maidsafe/safe_network/commit/b7530feb40987f433ff12c5176cfdbc375359dc6))
</details>

## v0.67.0 (2022-09-04)

<csr-id-bcbca889993268429636b003c5ae50ed6cbda527/>
<csr-id-dd89cac97da96ffe26ae78c4b7b62aa952ec53fc/>
<csr-id-921438659ccaf65b2ea8cc00efb61d8146ef71ef/>
<csr-id-933f4282fce2e28a1956fd1b50cc8061a68e1515/>
<csr-id-b040ea14e53247094838de6f1fa9af2830b051fa/>
<csr-id-3a718d8c0957957a75250b044c9d1ad1b5874ab0/>
<csr-id-906625895228de811515e7b71d5f55d067964d24/>
<csr-id-7d8956a8d7ac2bf0961b90b19c48e89dda6cfb21/>
<csr-id-39dd5a043c75492e416bb9371015a1365b06fa01/>
<csr-id-183d7f83985a36deeb5933ae9b1880df21da2866/>
<csr-id-070f7d8902c3bbc2a88b3be1a8f44de3c2726df6/>
<csr-id-4a9d4c4bdc8f81d11ea78f888368f64d76754d8e/>
<csr-id-63958a8629c9fbca8e6604edb17d9b61ca92a4ee/>
<csr-id-9f9a95614991197b240b3c1a363eb4e5946d3fae/>
<csr-id-62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2/>
<csr-id-277d925aa3ed5ff01eab8c1d2488f653cfe6effc/>
<csr-id-a6685348578fe546576bd13405e6485d984b3487/>
<csr-id-ed9f627d0e2c42ab1b7386888cced751ae28f98a/>
<csr-id-5b73b33b683991be9e9f6440c3d8d568edab3ad6/>
<csr-id-b7530feb40987f433ff12c5176cfdbc375359dc6/>

### Chore

 - <csr-id-bcbca889993268429636b003c5ae50ed6cbda527/> nightly question_mark lint applied
 - <csr-id-dd89cac97da96ffe26ae78c4b7b62aa952ec53fc/> replace implicit clones with clone
 - <csr-id-921438659ccaf65b2ea8cc00efb61d8146ef71ef/> unneeded iter methods removal
 - <csr-id-933f4282fce2e28a1956fd1b50cc8061a68e1515/> makefile targets for easier local network use
   There are now two targets for running a local network, one that does a clean build and one that
   doesn't. The nodes have debug logging set and also use a debug build for speed.
   
   Also added some additional logging output to the command processing code to indicate the return of
   an error to the client.
   
   This also fixes up a mistake made while resolving a merge conflict.
 - <csr-id-b040ea14e53247094838de6f1fa9af2830b051fa/> sn_interface lints and fixes
   Apply lints used in other crates, as far as they can easily be applied.
   The `unused_results` lint has been left out, as that is too much
   cleaning up to do, just like adding documentation to all the public
   interfaces.
 - <csr-id-3a718d8c0957957a75250b044c9d1ad1b5874ab0/> switch on clippy::unwrap_used as a warning


### New Features

 - <csr-id-08a0a8eb75a0ca9d51fa321686d17dbcf97fc04e/> fix time alignment; more states; mv to sn_interface
 - <csr-id-5b43e28eba3cb81d3122fe7d62737a2f51e174d6/> use err result to send errors back to client
   Previously the mechanism for handling errors in `handle_valid_service_msg` was to return an `Ok`
   result with an empty list of commands, or if you wanted to send an error response back to the
   client, to include a command in that `Ok` result.
   
   A new `CmdProcessingClientRespondError` variant is added to `Error` to enable returning an `Err`
   result when we want to return an error back to the client. When the command processing code
   encounters this error variant, it will process the commands included in the error, which will be the
   information to send back to the client.
   
   The intention is also to return `Err` results from `handle_valid_service_msg` when any errors are
   encountered, not just for the limited few we want to send back to the client.
 - <csr-id-b1a5590e84eb7a0f4872e42de61c3c5d33ae9fdf/> move statemap logs behind feature flag
 - <csr-id-5b9bb3bd70741fa7bd9cf0db3dbdf7284d62f343/> breakdown states by cmd type
 - <csr-id-1abeb803bfb7effbbb23d1fad75b341d2f3c9402/> better colors
 - <csr-id-8badef313d6f78e0183402983120f2184c130407/> some minimal infrastructure for generating statemaps

### Bug Fixes

 - <csr-id-d20f4e0aed02f99d44d5d1407bb7a42b67baf878/> send adult -> elder ae probes before split
 - <csr-id-fec85dbc3b857458de5c09a02490eab7c1227b40/> avoid testing data collision during bench test

### Other

 - <csr-id-906625895228de811515e7b71d5f55d067964d24/> fix missing block on in data_store bench
 - <csr-id-7d8956a8d7ac2bf0961b90b19c48e89dda6cfb21/> fix missing await in data_storage bench

### Refactor

 - <csr-id-39dd5a043c75492e416bb9371015a1365b06fa01/> small tweaks; clippy::equatable_if_let
 - <csr-id-183d7f83985a36deeb5933ae9b1880df21da2866/> skip spentbook register creation if it already exists
 - <csr-id-070f7d8902c3bbc2a88b3be1a8f44de3c2726df6/> optimise the way we apply and save replicated Register cmds on local storage
 - <csr-id-4a9d4c4bdc8f81d11ea78f888368f64d76754d8e/> remove stopped state from CmdCtrl
 - <csr-id-63958a8629c9fbca8e6604edb17d9b61ca92a4ee/> move probe creation to network knowledge
 - <csr-id-9f9a95614991197b240b3c1a363eb4e5946d3fae/> err result from gen spent proof share
   The spent proof share generation function now returns `SpentProofShare` directly rather than an
   `Option`, and we return `Err` results rather than `Ok(None)`. The unit tests were then updated to
   check for specific kinds of errors. The calling code is also a bit cleaner since it can just use the
   `?` operator.
   
   Note the use of an `allow` attribute to suppress a Clippy warning about this function having too
   many arguments. The additional arguments are just required for the composition of the error that
   gets sent back to the client, which I consider justified. Otherwise this error will need to be
   handled in a special way outside the function, which is an unnecessary complication.
 - <csr-id-62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2/> separating internal chunk from register store implementation layer

### Test

 - <csr-id-277d925aa3ed5ff01eab8c1d2488f653cfe6effc/> unit tests for register storage key internal helper functions

### Refactor (BREAKING)

 - <csr-id-a6685348578fe546576bd13405e6485d984b3487/> improving internal helpers in register storage mod to reuse some logic/code
   - Removing some storage Error types, while adding more context information to others.
   - Allowing the Register storage to store 'Register edit' cmds even when the 'Register create' cmd
   is not found in the local replica/store yet.
 - <csr-id-ed9f627d0e2c42ab1b7386888cced751ae28f98a/> removing unnecessary ReplicatedDataAddress type
 - <csr-id-5b73b33b683991be9e9f6440c3d8d568edab3ad6/> removing unnecessary types
 - <csr-id-b7530feb40987f433ff12c5176cfdbc375359dc6/> moving encoding/decoding utilities of data addresses types to storage impl

## v0.66.2 (2022-08-28)

<csr-id-b587893737bc51aee483f7cd53da782036dd6c5e/>
<csr-id-69b45973a58f9a866984b660fa13fef50d9f906e/>
<csr-id-2b268209e6910472558145a5d08b99e968550221/>

### New Features

 - <csr-id-7cc2a00907381e93db266f31545b12ff76907e5d/> implement `SecuredLinkedList` as a `MerkleRegister`
 - <csr-id-b87617e44e9b20b8a79864e30e29ecee86444352/> return error to client on unknown section key
   If one of the spent proofs sent by the client have been signed with a key this section is not
   currently aware of, return an error back to the client.
   
   This introduces a new SpentProofUnknownSectionKey variant to the messaging data errors, because none
   of the existing variants seemed appropriate for this scenario.

### Other

 - <csr-id-b587893737bc51aee483f7cd53da782036dd6c5e/> unit tests for spentbook handler
   Provide unit test coverage for the `SpentbookCmd::Spent` message handler.
   
   It's important to note that at this point, the failure cases only assert that no commands were
   returned from the handler, because this is the way we deal with failures at the moment.
   Unfortunately this means it's easy for there to be false positives because you can't check the error
   type or message. I will look into changing this as a separate PR.
   
   Most of the changes here are related to testing infrastructure:
   * Support setting a threshold when a secret key set is generated for the section. For use with the
     genesis DBC generation, the threshold had to be set to 0.
   * Support adults in the test section. The spent message generates data to be replicated on adults,
     so the mechanisms for creating a test section were extended for this. There are now
     `create_section` and `create_section_with_elders` functions, because some existing tests require
     the condition where only elders have been marked as members.
   * The genesis DBC is needed for these tests, so the scope of the function for generating it was
     changed to `pub(crate)`.
   * The `Cmd` struct was extended in the test module to provide utils to get at the content of
     messages, which are used for test verification.
   * Provide util function for wrapping a `ClientMsg` inside a `WireMsg` and so on. Keeps the testing
     code cleaner.
   * Provide util function for extracting the spent proof share from the replicated data so that we can
     verify the message handler assigned the correct values to its fields.
   * Various util functions related to the use of DBCs were provided in a `dbc_utils` module. The doc
     comments on the functions should hopefully make clear what they are for.
   
   A couple of superficial changes were also made to the message handler code:
   * The key image sent by the client is validated (along with a test case for that).
   * Change the format of debugging messages and comments to be more uniform.
   * Move some code into functions scoped at `pub(crate)`. This is so they can be shared for use with
     test setup. For further explanation, see the doc comments on these functions in the diff.

### Chore

 - <csr-id-2b268209e6910472558145a5d08b99e968550221/> sn_interface-0.10.2/sn_client-0.71.1/sn_node-0.66.2/sn_cli-0.62.1

### Refactor

 - <csr-id-69b45973a58f9a866984b660fa13fef50d9f906e/> removing unnecessary dbs layer from node storage implementation

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release over the course of 2 calendar days.
 - 3 days passed between releases.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.10.2/sn_client-0.71.1/sn_node-0.66.2/sn_cli-0.62.1 ([`2b26820`](https://github.com/maidsafe/safe_network/commit/2b268209e6910472558145a5d08b99e968550221))
    - implement `SecuredLinkedList` as a `MerkleRegister` ([`7cc2a00`](https://github.com/maidsafe/safe_network/commit/7cc2a00907381e93db266f31545b12ff76907e5d))
    - Merge #1512 ([`3ca0038`](https://github.com/maidsafe/safe_network/commit/3ca0038a32539cf20b61292661b755886d02717e))
    - return error to client on unknown section key ([`b87617e`](https://github.com/maidsafe/safe_network/commit/b87617e44e9b20b8a79864e30e29ecee86444352))
    - unit tests for spentbook handler ([`b587893`](https://github.com/maidsafe/safe_network/commit/b587893737bc51aee483f7cd53da782036dd6c5e))
    - removing unnecessary dbs layer from node storage implementation ([`69b4597`](https://github.com/maidsafe/safe_network/commit/69b45973a58f9a866984b660fa13fef50d9f906e))
</details>

## v0.66.1 (2022-08-25)

<csr-id-401bc416c7aea65ae55e9adee2cbecf782c999cf/>
<csr-id-d58f1c55e9502fd6e8a99509f7ca30640835458b/>
<csr-id-bbc06e4ee17d4bba60df54c084b82201b6b92e1b/>
<csr-id-ffc9c39daf4e6430334e7b1a36627c6e111a61fd/>
<csr-id-5a172a906f042bd6c5c32b672e13e73ddf2bced0/>
<csr-id-fd6b97b37bb875404ef2ba7f5f35d5675c122ea0/>
<csr-id-58a237e3985fa477226b09f62d5222d74d53fd9c/>
<csr-id-834ea1a1734b84b649690680cdba849abf7df3ea/>

### Chore

 - <csr-id-401bc416c7aea65ae55e9adee2cbecf782c999cf/> sn_interface-0.10.1/sn_client-0.71.0/sn_node-0.66.1/sn_api-0.69.0/sn_cli-0.62.0
 - <csr-id-d58f1c55e9502fd6e8a99509f7ca30640835458b/> make RegisterCmdId a hex-encodedstring
 - <csr-id-bbc06e4ee17d4bba60df54c084b82201b6b92e1b/> read register from individual files for each RegisterCmd
 - <csr-id-ffc9c39daf4e6430334e7b1a36627c6e111a61fd/> make RegisterLog a concrete type
 - <csr-id-5a172a906f042bd6c5c32b672e13e73ddf2bced0/> rename event store-> reg_op_store
 - <csr-id-fd6b97b37bb875404ef2ba7f5f35d5675c122ea0/> make RegisterCmds be stored under deterministic id

### Bug Fixes

 - <csr-id-175011ea4a14ef0ce2538ce9e69a6ffc8d47f2ac/> append RegsiterId as hex for storage folder
   Previously we used bitdepth which can clash for low depths, even for
   unique xornames.
   
   Now we also add the register folder id name, so we know all ops in a
   given folder are for that register.
 - <csr-id-604556e670d5fe0a9408bbd0d586363c7b4c0d6c/> Decode ReplicatedDataAddress from chunk filename
   We were previously encoding a ReplicatedDataAddress, but
   decoding as a ChunkAddress
 - <csr-id-884ddde34fa2e724ca4cca7e69c496f973e588c5/> properly extract list of chunks and register addresses from local storage
 - <csr-id-4da782096826f2074dac2a5628f9c9d9a85fcf1f/> paths for read/write RegisterCmd ops and support any order for reading them

### Other

 - <csr-id-58a237e3985fa477226b09f62d5222d74d53fd9c/> fix proptest now we error out with duplicate chunks

### Refactor

 - <csr-id-834ea1a1734b84b649690680cdba849abf7df3ea/> removing unnecessary internal layer and storage subfolder

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 12 commits contributed to the release.
 - 1 day passed between releases.
 - 12 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.10.1/sn_client-0.71.0/sn_node-0.66.1/sn_api-0.69.0/sn_cli-0.62.0 ([`401bc41`](https://github.com/maidsafe/safe_network/commit/401bc416c7aea65ae55e9adee2cbecf782c999cf))
    - fix proptest now we error out with duplicate chunks ([`58a237e`](https://github.com/maidsafe/safe_network/commit/58a237e3985fa477226b09f62d5222d74d53fd9c))
    - append RegsiterId as hex for storage folder ([`175011e`](https://github.com/maidsafe/safe_network/commit/175011ea4a14ef0ce2538ce9e69a6ffc8d47f2ac))
    - Decode ReplicatedDataAddress from chunk filename ([`604556e`](https://github.com/maidsafe/safe_network/commit/604556e670d5fe0a9408bbd0d586363c7b4c0d6c))
    - make RegisterCmdId a hex-encodedstring ([`d58f1c5`](https://github.com/maidsafe/safe_network/commit/d58f1c55e9502fd6e8a99509f7ca30640835458b))
    - removing unnecessary internal layer and storage subfolder ([`834ea1a`](https://github.com/maidsafe/safe_network/commit/834ea1a1734b84b649690680cdba849abf7df3ea))
    - properly extract list of chunks and register addresses from local storage ([`884ddde`](https://github.com/maidsafe/safe_network/commit/884ddde34fa2e724ca4cca7e69c496f973e588c5))
    - paths for read/write RegisterCmd ops and support any order for reading them ([`4da7820`](https://github.com/maidsafe/safe_network/commit/4da782096826f2074dac2a5628f9c9d9a85fcf1f))
    - read register from individual files for each RegisterCmd ([`bbc06e4`](https://github.com/maidsafe/safe_network/commit/bbc06e4ee17d4bba60df54c084b82201b6b92e1b))
    - make RegisterLog a concrete type ([`ffc9c39`](https://github.com/maidsafe/safe_network/commit/ffc9c39daf4e6430334e7b1a36627c6e111a61fd))
    - rename event store-> reg_op_store ([`5a172a9`](https://github.com/maidsafe/safe_network/commit/5a172a906f042bd6c5c32b672e13e73ddf2bced0))
    - make RegisterCmds be stored under deterministic id ([`fd6b97b`](https://github.com/maidsafe/safe_network/commit/fd6b97b37bb875404ef2ba7f5f35d5675c122ea0))
</details>

## v0.66.0 (2022-08-23)

<csr-id-0ae61c2877df283dde6f18800a40fc0e3afd603e/>
<csr-id-857ce2d13a354945ebc0c968ac94f1e119b3a43a/>
<csr-id-c8517a481e39bf688041cd8f8661bc663ee7bce7/>
<csr-id-7691f087b30805d68614581aa43b3d6933cd83c9/>
<csr-id-90b25e9b6aae86f2fc0b83911993aac64964c4b6/>
<csr-id-c994fb627165b03e6baf0d13cb2ce5b2e84b2d07/>
<csr-id-990a6d210329f65f6bcf97ca116cfaa2447e6b17/>
<csr-id-1dfaf5f758fa797463342ba0fe1815323e851a86/>
<csr-id-589f03ce8670544285f329fe35c19897d4bfced8/>
<csr-id-9f64d681e285de57a54f571e98ff68f1bf39b6f1/>
<csr-id-2936bf28e56e0086e687bd99979aa4b1c3bde1e3/>
<csr-id-93a13d896343f746718be228c46a37b03d6618bb/>
<csr-id-9fa9989657d6a272b5041008d7daf4281db39298/>
<csr-id-b2c1cd4f32c54c249aaaf932df014f50268bed0c/>
<csr-id-90e756aebb5ca0c900e6438b397b2d5739887611/>
<csr-id-7c11b1ea35770a2211ee4afc746bbafedb02caf8/>
<csr-id-1618cf6a93117942946d152efee24fe3c7020e55/>
<csr-id-11b8182a3de636a760d899cb15d7184d8153545a/>
<csr-id-e52028f1e9d7fcf19962a7643b272ba3a786c7c4/>
<csr-id-28d95a2e959e32ee69a70bdc855cba1fff1fc8d8/>
<csr-id-d3f66d6cfa838a5c65fb8f31fa68d48794b33dea/>
<csr-id-f0fbe5fd9bec0b2865271bb139c9fcb4ec225884/>
<csr-id-43fcc7c517f95eab0e27ddc79cd9c6de3631c7c6/>

### Chore

 - <csr-id-0ae61c2877df283dde6f18800a40fc0e3afd603e/> continue with periodics afer process batch has been done
 - <csr-id-857ce2d13a354945ebc0c968ac94f1e119b3a43a/> log node name when tracking dysfunction
 - <csr-id-c8517a481e39bf688041cd8f8661bc663ee7bce7/> fix clippy some/none issues
 - <csr-id-7691f087b30805d68614581aa43b3d6933cd83c9/> refactor flow ctrl msg and cmd processing
 - <csr-id-90b25e9b6aae86f2fc0b83911993aac64964c4b6/> remove AtomicUsize in CmdCtrl
 - <csr-id-c994fb627165b03e6baf0d13cb2ce5b2e84b2d07/> move event sender to FlowCtrl, refactor process_cmd
 - <csr-id-990a6d210329f65f6bcf97ca116cfaa2447e6b17/> remove Cmd watchers as unsused
 - <csr-id-1dfaf5f758fa797463342ba0fe1815323e851a86/> remove CmdCtrl.clone
   We move Cmd handling into main flowCtrl loop to avoid
   a clone and its knock on consequences of Arc/Rw
 - <csr-id-589f03ce8670544285f329fe35c19897d4bfced8/> upgrading sn_dbc to v8.0
 - <csr-id-9f64d681e285de57a54f571e98ff68f1bf39b6f1/> increase data query limit
   Now we differentiate queries per adult/index, we may need more queries.
 - <csr-id-2936bf28e56e0086e687bd99979aa4b1c3bde1e3/> initialise flow control earlier
 - <csr-id-93a13d896343f746718be228c46a37b03d6618bb/> run periodic checks on time
 - <csr-id-9fa9989657d6a272b5041008d7daf4281db39298/> refactor CmdCtrl, naming and remove retries
 - <csr-id-b2c1cd4f32c54c249aaaf932df014f50268bed0c/> do not merge client requests to different adult indexes
 - <csr-id-90e756aebb5ca0c900e6438b397b2d5739887611/> remove unnecessary unwrap
 - <csr-id-7c11b1ea35770a2211ee4afc746bbafedb02caf8/> dont have adults responding to AeProbe msgs that come through

### Chore

 - <csr-id-43fcc7c517f95eab0e27ddc79cd9c6de3631c7c6/> sn_interface-0.10.0/sn_dysfunction-0.9.0/sn_client-0.70.0/sn_node-0.66.0/sn_api-0.68.0/sn_cli-0.61.0

### New Features

 - <csr-id-f0f860efcf89cb7bf51bddd6364a9bec33bbf3c3/> remove ConnectivityCheck
   Now we have periodic health checks and dysfunciton, this
   check should not be needed, and can cause network strain
   with the frequent DKG we have now
 - <csr-id-e97ab2220d150706741549944c6e4bf77f2a5bae/> new cmd to display detailed information about a configured network
 - <csr-id-1e2a0a122f8c53d669916cded16876aa16d8ebfb/> make AntiEntropyProbe carry a current known section key for response

### Bug Fixes

 - <csr-id-ff10da14cae0dfdb6f5e46090794a762e9ee3252/> tests that were using Connectivity msging as placeholder
 - <csr-id-dfed2a8d2751b6627250b64e7a78213b68ec6733/> move data replication steps ahead of elder check in FlowCtrl

### Refactor

 - <csr-id-1618cf6a93117942946d152efee24fe3c7020e55/> expose serialisation/deserialisation utilities as public methods instead
   - Also include the genesis key of each network in the list shown by CLI networks cmd.
 - <csr-id-11b8182a3de636a760d899cb15d7184d8153545a/> clean up unused functionality
   `closest` is a method that will find a prefix that is closest, but if
   not returning any, it means the set is empty. The `closest_or_opposite`
   used this function internally, but actually never got to the opposite,
   because `closest` would always return a SAP.
   
   This method was used in a few places where no exclusions were given, so
   it is clear in that case that it would always find a prefix. In a single
   case, it was called with an exclusion, where it would find a section
   closer than its own section.
 - <csr-id-e52028f1e9d7fcf19962a7643b272ba3a786c7c4/> SAP reference instead of clone

### New Features (BREAKING)

 - <csr-id-991ccd452119137d9da046b7f222f091177e28f1/> adding more context information to sn_client::Error types

### Refactor (BREAKING)

 - <csr-id-28d95a2e959e32ee69a70bdc855cba1fff1fc8d8/> removing unused CreateRegister::Populated msg type
 - <csr-id-d3f66d6cfa838a5c65fb8f31fa68d48794b33dea/> removing unused sn_node::dbs::Error variants and RegisterExtend cmd
 - <csr-id-f0fbe5fd9bec0b2865271bb139c9fcb4ec225884/> renaming NetworkPrefixMap to SectionTree
   - Changing CLI and sn_client default path for network contacts to `$HOME/.safe/network_contacts`.
   - Renaming variables and functions referring to "prefix map" to now refer to "network contacts".

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 31 commits contributed to the release over the course of 8 calendar days.
 - 9 days passed between releases.
 - 28 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.10.0/sn_dysfunction-0.9.0/sn_client-0.70.0/sn_node-0.66.0/sn_api-0.68.0/sn_cli-0.61.0 ([`43fcc7c`](https://github.com/maidsafe/safe_network/commit/43fcc7c517f95eab0e27ddc79cd9c6de3631c7c6))
    - tests that were using Connectivity msging as placeholder ([`ff10da1`](https://github.com/maidsafe/safe_network/commit/ff10da14cae0dfdb6f5e46090794a762e9ee3252))
    - remove ConnectivityCheck ([`f0f860e`](https://github.com/maidsafe/safe_network/commit/f0f860efcf89cb7bf51bddd6364a9bec33bbf3c3))
    - removing unused CreateRegister::Populated msg type ([`28d95a2`](https://github.com/maidsafe/safe_network/commit/28d95a2e959e32ee69a70bdc855cba1fff1fc8d8))
    - removing unused sn_node::dbs::Error variants and RegisterExtend cmd ([`d3f66d6`](https://github.com/maidsafe/safe_network/commit/d3f66d6cfa838a5c65fb8f31fa68d48794b33dea))
    - continue with periodics afer process batch has been done ([`0ae61c2`](https://github.com/maidsafe/safe_network/commit/0ae61c2877df283dde6f18800a40fc0e3afd603e))
    - log node name when tracking dysfunction ([`857ce2d`](https://github.com/maidsafe/safe_network/commit/857ce2d13a354945ebc0c968ac94f1e119b3a43a))
    - fix clippy some/none issues ([`c8517a4`](https://github.com/maidsafe/safe_network/commit/c8517a481e39bf688041cd8f8661bc663ee7bce7))
    - refactor flow ctrl msg and cmd processing ([`7691f08`](https://github.com/maidsafe/safe_network/commit/7691f087b30805d68614581aa43b3d6933cd83c9))
    - remove AtomicUsize in CmdCtrl ([`90b25e9`](https://github.com/maidsafe/safe_network/commit/90b25e9b6aae86f2fc0b83911993aac64964c4b6))
    - move event sender to FlowCtrl, refactor process_cmd ([`c994fb6`](https://github.com/maidsafe/safe_network/commit/c994fb627165b03e6baf0d13cb2ce5b2e84b2d07))
    - remove Cmd watchers as unsused ([`990a6d2`](https://github.com/maidsafe/safe_network/commit/990a6d210329f65f6bcf97ca116cfaa2447e6b17))
    - remove CmdCtrl.clone ([`1dfaf5f`](https://github.com/maidsafe/safe_network/commit/1dfaf5f758fa797463342ba0fe1815323e851a86))
    - new cmd to display detailed information about a configured network ([`e97ab22`](https://github.com/maidsafe/safe_network/commit/e97ab2220d150706741549944c6e4bf77f2a5bae))
    - adding more context information to sn_client::Error types ([`991ccd4`](https://github.com/maidsafe/safe_network/commit/991ccd452119137d9da046b7f222f091177e28f1))
    - move data replication steps ahead of elder check in FlowCtrl ([`dfed2a8`](https://github.com/maidsafe/safe_network/commit/dfed2a8d2751b6627250b64e7a78213b68ec6733))
    - upgrading sn_dbc to v8.0 ([`589f03c`](https://github.com/maidsafe/safe_network/commit/589f03ce8670544285f329fe35c19897d4bfced8))
    - renaming NetworkPrefixMap to SectionTree ([`f0fbe5f`](https://github.com/maidsafe/safe_network/commit/f0fbe5fd9bec0b2865271bb139c9fcb4ec225884))
    - expose serialisation/deserialisation utilities as public methods instead ([`1618cf6`](https://github.com/maidsafe/safe_network/commit/1618cf6a93117942946d152efee24fe3c7020e55))
    - Update README.md ([`0af715e`](https://github.com/maidsafe/safe_network/commit/0af715e7f647ccae745c8adb41119be66af109a9))
    - increase data query limit ([`9f64d68`](https://github.com/maidsafe/safe_network/commit/9f64d681e285de57a54f571e98ff68f1bf39b6f1))
    - initialise flow control earlier ([`2936bf2`](https://github.com/maidsafe/safe_network/commit/2936bf28e56e0086e687bd99979aa4b1c3bde1e3))
    - chore: run periodic checks on time Instead of running when connection arrives. ([`93a13d8`](https://github.com/maidsafe/safe_network/commit/93a13d896343f746718be228c46a37b03d6618bb))
    - refactor CmdCtrl, naming and remove retries ([`9fa9989`](https://github.com/maidsafe/safe_network/commit/9fa9989657d6a272b5041008d7daf4281db39298))
    - do not merge client requests to different adult indexes ([`b2c1cd4`](https://github.com/maidsafe/safe_network/commit/b2c1cd4f32c54c249aaaf932df014f50268bed0c))
    - clean up unused functionality ([`11b8182`](https://github.com/maidsafe/safe_network/commit/11b8182a3de636a760d899cb15d7184d8153545a))
    - SAP reference instead of clone ([`e52028f`](https://github.com/maidsafe/safe_network/commit/e52028f1e9d7fcf19962a7643b272ba3a786c7c4))
    - remove unnecessary unwrap ([`90e756a`](https://github.com/maidsafe/safe_network/commit/90e756aebb5ca0c900e6438b397b2d5739887611))
    - dont have adults responding to AeProbe msgs that come through ([`7c11b1e`](https://github.com/maidsafe/safe_network/commit/7c11b1ea35770a2211ee4afc746bbafedb02caf8))
    - make AntiEntropyProbe carry a current known section key for response ([`1e2a0a1`](https://github.com/maidsafe/safe_network/commit/1e2a0a122f8c53d669916cded16876aa16d8ebfb))
    - feat(node) add elder health checks ([`93cc084`](https://github.com/maidsafe/safe_network/commit/93cc08468278995598938a8ed3dcdff33a23d066))
</details>

## v0.65.0 (2022-08-14)

<csr-id-1af888c09b2f5a49d04a7068b7f948cf096da8f3/>
<csr-id-aea5782e583ae353566abb0f10d94132bd9b14fe/>
<csr-id-6d60525874dc4efeb658433f1f253d54e0cba2d4/>
<csr-id-52ed23049c83e0da0b4dfefa7b30713a52f3c73a/>
<csr-id-68d28d5591c5ccc6a241832f9b7855827958372a/>
<csr-id-42bde15e9a96dbe759575d4bccf4f769e13a695d/>
<csr-id-3cf903367bfcd805ceff2f2508cd2b12eddc3ca5/>
<csr-id-29de67f1e3583eab867d517cb50ed2e404bd63fd/>
<csr-id-a8b0631a396ac96e000db22141ffd5d83fd7e987/>
<csr-id-8242f2f1035b1c0718e53954951badffa30f3393/>
<csr-id-848dba48e5959d0b9cfe182fde2f12ede71ba9c2/>
<csr-id-35483b3f322eeea2c10427e94e4750a8269811c0/>
<csr-id-820fcc9a77f756fca308f247c3ea1b82f65d30b9/>
<csr-id-afcf083469c732f10c7c80f4a45e4c33ab111101/>
<csr-id-72db95a092ea33f29e77b6101b16e219fadd47ab/>
<csr-id-aafc560d3b3b1e375f7be224e0e63a3b567bbd86/>
<csr-id-7394030fe5aeeb88f4524d2da2a71e36334c831d/>
<csr-id-73dc9b4a1757393270e62d265328bab0c0aa3b35/>
<csr-id-0a653e4becc4a8e14ffd6d0752cf035430067ce9/>
<csr-id-9789797e3f773285f23bd22957fe45a67aabec24/>
<csr-id-8e626f9fabbb07a126199ea5481e2ac524cbae0d/>
<csr-id-db22c6c8c1aedb347bea52199a5673695eff86f8/>
<csr-id-2809ed1177c416f933f3869bd11607c4e5e6a908/>
<csr-id-08af2a6ac3485a696d2a1e799af588943f207e6b/>
<csr-id-080f9ef83005ebda9e1c96b228f3d5096fd79b81/>
<csr-id-1b37f4bbf266c21d795bff6b4e6f2e1885405697/>
<csr-id-feaf3ef88b140a0f530082e851831b736320de59/>
<csr-id-7157ed27ddfc0d987272f1285b44faa9709a4c8f/>
<csr-id-2ea069543dbe6ffebac663d4d8d7e0bc33cfc566/>
<csr-id-322c69845e2e14eb029fdbebb24e08063a2323b0/>
<csr-id-5a5f0f4608c27f463178f4a560f1f9b4c020e764/>
<csr-id-83a3a98a6972c9a1824e41cb87325b037b65938c/>
<csr-id-9c855c187465a0594cf18fb359a082742593a4d4/>
<csr-id-f8b6ff0edd7a64389439081b6306296402887ab1/>
<csr-id-c6fa7735a5e48a4caa6ee3aac000785a9da9413a/>
<csr-id-302fb954360521d40efab0e26fd31f8278a74755/>
<csr-id-70f3ecc367ca9450be038f8ff806f40c324d1b00/>
<csr-id-8efbd96a5fd3907ace5ca6ac282027595fefd8ef/>
<csr-id-ea490ddf749ac9e0c7962c3c21c053663e6b6ee7/>
<csr-id-bf2902c18b900b8b4a8abae5f966d1e08d547910/>
<csr-id-6f03b93bd2d02f0ffe54b69fbf25070fbe64eab0/>
<csr-id-934bf6cbc86e252eb3859c757a0b66c02f7826d9/>
<csr-id-214adedc31bca576c7f28ff52a1f4ff0a2676757/>
<csr-id-893b4af340b0ced1a2b38dda07bd82cf833be776/>
<csr-id-feaca15b7c44297c16a4665ceec738226bb860ba/>
<csr-id-7d060f0e92e3b250e3fe1e0523aa0c30b439e0be/>
<csr-id-7a675f4889c4ef01b9040773184ab2e0ed78b208/>
<csr-id-c3778cd77e0c9cbc407449afe713ff7cdb4b9909/>
<csr-id-fba2f76ec23a00ca1da857e63af160a11904288c/>
<csr-id-5a121c19d395130e40df0134be36e4264b60972a/>
<csr-id-67c82cae6654423cae3567d8417a442a40ce1e5e/>
<csr-id-1e8180c23fab27ac92c93f201efd050cff00db10/>
<csr-id-900fa8c4803e9e45a1471a32fb4fe5b8cdd5112b/>
<csr-id-a95be6277a9ee8d66eccd40711392325fac986e2/>
<csr-id-a856d788131ef85414ee1f42a868abcbbfc0d2b6/>
<csr-id-00fae4d5fd5dbad5696888f0c796fbd39b7e49ed/>
<csr-id-549c4b169547e620471c416c1506afc6e3ee265b/>
<csr-id-847db2c487cd102af0cf9a477b4c1b65fc2c8aa6/>
<csr-id-0a5593b0512d6f059c6a8003634b07e7d2d3e514/>
<csr-id-707b80c3526ae727a7e91330dc386cdb41c51f4c/>
<csr-id-9bd6ae20c1207f99420093fd5c9f4eb53836e3c1/>
<csr-id-31d9f9f99b4e166986b8e51c3d41e0eac55621a4/>
<csr-id-30a7028dd702e2f6575e299a609a2416439cbaed/>
<csr-id-dedec486f85c1cf6cf2d538238f32e826e08da0a/>
<csr-id-3bef795923863d977f70c95647444ebbc97c5cf5/>
<csr-id-f142bbb0030233add4808427a2819ca386fef503/>
<csr-id-e39917d0635a071625f7961ce6d40cb44cc65da0/>
<csr-id-879678e986a722d216ee9a4f37e8ae398221a394/>
<csr-id-ca5120885e3e28229f298b81edf6090542e0e3f9/>
<csr-id-12360a6dcc204153a81adbf842a64dc018c750f9/>
<csr-id-6e65ed8e6c5872bd2c49a1ed2837b1fb16523af1/>
<csr-id-24227673b57954b1c53b9b88d714e42c39d8f000/>
<csr-id-a9885a8d0e3e59dc630cf605fc9e353e152a5bb3/>
<csr-id-2e865cf211a91bef3caaded6310eaddd7e03e997/>
<csr-id-c77f686e132f1ac58a113392bc65087bfb650bb9/>
<csr-id-a184ebe3f07b86a53c7e8b36b3d86034558a99fb/>
<csr-id-8eebc01007a49f1debaede519324d920e9628d46/>
<csr-id-46b6e53d220ebc7f60bb754a4c7ebf4ec7d83e58/>
<csr-id-13fc65dc634348740095226e1da0af1866f5b3a8/>
<csr-id-6b1fee8cf3d0b2995f4b81e59dd684547593b5fa/>
<csr-id-ed37bb56e5e17d4cba7c1b2165746c193241d618/>
<csr-id-a0c89ff0e451d2e5dd13fc29635075097f2c7b94/>
<csr-id-0f07efd9ef0b75de79f27772566b013bc886bcc8/>
<csr-id-db4f4d07b155d732ad76d263563d81b5fee535f7/>
<csr-id-ff1a10b4aa2b41b7028949101504a29b52927e71/>
<csr-id-e0fb940b24e87d86fe920095176362f73503ce79/>
<csr-id-81f5e252501174cb6367474a980b8a2a5da58dc2/>
<csr-id-35ebd8e872f9d9db16c42cbe8d61702f9660aece/>
<csr-id-042503a7d44f94ed3f0ce482984744e175d7752b/>
<csr-id-3f577d2a6fe70792d7d02e231b599ca3d44a5ed2/>
<csr-id-a1d74e894975d67f3293dbb0db73f6b62b9c378a/>
<csr-id-3a74f59269f57a50a14de9a35f7e725014ec8f0e/>
<csr-id-a727daea6e5a01a24c9bcdbeef033d0622f4ba39/>
<csr-id-1db6648954987ebce4c91f8e29bbec4e54b75edf/>
<csr-id-fa7dfa342e90acd0a681110e149df4400a8f392e/>
<csr-id-28c0a1063194eea66910c7d18653c558595ec17e/>
<csr-id-f467d5f45452244d2f8e3e81910b76d0d4b0f7cb/>
<csr-id-f45afa221a18638bbbbad5cf6121a68825ed3ff3/>
<csr-id-9895a2b9e82bdbf110a9805972290841860d1a49/>
<csr-id-1c7b47f48635bb7b0a8a13d01bb41b148e343ce8/>
<csr-id-f06a0260b058519ec858abf654cbce102eb00147/>
<csr-id-5c82df633e7c062fdf761a8e6e0a7ae8d26cc73a/>
<csr-id-24676dadb771bbd966b6a3e1aa53d1c736c90627/>
<csr-id-93614b18b4316af04ab8c74358a5c86510590b85/>
<csr-id-d3a05a728be8752ea9ebff4e38e7c4c85e5db09b/>
<csr-id-96da1171d0cac240f772e5d6a15c56f63441b4b3/>
<csr-id-dd2eb21352223f6340064e0021f4a7df402cd5c9/>
<csr-id-53f60c2327f8a69f0b2ef6d1a4e96644c10aa358/>

### Chore

 - <csr-id-1af888c09b2f5a49d04a7068b7f948cf096da8f3/> add README docs for join process and traceroute
 - <csr-id-aea5782e583ae353566abb0f10d94132bd9b14fe/> print full error during node startup fail
 - <csr-id-6d60525874dc4efeb658433f1f253d54e0cba2d4/> remove wiremsg.priority as uneeded
 - <csr-id-52ed23049c83e0da0b4dfefa7b30713a52f3c73a/> reduce AE sleep further
 - <csr-id-68d28d5591c5ccc6a241832f9b7855827958372a/> avoid logging short sleeps
 - <csr-id-42bde15e9a96dbe759575d4bccf4f769e13a695d/> misc. fixes
 - <csr-id-3cf903367bfcd805ceff2f2508cd2b12eddc3ca5/> remove unused severity; refactor weighted score
   Prev weighted score related everything to the std_deviation, but this
   has the effect of nullifying outliers and decreasing the impact of
   weighting.
   
   Instead we opt for a simple "threshold" score, above which, we're
   dysfunctional. So the sum of all issues tracked is used, and if
   we reach above this point, our node is deemed dysfunctional.
 - <csr-id-29de67f1e3583eab867d517cb50ed2e404bd63fd/> serialize NetworkPrefixMap into JSON
 - <csr-id-a8b0631a396ac96e000db22141ffd5d83fd7e987/> semantic tweaks
 - <csr-id-8242f2f1035b1c0718e53954951badffa30f3393/> organise usings, cleanup
 - <csr-id-848dba48e5959d0b9cfe182fde2f12ede71ba9c2/> use matches! macros, minor refactoring
 - <csr-id-35483b3f322eeea2c10427e94e4750a8269811c0/> remove unused async/await
 - <csr-id-820fcc9a77f756fca308f247c3ea1b82f65d30b9/> remove NetworkPrefxiMap::genesis_key, NetworkKnowledge::genesis_key
   Remove the feilds as they can be obtained from NetworkPrefixMap::sections_dag
 - <csr-id-afcf083469c732f10c7c80f4a45e4c33ab111101/> remove RwLock from NetworkPrefixMap
 - <csr-id-72db95a092ea33f29e77b6101b16e219fadd47ab/> refactor CmdCtrl, naming and remove retries
 - <csr-id-aafc560d3b3b1e375f7be224e0e63a3b567bbd86/> rename traceroute fns
 - <csr-id-7394030fe5aeeb88f4524d2da2a71e36334c831d/> traceroute update after cmds flow rebase
 - <csr-id-73dc9b4a1757393270e62d265328bab0c0aa3b35/> make traceroute a default feature
 - <csr-id-0a653e4becc4a8e14ffd6d0752cf035430067ce9/> improve Display, Debug impl for Traceroute
 - <csr-id-9789797e3f773285f23bd22957fe45a67aabec24/> improve traceroute readability and other improvements
   - simplfies creating identites for traceroute to avoid locking
   - implements Display and Debug for traceroute
   - add clearer logs for traceroute
 - <csr-id-8e626f9fabbb07a126199ea5481e2ac524cbae0d/> add traceroute for AE system messages
 - <csr-id-db22c6c8c1aedb347bea52199a5673695eff86f8/> cleanup unnecessary options and results
 - <csr-id-2809ed1177c416f933f3869bd11607c4e5e6a908/> reduce file db error logs
 - <csr-id-08af2a6ac3485a696d2a1e799af588943f207e6b/> clarify fn signatures
 - <csr-id-080f9ef83005ebda9e1c96b228f3d5096fd79b81/> delete commented out tests
 - <csr-id-1b37f4bbf266c21d795bff6b4e6f2e1885405697/> dont send health checks as an adult
 - <csr-id-feaf3ef88b140a0f530082e851831b736320de59/> simplify the send funcitonality
   Previously we've had a send to client and a send ot nodes, the latter
   which would only send to a subset (with the delivery group setting), and
   reported errors.
   
   We've stripped out the delivery group stuff already as unnecessary. So here
   we just have one function to send from nodes, any errors are handled
   appropriately if the recipient is in our section, otherwise ignored.
 - <csr-id-7157ed27ddfc0d987272f1285b44faa9709a4c8f/> use Cmd::TrackDysf in read_data_from_adults
 - <csr-id-2ea069543dbe6ffebac663d4d8d7e0bc33cfc566/> remove RwLock over Cache type
 - <csr-id-322c69845e2e14eb029fdbebb24e08063a2323b0/> remove write lock around non query service msg handling
 - <csr-id-5a5f0f4608c27f463178f4a560f1f9b4c020e764/> remove the write lock on node while validating msgs
   This adds a new Cmd for tracking dysfunction, so we can pull the
   write locks out of longer running Cmd processes
 - <csr-id-83a3a98a6972c9a1824e41cb87325b037b65938c/> make Sign of NodeMsgs higher prio as this could hold them back before sending
 - <csr-id-9c855c187465a0594cf18fb359a082742593a4d4/> log parent job id during processing
 - <csr-id-f8b6ff0edd7a64389439081b6306296402887ab1/> simplify data replication msg sending
 - <csr-id-c6fa7735a5e48a4caa6ee3aac000785a9da9413a/> temporarily allow DBC spent proofs singed by sections keys unknown to the Elder
 - <csr-id-302fb954360521d40efab0e26fd31f8278a74755/> make `&mut self` methods `&self`
 - <csr-id-70f3ecc367ca9450be038f8ff806f40c324d1b00/> removed unused lru_cache module
 - <csr-id-8efbd96a5fd3907ace5ca6ac282027595fefd8ef/> Cleanup non-joined member sessions, regardless of connected state
 - <csr-id-ea490ddf749ac9e0c7962c3c21c053663e6b6ee7/> reflect the semantics not the type
 - <csr-id-bf2902c18b900b8b4a8abae5f966d1e08d547910/> whitespace + typo fix
 - <csr-id-6f03b93bd2d02f0ffe54b69fbf25070fbe64eab0/> upgrade blsttc to 7.0.0
   This version has a more helpful error message for the shares interpolation problem.
 - <csr-id-934bf6cbc86e252eb3859c757a0b66c02f7826d9/> Cleanup non-joined member sessions, regardless of connected state.
   We've seen that we can get a crazy number of sessions (almost 1k), with no network activity if they are
   never cleaned up. They may well still be conncted (but the node name cycles... so the conn lives on).
   
   So here, anything not currently joined, is cleaned up. nodes we can reconnect to. clients can retry.
 - <csr-id-214adedc31bca576c7f28ff52a1f4ff0a2676757/> improve traceroute redability and resolve clippy
 - <csr-id-893b4af340b0ced1a2b38dda07bd82cf833be776/> fix outdated comm tests
   - `sn_node::comm::send_to_subset` test was failing because of the changes to delivery_group_size
   where we do not have subset sends anymore. Therefore this test has been removed.
   
   - this commit also removes old filters from GHA workflow which prevented running
   all the tests under sn_node and sn_interface.
 - <csr-id-feaca15b7c44297c16a4665ceec738226bb860ba/> increase Send priority, and tweak depending on DstLocation
 - <csr-id-7d060f0e92e3b250e3fe1e0523aa0c30b439e0be/> reduce number of Send retries in PeerSession
 - <csr-id-7a675f4889c4ef01b9040773184ab2e0ed78b208/> remove SendMsgDeliveryGroup Cmd
   This is not needed or really utilised in any real way.
   
   We can just use SendMsg cmds
 - <csr-id-c3778cd77e0c9cbc407449afe713ff7cdb4b9909/> use SendMsg for send_node_msg_to_nodes
   As it always results in a delivery group of 1. Which is pointless
 - <csr-id-fba2f76ec23a00ca1da857e63af160a11904288c/> increase CleanUpPeerLinks prio
 - <csr-id-5a121c19d395130e40df0134be36e4264b60972a/> fix benchamrking groups in data_storage bench
 - <csr-id-67c82cae6654423cae3567d8417a442a40ce1e5e/> update data_storage benchmark ranges
 - <csr-id-1e8180c23fab27ac92c93f201efd050cff00db10/> re-enable registers benchmark and tidy sled residue
 - <csr-id-900fa8c4803e9e45a1471a32fb4fe5b8cdd5112b/> reduce service msg response SendMsg cmds
   We can now have one command to many recipients...
 - <csr-id-a95be6277a9ee8d66eccd40711392325fac986e2/> tweak some WireMsg.serialize calls to minimize them
   Refactors some client send flow so we can now send the same msg
   to many clients at once (as is baked in elsewhere...
   this logic would have scuppered some client optimisations
 - <csr-id-a856d788131ef85414ee1f42a868abcbbfc0d2b6/> use timestamp for log files
   This means that once written a log file's name will not change.
   This hould make debugging a live network easier (your log file is less likely to change while viewing), and also pulling logs from a network via rsync fafster (as the namess/content aren't changing all the time).
 - <csr-id-00fae4d5fd5dbad5696888f0c796fbd39b7e49ed/> formatting with cargo fmt
 - <csr-id-549c4b169547e620471c416c1506afc6e3ee265b/> increase wait on ci & decrease delay before node retry
 - <csr-id-847db2c487cd102af0cf9a477b4c1b65fc2c8aa6/> remove locking from section key cache
 - <csr-id-0a5593b0512d6f059c6a8003634b07e7d2d3e514/> remove refcell on NetworkKnowledge::all_section_chains
 - <csr-id-707b80c3526ae727a7e91330dc386cdb41c51f4c/> remove refcell around NetworkKnowledge::signed_sap
 - <csr-id-9bd6ae20c1207f99420093fd5c9f4eb53836e3c1/> remove refcell on NetworkKnowledge::chain
 - <csr-id-31d9f9f99b4e166986b8e51c3d41e0eac55621a4/> remove awaits from tests as well
 - <csr-id-30a7028dd702e2f6575e299a609a2416439cbaed/> remove locking around signature aggregation
 - <csr-id-dedec486f85c1cf6cf2d538238f32e826e08da0a/> remove unused async
 - <csr-id-3bef795923863d977f70c95647444ebbc97c5cf5/> remove logs in looping data replication
 - <csr-id-f142bbb0030233add4808427a2819ca386fef503/> relax when we track DkgIssues
   Previously we did this on any outgoing msg.
   Now we only track on start/failure.
   This should hopefully prevent false positives over short times
   during intensive DKG rounds
 - <csr-id-e39917d0635a071625f7961ce6d40cb44cc65da0/> Tweak dysf interval, reducing to report on issues more rapidly
   If not, we can only ever propose one node for a membership change
   (lost), every 30s... which may not succeed under churn...
 - <csr-id-879678e986a722d216ee9a4f37e8ae398221a394/> logging sn_consensus in CI, tweak section min and max elder age

### Chore

 - <csr-id-53f60c2327f8a69f0b2ef6d1a4e96644c10aa358/> sn_interface-0.9.0/sn_dysfunction-0.8.0/sn_client-0.69.0/sn_node-0.65.0/sn_api-0.67.0/sn_cli-0.60.0

### Documentation

 - <csr-id-e3c90998e1abd10768e861370a65a934f52e2ec3/> broken links

### New Features

<csr-id-01ea9c0bdb88da4f181d8f1638f2f2ad692d0ca3/>
<csr-id-c6999a92d06f275f7506a24492bec50042466459/>
<csr-id-6f781b96da745839206352ae02c18b759b49f1f2/>

 - <csr-id-f817fa5917f48faaeb3714c7faeaee5392b82f55/> log with unique id around sleep
 - <csr-id-366719f9e158fb35093382739d48cdb8dac65b60/> make the content explicit
 - <csr-id-005b84cab0ca91762cbedd208b022d4c4983fe26/> retry twice if it fails spending inputs when reissuing DBCs
 - <csr-id-47179e695a7f351d9298c5bdbea006abb26b8e89/> consolidate join / leave into decision handling
 - <csr-id-175d1b909dff8c6729ac7f156ce1d0d22be8cc12/> make traceroute default for now
 - <csr-id-df8ea32f9218d344cd1f291359969b38a05b4642/> join approval has membership decision
 - <csr-id-7f40e89f9c0aa767bd372278e1d50de5a8e7869b/> add intermitent health checks
   We now periodically send a random chunk query to the section to check if
   all our members respond in some fashion.
 - <csr-id-4f2cf267ee030e5924a2fa999a2a46dbc072d208/> impl traceroute for client cmds and cmd responses
 - <csr-id-a6fb1fc516a9ef6dae7aa236f3dd440d50697ae2/> impl traceroute feature to trace a message's flow in the network
   - implements traceroute for Client queries and is logged at the client on return

### Bug Fixes

<csr-id-b492cc25cfadf7dd3aaf3584cd5dbbd0dc8532ce/>
<csr-id-1694c1566ac562216447eb491cc3b2b00b0c5979/>
<csr-id-6dbc032907bcb1fc6e3e9034f794643097260e17/>
<csr-id-7fc6231f241bcae4447839989b712b1b410a2523/>
<csr-id-19bb0b99afee53dd7b6e109919249b25e0a55e48/>
<csr-id-e1a0474e78fd00a881b13ad036c1aabc9ce2f02a/>
<csr-id-f0d1abf6dd8731310b7749cd6cc7077886215997/>
<csr-id-640b64d2b5f19df5f5439a8fce31a848a47526cd/>
<csr-id-38d25d6df71e3bb71e8efda50a4bf64345f69f81/>
<csr-id-ae38fdce6499d8245025d7bd82fa6c583f04060d/>
<csr-id-6d237e5e7d8306cb955f436910aa01ed7221cd84/>
<csr-id-78be9fb24cf66d9f8f06ac31895302eae875661e/>
<csr-id-abddc53df9fbbd5c35b6ce473646f3183bf423df/>
<csr-id-8f3d3f7acf62f0f5c50cc68280214a4119801abd/>
<csr-id-e12479f8db891107215192e4824df92999fb23af/>
<csr-id-4a277b6a290ee7b8ec99dba4e421ac19023fe08e/>
<csr-id-4773e185302ada27cd08c8dfd04582e7fdaf42aa/>
<csr-id-f6ea1da4a57e40a051c7d1ee3b87fe9b442c537b/>
<csr-id-4e44e373da9ee75b2563b39c26794299e607f48f/>

 - <csr-id-f531c62b0d82f14b6aa6df4f1f82bcd0ce95b9ce/> modify the join process timeout mechanism
 - <csr-id-26ad7578f0d224528a1be6509453f695f6530eb4/> reduce to 5s
 - <csr-id-8bf0aeed0af193322f341bd718f7a5f84fa2d02f/> gossip all votes and start timer after first vote
 - <csr-id-a5afec84bc687327df3951eefc6c566c898f2332/> Add braces around expected age calc; use membership
   Previously we were occasionally seeing _large_ expected ages (246 eg),
   here we add braces for clarity and hopefully prevent such calculatory oddness.
   
   We also use membership to know the current section size instead of SAP which
   may well be outdated.
 - <csr-id-98279c2243ec2c5351947e26650d64dc5bf81797/> gradually slowdown when send to peer too fast
 - <csr-id-0d5ddf04001fc65c05865cf2a2ea08cd3cd45615/> make cmd ctrl response quicker
 - <csr-id-0ed5075304b090597f7760fb51c4a33435a853f1/> fix deadlock introduced after removal of Arc from NetworkPrefixMap
   Removing the checks in compare_and_write_prefix_map and directly
   writing the prefix_map fixed the issue
 - <csr-id-cebf37ea8ef44c51ce84646a83a5d1e6dcab3e7a/> use correct data address type
 - <csr-id-ec625349494b267d24c80fd4c8b176b4524f48cd/> add actual id of parent
 - <csr-id-22f4f1512da8f9a2245addf379477823364edd6c/> create query cmd for healthcheck
 - <csr-id-5a83a4ff08b7a3c2be0e3532589d209e72367d92/> only do connectivity test for failed sends via dysfunction
   This means we dont message around for every failed send (it may just be
   due to _us_ cleaning up comms).
   
   Instead we tie this check to a node we see as dysfunctional and then ask
   all elders to check it
 - <csr-id-f200909516739769d087b20712a33b5047858ba9/> improve the accuracy of data_storage bench tests
 - <csr-id-6ca512e7adc6681bc85122847fa8ac08550cfb09/> remove unused get mut access
 - <csr-id-a5028c551d1b3db2c4c912c2897490e9a4b34a0d/> disable rejoins
   It seems that we may have not fully thought through the implications
   of nodes rejoining.
   
   The flow was:
   1. a node leaves the section (reason is not tracked)
2. the node rejoins
3. the section accepts them back and attempts to relocate them

### Other

 - <csr-id-ca5120885e3e28229f298b81edf6090542e0e3f9/> increase dysfunciton test timeout

### Refactor

 - <csr-id-12360a6dcc204153a81adbf842a64dc018c750f9/> reorganise flow control unit tests
   The unit tests in the `flow_ctrl` module provide coverage for messaging handling in the node. To run
   each test, a `Node` must constructed, and this involves a lot of tedious setup code.
   
   A `network_utils` testing module is introduced to organise the code related to this setup and to
   also provide a `TestNodeBuilder` to reduce duplication and hopefully make it easier to provide more
   coverage for message handlers. The doc comments should hopefully make clear how the struct can be
   used in various different testing contexts. I will also be looking to extend its functionality a bit
   further when I come to unit testing the message handlers for the DBC spentbook commands.
   
   There were a few tests whose setup was too complex to use the builder because they require too much
   customisation and seem best left alone.
   
   A `cmd_utils` module is also introduced to organise the code for processing commands. I again also
   plan on extending this when considering the DBC tests.
 - <csr-id-6e65ed8e6c5872bd2c49a1ed2837b1fb16523af1/> remove NetworkKnowledge::chain
 - <csr-id-24227673b57954b1c53b9b88d714e42c39d8f000/> consolidate AE retry/redirect logic
 - <csr-id-a9885a8d0e3e59dc630cf605fc9e353e152a5bb3/> expand churn-join-miss check to all AE msgs
 - <csr-id-2e865cf211a91bef3caaded6310eaddd7e03e997/> pull out common AE update code
 - <csr-id-c77f686e132f1ac58a113392bc65087bfb650bb9/> inline handle_network_update
 - <csr-id-a184ebe3f07b86a53c7e8b36b3d86034558a99fb/> handle_network_update doesn't deal with msg
 - <csr-id-8eebc01007a49f1debaede519324d920e9628d46/> move lines around to prepare for merge
 - <csr-id-46b6e53d220ebc7f60bb754a4c7ebf4ec7d83e58/> reduce nesting in ae-retry handling
 - <csr-id-13fc65dc634348740095226e1da0af1866f5b3a8/> reducing nesting in ae-redirect/retry handler
 - <csr-id-6b1fee8cf3d0b2995f4b81e59dd684547593b5fa/> reduce AE msgs to one msg with a kind field
 - <csr-id-ed37bb56e5e17d4cba7c1b2165746c193241d618/> move SectionChain into NetworkPrefixMap
 - <csr-id-a0c89ff0e451d2e5dd13fc29635075097f2c7b94/> do not require node write lock on query
 - <csr-id-0f07efd9ef0b75de79f27772566b013bc886bcc8/> remove optional field
 - <csr-id-db4f4d07b155d732ad76d263563d81b5fee535f7/> remove more unused code
 - <csr-id-ff1a10b4aa2b41b7028949101504a29b52927e71/> simplify send msg
 - <csr-id-e0fb940b24e87d86fe920095176362f73503ce79/> use sn_dbc::SpentProof API for verifying SpentProofShares
 - <csr-id-81f5e252501174cb6367474a980b8a2a5da58dc2/> remove one level of indentation
 - <csr-id-35ebd8e872f9d9db16c42cbe8d61702f9660aece/> expose known keys on network knowledge
 - <csr-id-042503a7d44f94ed3f0ce482984744e175d7752b/> check for dst location to send to clients instead of msg authority type
 - <csr-id-3f577d2a6fe70792d7d02e231b599ca3d44a5ed2/> rename gen_section_authority_provider to random_sap
 - <csr-id-a1d74e894975d67f3293dbb0db73f6b62b9c378a/> mv join handler to membership module
 - <csr-id-3a74f59269f57a50a14de9a35f7e725014ec8f0e/> pull inner loop of join handling into helper
 - <csr-id-a727daea6e5a01a24c9bcdbeef033d0622f4ba39/> allow more log layers in future
 - <csr-id-1db6648954987ebce4c91f8e29bbec4e54b75edf/> remove cfg() directives
   And use a vec to collect log layers that are eventually added to a
   subscriber/registry.
 - <csr-id-fa7dfa342e90acd0a681110e149df4400a8f392e/> split logging init into functions
 - <csr-id-28c0a1063194eea66910c7d18653c558595ec17e/> further modularize logging
 - <csr-id-f467d5f45452244d2f8e3e81910b76d0d4b0f7cb/> move log rotater into own module
 - <csr-id-f45afa221a18638bbbbad5cf6121a68825ed3ff3/> move sn_node binary into own dir
 - <csr-id-9895a2b9e82bdbf110a9805972290841860d1a49/> remove one dir layer
 - <csr-id-1c7b47f48635bb7b0a8a13d01bb41b148e343ce8/> extract test only used fns
 - <csr-id-f06a0260b058519ec858abf654cbce102eb00147/> remove one layer of indirection
 - <csr-id-5c82df633e7c062fdf761a8e6e0a7ae8d26cc73a/> setup step for tests to reissue a set of DBCs from genesis only once
 - <csr-id-24676dadb771bbd966b6a3e1aa53d1c736c90627/> replace sled with filestore for storing registers
 - <csr-id-93614b18b4316af04ab8c74358a5c86510590b85/> make chunk_store accept all datatypes

### Chore (BREAKING)

 - <csr-id-d3a05a728be8752ea9ebff4e38e7c4c85e5db09b/> having spent proofs and Txs within SpentbookCmd::Send msg to be a set instead of a vec

### Refactor (BREAKING)

 - <csr-id-96da1171d0cac240f772e5d6a15c56f63441b4b3/> nodes to cache their own individual prefix map file on disk
 - <csr-id-dd2eb21352223f6340064e0021f4a7df402cd5c9/> removing Token from sn_interfaces::type as it is now exposed by sn_dbc

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 160 commits contributed to the release over the course of 32 calendar days.
 - 32 days passed between releases.
 - 134 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.9.0/sn_dysfunction-0.8.0/sn_client-0.69.0/sn_node-0.65.0/sn_api-0.67.0/sn_cli-0.60.0 ([`53f60c2`](https://github.com/maidsafe/safe_network/commit/53f60c2327f8a69f0b2ef6d1a4e96644c10aa358))
    - add README docs for join process and traceroute ([`1af888c`](https://github.com/maidsafe/safe_network/commit/1af888c09b2f5a49d04a7068b7f948cf096da8f3))
    - print full error during node startup fail ([`aea5782`](https://github.com/maidsafe/safe_network/commit/aea5782e583ae353566abb0f10d94132bd9b14fe))
    - reorganise flow control unit tests ([`12360a6`](https://github.com/maidsafe/safe_network/commit/12360a6dcc204153a81adbf842a64dc018c750f9))
    - remove wiremsg.priority as uneeded ([`6d60525`](https://github.com/maidsafe/safe_network/commit/6d60525874dc4efeb658433f1f253d54e0cba2d4))
    - modify the join process timeout mechanism ([`f531c62`](https://github.com/maidsafe/safe_network/commit/f531c62b0d82f14b6aa6df4f1f82bcd0ce95b9ce))
    - reduce AE sleep further ([`52ed230`](https://github.com/maidsafe/safe_network/commit/52ed23049c83e0da0b4dfefa7b30713a52f3c73a))
    - avoid logging short sleeps ([`68d28d5`](https://github.com/maidsafe/safe_network/commit/68d28d5591c5ccc6a241832f9b7855827958372a))
    - reduce to 5s ([`26ad757`](https://github.com/maidsafe/safe_network/commit/26ad7578f0d224528a1be6509453f695f6530eb4))
    - gossip all votes and start timer after first vote ([`8bf0aee`](https://github.com/maidsafe/safe_network/commit/8bf0aeed0af193322f341bd718f7a5f84fa2d02f))
    - misc. fixes ([`42bde15`](https://github.com/maidsafe/safe_network/commit/42bde15e9a96dbe759575d4bccf4f769e13a695d))
    - remove unused severity; refactor weighted score ([`3cf9033`](https://github.com/maidsafe/safe_network/commit/3cf903367bfcd805ceff2f2508cd2b12eddc3ca5))
    - Add braces around expected age calc; use membership ([`a5afec8`](https://github.com/maidsafe/safe_network/commit/a5afec84bc687327df3951eefc6c566c898f2332))
    - remove NetworkKnowledge::chain ([`6e65ed8`](https://github.com/maidsafe/safe_network/commit/6e65ed8e6c5872bd2c49a1ed2837b1fb16523af1))
    - serialize NetworkPrefixMap into JSON ([`29de67f`](https://github.com/maidsafe/safe_network/commit/29de67f1e3583eab867d517cb50ed2e404bd63fd))
    - nodes to cache their own individual prefix map file on disk ([`96da117`](https://github.com/maidsafe/safe_network/commit/96da1171d0cac240f772e5d6a15c56f63441b4b3))
    - consolidate AE retry/redirect logic ([`2422767`](https://github.com/maidsafe/safe_network/commit/24227673b57954b1c53b9b88d714e42c39d8f000))
    - expand churn-join-miss check to all AE msgs ([`a9885a8`](https://github.com/maidsafe/safe_network/commit/a9885a8d0e3e59dc630cf605fc9e353e152a5bb3))
    - pull out common AE update code ([`2e865cf`](https://github.com/maidsafe/safe_network/commit/2e865cf211a91bef3caaded6310eaddd7e03e997))
    - inline handle_network_update ([`c77f686`](https://github.com/maidsafe/safe_network/commit/c77f686e132f1ac58a113392bc65087bfb650bb9))
    - handle_network_update doesn't deal with msg ([`a184ebe`](https://github.com/maidsafe/safe_network/commit/a184ebe3f07b86a53c7e8b36b3d86034558a99fb))
    - move lines around to prepare for merge ([`8eebc01`](https://github.com/maidsafe/safe_network/commit/8eebc01007a49f1debaede519324d920e9628d46))
    - reduce nesting in ae-retry handling ([`46b6e53`](https://github.com/maidsafe/safe_network/commit/46b6e53d220ebc7f60bb754a4c7ebf4ec7d83e58))
    - reducing nesting in ae-redirect/retry handler ([`13fc65d`](https://github.com/maidsafe/safe_network/commit/13fc65dc634348740095226e1da0af1866f5b3a8))
    - reduce AE msgs to one msg with a kind field ([`6b1fee8`](https://github.com/maidsafe/safe_network/commit/6b1fee8cf3d0b2995f4b81e59dd684547593b5fa))
    - semantic tweaks ([`a8b0631`](https://github.com/maidsafe/safe_network/commit/a8b0631a396ac96e000db22141ffd5d83fd7e987))
    - log with unique id around sleep ([`f817fa5`](https://github.com/maidsafe/safe_network/commit/f817fa5917f48faaeb3714c7faeaee5392b82f55))
    - gradually slowdown when send to peer too fast ([`98279c2`](https://github.com/maidsafe/safe_network/commit/98279c2243ec2c5351947e26650d64dc5bf81797))
    - removing Token from sn_interfaces::type as it is now exposed by sn_dbc ([`dd2eb21`](https://github.com/maidsafe/safe_network/commit/dd2eb21352223f6340064e0021f4a7df402cd5c9))
    - make cmd ctrl response quicker ([`0d5ddf0`](https://github.com/maidsafe/safe_network/commit/0d5ddf04001fc65c05865cf2a2ea08cd3cd45615))
    - chore(style): organise usings, cleanup - Removes some boilerplate, using fn of `Cmd` to instantiate a send cmd. - Housekeeping, continuing to minimize bloat of usings, by colocating them. - Housekeeping, continuing keeping positions of usings in a file according to a system, from closest (self) on top, down to furthest away (3rd part). ([`8242f2f`](https://github.com/maidsafe/safe_network/commit/8242f2f1035b1c0718e53954951badffa30f3393))
    - use matches! macros, minor refactoring ([`848dba4`](https://github.com/maidsafe/safe_network/commit/848dba48e5959d0b9cfe182fde2f12ede71ba9c2))
    - remove unused async/await ([`35483b3`](https://github.com/maidsafe/safe_network/commit/35483b3f322eeea2c10427e94e4750a8269811c0))
    - remove NetworkPrefxiMap::genesis_key, NetworkKnowledge::genesis_key ([`820fcc9`](https://github.com/maidsafe/safe_network/commit/820fcc9a77f756fca308f247c3ea1b82f65d30b9))
    - fix deadlock introduced after removal of Arc from NetworkPrefixMap ([`0ed5075`](https://github.com/maidsafe/safe_network/commit/0ed5075304b090597f7760fb51c4a33435a853f1))
    - remove RwLock from NetworkPrefixMap ([`afcf083`](https://github.com/maidsafe/safe_network/commit/afcf083469c732f10c7c80f4a45e4c33ab111101))
    - move SectionChain into NetworkPrefixMap ([`ed37bb5`](https://github.com/maidsafe/safe_network/commit/ed37bb56e5e17d4cba7c1b2165746c193241d618))
    - refactor CmdCtrl, naming and remove retries ([`72db95a`](https://github.com/maidsafe/safe_network/commit/72db95a092ea33f29e77b6101b16e219fadd47ab))
    - rename traceroute fns ([`aafc560`](https://github.com/maidsafe/safe_network/commit/aafc560d3b3b1e375f7be224e0e63a3b567bbd86))
    - traceroute update after cmds flow rebase ([`7394030`](https://github.com/maidsafe/safe_network/commit/7394030fe5aeeb88f4524d2da2a71e36334c831d))
    - make traceroute a default feature ([`73dc9b4`](https://github.com/maidsafe/safe_network/commit/73dc9b4a1757393270e62d265328bab0c0aa3b35))
    - improve Display, Debug impl for Traceroute ([`0a653e4`](https://github.com/maidsafe/safe_network/commit/0a653e4becc4a8e14ffd6d0752cf035430067ce9))
    - improve traceroute readability and other improvements ([`9789797`](https://github.com/maidsafe/safe_network/commit/9789797e3f773285f23bd22957fe45a67aabec24))
    - add traceroute for AE system messages ([`8e626f9`](https://github.com/maidsafe/safe_network/commit/8e626f9fabbb07a126199ea5481e2ac524cbae0d))
    - feat(churnid): make the content explicit We previously allowed any length vec, while we only based it on signatures. Therefore, we could restrict it to the signature contents boundary. ([`366719f`](https://github.com/maidsafe/safe_network/commit/366719f9e158fb35093382739d48cdb8dac65b60))
    - fix(file_store): use correct data address type The type including `SafeKey` had been incorrectly used (since it is not a network side concept), which caused a lot of `Result` return values bloating the call tree unecessarily. ([`cebf37e`](https://github.com/maidsafe/safe_network/commit/cebf37ea8ef44c51ce84646a83a5d1e6dcab3e7a))
    - chore: cleanup unnecessary options and results These were remaining in places where there was no path for None or Error. Cleaning these up removes accidental complexity. ([`db22c6c`](https://github.com/maidsafe/safe_network/commit/db22c6c8c1aedb347bea52199a5673695eff86f8))
    - chore: reduce file db error logs Listing files on a path that doesn't yet exist, resulted in lots of unnecessary error logs. ([`2809ed1`](https://github.com/maidsafe/safe_network/commit/2809ed1177c416f933f3869bd11607c4e5e6a908))
    - fix(cmd_ctrl): add actual id of parent It tried to add parent id of parent, which would always be None. ([`ec62534`](https://github.com/maidsafe/safe_network/commit/ec625349494b267d24c80fd4c8b176b4524f48cd))
    - refactor: do not require node write lock on query This creates the `AddToPendingQieries` cmd, which adds asyncly to the list. Also cleans up the `read_data_from_adults` fn a bit. ([`a0c89ff`](https://github.com/maidsafe/safe_network/commit/a0c89ff0e451d2e5dd13fc29635075097f2c7b94))
    - remove optional field ([`0f07efd`](https://github.com/maidsafe/safe_network/commit/0f07efd9ef0b75de79f27772566b013bc886bcc8))
    - refactor(messaging): remove more unused code More reuse of methods to replace duplication of code. Deprecates delivery group, since it is no longer used. Also, `DstLocation` and `SrcLocation` are removed. BREAKING CHANGE: WireMsg public type is changed. ([`db4f4d0`](https://github.com/maidsafe/safe_network/commit/db4f4d07b155d732ad76d263563d81b5fee535f7))
    - chore: clarify fn signatures Return single cmd when only one can be returned. Remove some unnecessary Results. Also fixes insufficient adults error being triggered falsely. ([`08af2a6`](https://github.com/maidsafe/safe_network/commit/08af2a6ac3485a696d2a1e799af588943f207e6b))
    - refactor(send): simplify send msg This places signing and wire msg instantiation in one location, and removes lots of old variables that aren't used in the flow anymore. ([`ff1a10b`](https://github.com/maidsafe/safe_network/commit/ff1a10b4aa2b41b7028949101504a29b52927e71))
    - delete commented out tests ([`080f9ef`](https://github.com/maidsafe/safe_network/commit/080f9ef83005ebda9e1c96b228f3d5096fd79b81))
    - use sn_dbc::SpentProof API for verifying SpentProofShares ([`e0fb940`](https://github.com/maidsafe/safe_network/commit/e0fb940b24e87d86fe920095176362f73503ce79))
    - create query cmd for healthcheck ([`22f4f15`](https://github.com/maidsafe/safe_network/commit/22f4f1512da8f9a2245addf379477823364edd6c))
    - dont send health checks as an adult ([`1b37f4b`](https://github.com/maidsafe/safe_network/commit/1b37f4bbf266c21d795bff6b4e6f2e1885405697))
    - simplify the send funcitonality ([`feaf3ef`](https://github.com/maidsafe/safe_network/commit/feaf3ef88b140a0f530082e851831b736320de59))
    - use Cmd::TrackDysf in read_data_from_adults ([`7157ed2`](https://github.com/maidsafe/safe_network/commit/7157ed27ddfc0d987272f1285b44faa9709a4c8f))
    - remove RwLock over Cache type ([`2ea0695`](https://github.com/maidsafe/safe_network/commit/2ea069543dbe6ffebac663d4d8d7e0bc33cfc566))
    - remove write lock around non query service msg handling ([`322c698`](https://github.com/maidsafe/safe_network/commit/322c69845e2e14eb029fdbebb24e08063a2323b0))
    - retry twice if it fails spending inputs when reissuing DBCs ([`005b84c`](https://github.com/maidsafe/safe_network/commit/005b84cab0ca91762cbedd208b022d4c4983fe26))
    - remove the write lock on node while validating msgs ([`5a5f0f4`](https://github.com/maidsafe/safe_network/commit/5a5f0f4608c27f463178f4a560f1f9b4c020e764))
    - Revert "feat: make traceroute default for now" ([`e9b97c7`](https://github.com/maidsafe/safe_network/commit/e9b97c72b860053285ba866b098937f6b25d99bf))
    - having spent proofs and Txs within SpentbookCmd::Send msg to be a set instead of a vec ([`d3a05a7`](https://github.com/maidsafe/safe_network/commit/d3a05a728be8752ea9ebff4e38e7c4c85e5db09b))
    - make Sign of SystemMsgs higher prio as this could hold them back before sending ([`83a3a98`](https://github.com/maidsafe/safe_network/commit/83a3a98a6972c9a1824e41cb87325b037b65938c))
    - consolidate join / leave into decision handling ([`47179e6`](https://github.com/maidsafe/safe_network/commit/47179e695a7f351d9298c5bdbea006abb26b8e89))
    - make traceroute default for now ([`175d1b9`](https://github.com/maidsafe/safe_network/commit/175d1b909dff8c6729ac7f156ce1d0d22be8cc12))
    - log parent job id during processing ([`9c855c1`](https://github.com/maidsafe/safe_network/commit/9c855c187465a0594cf18fb359a082742593a4d4))
    - simplify data replication msg sending ([`f8b6ff0`](https://github.com/maidsafe/safe_network/commit/f8b6ff0edd7a64389439081b6306296402887ab1))
    - temporarily allow DBC spent proofs singed by sections keys unknown to the Elder ([`c6fa773`](https://github.com/maidsafe/safe_network/commit/c6fa7735a5e48a4caa6ee3aac000785a9da9413a))
    - make `&mut self` methods `&self` ([`302fb95`](https://github.com/maidsafe/safe_network/commit/302fb954360521d40efab0e26fd31f8278a74755))
    - removed unused lru_cache module ([`70f3ecc`](https://github.com/maidsafe/safe_network/commit/70f3ecc367ca9450be038f8ff806f40c324d1b00))
    - only do connectivity test for failed sends via dysfunction ([`5a83a4f`](https://github.com/maidsafe/safe_network/commit/5a83a4ff08b7a3c2be0e3532589d209e72367d92))
    - chore: Cleanup non-joined member sessions, regardless of connected state This reverts commit 7d12399edec6c1191c521528c1d569afc96bca99. ([`8efbd96`](https://github.com/maidsafe/safe_network/commit/8efbd96a5fd3907ace5ca6ac282027595fefd8ef))
    - improve the accuracy of data_storage bench tests ([`f200909`](https://github.com/maidsafe/safe_network/commit/f200909516739769d087b20712a33b5047858ba9))
    - remove one level of indentation ([`81f5e25`](https://github.com/maidsafe/safe_network/commit/81f5e252501174cb6367474a980b8a2a5da58dc2))
    - chore(naming): reflect the semantics not the type The type is named Kind but the semantics of it is Auth. Often we mindlessly name things after the type names instead of what they represent in the domain. BREAKING CHANGE: fields of public msg renamed ([`ea490dd`](https://github.com/maidsafe/safe_network/commit/ea490ddf749ac9e0c7962c3c21c053663e6b6ee7))
    - refactor: expose known keys on network knowledge The method can be called directly instead of passing known keys in the cmds. ([`35ebd8e`](https://github.com/maidsafe/safe_network/commit/35ebd8e872f9d9db16c42cbe8d61702f9660aece))
    - check for dst location to send to clients instead of msg authority type ([`042503a`](https://github.com/maidsafe/safe_network/commit/042503a7d44f94ed3f0ce482984744e175d7752b))
    - fix(datastorage): remove unused get mut access Gets/Reads should not mutate, and nothing here did require mut access, so it could be removed. ([`6ca512e`](https://github.com/maidsafe/safe_network/commit/6ca512e7adc6681bc85122847fa8ac08550cfb09))
    - whitespace + typo fix ([`bf2902c`](https://github.com/maidsafe/safe_network/commit/bf2902c18b900b8b4a8abae5f966d1e08d547910))
    - disable rejoins ([`a5028c5`](https://github.com/maidsafe/safe_network/commit/a5028c551d1b3db2c4c912c2897490e9a4b34a0d))
    - reduce re-join tests to one case ([`b492cc2`](https://github.com/maidsafe/safe_network/commit/b492cc25cfadf7dd3aaf3584cd5dbbd0dc8532ce))
    - prevent rejoins of archived nodes ([`1694c15`](https://github.com/maidsafe/safe_network/commit/1694c1566ac562216447eb491cc3b2b00b0c5979))
    - update rejoin tests to test proposal validation ([`6dbc032`](https://github.com/maidsafe/safe_network/commit/6dbc032907bcb1fc6e3e9034f794643097260e17))
    - rename gen_section_authority_provider to random_sap ([`3f577d2`](https://github.com/maidsafe/safe_network/commit/3f577d2a6fe70792d7d02e231b599ca3d44a5ed2))
    - mv join handler to membership module ([`a1d74e8`](https://github.com/maidsafe/safe_network/commit/a1d74e894975d67f3293dbb0db73f6b62b9c378a))
    - pull inner loop of join handling into helper ([`3a74f59`](https://github.com/maidsafe/safe_network/commit/3a74f59269f57a50a14de9a35f7e725014ec8f0e))
    - join approval has membership decision ([`df8ea32`](https://github.com/maidsafe/safe_network/commit/df8ea32f9218d344cd1f291359969b38a05b4642))
    - Revert "chore: Cleanup non-joined member sessions, regardless of connected state." ([`7d12399`](https://github.com/maidsafe/safe_network/commit/7d12399edec6c1191c521528c1d569afc96bca99))
    - upgrade blsttc to 7.0.0 ([`6f03b93`](https://github.com/maidsafe/safe_network/commit/6f03b93bd2d02f0ffe54b69fbf25070fbe64eab0))
    - support the traceroute feature flag ([`7fc6231`](https://github.com/maidsafe/safe_network/commit/7fc6231f241bcae4447839989b712b1b410a2523))
    - add intermitent health checks ([`7f40e89`](https://github.com/maidsafe/safe_network/commit/7f40e89f9c0aa767bd372278e1d50de5a8e7869b))
    - adds unique conn info validation to membership ([`19bb0b9`](https://github.com/maidsafe/safe_network/commit/19bb0b99afee53dd7b6e109919249b25e0a55e48))
    - avoid un-necessary dysfunction log_dkg_issue ([`e1a0474`](https://github.com/maidsafe/safe_network/commit/e1a0474e78fd00a881b13ad036c1aabc9ce2f02a))
    - Cleanup non-joined member sessions, regardless of connected state. ([`934bf6c`](https://github.com/maidsafe/safe_network/commit/934bf6cbc86e252eb3859c757a0b66c02f7826d9))
    - impl traceroute for client cmds and cmd responses ([`4f2cf26`](https://github.com/maidsafe/safe_network/commit/4f2cf267ee030e5924a2fa999a2a46dbc072d208))
    - improve traceroute redability and resolve clippy ([`214aded`](https://github.com/maidsafe/safe_network/commit/214adedc31bca576c7f28ff52a1f4ff0a2676757))
    - impl traceroute feature to trace a message's flow in the network ([`a6fb1fc`](https://github.com/maidsafe/safe_network/commit/a6fb1fc516a9ef6dae7aa236f3dd440d50697ae2))
    - fix outdated comm tests ([`893b4af`](https://github.com/maidsafe/safe_network/commit/893b4af340b0ced1a2b38dda07bd82cf833be776))
    - add OTLP support to sn_node bin ([`01ea9c0`](https://github.com/maidsafe/safe_network/commit/01ea9c0bdb88da4f181d8f1638f2f2ad692d0ca3))
    - Merge #1364 ([`39e4341`](https://github.com/maidsafe/safe_network/commit/39e43415bd4da8298919640e4102dc41be3f8bed))
    - allow more log layers in future ([`a727dae`](https://github.com/maidsafe/safe_network/commit/a727daea6e5a01a24c9bcdbeef033d0622f4ba39))
    - remove cfg() directives ([`1db6648`](https://github.com/maidsafe/safe_network/commit/1db6648954987ebce4c91f8e29bbec4e54b75edf))
    - split logging init into functions ([`fa7dfa3`](https://github.com/maidsafe/safe_network/commit/fa7dfa342e90acd0a681110e149df4400a8f392e))
    - further modularize logging ([`28c0a10`](https://github.com/maidsafe/safe_network/commit/28c0a1063194eea66910c7d18653c558595ec17e))
    - move log rotater into own module ([`f467d5f`](https://github.com/maidsafe/safe_network/commit/f467d5f45452244d2f8e3e81910b76d0d4b0f7cb))
    - move sn_node binary into own dir ([`f45afa2`](https://github.com/maidsafe/safe_network/commit/f45afa221a18638bbbbad5cf6121a68825ed3ff3))
    - broken links ([`e3c9099`](https://github.com/maidsafe/safe_network/commit/e3c90998e1abd10768e861370a65a934f52e2ec3))
    - increase Send priority, and tweak depending on DstLocation ([`feaca15`](https://github.com/maidsafe/safe_network/commit/feaca15b7c44297c16a4665ceec738226bb860ba))
    - remove one dir layer ([`9895a2b`](https://github.com/maidsafe/safe_network/commit/9895a2b9e82bdbf110a9805972290841860d1a49))
    - refactor(node_api): extract test only used fns The api was only used in tests and can be moved to a separate struct. ([`1c7b47f`](https://github.com/maidsafe/safe_network/commit/1c7b47f48635bb7b0a8a13d01bb41b148e343ce8))
    - reduce number of Send retries in PeerSession ([`7d060f0`](https://github.com/maidsafe/safe_network/commit/7d060f0e92e3b250e3fe1e0523aa0c30b439e0be))
    - remove SendMsgDeliveryGroup Cmd ([`7a675f4`](https://github.com/maidsafe/safe_network/commit/7a675f4889c4ef01b9040773184ab2e0ed78b208))
    - use SendMsg for send_node_msg_to_nodes ([`c3778cd`](https://github.com/maidsafe/safe_network/commit/c3778cd77e0c9cbc407449afe713ff7cdb4b9909))
    - remove redundant generation field ([`f0d1abf`](https://github.com/maidsafe/safe_network/commit/f0d1abf6dd8731310b7749cd6cc7077886215997))
    - refactor: remove one layer of indirection `HandleNewEldersAgreement` variant of `Cmd` had an unnecessary wrapper of `Proposal` around the actual object being conveyed. ([`f06a026`](https://github.com/maidsafe/safe_network/commit/f06a0260b058519ec858abf654cbce102eb00147))
    - Merge #1356 ([`d9b4608`](https://github.com/maidsafe/safe_network/commit/d9b46080ac849cac259983dc80b4b879e58c13ba))
    - drop RwLock guards after job is done ([`640b64d`](https://github.com/maidsafe/safe_network/commit/640b64d2b5f19df5f5439a8fce31a848a47526cd))
    - setup step for tests to reissue a set of DBCs from genesis only once ([`5c82df6`](https://github.com/maidsafe/safe_network/commit/5c82df633e7c062fdf761a8e6e0a7ae8d26cc73a))
    - increase CleanUpPeerLinks prio ([`fba2f76`](https://github.com/maidsafe/safe_network/commit/fba2f76ec23a00ca1da857e63af160a11904288c))
    - fix benchamrking groups in data_storage bench ([`5a121c1`](https://github.com/maidsafe/safe_network/commit/5a121c19d395130e40df0134be36e4264b60972a))
    - update data_storage benchmark ranges ([`67c82ca`](https://github.com/maidsafe/safe_network/commit/67c82cae6654423cae3567d8417a442a40ce1e5e))
    - re-enable registers benchmark and tidy sled residue ([`1e8180c`](https://github.com/maidsafe/safe_network/commit/1e8180c23fab27ac92c93f201efd050cff00db10))
    - replace sled with filestore for storing registers ([`24676da`](https://github.com/maidsafe/safe_network/commit/24676dadb771bbd966b6a3e1aa53d1c736c90627))
    - make chunk_store accept all datatypes ([`93614b1`](https://github.com/maidsafe/safe_network/commit/93614b18b4316af04ab8c74358a5c86510590b85))
    - Merge #1324 #1345 #1346 ([`f664d79`](https://github.com/maidsafe/safe_network/commit/f664d797b56e7cbf03893c98d6c27d9c6d882be4))
    - reduce service msg response SendMsg cmds ([`900fa8c`](https://github.com/maidsafe/safe_network/commit/900fa8c4803e9e45a1471a32fb4fe5b8cdd5112b))
    - tweak some WireMsg.serialize calls to minimize them ([`a95be62`](https://github.com/maidsafe/safe_network/commit/a95be6277a9ee8d66eccd40711392325fac986e2))
    - fix(tests): update for the split of HandleCmd Also removes order requirement of resulting cmds in one test, as the order is not a system requirement. ([`38d25d6`](https://github.com/maidsafe/safe_network/commit/38d25d6df71e3bb71e8efda50a4bf64345f69f81))
    - feat(msgs): validate w low prio before handling Deserializes and validates at lower prio, to cut off an overload attack angle. After validity of msg has been established, the msg is dealt with according to its type prio. ([`c6999a9`](https://github.com/maidsafe/safe_network/commit/c6999a92d06f275f7506a24492bec50042466459))
    - fix(tests): update for the split of HandleCmd Also removes order requirement of resulting cmds in one test, as the order is not a system requirement. ([`ae38fdc`](https://github.com/maidsafe/safe_network/commit/ae38fdce6499d8245025d7bd82fa6c583f04060d))
    - feat(msgs): validate w low prio before handling Deserializes and validates at lower prio, to cut off an overload attack angle. After validity of msg has been established, the msg is dealt with according to its type prio. ([`6f781b9`](https://github.com/maidsafe/safe_network/commit/6f781b96da745839206352ae02c18b759b49f1f2))
    - use timestamp for log files ([`a856d78`](https://github.com/maidsafe/safe_network/commit/a856d788131ef85414ee1f42a868abcbbfc0d2b6))
    - unused async in CLI ([`6d237e5`](https://github.com/maidsafe/safe_network/commit/6d237e5e7d8306cb955f436910aa01ed7221cd84))
    - unused async in node/dkg etc ([`78be9fb`](https://github.com/maidsafe/safe_network/commit/78be9fb24cf66d9f8f06ac31895302eae875661e))
    - more node messaging async removal ([`abddc53`](https://github.com/maidsafe/safe_network/commit/abddc53df9fbbd5c35b6ce473646f3183bf423df))
    - formatting with cargo fmt ([`00fae4d`](https://github.com/maidsafe/safe_network/commit/00fae4d5fd5dbad5696888f0c796fbd39b7e49ed))
    - node messaging unused async removal ([`8f3d3f7`](https://github.com/maidsafe/safe_network/commit/8f3d3f7acf62f0f5c50cc68280214a4119801abd))
    - more async removal from node/mod.rs ([`e12479f`](https://github.com/maidsafe/safe_network/commit/e12479f8db891107215192e4824df92999fb23af))
    - unused async in comm/node methods ([`4a277b6`](https://github.com/maidsafe/safe_network/commit/4a277b6a290ee7b8ec99dba4e421ac19023fe08e))
    - removed unused async at dysfunction ([`4773e18`](https://github.com/maidsafe/safe_network/commit/4773e185302ada27cd08c8dfd04582e7fdaf42aa))
    - cleanup unused async ([`f6ea1da`](https://github.com/maidsafe/safe_network/commit/f6ea1da4a57e40a051c7d1ee3b87fe9b442c537b))
    - only process each membership decision once ([`4e44e37`](https://github.com/maidsafe/safe_network/commit/4e44e373da9ee75b2563b39c26794299e607f48f))
    - increase wait on ci & decrease delay before node retry ([`549c4b1`](https://github.com/maidsafe/safe_network/commit/549c4b169547e620471c416c1506afc6e3ee265b))
    - remove locking from section key cache ([`847db2c`](https://github.com/maidsafe/safe_network/commit/847db2c487cd102af0cf9a477b4c1b65fc2c8aa6))
    - remove refcell on NetworkKnowledge::all_section_chains ([`0a5593b`](https://github.com/maidsafe/safe_network/commit/0a5593b0512d6f059c6a8003634b07e7d2d3e514))
    - remove refcell around NetworkKnowledge::signed_sap ([`707b80c`](https://github.com/maidsafe/safe_network/commit/707b80c3526ae727a7e91330dc386cdb41c51f4c))
    - remove refcell on NetworkKnowledge::chain ([`9bd6ae2`](https://github.com/maidsafe/safe_network/commit/9bd6ae20c1207f99420093fd5c9f4eb53836e3c1))
    - remove awaits from tests as well ([`31d9f9f`](https://github.com/maidsafe/safe_network/commit/31d9f9f99b4e166986b8e51c3d41e0eac55621a4))
    - remove locking around signature aggregation ([`30a7028`](https://github.com/maidsafe/safe_network/commit/30a7028dd702e2f6575e299a609a2416439cbaed))
    - remove unused async ([`dedec48`](https://github.com/maidsafe/safe_network/commit/dedec486f85c1cf6cf2d538238f32e826e08da0a))
    - remove logs in looping data replication ([`3bef795`](https://github.com/maidsafe/safe_network/commit/3bef795923863d977f70c95647444ebbc97c5cf5))
    - increase dysfunciton test timeout ([`ca51208`](https://github.com/maidsafe/safe_network/commit/ca5120885e3e28229f298b81edf6090542e0e3f9))
    - relax when we track DkgIssues ([`f142bbb`](https://github.com/maidsafe/safe_network/commit/f142bbb0030233add4808427a2819ca386fef503))
    - Tweak dysf interval, reducing to report on issues more rapidly ([`e39917d`](https://github.com/maidsafe/safe_network/commit/e39917d0635a071625f7961ce6d40cb44cc65da0))
    - logging sn_consensus in CI, tweak section min and max elder age ([`879678e`](https://github.com/maidsafe/safe_network/commit/879678e986a722d216ee9a4f37e8ae398221a394))
    - Merge branch 'main' into feat-cat-wallet-improvements ([`9409bf4`](https://github.com/maidsafe/safe_network/commit/9409bf42e99b4eb3da883f76c802e7dc6ea1a4a0))
</details>

## v0.64.3 (2022-07-12)

<csr-id-0dcd4917c9a7bfbf6706f3b8a18e68d010c9b50d/>
<csr-id-5523e237464a76ef682ae2dbc183692502018682/>
<csr-id-5068b155ce42f0902f9f3847e8069dc415910f34/>

### Other

 - <csr-id-0dcd4917c9a7bfbf6706f3b8a18e68d010c9b50d/> unit tests for JoiningAsRelocated

### Chore

 - <csr-id-5068b155ce42f0902f9f3847e8069dc415910f34/> sn_node-0.64.3

### Refactor

 - <csr-id-5523e237464a76ef682ae2dbc183692502018682/> move core one level up
   Move `node::core` to `node`.
   Rename `api` module to `node_api`
   Move `messages::mod` to `messages.rs`
   Move `create_test_max_capacity_and_root_storage` from `node::mod` to `node::cfg::mod` where it is more appropriate.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 1 day passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.64.3 ([`5068b15`](https://github.com/maidsafe/safe_network/commit/5068b155ce42f0902f9f3847e8069dc415910f34))
    - Merge branch 'main' into feat-cat-wallet-improvements ([`8e6eecf`](https://github.com/maidsafe/safe_network/commit/8e6eecf0da8df5cdac55bbf1f81d00bcb19558b4))
    - Merge #1269 ([`09c2fb3`](https://github.com/maidsafe/safe_network/commit/09c2fb304c3a66902d81353153d5138f3e5a0a79))
    - unit tests for JoiningAsRelocated ([`0dcd491`](https://github.com/maidsafe/safe_network/commit/0dcd4917c9a7bfbf6706f3b8a18e68d010c9b50d))
    - move core one level up ([`5523e23`](https://github.com/maidsafe/safe_network/commit/5523e237464a76ef682ae2dbc183692502018682))
</details>

## v0.64.2 (2022-07-10)

<csr-id-f2ab97c053f173878ae8a355454818b38e7d72a9/>
<csr-id-49e223e2c07695b4c63e253ba19ce43ec24d7112/>
<csr-id-5cff2c5325a854f04788f9111439bca75b21c60f/>
<csr-id-dce3ba214354ad007900efce78273670cfb725d5/>
<csr-id-34bd9bd01a3f042c35e0432df2f0cfcebc32a8a8/>

### Chore

 - <csr-id-f2ab97c053f173878ae8a355454818b38e7d72a9/> inline generic write_file function
 - <csr-id-49e223e2c07695b4c63e253ba19ce43ec24d7112/> move more deps to clap-v3; rm some deps on rand
 - <csr-id-5cff2c5325a854f04788f9111439bca75b21c60f/> ignore store_and_read_40mb as too heavy for CI

### Chore

 - <csr-id-34bd9bd01a3f042c35e0432df2f0cfcebc32a8a8/> sn_interface-0.8.2/sn_client-0.68.2/sn_node-0.64.2/sn_api-0.66.3/sn_cli-0.59.3

### Bug Fixes

 - <csr-id-64eb333d532694f46f1d0b9dd5109961b3551802/> for QueryResponse, set correlation_id to be the origin msg_id
 - <csr-id-3c383ccf9ad0ed77080fb3e3ec459e5b02158505/> passing the churn test
   This commit contains the work to passing the churn test.
   There are mainly two fixes:
   1, Only trigger data reorganization when there is membership update.
   Previously, data reorganzation get undertaken whenever there is
   incoming message. Which result in a looping of messaging among
   nodes.
   2, Only broadcast result when the QueryResponse is not an error.
   Previously, this will cause the client thinking the whole query
   is failed whenever an error response received.

### Refactor

 - <csr-id-dce3ba214354ad007900efce78273670cfb725d5/> move dkg util method definitions onto the DKG structs

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 11 commits contributed to the release over the course of 2 calendar days.
 - 2 days passed between releases.
 - 7 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.8.2/sn_client-0.68.2/sn_node-0.64.2/sn_api-0.66.3/sn_cli-0.59.3 ([`34bd9bd`](https://github.com/maidsafe/safe_network/commit/34bd9bd01a3f042c35e0432df2f0cfcebc32a8a8))
    - inline generic write_file function ([`f2ab97c`](https://github.com/maidsafe/safe_network/commit/f2ab97c053f173878ae8a355454818b38e7d72a9))
    - move dkg util method definitions onto the DKG structs ([`dce3ba2`](https://github.com/maidsafe/safe_network/commit/dce3ba214354ad007900efce78273670cfb725d5))
    - move more deps to clap-v3; rm some deps on rand ([`49e223e`](https://github.com/maidsafe/safe_network/commit/49e223e2c07695b4c63e253ba19ce43ec24d7112))
    - Merge #1301 ([`9c6914e`](https://github.com/maidsafe/safe_network/commit/9c6914e2688f70a25ad5dfe74307572cb8e8fcc2))
    - Merge branch 'main' into feat-dbc-spent-proof-validations ([`45418f2`](https://github.com/maidsafe/safe_network/commit/45418f2f9b5cc58f2a153bf40966beb2bf36a62a))
    - ignore store_and_read_40mb as too heavy for CI ([`5cff2c5`](https://github.com/maidsafe/safe_network/commit/5cff2c5325a854f04788f9111439bca75b21c60f))
    - for QueryResponse, set correlation_id to be the origin msg_id ([`64eb333`](https://github.com/maidsafe/safe_network/commit/64eb333d532694f46f1d0b9dd5109961b3551802))
    - passing the churn test ([`3c383cc`](https://github.com/maidsafe/safe_network/commit/3c383ccf9ad0ed77080fb3e3ec459e5b02158505))
    - Merge branch 'main' into feat-dbc-spent-proof-validations ([`45309c4`](https://github.com/maidsafe/safe_network/commit/45309c4c0463dd9198a49537187417bf4bfdb847))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`6268fe7`](https://github.com/maidsafe/safe_network/commit/6268fe76e9dd81d291492b4611094273f8d1e223))
</details>

## v0.64.1 (2022-07-07)

<csr-id-c79e2aac378b28b373fd7c18c4b9006348960071/>
<csr-id-d2c3f6e40156ff092e63640bee7c569017d5da95/>
<csr-id-46262268fc167c05963e5b7bd6261310496e2379/>
<csr-id-8dccb7f1fc81385f9f5f25e6c354ad1d35759528/>
<csr-id-aa5dab0c10ccf4732e5ac43fa7d893d597c22f25/>
<csr-id-262c10427e80a79edc5d9d5416fa51116ead284e/>
<csr-id-b979a27793c8618610881ce8c8a71635f766556b/>
<csr-id-ed85c630919a03c8980d497becbcf7fe5deb9450/>
<csr-id-8927a7232b6fdd717a3c9a517da52a1757533e19/>
<csr-id-6b574bd53f7e51839380b7be914dbab015726d1e/>
<csr-id-2f6fff23a29cc4f04415a9a606fec88167551268/>
<csr-id-dfcda427a51a88add7e935427722d9af7645ba37/>
<csr-id-56a41f0ef175050e8e7c569e84ee6bdc2253f59a/>
<csr-id-b3500b717ffbce34d80c067225ac6b368f142c03/>
<csr-id-49a03bcc2b51af262cc6931f7d5f006226076ca0/>
<csr-id-6da8309b924357bbd8a1f576face89f8390ad8cf/>
<csr-id-bb68a98f5f29ecdd25d069aa57e5f49e57352445/>
<csr-id-6cae68742e8b32105f87aeac463495b892d48397/>
<csr-id-dfeb7b0c8dfde25af6eac4374a2cf1691b321907/>
<csr-id-92336b180c0b13290a3b83054545332867bc2d3f/>
<csr-id-2b00cec961561281f6b927e13e501342843f6a0f/>

### Chore

 - <csr-id-c79e2aac378b28b373fd7c18c4b9006348960071/> bit more low hanging clippy fruit
 - <csr-id-d2c3f6e40156ff092e63640bee7c569017d5da95/> replace StructOpt with Clap in sn_node
 - <csr-id-46262268fc167c05963e5b7bd6261310496e2379/> `try!` macro is deprecated
   No need for rustfmt to check/replace this, as the compiler will already
   warn for this. Deprecated since 1.39.
   
   Removing the option seems to trigger a couple of formatting changes that
   rustfmt did not seem to pick on before.
 - <csr-id-8dccb7f1fc81385f9f5f25e6c354ad1d35759528/> clippy runs cargo check already
 - <csr-id-aa5dab0c10ccf4732e5ac43fa7d893d597c22f25/> prevent lock in data_replication loop
 - <csr-id-262c10427e80a79edc5d9d5416fa51116ead284e/> remove DashMap from pending_data_to_replicate_to_peers
 - <csr-id-b979a27793c8618610881ce8c8a71635f766556b/> remove unneeded async over data storage.keys()
 - <csr-id-ed85c630919a03c8980d497becbcf7fe5deb9450/> log for data batch sending
 - <csr-id-8927a7232b6fdd717a3c9a517da52a1757533e19/> increase join timeout
 - <csr-id-6b574bd53f7e51839380b7be914dbab015726d1e/> Remove registerStorage cache
 - <csr-id-2f6fff23a29cc4f04415a9a606fec88167551268/> remove dysfunction arc/rwlock
 - <csr-id-dfcda427a51a88add7e935427722d9af7645ba37/> remove data storage arc/rwlock
 - <csr-id-56a41f0ef175050e8e7c569e84ee6bdc2253f59a/> remove capacity arc/rwlock
 - <csr-id-b3500b717ffbce34d80c067225ac6b368f142c03/> remove split barrier arc/rwlock
 - <csr-id-49a03bcc2b51af262cc6931f7d5f006226076ca0/> remove ae_backoff_cache arc/rwlock
 - <csr-id-6da8309b924357bbd8a1f576face89f8390ad8cf/> remove RwLock/Arc from LruCache
 - <csr-id-bb68a98f5f29ecdd25d069aa57e5f49e57352445/> some read/write tweaks for node
 - <csr-id-6cae68742e8b32105f87aeac463495b892d48397/> clippy and remove some unneccessary async
 - <csr-id-dfeb7b0c8dfde25af6eac4374a2cf1691b321907/> add RwLock to Node inside dispatcher

### Chore

 - <csr-id-2b00cec961561281f6b927e13e501342843f6a0f/> sn_interface-0.8.1/sn_dysfunction-0.7.1/sn_client-0.68.1/sn_node-0.64.1/sn_api-0.66.1/sn_cli-0.59.1

### New Features

 - <csr-id-a2fd9b01638c2ec385656e01c4dddbc601cd5beb/> add parent id
 - <csr-id-57f635fbe80392574f7f122a9d157fbb6320c4cc/> generate the genesis DBC when launching first node and write it to disk
 - <csr-id-8313ed8d5b45b7f4ed3b36ada231e74c49c9f9e6/> perform signature verifications on input DBC SpentProof before signing new spent proof share

### Refactor

 - <csr-id-92336b180c0b13290a3b83054545332867bc2d3f/> combine handling, sending sub-modules
   Divide messaging in terms of components rather than handling, sending sub-modules

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 39 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
 - 23 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.8.1/sn_dysfunction-0.7.1/sn_client-0.68.1/sn_node-0.64.1/sn_api-0.66.1/sn_cli-0.59.1 ([`2b00cec`](https://github.com/maidsafe/safe_network/commit/2b00cec961561281f6b927e13e501342843f6a0f))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`7e2a25a`](https://github.com/maidsafe/safe_network/commit/7e2a25ae31ead0fae7824ca794b6c407695080cd))
    - Merge #1315 ([`67686f7`](https://github.com/maidsafe/safe_network/commit/67686f73f9e7b18bb6fbf1eadc3fd3a256285396))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`7cf2eb6`](https://github.com/maidsafe/safe_network/commit/7cf2eb64e1176d2b23d63091f6f459d92bdccb57))
    - Merge branch 'main' into feat-dbc-spent-proof-validations ([`f83724c`](https://github.com/maidsafe/safe_network/commit/f83724cff1e63b35f1612fc82dffdefbeaab6cc1))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`cd2f9aa`](https://github.com/maidsafe/safe_network/commit/cd2f9aa2f7001ae779273745f9ac78fc289525e3))
    - Merge #1313 ([`7fe7be3`](https://github.com/maidsafe/safe_network/commit/7fe7be336799dec811c5b17e6d753ebe31e625f1))
    - Merge #1308 ([`8421959`](https://github.com/maidsafe/safe_network/commit/8421959b6a80e4386c34fcd6f86a1af5044280ec))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`39bd5b4`](https://github.com/maidsafe/safe_network/commit/39bd5b471b6b3acb6ebe90489335c995b0aca82f))
    - Merge branch 'main' into cargo-husky-tweaks ([`6881855`](https://github.com/maidsafe/safe_network/commit/688185573bb71cc44a7103df17f3fbeea6740247))
    - perform signature verifications on input DBC SpentProof before signing new spent proof share ([`8313ed8`](https://github.com/maidsafe/safe_network/commit/8313ed8d5b45b7f4ed3b36ada231e74c49c9f9e6))
    - Merge #1309 ([`f9fa4f7`](https://github.com/maidsafe/safe_network/commit/f9fa4f7857d8161e8c036cca06006bf187a6c6c3))
    - bit more low hanging clippy fruit ([`c79e2aa`](https://github.com/maidsafe/safe_network/commit/c79e2aac378b28b373fd7c18c4b9006348960071))
    - Merge branch 'main' into feat-cmd-parent-id ([`e10aaa2`](https://github.com/maidsafe/safe_network/commit/e10aaa2cf0404bfa10ef55b7c9dc7ae6fc0d28e5))
    - Merge branch 'main' into cargo-husky-tweaks ([`52dd02e`](https://github.com/maidsafe/safe_network/commit/52dd02e45ab4e160b0a26498919a79ce1aefb1bd))
    - Merge #1312 ([`00dc24c`](https://github.com/maidsafe/safe_network/commit/00dc24c9263a276797b4abdff0963df5e70c4231))
    - Merge branch 'main' into refactor_messaging ([`349d432`](https://github.com/maidsafe/safe_network/commit/349d43295a44b529cbb138cf2fff9483b03fea07))
    - combine handling, sending sub-modules ([`92336b1`](https://github.com/maidsafe/safe_network/commit/92336b180c0b13290a3b83054545332867bc2d3f))
    - replace StructOpt with Clap in sn_node ([`d2c3f6e`](https://github.com/maidsafe/safe_network/commit/d2c3f6e40156ff092e63640bee7c569017d5da95))
    - feat(cmd): add parent id This facilitates correlation in logging. ([`a2fd9b0`](https://github.com/maidsafe/safe_network/commit/a2fd9b01638c2ec385656e01c4dddbc601cd5beb))
    - `try!` macro is deprecated ([`4626226`](https://github.com/maidsafe/safe_network/commit/46262268fc167c05963e5b7bd6261310496e2379))
    - clippy runs cargo check already ([`8dccb7f`](https://github.com/maidsafe/safe_network/commit/8dccb7f1fc81385f9f5f25e6c354ad1d35759528))
    - Merge #1304 ([`6af41dc`](https://github.com/maidsafe/safe_network/commit/6af41dcbad76903cb5526b270100e650aa483191))
    - prevent lock in data_replication loop ([`aa5dab0`](https://github.com/maidsafe/safe_network/commit/aa5dab0c10ccf4732e5ac43fa7d893d597c22f25))
    - remove DashMap from pending_data_to_replicate_to_peers ([`262c104`](https://github.com/maidsafe/safe_network/commit/262c10427e80a79edc5d9d5416fa51116ead284e))
    - remove unneeded async over data storage.keys() ([`b979a27`](https://github.com/maidsafe/safe_network/commit/b979a27793c8618610881ce8c8a71635f766556b))
    - log for data batch sending ([`ed85c63`](https://github.com/maidsafe/safe_network/commit/ed85c630919a03c8980d497becbcf7fe5deb9450))
    - increase join timeout ([`8927a72`](https://github.com/maidsafe/safe_network/commit/8927a7232b6fdd717a3c9a517da52a1757533e19))
    - Remove registerStorage cache ([`6b574bd`](https://github.com/maidsafe/safe_network/commit/6b574bd53f7e51839380b7be914dbab015726d1e))
    - remove dysfunction arc/rwlock ([`2f6fff2`](https://github.com/maidsafe/safe_network/commit/2f6fff23a29cc4f04415a9a606fec88167551268))
    - remove data storage arc/rwlock ([`dfcda42`](https://github.com/maidsafe/safe_network/commit/dfcda427a51a88add7e935427722d9af7645ba37))
    - remove capacity arc/rwlock ([`56a41f0`](https://github.com/maidsafe/safe_network/commit/56a41f0ef175050e8e7c569e84ee6bdc2253f59a))
    - remove split barrier arc/rwlock ([`b3500b7`](https://github.com/maidsafe/safe_network/commit/b3500b717ffbce34d80c067225ac6b368f142c03))
    - remove ae_backoff_cache arc/rwlock ([`49a03bc`](https://github.com/maidsafe/safe_network/commit/49a03bcc2b51af262cc6931f7d5f006226076ca0))
    - remove RwLock/Arc from LruCache ([`6da8309`](https://github.com/maidsafe/safe_network/commit/6da8309b924357bbd8a1f576face89f8390ad8cf))
    - some read/write tweaks for node ([`bb68a98`](https://github.com/maidsafe/safe_network/commit/bb68a98f5f29ecdd25d069aa57e5f49e57352445))
    - clippy and remove some unneccessary async ([`6cae687`](https://github.com/maidsafe/safe_network/commit/6cae68742e8b32105f87aeac463495b892d48397))
    - add RwLock to Node inside dispatcher ([`dfeb7b0`](https://github.com/maidsafe/safe_network/commit/dfeb7b0c8dfde25af6eac4374a2cf1691b321907))
    - generate the genesis DBC when launching first node and write it to disk ([`57f635f`](https://github.com/maidsafe/safe_network/commit/57f635fbe80392574f7f122a9d157fbb6320c4cc))
</details>

## v0.64.0 (2022-07-04)

<csr-id-9314a2db5dc1ae91bc4d80a65c1a8825492fc7c7/>
<csr-id-c09a983cf17fb41cd92c9f05b5605888f202af11/>
<csr-id-ddb7798a7b0c5e60960e123414277d58f3da27eb/>
<csr-id-2ab264744de8eeff8e26ff1423de32dadded688f/>
<csr-id-ea46890a1706fc72787a8251b6d289a075e8ad2b/>
<csr-id-871cd9e09bde31a7f99729fd1f7db1884e533037/>
<csr-id-bb1cf29c2ff5d5a4ff315cab3d1affe0efd30290/>
<csr-id-e03e5c87cdb46c74ba48ea3a2467f0193be7315b/>
<csr-id-a348b52a40f23040adfec51e70d5d8652636d4f9/>
<csr-id-c756918f09e742753b8686edf9472b15ec785abb/>
<csr-id-3fc8df35510464e9003f6619cd4b98a929d6648a/>
<csr-id-4e04a2b0acc79140bf1d0aefd82c0ad5b046a3cd/>
<csr-id-976e8c3d8c610d2a34c1bfa6678132a1bad234e8/>
<csr-id-849dfba283362d8fbdddd92be1078c3a963fb564/>
<csr-id-91da4d4ac7aab039853b0651e5aafd9cdd31b9c4/>
<csr-id-5f085f3765ab3156c74a4b7a7d7ab63a3bf6a670/>
<csr-id-e4e2eb56611a328806c59ed8bc80ca2567206bbb/>

### Chore

 - <csr-id-9314a2db5dc1ae91bc4d80a65c1a8825492fc7c7/> Docs - put symbols in backticks
 - <csr-id-c09a983cf17fb41cd92c9f05b5605888f202af11/> tweak join_timeout and bootstra_retry_time
   Aiming for 30s total to be in line with previous total.
   
   The larger total time of 15s per retry appears to be slowing down joins on CI a fair bit.
 - <csr-id-ddb7798a7b0c5e60960e123414277d58f3da27eb/> remove let bindings for unit returns
 - <csr-id-2ab264744de8eeff8e26ff1423de32dadded688f/> small cleanup tweaks to node bin
 - <csr-id-ea46890a1706fc72787a8251b6d289a075e8ad2b/> cleanup link code now that it doesn't make use of locking
 - <csr-id-871cd9e09bde31a7f99729fd1f7db1884e533037/> clippy
 - <csr-id-bb1cf29c2ff5d5a4ff315cab3d1affe0efd30290/> reenable reachability check
   Even if we're passing around comm, it's still
   a big improvement
 - <csr-id-e03e5c87cdb46c74ba48ea3a2467f0193be7315b/> remove Link locks
 - <csr-id-a348b52a40f23040adfec51e70d5d8652636d4f9/> clippy
 - <csr-id-c756918f09e742753b8686edf9472b15ec785abb/> temporarily disable reachability check on join
 - <csr-id-3fc8df35510464e9003f6619cd4b98a929d6648a/> move Comm out of node

 - <csr-id-4e04a2b0acc79140bf1d0aefd82c0ad5b046a3cd/> remove unused asyncs (clippy)
   Upon removing async keywords from
   sn_interface/src/network_knowledge/mod.rs a lot of removal propagated up
   and removed most of it with help of Clippy. Clippy does not yet detect
   unnecessary async in methods
   (https://github.com/rust-lang/rust-clippy/issues/9024), but will soon.
   
   With the help of a new Clippy lint:
   cargo clippy --all-targets --all-features -- -W clippy::unused_async
   And automatically fixing code with:
   cargo fix --broken-code --allow-dirty --all-targets --all-features
   
   Results mostly from the single thread work of @joshuef in #1253 (and
   ongoing efforts).

### Chore

 - <csr-id-e4e2eb56611a328806c59ed8bc80ca2567206bbb/> sn_interface-0.8.0/sn_dysfunction-0.7.0/sn_client-0.68.0/sn_node-0.64.0/sn_api-0.66.0/sn_cli-0.59.0

### New Features

 - <csr-id-26aa5b62742d09ad150db7565a4e7e694a9e2daa/> rewrite PeerSession to use a channel
 - <csr-id-5c8bbf5397ae83070644ccd6dace12efb720fa2a/> add Comm command delegation; add Regulate cmd

### Bug Fixes

 - <csr-id-cdec0693613008320c4dccc0753b2ef3ce82633c/> do not leave loop running on timeout
 - <csr-id-649e58a8608fb5a195160b56a29007cd3c578d57/> re-enable send job retries on transient errors
 - <csr-id-fe75d0575b215eaa29908783e6ee9b7daa6dc455/> rename SessionStatus::Terminate to Terminating
 - <csr-id-94fe2b5fa22390402a9ba6f55ce075bb3e34dcb5/> don't break out of watcher on transient errors

### Refactor

 - <csr-id-976e8c3d8c610d2a34c1bfa6678132a1bad234e8/> sn_cli uses NetworkPrefixMap instead of node_conn_info.config
 - <csr-id-849dfba283362d8fbdddd92be1078c3a963fb564/> update PrefixMap symlink if incorrect
 - <csr-id-91da4d4ac7aab039853b0651e5aafd9cdd31b9c4/> remove node_connection_info.config from sn_node, sn_interface, sn_client
 - <csr-id-5f085f3765ab3156c74a4b7a7d7ab63a3bf6a670/> remove NodeInfo in Node struct
   NodeInfo store a copy of our current socket address, which is
   available from `Comm`.
   
   Throughout our code we have to ask Comm for our current address and
   replace the copy in NodeInfo with the address from Comm.
   
   Next changes will hopefully remove more of our reliance on Comm inside
   of Node.

### New Features (BREAKING)

 - <csr-id-5dad80d3f239f5844243fedb89f8d4baaee3b640/> have the nodes to attach valid Commitments to signed SpentProofShares

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 24 commits contributed to the release over the course of 6 calendar days.
 - 6 days passed between releases.
 - 23 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.8.0/sn_dysfunction-0.7.0/sn_client-0.68.0/sn_node-0.64.0/sn_api-0.66.0/sn_cli-0.59.0 ([`e4e2eb5`](https://github.com/maidsafe/safe_network/commit/e4e2eb56611a328806c59ed8bc80ca2567206bbb))
    - sn_cli uses NetworkPrefixMap instead of node_conn_info.config ([`976e8c3`](https://github.com/maidsafe/safe_network/commit/976e8c3d8c610d2a34c1bfa6678132a1bad234e8))
    - update PrefixMap symlink if incorrect ([`849dfba`](https://github.com/maidsafe/safe_network/commit/849dfba283362d8fbdddd92be1078c3a963fb564))
    - remove node_connection_info.config from sn_node, sn_interface, sn_client ([`91da4d4`](https://github.com/maidsafe/safe_network/commit/91da4d4ac7aab039853b0651e5aafd9cdd31b9c4))
    - Docs - put symbols in backticks ([`9314a2d`](https://github.com/maidsafe/safe_network/commit/9314a2db5dc1ae91bc4d80a65c1a8825492fc7c7))
    - tweak join_timeout and bootstra_retry_time ([`c09a983`](https://github.com/maidsafe/safe_network/commit/c09a983cf17fb41cd92c9f05b5605888f202af11))
    - fix(join): do not leave loop running on timeout Fixes the case when a node start times out and a new node is started while the previous one is still looping in some task. This used to cause address already in use errors. Even though a random address would have solved that, it's better to ensure that the running node actually is dropped, before starting a new one. ([`cdec069`](https://github.com/maidsafe/safe_network/commit/cdec0693613008320c4dccc0753b2ef3ce82633c))
    - re-enable send job retries on transient errors ([`649e58a`](https://github.com/maidsafe/safe_network/commit/649e58a8608fb5a195160b56a29007cd3c578d57))
    - remove let bindings for unit returns ([`ddb7798`](https://github.com/maidsafe/safe_network/commit/ddb7798a7b0c5e60960e123414277d58f3da27eb))
    - have the nodes to attach valid Commitments to signed SpentProofShares ([`5dad80d`](https://github.com/maidsafe/safe_network/commit/5dad80d3f239f5844243fedb89f8d4baaee3b640))
    - small cleanup tweaks to node bin ([`2ab2647`](https://github.com/maidsafe/safe_network/commit/2ab264744de8eeff8e26ff1423de32dadded688f))
    - rename SessionStatus::Terminate to Terminating ([`fe75d05`](https://github.com/maidsafe/safe_network/commit/fe75d0575b215eaa29908783e6ee9b7daa6dc455))
    - cleanup link code now that it doesn't make use of locking ([`ea46890`](https://github.com/maidsafe/safe_network/commit/ea46890a1706fc72787a8251b6d289a075e8ad2b))
    - clippy ([`871cd9e`](https://github.com/maidsafe/safe_network/commit/871cd9e09bde31a7f99729fd1f7db1884e533037))
    - reenable reachability check ([`bb1cf29`](https://github.com/maidsafe/safe_network/commit/bb1cf29c2ff5d5a4ff315cab3d1affe0efd30290))
    - remove Link locks ([`e03e5c8`](https://github.com/maidsafe/safe_network/commit/e03e5c87cdb46c74ba48ea3a2467f0193be7315b))
    - don't break out of watcher on transient errors ([`94fe2b5`](https://github.com/maidsafe/safe_network/commit/94fe2b5fa22390402a9ba6f55ce075bb3e34dcb5))
    - clippy ([`a348b52`](https://github.com/maidsafe/safe_network/commit/a348b52a40f23040adfec51e70d5d8652636d4f9))
    - rewrite PeerSession to use a channel ([`26aa5b6`](https://github.com/maidsafe/safe_network/commit/26aa5b62742d09ad150db7565a4e7e694a9e2daa))
    - temporarily disable reachability check on join ([`c756918`](https://github.com/maidsafe/safe_network/commit/c756918f09e742753b8686edf9472b15ec785abb))
    - add Comm command delegation; add Regulate cmd ([`5c8bbf5`](https://github.com/maidsafe/safe_network/commit/5c8bbf5397ae83070644ccd6dace12efb720fa2a))
    - move Comm out of node ([`3fc8df3`](https://github.com/maidsafe/safe_network/commit/3fc8df35510464e9003f6619cd4b98a929d6648a))
    - remove unused asyncs (clippy) ([`4e04a2b`](https://github.com/maidsafe/safe_network/commit/4e04a2b0acc79140bf1d0aefd82c0ad5b046a3cd))
    - remove NodeInfo in Node struct ([`5f085f3`](https://github.com/maidsafe/safe_network/commit/5f085f3765ab3156c74a4b7a7d7ab63a3bf6a670))
</details>

## v0.63.1 (2022-06-28)

<csr-id-b5b833a18b2b0ec9a5083036ccb4c068be3f1d7b/>
<csr-id-8c69306dc86a99a8be443ab8213253983540f1cf/>
<csr-id-eebbc30f5dd449b786115c37813a4554309875e0/>
<csr-id-58890e5c919ada30f27d4e80c6b5e7291b99ed5c/>

### Chore

 - <csr-id-b5b833a18b2b0ec9a5083036ccb4c068be3f1d7b/> only log vote time if fresh vote

### Chore

 - <csr-id-58890e5c919ada30f27d4e80c6b5e7291b99ed5c/> sn_interface-0.7.1/sn_dysfunction-0.6.1/sn_client-0.67.1/sn_node-0.63.1

### New Features

 - <csr-id-44b93fde435214b363c009e555a2579bb3404e75/> use node's section_key and own key for register
 - <csr-id-6bfd101ed12a16f3f6a9a0b55252d45d200af7c6/> Select which adult to query
   Let the client pick the adult to query, based on the XOR distance.

### Bug Fixes

 - <csr-id-752824774884ef77616d26734517c58530cdae1f/> resend last vote if nothing received after an interval.
   We were seeing stalled membership, perhaps due to dropped packages. This means we don't rest
   and if after an interval we haven't seen anything new, we trigger nodes to resend their votes out, which
   should hopefully complete the current gen

### Refactor

 - <csr-id-8c69306dc86a99a8be443ab8213253983540f1cf/> Rename DataQuery with suffix Variant
   A new structure with the name DataQuery will be introduced that has common data for all these
   variants.

### Test

 - <csr-id-eebbc30f5dd449b786115c37813a4554309875e0/> adding new dysf test for DKG rounds

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release.
 - 2 days passed between releases.
 - 7 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.7.1/sn_dysfunction-0.6.1/sn_client-0.67.1/sn_node-0.63.1 ([`58890e5`](https://github.com/maidsafe/safe_network/commit/58890e5c919ada30f27d4e80c6b5e7291b99ed5c))
    - use node's section_key and own key for register ([`44b93fd`](https://github.com/maidsafe/safe_network/commit/44b93fde435214b363c009e555a2579bb3404e75))
    - only log vote time if fresh vote ([`b5b833a`](https://github.com/maidsafe/safe_network/commit/b5b833a18b2b0ec9a5083036ccb4c068be3f1d7b))
    - resend last vote if nothing received after an interval. ([`7528247`](https://github.com/maidsafe/safe_network/commit/752824774884ef77616d26734517c58530cdae1f))
    - Select which adult to query ([`6bfd101`](https://github.com/maidsafe/safe_network/commit/6bfd101ed12a16f3f6a9a0b55252d45d200af7c6))
    - Rename DataQuery with suffix Variant ([`8c69306`](https://github.com/maidsafe/safe_network/commit/8c69306dc86a99a8be443ab8213253983540f1cf))
    - adding new dysf test for DKG rounds ([`eebbc30`](https://github.com/maidsafe/safe_network/commit/eebbc30f5dd449b786115c37813a4554309875e0))
</details>

## v0.63.0 (2022-06-26)

<csr-id-243cfc48a7f4a9b60b5b7f1fdd609c02197aba5e/>

### Chore

 - <csr-id-243cfc48a7f4a9b60b5b7f1fdd609c02197aba5e/> sn_interface-0.7.0/sn_dysfunction-0.6.0/sn_client-0.67.0/sn_node-0.63.0/sn_api-0.65.0/sn_cli-0.58.0

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 1 commit contributed to the release.
 - 2 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.7.0/sn_dysfunction-0.6.0/sn_client-0.67.0/sn_node-0.63.0/sn_api-0.65.0/sn_cli-0.58.0 ([`243cfc4`](https://github.com/maidsafe/safe_network/commit/243cfc48a7f4a9b60b5b7f1fdd609c02197aba5e))
</details>

## v0.62.8 (2022-06-24)

<csr-id-d7a831329ad600ad7b5a1b6b68841f96b8ef8cfa/>
<csr-id-402fb2c13860d89f6c7e5aa0858974ccc75dc1ec/>
<csr-id-024ddf06fc517935f1a55af42b2aa1707a05f2e8/>
<csr-id-0a46f508d22141eb06717012fc4cc0b37c7f025f/>
<csr-id-1fbc762305a581680b52e2cbdaa7aea2feaf05ab/>
<csr-id-dc69a62eec590b2d621ab2cbc3009cb052955e66/>

### Chore

 - <csr-id-d7a831329ad600ad7b5a1b6b68841f96b8ef8cfa/> misc cleanup and fixes
 - <csr-id-402fb2c13860d89f6c7e5aa0858974ccc75dc1ec/> reduce dysfunction interval to 30s

### Chore

 - <csr-id-dc69a62eec590b2d621ab2cbc3009cb052955e66/> sn_interface-0.6.5/sn_dysfunction-0.5.3/sn_client-0.66.5/sn_node-0.62.8/sn_cli-0.57.6

### New Features

 - <csr-id-71eb46e47032074cdca678783e815b8d55ae39a0/> organize internal work

### Other

 - <csr-id-024ddf06fc517935f1a55af42b2aa1707a05f2e8/> remove register data_storage benchmark for now as sled db keeps erroring

### Refactor

 - <csr-id-0a46f508d22141eb06717012fc4cc0b37c7f025f/> improve efficiency of load monitoring
 - <csr-id-1fbc762305a581680b52e2cbdaa7aea2feaf05ab/> move it to its own file

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 14 commits contributed to the release over the course of 3 calendar days.
 - 3 days passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.5/sn_dysfunction-0.5.3/sn_client-0.66.5/sn_node-0.62.8/sn_cli-0.57.6 ([`dc69a62`](https://github.com/maidsafe/safe_network/commit/dc69a62eec590b2d621ab2cbc3009cb052955e66))
    - Merge #1266 ([`366be4d`](https://github.com/maidsafe/safe_network/commit/366be4d3ddc39f32beea0e26d0addd161acc90c2))
    - chore(misc): misc cleanup and fixes - Complete `msg_kind` => `auth_kind` renaming. - Fix broken `routing_stress` startup. - Clarify context of `HandleTimeout` and `ScheduleTimeout` by inserting `Dkg`. - Tweak `network_split` example. - Set various things, such as payload debug, under `test-utils` flag. - Fix comments/logs: the opposite group of `full` adults are `non-full`, not `empty`. ([`d7a8313`](https://github.com/maidsafe/safe_network/commit/d7a831329ad600ad7b5a1b6b68841f96b8ef8cfa))
    - Merge #1261 ([`2415f16`](https://github.com/maidsafe/safe_network/commit/2415f169917f101459ec6273375dc5e2cbbd06d4))
    - feat(flow_control): organize internal work - Organizes internal work so that internal cmds (work) are now dealt with according to priority. - Enables adaptive throughput of cmds. - Prepares for logging of cmds separately (future feat). ([`71eb46e`](https://github.com/maidsafe/safe_network/commit/71eb46e47032074cdca678783e815b8d55ae39a0))
    - Merge #1264 ([`7f4f4cb`](https://github.com/maidsafe/safe_network/commit/7f4f4cb0b1664c2d6f30962de25d5fdcbc5074de))
    - reduce dysfunction interval to 30s ([`402fb2c`](https://github.com/maidsafe/safe_network/commit/402fb2c13860d89f6c7e5aa0858974ccc75dc1ec))
    - Merge #1255 #1258 ([`ed0b5d8`](https://github.com/maidsafe/safe_network/commit/ed0b5d890e8404a59c25f8131eab5d23ce12eb7d))
    - Merge #1257 #1260 ([`19d89df`](https://github.com/maidsafe/safe_network/commit/19d89dfbbf8ac8ab2b08380ce9b4bed58a5dc0d9))
    - refactor: improve efficiency of load monitoring Refactors load monitoring so that it is more efficiently used, and for both outgoing msgs and (in coming commit) cmds. ([`0a46f50`](https://github.com/maidsafe/safe_network/commit/0a46f508d22141eb06717012fc4cc0b37c7f025f))
    - refactor(msg_type):  move it to its own file - Moves priority fns to service- and system msg. - Moves deserialise of payload to wire_msg fn when getting priority. ([`1fbc762`](https://github.com/maidsafe/safe_network/commit/1fbc762305a581680b52e2cbdaa7aea2feaf05ab))
    - remove register data_storage benchmark for now as sled db keeps erroring ([`024ddf0`](https://github.com/maidsafe/safe_network/commit/024ddf06fc517935f1a55af42b2aa1707a05f2e8))
    - Merge #1256 ([`cff8b33`](https://github.com/maidsafe/safe_network/commit/cff8b337be20f3e1c0cddc5464c2eee0c8cc9e1c))
    - Merge branch 'main' into refactor-event-channel ([`024883e`](https://github.com/maidsafe/safe_network/commit/024883e9a1b853c02c29daa5c447b03570af2473))
</details>

## v0.62.7 (2022-06-21)

<csr-id-fd7f845f7d1534cf9ff93ee9dc9f3009ab7e5664/>
<csr-id-c038635cf88d32c52da89d11a8532e6c91c8bf38/>
<csr-id-d8730f1bbd276b0686c68b00714c54ed86b7241a/>
<csr-id-d26d26df6ddd0321555fa3653be966fe91e2dca4/>
<csr-id-d526e0a32d3f09a788899d82db4fe6f13258568c/>

### Chore

 - <csr-id-fd7f845f7d1534cf9ff93ee9dc9f3009ab7e5664/> make NetworkKnowledge single threaded
 - <csr-id-c038635cf88d32c52da89d11a8532e6c91c8bf38/> misc cleanup

### Refactor

 - <csr-id-d26d26df6ddd0321555fa3653be966fe91e2dca4/> cleanup and restructure of enum

### Chore

 - <csr-id-d526e0a32d3f09a788899d82db4fe6f13258568c/> sn_interface-0.6.4/sn_dysfunction-0.5.2/sn_client-0.66.4/sn_node-0.62.7/sn_api-0.64.4

### New Features

 - <csr-id-a68caa7bb6e998b52f052e322f4485c4b836939c/> change to single threaded runtime by default

### Bug Fixes

 - <csr-id-d5c65440b2152cf570a2014eee102b353678af00/> routing_stress example compiles once more

### Other

 - <csr-id-d8730f1bbd276b0686c68b00714c54ed86b7241a/> add LocalSet to node test runs

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 8 commits contributed to the release.
 - 2 days passed between releases.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.4/sn_dysfunction-0.5.2/sn_client-0.66.4/sn_node-0.62.7/sn_api-0.64.4 ([`d526e0a`](https://github.com/maidsafe/safe_network/commit/d526e0a32d3f09a788899d82db4fe6f13258568c))
    - refactor(events): cleanup and restructure of enum  - Initiates the use of the node event channel for more structured logging. BREAKING CHANGE: events renamed and restructured ([`d26d26d`](https://github.com/maidsafe/safe_network/commit/d26d26df6ddd0321555fa3653be966fe91e2dca4))
    - Merge #1253 ([`abc95c1`](https://github.com/maidsafe/safe_network/commit/abc95c1093479938a5efbef279190540156ee23a))
    - routing_stress example compiles once more ([`d5c6544`](https://github.com/maidsafe/safe_network/commit/d5c65440b2152cf570a2014eee102b353678af00))
    - add LocalSet to node test runs ([`d8730f1`](https://github.com/maidsafe/safe_network/commit/d8730f1bbd276b0686c68b00714c54ed86b7241a))
    - make NetworkKnowledge single threaded ([`fd7f845`](https://github.com/maidsafe/safe_network/commit/fd7f845f7d1534cf9ff93ee9dc9f3009ab7e5664))
    - change to single threaded runtime by default ([`a68caa7`](https://github.com/maidsafe/safe_network/commit/a68caa7bb6e998b52f052e322f4485c4b836939c))
    - chore: misc cleanup - Organise usings - Add missing license headers - Update license years As it would take too long to go through all files, a partial cleanup of the code base is made here. It is based on where the using of `sn_interface` has been introduced, as it was a low hanging fruit to cover many occurrences of duplication in many files. ([`c038635`](https://github.com/maidsafe/safe_network/commit/c038635cf88d32c52da89d11a8532e6c91c8bf38))
</details>

## v0.62.6 (2022-06-18)

<csr-id-e4b43ce9a8655cc8deea8d459ac0a0755d9153c1/>
<csr-id-eda5d22e57779a7d1ecab1707f01bccd5f94706d/>

### Chore

 - <csr-id-e4b43ce9a8655cc8deea8d459ac0a0755d9153c1/> reorder nodeacceptance cmds to inform node first of all

### Chore

 - <csr-id-eda5d22e57779a7d1ecab1707f01bccd5f94706d/> sn_node-0.62.6/sn_cli-0.57.5

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.62.6/sn_cli-0.57.5 ([`eda5d22`](https://github.com/maidsafe/safe_network/commit/eda5d22e57779a7d1ecab1707f01bccd5f94706d))
    - Merge #1248 ([`1fad3d5`](https://github.com/maidsafe/safe_network/commit/1fad3d530f1e38544197672639e029a13d3e2207))
    - reorder nodeacceptance cmds to inform node first of all ([`e4b43ce`](https://github.com/maidsafe/safe_network/commit/e4b43ce9a8655cc8deea8d459ac0a0755d9153c1))
</details>

## v0.62.5 (2022-06-16)

<csr-id-f599c5973d50324aad1720166156666d5db1ed3d/>
<csr-id-4eb43fa884d7b047febb18c067ae905969a113bf/>
<csr-id-83fa804ed62f30344dda48cef7ad852a4ac4118b/>
<csr-id-fa709fb9fea7926048525e5a2f21ff0017676f41/>
<csr-id-e195c2504bee67fdc1ebbbbef9b3b4428ee8a32d/>
<csr-id-ef5cb8c050cec82d215dcf98aeb0dc237cf1b574/>
<csr-id-3a6d83bd9d406b824b538ee7de7da95119f536da/>
<csr-id-9679f0cde6f4e5a2b1fbf2fded954f17b243e518/>

### Chore

 - <csr-id-f599c5973d50324aad1720166156666d5db1ed3d/> sn_interface-0.6.3/sn_dysfunction-0.5.1/sn_client-0.66.3/sn_api-0.64.3/sn_cli-0.57.4
 - <csr-id-4eb43fa884d7b047febb18c067ae905969a113bf/> upgrade blsttc to 6.0.0
   There were various other crates that had to be upgraded in this process:
   * secured_linked_list to v0.5.2 because it was also upgraded to reference v6.0.0 of blsttc
   * bls_dkg to v0.10.3 because it was also upgraded to reference v6.0.0 of blsttc
   * sn_consensus to v2.1.1 because it was also upgraded to reference v6.0.0 of blsttc
   * sn_dbc to v4.0.0 because it was also upgraded to reference v6.0.0 of blsttc
 - <csr-id-83fa804ed62f30344dda48cef7ad852a4ac4118b/> increase dysfunction check interval
 - <csr-id-fa709fb9fea7926048525e5a2f21ff0017676f41/> log dkg dysfunction in node
 - <csr-id-e195c2504bee67fdc1ebbbbef9b3b4428ee8a32d/> add VotedOffline LogMarker
 - <csr-id-ef5cb8c050cec82d215dcf98aeb0dc237cf1b574/> update cargo deps across the baord
 - <csr-id-3a6d83bd9d406b824b538ee7de7da95119f536da/> use DashMap in comms to avoid RwLocks

### Chore

 - <csr-id-9679f0cde6f4e5a2b1fbf2fded954f17b243e518/> sn_node-0.62.5

### New Features

 - <csr-id-7ccb02a7ded7579bb8645c918b9a6108b1b585af/> enable tracking of Dkg issues

### Bug Fixes

 - <csr-id-38f82ee4dbf65a9bd94411ae56caae6a7296e129/> dont error out of node run thread entirely, just log issues
   This allows us to keep looping and attempting to rejoin as a new node

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 12 commits contributed to the release over the course of 1 calendar day.
 - 10 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.62.5 ([`9679f0c`](https://github.com/maidsafe/safe_network/commit/9679f0cde6f4e5a2b1fbf2fded954f17b243e518))
    - sn_interface-0.6.3/sn_dysfunction-0.5.1/sn_client-0.66.3/sn_api-0.64.3/sn_cli-0.57.4 ([`f599c59`](https://github.com/maidsafe/safe_network/commit/f599c5973d50324aad1720166156666d5db1ed3d))
    - Merge #1241 ([`f9c7544`](https://github.com/maidsafe/safe_network/commit/f9c7544f369e15fb3b6f91158ac3277656737fa4))
    - upgrade blsttc to 6.0.0 ([`4eb43fa`](https://github.com/maidsafe/safe_network/commit/4eb43fa884d7b047febb18c067ae905969a113bf))
    - Merge #1234 ([`05b9b75`](https://github.com/maidsafe/safe_network/commit/05b9b755165304c282cc415419030eee8b6a3636))
    - increase dysfunction check interval ([`83fa804`](https://github.com/maidsafe/safe_network/commit/83fa804ed62f30344dda48cef7ad852a4ac4118b))
    - log dkg dysfunction in node ([`fa709fb`](https://github.com/maidsafe/safe_network/commit/fa709fb9fea7926048525e5a2f21ff0017676f41))
    - add VotedOffline LogMarker ([`e195c25`](https://github.com/maidsafe/safe_network/commit/e195c2504bee67fdc1ebbbbef9b3b4428ee8a32d))
    - enable tracking of Dkg issues ([`7ccb02a`](https://github.com/maidsafe/safe_network/commit/7ccb02a7ded7579bb8645c918b9a6108b1b585af))
    - update cargo deps across the baord ([`ef5cb8c`](https://github.com/maidsafe/safe_network/commit/ef5cb8c050cec82d215dcf98aeb0dc237cf1b574))
    - dont error out of node run thread entirely, just log issues ([`38f82ee`](https://github.com/maidsafe/safe_network/commit/38f82ee4dbf65a9bd94411ae56caae6a7296e129))
    - use DashMap in comms to avoid RwLocks ([`3a6d83b`](https://github.com/maidsafe/safe_network/commit/3a6d83bd9d406b824b538ee7de7da95119f536da))
</details>

## v0.62.4 (2022-06-15)

<csr-id-08ea69a0084b0e8d8aea51f06b99af426a7255c9/>
<csr-id-46246f155ab65f3fcd61381345f1a7f747dfe957/>
<csr-id-4eb43fa884d7b047febb18c067ae905969a113bf/>
<csr-id-83fa804ed62f30344dda48cef7ad852a4ac4118b/>
<csr-id-fa709fb9fea7926048525e5a2f21ff0017676f41/>
<csr-id-e195c2504bee67fdc1ebbbbef9b3b4428ee8a32d/>
<csr-id-ef5cb8c050cec82d215dcf98aeb0dc237cf1b574/>
<csr-id-3a6d83bd9d406b824b538ee7de7da95119f536da/>

### Chore

 - <csr-id-08ea69a0084b0e8d8aea51f06b99af426a7255c9/> BREAKING CHANGE: removed private scope

### Chore

 - <csr-id-46246f155ab65f3fcd61381345f1a7f747dfe957/> sn_interface-0.6.2/sn_client-0.66.2/sn_node-0.62.4/sn_api-0.64.2/sn_cli-0.57.3
 - <csr-id-4eb43fa884d7b047febb18c067ae905969a113bf/> upgrade blsttc to 6.0.0
   There were various other crates that had to be upgraded in this process:
   * secured_linked_list to v0.5.2 because it was also upgraded to reference v6.0.0 of blsttc
   * bls_dkg to v0.10.3 because it was also upgraded to reference v6.0.0 of blsttc
   * sn_consensus to v2.1.1 because it was also upgraded to reference v6.0.0 of blsttc
   * sn_dbc to v4.0.0 because it was also upgraded to reference v6.0.0 of blsttc
 - <csr-id-83fa804ed62f30344dda48cef7ad852a4ac4118b/> increase dysfunction check interval
 - <csr-id-fa709fb9fea7926048525e5a2f21ff0017676f41/> log dkg dysfunction in node
 - <csr-id-e195c2504bee67fdc1ebbbbef9b3b4428ee8a32d/> add VotedOffline LogMarker
 - <csr-id-ef5cb8c050cec82d215dcf98aeb0dc237cf1b574/> update cargo deps across the baord
 - <csr-id-3a6d83bd9d406b824b538ee7de7da95119f536da/> use DashMap in comms to avoid RwLocks

### New Features

 - <csr-id-1b1cb77df6c2805ecfa741bb824b359214558929/> remove private registers
 - <csr-id-7ccb02a7ded7579bb8645c918b9a6108b1b585af/> enable tracking of Dkg issues

### Bug Fixes

 - <csr-id-60f5a68a1df6114b65d7c57099fea0347ba3d1dd/> some changes I missed in the initial private removal
 - <csr-id-38f82ee4dbf65a9bd94411ae56caae6a7296e129/> dont error out of node run thread entirely, just log issues
   This allows us to keep looping and attempting to rejoin as a new node

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 1 calendar day.
 - 4 days passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.2/sn_client-0.66.2/sn_node-0.62.4/sn_api-0.64.2/sn_cli-0.57.3 ([`46246f1`](https://github.com/maidsafe/safe_network/commit/46246f155ab65f3fcd61381345f1a7f747dfe957))
    - Merge #1216 ([`9877101`](https://github.com/maidsafe/safe_network/commit/9877101c74dcf75d78520a804cb6f2b7aaddaffb))
    - BREAKING CHANGE: removed private scope ([`08ea69a`](https://github.com/maidsafe/safe_network/commit/08ea69a0084b0e8d8aea51f06b99af426a7255c9))
    - some changes I missed in the initial private removal ([`60f5a68`](https://github.com/maidsafe/safe_network/commit/60f5a68a1df6114b65d7c57099fea0347ba3d1dd))
    - remove private registers ([`1b1cb77`](https://github.com/maidsafe/safe_network/commit/1b1cb77df6c2805ecfa741bb824b359214558929))
</details>

## v0.62.3 (2022-06-11)

<csr-id-fe2010fa66622cfcb52325ad6139bb1bf1783251/>
<csr-id-537767d0d33d3ba9ee530863761c7c3df102d00b/>

### Chore

 - <csr-id-fe2010fa66622cfcb52325ad6139bb1bf1783251/> add basic chaos to node startup
   random crashes to ensure that the node startup looping is in effect

### Chore

 - <csr-id-537767d0d33d3ba9ee530863761c7c3df102d00b/> sn_node-0.62.3

### Bug Fixes

 - <csr-id-155d66436b1cdb3c1520c17f0e827124f9e6cac5/> Node init and restart on error
   Previously an error from the node was not triggering the restart of the
   node instance. (eg, ChurnJoinMiss).
   
   Here, the node bin start process has been tweaked. Kepeing all
   node processes within the sn_node spawned thread. But, each 'node'
   with its own runtime, to aid in shutting down any background processes
   between restarts.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.62.3 ([`537767d`](https://github.com/maidsafe/safe_network/commit/537767d0d33d3ba9ee530863761c7c3df102d00b))
    - add basic chaos to node startup ([`fe2010f`](https://github.com/maidsafe/safe_network/commit/fe2010fa66622cfcb52325ad6139bb1bf1783251))
    - Node init and restart on error ([`155d664`](https://github.com/maidsafe/safe_network/commit/155d66436b1cdb3c1520c17f0e827124f9e6cac5))
</details>

## v0.62.2 (2022-06-10)

<csr-id-aa05d4eac37ebf6969275555e787dd81f5f65de7/>
<csr-id-6253530bf609e214de3a04433dcc260aa71721e0/>

### Chore

 - <csr-id-aa05d4eac37ebf6969275555e787dd81f5f65de7/> cleanup unneeded logs

### Chore

 - <csr-id-6253530bf609e214de3a04433dcc260aa71721e0/> sn_node-0.62.2

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.62.2 ([`6253530`](https://github.com/maidsafe/safe_network/commit/6253530bf609e214de3a04433dcc260aa71721e0))
    - Merge #1220 ([`8c7e42e`](https://github.com/maidsafe/safe_network/commit/8c7e42e9a91f803579426c5c5fcef14ace10fea0))
    - cleanup unneeded logs ([`aa05d4e`](https://github.com/maidsafe/safe_network/commit/aa05d4eac37ebf6969275555e787dd81f5f65de7))
</details>

## v0.62.1 (2022-06-07)

<csr-id-a8aa4e749670db3a930f2351ac46b5dd7d72041e/>
<csr-id-d4088d4c5d26904187fa8b08fa6a9c9f8b04c5b2/>
<csr-id-489904e325cfb8efca4289b05125904ad4029f3b/>

### Chore

 - <csr-id-a8aa4e749670db3a930f2351ac46b5dd7d72041e/> resolve issue after rebase
 - <csr-id-d4088d4c5d26904187fa8b08fa6a9c9f8b04c5b2/> remove accidental log commit

### Chore

 - <csr-id-489904e325cfb8efca4289b05125904ad4029f3b/> sn_interface-0.6.1/sn_client-0.66.1/sn_node-0.62.1/sn_api-0.64.1

### New Features

 - <csr-id-dbda86be03f912079776be514828ff5fd034830c/> first version of Spentbook messaging, storage, and client API
   - Storage is implemented using Register as the underlying data type. To be changed when
   actual SpentBook native data type is put in place.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.1/sn_client-0.66.1/sn_node-0.62.1/sn_api-0.64.1 ([`489904e`](https://github.com/maidsafe/safe_network/commit/489904e325cfb8efca4289b05125904ad4029f3b))
    - Merge #1214 ([`992c495`](https://github.com/maidsafe/safe_network/commit/992c4951670afc769feea7e6cd38db021aed88a7))
    - Merge branch 'main' into Gabriel_Spentbook_PR1143 ([`0eda02a`](https://github.com/maidsafe/safe_network/commit/0eda02ac126be4f088af6bf9e7247c8496a389ba))
    - resolve issue after rebase ([`a8aa4e7`](https://github.com/maidsafe/safe_network/commit/a8aa4e749670db3a930f2351ac46b5dd7d72041e))
    - first version of Spentbook messaging, storage, and client API ([`dbda86b`](https://github.com/maidsafe/safe_network/commit/dbda86be03f912079776be514828ff5fd034830c))
    - remove accidental log commit ([`d4088d4`](https://github.com/maidsafe/safe_network/commit/d4088d4c5d26904187fa8b08fa6a9c9f8b04c5b2))
    - Merge #1217 ([`2f26043`](https://github.com/maidsafe/safe_network/commit/2f2604325d533357bad7d917315cf4cba0b2d3c0))
</details>

## v0.62.0 (2022-06-05)

<csr-id-1bf7dfb3ce8b14cbed7a4a8ed98c8310653a2da9/>
<csr-id-ebb8f26a394f595b725fdf52e1a588c1f8136562/>

### New Features

 - <csr-id-95de2ffe6f57ae0e6cebf123da3e9b6c3ad84aaf/> handover sap elder checks with membership knowledge

### Chore

 - <csr-id-1bf7dfb3ce8b14cbed7a4a8ed98c8310653a2da9/> sn_interface-0.6.0/sn_dysfunction-0.5.0/sn_client-0.66.0/sn_node-0.62.0/sn_api-0.64.0/sn_cli-0.57.0
 - <csr-id-ebb8f26a394f595b725fdf52e1a588c1f8136562/> improve error msg and style

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release over the course of 1 calendar day.
 - 3 days passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.0/sn_dysfunction-0.5.0/sn_client-0.66.0/sn_node-0.62.0/sn_api-0.64.0/sn_cli-0.57.0 ([`1bf7dfb`](https://github.com/maidsafe/safe_network/commit/1bf7dfb3ce8b14cbed7a4a8ed98c8310653a2da9))
    - improve error msg and style ([`ebb8f26`](https://github.com/maidsafe/safe_network/commit/ebb8f26a394f595b725fdf52e1a588c1f8136562))
    - Merge branch 'main' into handover_byz_sap_check_squashed ([`6769996`](https://github.com/maidsafe/safe_network/commit/6769996e3ea78a6be306437193687b422a21ce80))
    - handover sap elder checks with membership knowledge ([`95de2ff`](https://github.com/maidsafe/safe_network/commit/95de2ffe6f57ae0e6cebf123da3e9b6c3ad84aaf))
</details>

## v0.61.1 (2022-06-01)

<csr-id-6f32e0363546fd2e39fefc78fff68ff99be04fea/>

### Chore

 - <csr-id-6f32e0363546fd2e39fefc78fff68ff99be04fea/> sn_node-0.61.1

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 5 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.61.1 ([`6f32e03`](https://github.com/maidsafe/safe_network/commit/6f32e0363546fd2e39fefc78fff68ff99be04fea))
    - Merge #1192 ([`f9fc2a7`](https://github.com/maidsafe/safe_network/commit/f9fc2a76f083ba5161c8c4eef9013c53586b4693))
</details>

## v0.61.0 (2022-05-27)

<csr-id-e5fcd032e1dd904e05bc23e119af1d06e3b85a06/>
<csr-id-05fcc31c13a2a09da83027655bd8feb1e31660d1/>
<csr-id-f82864be6f72876dc5ad5d38ca4cc104fbdcdde7/>
<csr-id-b9e5db241f437f9bb8fd03ca9080a0331757b9a5/>
<csr-id-f9700e3b6bb8b2b9949f33d627c99974c355ca2b/>
<csr-id-7544945e15e3df18d5bf666ab7c8fbcee0766d06/>
<csr-id-ea9906fda233cc1299c64d703267fb3b250364c2/>
<csr-id-14c92fb0f18fc40176963ca5290914442d340256/>
<csr-id-f65a35128c4b4fa76bc97c089f313633a8e43f79/>

### Chore

 - <csr-id-e5fcd032e1dd904e05bc23e119af1d06e3b85a06/> sn_interface-0.5.0/sn_dysfunction-0.4.0/sn_client-0.65.0/sn_node-0.61.0/sn_api-0.63.0/sn_cli-0.56.0
 - <csr-id-05fcc31c13a2a09da83027655bd8feb1e31660d1/> explicitly drop Node on join retry
   Add more logs to aid debugging
 - <csr-id-f82864be6f72876dc5ad5d38ca4cc104fbdcdde7/> improve data storage benches, more runs, more data
 - <csr-id-b9e5db241f437f9bb8fd03ca9080a0331757b9a5/> cleanup of churn test
 - <csr-id-f9700e3b6bb8b2b9949f33d627c99974c355ca2b/> split out dispatcher periodic code
   Just clean up the dispatcher file, splitting
   out all the periodic checks etc into their
   own file, leaving the bones of dispatcher in one
   place
 - <csr-id-7544945e15e3df18d5bf666ab7c8fbcee0766d06/> cleanup commented code
 - <csr-id-ea9906fda233cc1299c64d703267fb3b250364c2/> dont cleanup section elder PeerLinks

### New Features

 - <csr-id-0c449a731b22eb25e616d83182899e12aba95d7f/> handover AE, empty consensus handling, generations

### Bug Fixes

 - <csr-id-77d962abb97f8b00e9295419079b43224ca67341/> shutdown runtime in node loop
   this should hopefully shutdown any bg processes running which may be blocking ports on reconnect
 - <csr-id-5b6a88a017988b7f4b10534167e1f67b3d2b0e71/> remove avoidable error case
 - <csr-id-0e51f043cf7c7fa117c433de41c6c66213cb1442/> update DstLocation for all send_msg in node/comms

### Other

 - <csr-id-14c92fb0f18fc40176963ca5290914442d340256/> add more intesive churn data integrity test
   The network split test doesnt cover the basic 'new nodes added' membership case. This churn test now does that.

### Test

 - <csr-id-f65a35128c4b4fa76bc97c089f313633a8e43f79/> unit test for empty set decision

### New Features (BREAKING)

 - <csr-id-294549ebc998d11a2f3621e2a9fd20a0dd9bcce5/> remove sus node flows, replicate data per data

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 20 commits contributed to the release over the course of 2 calendar days.
 - 2 days passed between releases.
 - 14 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.5.0/sn_dysfunction-0.4.0/sn_client-0.65.0/sn_node-0.61.0/sn_api-0.63.0/sn_cli-0.56.0 ([`e5fcd03`](https://github.com/maidsafe/safe_network/commit/e5fcd032e1dd904e05bc23e119af1d06e3b85a06))
    - shutdown runtime in node loop ([`77d962a`](https://github.com/maidsafe/safe_network/commit/77d962abb97f8b00e9295419079b43224ca67341))
    - Merge branch 'main' into Chore-ImproveDataStorageBench ([`9904a7a`](https://github.com/maidsafe/safe_network/commit/9904a7a3b53a81248379c44bf4e88688d278582f))
    - Merge #1210 ([`c08fbb9`](https://github.com/maidsafe/safe_network/commit/c08fbb94e9306a00cdd24db9be73f903cb1f3362))
    - explicitly drop Node on join retry ([`05fcc31`](https://github.com/maidsafe/safe_network/commit/05fcc31c13a2a09da83027655bd8feb1e31660d1))
    - improve data storage benches, more runs, more data ([`f82864b`](https://github.com/maidsafe/safe_network/commit/f82864be6f72876dc5ad5d38ca4cc104fbdcdde7))
    - Merge #1202 ([`e42a2e3`](https://github.com/maidsafe/safe_network/commit/e42a2e3c212597e68238451a5bb4a8725c4761be))
    - unit test for empty set decision ([`f65a351`](https://github.com/maidsafe/safe_network/commit/f65a35128c4b4fa76bc97c089f313633a8e43f79))
    - remove avoidable error case ([`5b6a88a`](https://github.com/maidsafe/safe_network/commit/5b6a88a017988b7f4b10534167e1f67b3d2b0e71))
    - handover AE, empty consensus handling, generations ([`0c449a7`](https://github.com/maidsafe/safe_network/commit/0c449a731b22eb25e616d83182899e12aba95d7f))
    - Merge #1208 ([`6c9b851`](https://github.com/maidsafe/safe_network/commit/6c9b851dd5bab8b2f5d9b3ef1db72d198706ac9d))
    - cleanup of churn test ([`b9e5db2`](https://github.com/maidsafe/safe_network/commit/b9e5db241f437f9bb8fd03ca9080a0331757b9a5))
    - update DstLocation for all send_msg in node/comms ([`0e51f04`](https://github.com/maidsafe/safe_network/commit/0e51f043cf7c7fa117c433de41c6c66213cb1442))
    - add more intesive churn data integrity test ([`14c92fb`](https://github.com/maidsafe/safe_network/commit/14c92fb0f18fc40176963ca5290914442d340256))
    - split out dispatcher periodic code ([`f9700e3`](https://github.com/maidsafe/safe_network/commit/f9700e3b6bb8b2b9949f33d627c99974c355ca2b))
    - remove sus node flows, replicate data per data ([`294549e`](https://github.com/maidsafe/safe_network/commit/294549ebc998d11a2f3621e2a9fd20a0dd9bcce5))
    - Merge #1203 ([`cd32ca6`](https://github.com/maidsafe/safe_network/commit/cd32ca6535b17aedacfb4051e97e4b3540bb8a71))
    - Merge branch 'main' into bump-consensus-2.0.0 ([`a1c592a`](https://github.com/maidsafe/safe_network/commit/a1c592a71247660e7372e019e5f9a6ea23299e0f))
    - cleanup commented code ([`7544945`](https://github.com/maidsafe/safe_network/commit/7544945e15e3df18d5bf666ab7c8fbcee0766d06))
    - dont cleanup section elder PeerLinks ([`ea9906f`](https://github.com/maidsafe/safe_network/commit/ea9906fda233cc1299c64d703267fb3b250364c2))
</details>

## v0.60.0 (2022-05-25)

<csr-id-ef56cf9cf8de45a9f13c2510c63de245b12aeae8/>
<csr-id-392e522c69803fddbeb3cd9e1cbae8060188578f/>

### Chore

 - <csr-id-ef56cf9cf8de45a9f13c2510c63de245b12aeae8/> sn_interface-0.4.0/sn_dysfunction-0.3.0/sn_client-0.64.0/sn_node-0.60.0/sn_api-0.62.0/sn_cli-0.55.0
 - <csr-id-392e522c69803fddbeb3cd9e1cbae8060188578f/> bump consensus 1.16.0 -> 2.0.0

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 3 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.4.0/sn_dysfunction-0.3.0/sn_client-0.64.0/sn_node-0.60.0/sn_api-0.62.0/sn_cli-0.55.0 ([`ef56cf9`](https://github.com/maidsafe/safe_network/commit/ef56cf9cf8de45a9f13c2510c63de245b12aeae8))
    - bump consensus 1.16.0 -> 2.0.0 ([`392e522`](https://github.com/maidsafe/safe_network/commit/392e522c69803fddbeb3cd9e1cbae8060188578f))
    - Merge #1195 ([`c6e6e32`](https://github.com/maidsafe/safe_network/commit/c6e6e324164028c6c15a78643783a9f86679f39e))
</details>

## v0.59.0 (2022-05-21)

<csr-id-cf21d66b9b726123e0a4320cd68481b67f7af03d/>
<csr-id-48ffebf35093b68923d2139f12fc946d94ae4f4a/>
<csr-id-7aad3458b8efc7b01a6c97280b3fb59c45dd0228/>
<csr-id-cf1c11ab576810cc6e063331def6f3a9a95fe663/>
<csr-id-0a7adcab104dcda7baed80e2dd96913d6f93e541/>
<csr-id-9b06304f46e1a1bda90a0fc6ff82edc928c2529d/>
<csr-id-07504faeda6cbfd0b27abea25facde992398ecf9/>
<csr-id-8e2731d8b7923a9050451b31ef3a92f892d2d6d3/>
<csr-id-f2742d92b3c3b56ed80732aa1d6943885fcd4317/>
<csr-id-cb733fd4b1ed642da73f1e9db4fc3d8a1ec49a2b/>

### Chore

 - <csr-id-cf21d66b9b726123e0a4320cd68481b67f7af03d/> sn_dysfunction-0.2.0/sn_client-0.63.0/sn_node-0.59.0/sn_api-0.61.0/sn_cli-0.54.0
 - <csr-id-48ffebf35093b68923d2139f12fc946d94ae4f4a/> clippy after rebase
 - <csr-id-7aad3458b8efc7b01a6c97280b3fb59c45dd0228/> make section probe interval 5mins
 - <csr-id-cf1c11ab576810cc6e063331def6f3a9a95fe663/> cleanup unused import
 - <csr-id-0a7adcab104dcda7baed80e2dd96913d6f93e541/> benchmarking data storage
   Adds basic, 1 key data storage benchmarks.
   
   Currently this fails for registers...
 - <csr-id-9b06304f46e1a1bda90a0fc6ff82edc928c2529d/> sn_interface-0.2.4/sn_client-0.62.3/sn_api-0.60.2/sn_cli-0.53.1
 - <csr-id-07504faeda6cbfd0b27abea25facde992398ecf9/> upgrade blsttc to v5.2.0 and rand to v0.8

### New Features

 - <csr-id-910e1078b6db0cd740547ea1ad92592d3365a616/> rejoins through events
 - <csr-id-fa0d4036d86ca78956b79b22154d076dc850121b/> Revert "feat: node in an Arc Rwlock and factor out join code"
   This reverts commit e0b0a83946cec21bbb8bd8e98513c326009d0bc4.
 - <csr-id-2c12b4993f06c9c696475404e37f4e080d01a3b0/> node in an Arc Rwlock and factor out join code
 - <csr-id-941703e23a53d8d894d5a9a7a253ad1735e900e0/> error triggering on churn join miss
 - <csr-id-fe073bc0674c2099b7cd3f30ac744ea6172e24c2/> section probing for all nodes every 120s

### Bug Fixes

 - <csr-id-06591a11458adb5cfd917cc1239371acf4f8834f/> prevent deadlock in lru cache impl.
   We were locking over the queue, and then attempting to purge the queue
   within the self.priority() func, which required a lock

### Refactor

 - <csr-id-8e2731d8b7923a9050451b31ef3a92f892d2d6d3/> de-dupe init_test_logger
 - <csr-id-f2742d92b3c3b56ed80732aa1d6943885fcd4317/> cargo test works without feature flag
 - <csr-id-cb733fd4b1ed642da73f1e9db4fc3d8a1ec49a2b/> move NodeState validations to NodeState struct

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 21 commits contributed to the release over the course of 4 calendar days.
 - 4 days passed between releases.
 - 16 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_dysfunction-0.2.0/sn_client-0.63.0/sn_node-0.59.0/sn_api-0.61.0/sn_cli-0.54.0 ([`cf21d66`](https://github.com/maidsafe/safe_network/commit/cf21d66b9b726123e0a4320cd68481b67f7af03d))
    - de-dupe init_test_logger ([`8e2731d`](https://github.com/maidsafe/safe_network/commit/8e2731d8b7923a9050451b31ef3a92f892d2d6d3))
    - cargo test works without feature flag ([`f2742d9`](https://github.com/maidsafe/safe_network/commit/f2742d92b3c3b56ed80732aa1d6943885fcd4317))
    - Merge #1178 ([`d9ba264`](https://github.com/maidsafe/safe_network/commit/d9ba264a6b2b657dce60b5ded78f1cecd840dbb1))
    - Merge branch 'main' into move-membership-history-to-network-knowledge ([`57de06b`](https://github.com/maidsafe/safe_network/commit/57de06b828191e093de06750f94fe6f500890112))
    - move NodeState validations to NodeState struct ([`cb733fd`](https://github.com/maidsafe/safe_network/commit/cb733fd4b1ed642da73f1e9db4fc3d8a1ec49a2b))
    - clippy after rebase ([`48ffebf`](https://github.com/maidsafe/safe_network/commit/48ffebf35093b68923d2139f12fc946d94ae4f4a))
    - make section probe interval 5mins ([`7aad345`](https://github.com/maidsafe/safe_network/commit/7aad3458b8efc7b01a6c97280b3fb59c45dd0228))
    - rejoins through events ([`910e107`](https://github.com/maidsafe/safe_network/commit/910e1078b6db0cd740547ea1ad92592d3365a616))
    - Revert "feat: node in an Arc Rwlock and factor out join code" ([`fa0d403`](https://github.com/maidsafe/safe_network/commit/fa0d4036d86ca78956b79b22154d076dc850121b))
    - node in an Arc Rwlock and factor out join code ([`2c12b49`](https://github.com/maidsafe/safe_network/commit/2c12b4993f06c9c696475404e37f4e080d01a3b0))
    - error triggering on churn join miss ([`941703e`](https://github.com/maidsafe/safe_network/commit/941703e23a53d8d894d5a9a7a253ad1735e900e0))
    - section probing for all nodes every 120s ([`fe073bc`](https://github.com/maidsafe/safe_network/commit/fe073bc0674c2099b7cd3f30ac744ea6172e24c2))
    - Merge #1193 ([`c5b0f1b`](https://github.com/maidsafe/safe_network/commit/c5b0f1b6d4f288737bc1f4fbda162386149ec402))
    - cleanup unused import ([`cf1c11a`](https://github.com/maidsafe/safe_network/commit/cf1c11ab576810cc6e063331def6f3a9a95fe663))
    - prevent deadlock in lru cache impl. ([`06591a1`](https://github.com/maidsafe/safe_network/commit/06591a11458adb5cfd917cc1239371acf4f8834f))
    - benchmarking data storage ([`0a7adca`](https://github.com/maidsafe/safe_network/commit/0a7adcab104dcda7baed80e2dd96913d6f93e541))
    - sn_interface-0.2.4/sn_client-0.62.3/sn_api-0.60.2/sn_cli-0.53.1 ([`9b06304`](https://github.com/maidsafe/safe_network/commit/9b06304f46e1a1bda90a0fc6ff82edc928c2529d))
    - Merge #1190 ([`8833cb8`](https://github.com/maidsafe/safe_network/commit/8833cb8a4ae13f04ea86c67e92fce4d82a107f5a))
    - upgrade blsttc to v5.2.0 and rand to v0.8 ([`07504fa`](https://github.com/maidsafe/safe_network/commit/07504faeda6cbfd0b27abea25facde992398ecf9))
    - Merge #1150 ([`afda86c`](https://github.com/maidsafe/safe_network/commit/afda86c5bd759f6a19cb921c356fad51f76daecd))
</details>

## v0.58.20 (2022-05-17)

<csr-id-78ad1d4ebddd5a9f9d6acb1b0ed0fd9b63bf0385/>
<csr-id-fb324b077d601503d012c032cc9889a70d82e75f/>
<csr-id-332e8f126e0e9351e8698ce2604e6fdd8ce6f7b5/>
<csr-id-f863e07357fab813246ee92ad62adf59e476312a/>
<csr-id-07504faeda6cbfd0b27abea25facde992398ecf9/>
<csr-id-e25fb53a299dd5daa755799c36a316e4b011f4d7/>
<csr-id-7bac8dfa016ac9ab2fc58c2ae6de02261fc9dc1a/>

### Chore

 - <csr-id-78ad1d4ebddd5a9f9d6acb1b0ed0fd9b63bf0385/> add PeerLink count log
 - <csr-id-fb324b077d601503d012c032cc9889a70d82e75f/> dont hold comms session lock over session cleanup
   It's still not clear if this is where we may be deadlocking. But moving to hold the lock over a shorter duration certainly seems sensible

### New Features

 - <csr-id-2b18ba8a1b0e8342af176bb78dba08f3e7b65d26/> add membership generation to DKG and SectionInfo agreement
   This prevents bogus DKG failure when two generations (of same prefix)
   may crossover under heavy churn

### Chore

 - <csr-id-e25fb53a299dd5daa755799c36a316e4b011f4d7/> rename DkgId generation to section chain len
 - <csr-id-7bac8dfa016ac9ab2fc58c2ae6de02261fc9dc1a/> reset split barrier when initializing handover

### Chore

 - <csr-id-332e8f126e0e9351e8698ce2604e6fdd8ce6f7b5/> sn_node-0.58.20
 - <csr-id-f863e07357fab813246ee92ad62adf59e476312a/> update sap checks for generation addition
 - <csr-id-07504faeda6cbfd0b27abea25facde992398ecf9/> upgrade blsttc to v5.2.0 and rand to v0.8

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 10 commits contributed to the release over the course of 1 calendar day.
 - 3 days passed between releases.
 - 7 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.58.20 ([`332e8f1`](https://github.com/maidsafe/safe_network/commit/332e8f126e0e9351e8698ce2604e6fdd8ce6f7b5))
    - Merge branch 'main' into Handover ([`7734f36`](https://github.com/maidsafe/safe_network/commit/7734f36ce326277647ac2b680a2d3f562d92917b))
    - update sap checks for generation addition ([`f863e07`](https://github.com/maidsafe/safe_network/commit/f863e07357fab813246ee92ad62adf59e476312a))
    - rename DkgId generation to section chain len ([`e25fb53`](https://github.com/maidsafe/safe_network/commit/e25fb53a299dd5daa755799c36a316e4b011f4d7))
    - reset split barrier when initializing handover ([`7bac8df`](https://github.com/maidsafe/safe_network/commit/7bac8dfa016ac9ab2fc58c2ae6de02261fc9dc1a))
    - add membership generation to DKG and SectionInfo agreement ([`2b18ba8`](https://github.com/maidsafe/safe_network/commit/2b18ba8a1b0e8342af176bb78dba08f3e7b65d26))
    - Merge #1184 ([`f22b19d`](https://github.com/maidsafe/safe_network/commit/f22b19dc1e391cc5f5409f4cec2d664ad199cbcc))
    - add PeerLink count log ([`78ad1d4`](https://github.com/maidsafe/safe_network/commit/78ad1d4ebddd5a9f9d6acb1b0ed0fd9b63bf0385))
    - Merge #1182 ([`b5a222c`](https://github.com/maidsafe/safe_network/commit/b5a222c7facb6f1617281ed1133464a435db01f8))
    - dont hold comms session lock over session cleanup ([`fb324b0`](https://github.com/maidsafe/safe_network/commit/fb324b077d601503d012c032cc9889a70d82e75f))
</details>

## v0.58.19 (2022-05-13)

<csr-id-53ee4c51b82ebd0060c9adba32dac1a102890120/>
<csr-id-aeb2945e164ca9a07390b4b7fc5220daf07f9401/>
<csr-id-e25fb53a299dd5daa755799c36a316e4b011f4d7/>
<csr-id-7bac8dfa016ac9ab2fc58c2ae6de02261fc9dc1a/>

### Chore

 - <csr-id-53ee4c51b82ebd0060c9adba32dac1a102890120/> simplify cleanupPeerLinks
   There was a suspected deadlock in the CleanUpPeerLinks code, so here
   we simplify things in order to hopefully prevent any deadlock.
   
   Moving the cleanup into comms, removing any checks against membership
   (as all nodes should be connectable; clients can always retry).
   
   And removing PeerLinks that are not conncted at all.

### Chore

 - <csr-id-e25fb53a299dd5daa755799c36a316e4b011f4d7/> rename DkgId generation to section chain len
 - <csr-id-7bac8dfa016ac9ab2fc58c2ae6de02261fc9dc1a/> reset split barrier when initializing handover

### Chore

 - <csr-id-aeb2945e164ca9a07390b4b7fc5220daf07f9401/> sn_node-0.58.19

### New Features

 - <csr-id-e552c17d89d8747b1de32598bf6c58ba43a4f285/> sort relocate candidates by distance to the churn_id
 - <csr-id-2b18ba8a1b0e8342af176bb78dba08f3e7b65d26/> add membership generation to DKG and SectionInfo agreement
   This prevents bogus DKG failure when two generations (of same prefix)
   may crossover under heavy churn

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release over the course of 1 calendar day.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.58.19 ([`aeb2945`](https://github.com/maidsafe/safe_network/commit/aeb2945e164ca9a07390b4b7fc5220daf07f9401))
    - Merge #1180 ([`aed6d50`](https://github.com/maidsafe/safe_network/commit/aed6d5050c0b2cc37cc66d4c7b6ada70ee79808a))
    - simplify cleanupPeerLinks ([`53ee4c5`](https://github.com/maidsafe/safe_network/commit/53ee4c51b82ebd0060c9adba32dac1a102890120))
    - sort relocate candidates by distance to the churn_id ([`e552c17`](https://github.com/maidsafe/safe_network/commit/e552c17d89d8747b1de32598bf6c58ba43a4f285))
</details>

## v0.58.18 (2022-05-12)

<csr-id-a49a007ef8fde53a346403824f09eb0fd25e1109/>
<csr-id-00dc9c0ba9afb6de038dda9e20a10e6727a0b0e6/>

### Chore

 - <csr-id-a49a007ef8fde53a346403824f09eb0fd25e1109/> sn_interface-0.2.3/sn_node-0.58.18

### Chore

 - <csr-id-00dc9c0ba9afb6de038dda9e20a10e6727a0b0e6/> reduce handover logging

### New Features

 - <csr-id-965310a12e09f399d125e8c5227c237d307bc20f/> disable backpressure as a deafult feature
 - <csr-id-fe017fd27a65a3c432209d2351df8d6397572ec3/> reenable backpressure as a default feature
   This moves backpressure report sending to be periodic and only to our
   section.
   
   Previously it _seems_ like the extra load from running backpressure
   analysis on every message was problematic (removing it shored up the
   membership change prs).
   
   So here we remove that, and look to only inform our section (which is
   where most messaging will likely be coming from). Intra section messages
   just now are purely AE... so should be minimal and/or update our node
   with new info so that the sender will thereafter receive updates.
   
   We _could_ still send messages to all known Links... but that seems
   overkill just now.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.2.3/sn_node-0.58.18 ([`a49a007`](https://github.com/maidsafe/safe_network/commit/a49a007ef8fde53a346403824f09eb0fd25e1109))
    - Merge #1127 ([`a03107e`](https://github.com/maidsafe/safe_network/commit/a03107ea7ea8a393c818a193eb2489e92cbbda20))
    - disable backpressure as a deafult feature ([`965310a`](https://github.com/maidsafe/safe_network/commit/965310a12e09f399d125e8c5227c237d307bc20f))
    - reduce handover logging ([`00dc9c0`](https://github.com/maidsafe/safe_network/commit/00dc9c0ba9afb6de038dda9e20a10e6727a0b0e6))
    - reenable backpressure as a default feature ([`fe017fd`](https://github.com/maidsafe/safe_network/commit/fe017fd27a65a3c432209d2351df8d6397572ec3))
</details>

## v0.58.17 (2022-05-11)

<csr-id-66638f508ad4df12b757672df589ba8ad09fbdfc/>
<csr-id-00dc9c0ba9afb6de038dda9e20a10e6727a0b0e6/>

### Chore

 - <csr-id-66638f508ad4df12b757672df589ba8ad09fbdfc/> sn_dysfunction-0.1.3/sn_node-0.58.17
 - <csr-id-00dc9c0ba9afb6de038dda9e20a10e6727a0b0e6/> reduce handover logging

### New Features

 - <csr-id-db8b58477b5b953ff2ce34163ebcc45f47cc0ab8/> return an error when received an invalid SAP
 - <csr-id-fe017fd27a65a3c432209d2351df8d6397572ec3/> reenable backpressure as a default feature
   This moves backpressure report sending to be periodic and only to our
   section.
   
   Previously it _seems_ like the extra load from running backpressure
   analysis on every message was problematic (removing it shored up the
   membership change prs).
   
   So here we remove that, and look to only inform our section (which is
   where most messaging will likely be coming from). Intra section messages
   just now are purely AE... so should be minimal and/or update our node
   with new info so that the sender will thereafter receive updates.
   
   We _could_ still send messages to all known Links... but that seems
   overkill just now.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 1 day passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_dysfunction-0.1.3/sn_node-0.58.17 ([`66638f5`](https://github.com/maidsafe/safe_network/commit/66638f508ad4df12b757672df589ba8ad09fbdfc))
    - Merge #1168 ([`375424b`](https://github.com/maidsafe/safe_network/commit/375424bab5dca59adddcc6b691ba0deac09a1bcb))
    - return an error when received an invalid SAP ([`db8b584`](https://github.com/maidsafe/safe_network/commit/db8b58477b5b953ff2ce34163ebcc45f47cc0ab8))
    - Merge branch 'main' into sap_sig_checks ([`f8ec2e5`](https://github.com/maidsafe/safe_network/commit/f8ec2e54943eaa18b50bd9d7562d41f57d5d3248))
</details>

## v0.58.16 (2022-05-10)

<csr-id-61ba367c308a846cb3f1ae065b1fbbdfb85838e4/>
<csr-id-bd04335d3ee074610ffd5fd4daff15a080b6353d/>
<csr-id-26db0aaf0acba424c6a93b49e066f036baa42e8b/>
<csr-id-ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9/>

### Chore

 - <csr-id-61ba367c308a846cb3f1ae065b1fbbdfb85838e4/> sn_interface-0.2.2/sn_client-0.62.2/sn_node-0.58.16/sn_api-0.60.1

### Other

 - <csr-id-26db0aaf0acba424c6a93b49e066f036baa42e8b/> polish remove operation

### Chore

 - <csr-id-ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9/> add ProposalAgreed log marker

### New Features

 - <csr-id-9caadf9f3c64b98aa5edbe992dcde98b5fce8bf7/> check incoming authed SAP in signed votes

### Bug Fixes

 - <csr-id-9203456e5b5f53e628566d8591dca447481ecb57/> check against the new section key instead of the current
 - <csr-id-ffc1a23b094b6ab99daaa61c91a628e478acb0e1/> log error when dropping vote due to bad signature
 - <csr-id-ae4156228a4bb684ff10ac8c98917dd4dae434ea/> check Register permissions on ops locally to prevent failures when broadcasted to the network

### Other

 - <csr-id-bd04335d3ee074610ffd5fd4daff15a080b6353d/> nightly improvements and fix release process issues
   A few changes for things spotted in the last nightly/release run:
   
   * Use `usize::MAX` for max capacity on ARM/ARMv7. A change to use a max capacity of 10GB wouldn't
     compile on these 32-bit architectures, since the value exceeded 2^32.
   * Exit on error if ARM builds fail. Even though compilation failed, the release process didn't
     report an error for the failure. The outer process must be disabling the `set -e` effect.
   * During the publishing process, instruct `sn_node` to wait on `sn_interface` rather than
     `sn_dysfunction`, since `sn_interface` is published immediately before `sn_node`. The last release
     failed when it tried to publish `sn_node` because `sn_interface` wasn't available yet.
   * Use 30 nodes in the testnet for the nightly run.
   * Run the CLI test suite in parallel with the API and client tests. Previously we didn't try this
     because we never knew if the network would handle the load.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 13 commits contributed to the release over the course of 2 calendar days.
 - 3 days passed between releases.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.2.2/sn_client-0.62.2/sn_node-0.58.16/sn_api-0.60.1 ([`61ba367`](https://github.com/maidsafe/safe_network/commit/61ba367c308a846cb3f1ae065b1fbbdfb85838e4))
    - Merge #1175 ([`9a8789e`](https://github.com/maidsafe/safe_network/commit/9a8789e307fa09b9624a8602978f720d3dc9fc8b))
    - Merge branch 'main' into nightly-improvements ([`ee3bbe1`](https://github.com/maidsafe/safe_network/commit/ee3bbe188cea756384dc38d490fe58c59c050292))
    - nightly improvements and fix release process issues ([`bd04335`](https://github.com/maidsafe/safe_network/commit/bd04335d3ee074610ffd5fd4daff15a080b6353d))
    - Merge #1172 ([`837c44c`](https://github.com/maidsafe/safe_network/commit/837c44cda38c2757f689cc4db4a84fa7c02091c0))
    - check Register permissions on ops locally to prevent failures when broadcasted to the network ([`ae41562`](https://github.com/maidsafe/safe_network/commit/ae4156228a4bb684ff10ac8c98917dd4dae434ea))
    - Merge #1140 ([`459b641`](https://github.com/maidsafe/safe_network/commit/459b641f22b488f33825777b974da80512eabed5))
    - Merge #1167 ([`5b21c66`](https://github.com/maidsafe/safe_network/commit/5b21c663c7f11124f0ed2f330b2f8687745f7da7))
    - polish remove operation ([`26db0aa`](https://github.com/maidsafe/safe_network/commit/26db0aaf0acba424c6a93b49e066f036baa42e8b))
    - Merge #1169 ([`e5d0c17`](https://github.com/maidsafe/safe_network/commit/e5d0c17c335a3a25ee0bb4c81906fa176abeb7f5))
    - add ProposalAgreed log marker ([`ae9aeeb`](https://github.com/maidsafe/safe_network/commit/ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9))
    - Merge branch 'main' into main ([`d3f07bb`](https://github.com/maidsafe/safe_network/commit/d3f07bbe5192174082e24869ba86125b6a7b1b20))
    - Merge branch 'main' into retry-count-input ([`925a8a4`](https://github.com/maidsafe/safe_network/commit/925a8a4aaade025433c29028229947de28fcb214))
</details>

## v0.58.15 (2022-05-06)

<csr-id-3757300caaea01c99a1d692ce4f572d790570ac2/>
<csr-id-c71d179e8b8089d638853956a1a39676c01c39b8/>
<csr-id-c5ba36e5ab89b2c440db38f1d1f06d4cf6c90b6a/>
<csr-id-3894e8ed5ab48bc72287c4ae74fa53ef0ba51aaa/>
<csr-id-0a87a96a911b6497d6cd667c18ebbe75e86876dc/>
<csr-id-7766e7d20b392cf5b8563d1dbc9560254b44e756/>
<csr-id-1f2d7037d3178e211842f9b554d8fd0d462709e2/>
<csr-id-e17baffdc356d244075a97e9422d5ffab2ca46c7/>
<csr-id-4de29a018a5305d18589bdd5a3d557f7979eafd7/>
<csr-id-26db0aaf0acba424c6a93b49e066f036baa42e8b/>
<csr-id-937ea457ea5d7e6f7f123eeba6b5da45fef5b404/>
<csr-id-d7b288726368fcc44c10beee42616468c64ffaae/>
<csr-id-56dc95e12310abac60531d50e79895113df49853/>
<csr-id-ca5e9226a41a878ff6ac255d0d7ca26b41e07c7d/>
<csr-id-dd8b50cff2ac35dd4134a3a6179a06b798305e91/>
<csr-id-737d906a61f772593ac7df755d995d66059e8b5e/>
<csr-id-ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9/>

### Chore

 - <csr-id-3757300caaea01c99a1d692ce4f572d790570ac2/> validate votes are for our section
 - <csr-id-c71d179e8b8089d638853956a1a39676c01c39b8/> send membership ae to only requesting peer
 - <csr-id-c5ba36e5ab89b2c440db38f1d1f06d4cf6c90b6a/> remove outdated comment
 - <csr-id-3894e8ed5ab48bc72287c4ae74fa53ef0ba51aaa/> remove the max-capacity flag from sn_node cli
 - <csr-id-0a87a96a911b6497d6cd667c18ebbe75e86876dc/> remove the max-capacity flag from sn_node cli
 - <csr-id-7766e7d20b392cf5b8563d1dbc9560254b44e756/> rename MsgKind -> AuthKind
   This feels more correct given that the kind is actually about the authority that
   the message carries.
 - <csr-id-1f2d7037d3178e211842f9b554d8fd0d462709e2/> change default node max cpacity to 10GB
   - Also delete an outdated warning output by CLI about re-enabling authd after switching networks.
 - <csr-id-e17baffdc356d244075a97e9422d5ffab2ca46c7/> change default node max cpacity to 10GB
   - Also delete an outdated warning output by CLI about re-enabling authd after switching networks.

### Other

 - <csr-id-26db0aaf0acba424c6a93b49e066f036baa42e8b/> polish remove operation
 - <csr-id-937ea457ea5d7e6f7f123eeba6b5da45fef5b404/> deterministic operations, insert stored data
 - <csr-id-d7b288726368fcc44c10beee42616468c64ffaae/> use btreemap
 - <csr-id-56dc95e12310abac60531d50e79895113df49853/> add delay during Store Op
 - <csr-id-ca5e9226a41a878ff6ac255d0d7ca26b41e07c7d/> model-based test using proptest module
 - <csr-id-dd8b50cff2ac35dd4134a3a6179a06b798305e91/> create proptest Strategy to generate ops

### Documentation

 - <csr-id-20fa632dab329f589001355a52bea70f7ddf6e86/> Add recursive flag to rm of dir
   In addition removed unneeded map_err

### Chore

 - <csr-id-737d906a61f772593ac7df755d995d66059e8b5e/> sn_interface-0.2.1/sn_client-0.62.1/sn_node-0.58.15/sn_api-0.60.0/sn_cli-0.53.0
 - <csr-id-ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9/> add ProposalAgreed log marker

### New Features

 - <csr-id-0d5cdf940afc390de22d94e91621e76d45a9eaad/> handover integration squashed
 - <csr-id-9caadf9f3c64b98aa5edbe992dcde98b5fce8bf7/> check incoming authed SAP in signed votes

### Bug Fixes

 - <csr-id-ffc1a23b094b6ab99daaa61c91a628e478acb0e1/> log error when dropping vote due to bad signature
 - <csr-id-7491323ded9a484ce0ed8f0253c76848921c54a9/> dont keep processing a vote batch when errored
   Previously we process each vote, even if an earlier one failed.
   This could lead to us parsing invalid votes from another section (if messages are off), and requesting ae updates in a loop
 - <csr-id-23aafad827a4b5b738db17a966f835f13b9cdf65/> stop DKG message loops post-split
   It could happen that during a split DKG messages are still ongoing post-split
   and are sent to the neighbouring section, which causes an AE loop as
   section keys are not in chain.
 - <csr-id-dd353b969ace383c3e89c94f7f242b84b6aee89f/> early return when AE required from a vote batch
   With latest changes we can have vote batches, and if for some reason
   we were not up to speed with the provided info, we were requesting AE
   updates for _every_ vote in the batch.
   
   Here we change that to request only one AE for the first one that fails.
 - <csr-id-9f4c3a523212c41079afcde8052a0891f3895f3b/> client knowledge could not update
   adds network knowledge storage to clients.
   Previously we were seeing issues where knowledge could not be
   updated after receiving one of two sibling saps after split.
   
   now we store the whole knowledge and validate against this chain
 - <csr-id-829eb33184c6012faa2020333e72a7c811fdb660/> batch MembershipVotes in order to ensure that order is preserved.
   Membership AE could trigger looping if response messages were processed in a bad
   order, so now we just send all the votes in a oner, in order, and those will be handled
   in the correct order. Hopefully cutting down on potential AE looping.
 - <csr-id-2f69548f5250d8c4bbcd03052bb9a49f9a2bc091/> avoid AE loop by being judicious with AE requests
 - <csr-id-a4b7597853c9f154e6fd04f1f82133cab0b3c784/> add missing backpressure feature gate.
   We were trying to count messages when thsi wasn't instantiated w/o
   backpressure. So were logging a looot of errors.
 - <csr-id-ae4156228a4bb684ff10ac8c98917dd4dae434ea/> check Register permissions on ops locally to prevent failures when broadcasted to the network
 - <csr-id-9203456e5b5f53e628566d8591dca447481ecb57/> check against the new section key instead of the current

### Other

 - <csr-id-4de29a018a5305d18589bdd5a3d557f7979eafd7/> fix split/demotion test
   We were incorrectly assuming no AE update from prefix0 for... no reason
   I can see. This was being hit after the recent handover/dkg generation
   updates. Removing the check and ensuring we checked _both_ sections
   for updates solves this

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 33 commits contributed to the release over the course of 11 calendar days.
 - 11 days passed between releases.
 - 27 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.2.1/sn_client-0.62.1/sn_node-0.58.15/sn_api-0.60.0/sn_cli-0.53.0 ([`737d906`](https://github.com/maidsafe/safe_network/commit/737d906a61f772593ac7df755d995d66059e8b5e))
    - check against the new section key instead of the current ([`9203456`](https://github.com/maidsafe/safe_network/commit/9203456e5b5f53e628566d8591dca447481ecb57))
    - Merge branch 'main' into retry-count-input ([`f514f47`](https://github.com/maidsafe/safe_network/commit/f514f471275a54edb26b1b520f76693174796dee))
    - check incoming authed SAP in signed votes ([`9caadf9`](https://github.com/maidsafe/safe_network/commit/9caadf9f3c64b98aa5edbe992dcde98b5fce8bf7))
    - deterministic operations, insert stored data ([`937ea45`](https://github.com/maidsafe/safe_network/commit/937ea457ea5d7e6f7f123eeba6b5da45fef5b404))
    - Merge #1162 ([`a4c5ccb`](https://github.com/maidsafe/safe_network/commit/a4c5ccb8bb7fbbf8ab4052d3b1051f8cac100d53))
    - log error when dropping vote due to bad signature ([`ffc1a23`](https://github.com/maidsafe/safe_network/commit/ffc1a23b094b6ab99daaa61c91a628e478acb0e1))
    - validate votes are for our section ([`3757300`](https://github.com/maidsafe/safe_network/commit/3757300caaea01c99a1d692ce4f572d790570ac2))
    - dont keep processing a vote batch when errored ([`7491323`](https://github.com/maidsafe/safe_network/commit/7491323ded9a484ce0ed8f0253c76848921c54a9))
    - stop DKG message loops post-split ([`23aafad`](https://github.com/maidsafe/safe_network/commit/23aafad827a4b5b738db17a966f835f13b9cdf65))
    - early return when AE required from a vote batch ([`dd353b9`](https://github.com/maidsafe/safe_network/commit/dd353b969ace383c3e89c94f7f242b84b6aee89f))
    - Add recursive flag to rm of dir ([`20fa632`](https://github.com/maidsafe/safe_network/commit/20fa632dab329f589001355a52bea70f7ddf6e86))
    - batch MembershipVotes in order to ensure that order is preserved. ([`829eb33`](https://github.com/maidsafe/safe_network/commit/829eb33184c6012faa2020333e72a7c811fdb660))
    - send membership ae to only requesting peer ([`c71d179`](https://github.com/maidsafe/safe_network/commit/c71d179e8b8089d638853956a1a39676c01c39b8))
    - use btreemap ([`d7b2887`](https://github.com/maidsafe/safe_network/commit/d7b288726368fcc44c10beee42616468c64ffaae))
    - add delay during Store Op ([`56dc95e`](https://github.com/maidsafe/safe_network/commit/56dc95e12310abac60531d50e79895113df49853))
    - model-based test using proptest module ([`ca5e922`](https://github.com/maidsafe/safe_network/commit/ca5e9226a41a878ff6ac255d0d7ca26b41e07c7d))
    - create proptest Strategy to generate ops ([`dd8b50c`](https://github.com/maidsafe/safe_network/commit/dd8b50cff2ac35dd4134a3a6179a06b798305e91))
    - Merge #1160 ([`d46e85b`](https://github.com/maidsafe/safe_network/commit/d46e85bf508be983017b90e6ce18f588039b16ac))
    - client knowledge could not update ([`9f4c3a5`](https://github.com/maidsafe/safe_network/commit/9f4c3a523212c41079afcde8052a0891f3895f3b))
    - fix split/demotion test ([`4de29a0`](https://github.com/maidsafe/safe_network/commit/4de29a018a5305d18589bdd5a3d557f7979eafd7))
    - remove outdated comment ([`c5ba36e`](https://github.com/maidsafe/safe_network/commit/c5ba36e5ab89b2c440db38f1d1f06d4cf6c90b6a))
    - handover integration squashed ([`0d5cdf9`](https://github.com/maidsafe/safe_network/commit/0d5cdf940afc390de22d94e91621e76d45a9eaad))
    - Merge #1149 ([`7058ecc`](https://github.com/maidsafe/safe_network/commit/7058ecce9a1f9ca90c353a8f0705d81aad8943a2))
    - avoid AE loop by being judicious with AE requests ([`2f69548`](https://github.com/maidsafe/safe_network/commit/2f69548f5250d8c4bbcd03052bb9a49f9a2bc091))
    - add missing backpressure feature gate. ([`a4b7597`](https://github.com/maidsafe/safe_network/commit/a4b7597853c9f154e6fd04f1f82133cab0b3c784))
    - Merge #1139 ([`22abbc7`](https://github.com/maidsafe/safe_network/commit/22abbc73f909131a0208ddc6e9471d073061134a))
    - Merge branch 'main' into Feat-InterfaceAuthKind ([`5db6533`](https://github.com/maidsafe/safe_network/commit/5db6533b2151e2377299a0be11e513210adfabd4))
    - rename MsgKind -> AuthKind ([`7766e7d`](https://github.com/maidsafe/safe_network/commit/7766e7d20b392cf5b8563d1dbc9560254b44e756))
    - remove the max-capacity flag from sn_node cli ([`3894e8e`](https://github.com/maidsafe/safe_network/commit/3894e8ed5ab48bc72287c4ae74fa53ef0ba51aaa))
    - change default node max cpacity to 10GB ([`1f2d703`](https://github.com/maidsafe/safe_network/commit/1f2d7037d3178e211842f9b554d8fd0d462709e2))
    - remove the max-capacity flag from sn_node cli ([`0a87a96`](https://github.com/maidsafe/safe_network/commit/0a87a96a911b6497d6cd667c18ebbe75e86876dc))
    - change default node max cpacity to 10GB ([`e17baff`](https://github.com/maidsafe/safe_network/commit/e17baffdc356d244075a97e9422d5ffab2ca46c7))
</details>

## v0.58.14 (2022-04-25)

<csr-id-2f4e7e6305ba387f2e28945aee71df650ac1d3eb/>
<csr-id-318ee1d22970b5f06e93a99b6e8fff6da638c589/>
<csr-id-826dfa48cc7c73f19adcd67bb06c7464dba4921d/>
<csr-id-8d041a80b75bc773fcbe0e4c88940ade9bda4b9d/>
<csr-id-2a731b990dbe67a700468865288585ee8dff0d71/>
<csr-id-aad69387240b067604a3d54bcf631a726c9d0956/>
<csr-id-0fc38442008ff62a6bf5398ff36cd67f99a6e172/>
<csr-id-6383f038449ebba5e7c5dec1d3f8cc1f7deca581/>
<csr-id-5580cac3d7aeab7e809729697753a9a38e8f2270/>
<csr-id-a6cb9e6c5bd63d61c4114afdcc632532f48ba208/>
<csr-id-9945bf8fb5981c1a64b23d6ea1afba5089aa5c3a/>
<csr-id-54000b43cdd3688e6c691bef9dedc299da3c22aa/>
<csr-id-1f3af46aea59ebeb1b6a4b736e80e86ce2f724d8/>
<csr-id-cbf5d45ec4522961fc7ef0860d86cc7d5e0ecca8/>

### Chore

 - <csr-id-2f4e7e6305ba387f2e28945aee71df650ac1d3eb/> sn_interface-0.2.0/sn_dysfunction-0.1.2/sn_api-0.59.0/sn_cli-0.52.0
 - <csr-id-318ee1d22970b5f06e93a99b6e8fff6da638c589/> tidy references in cargo manifests
   All references are organised alphabetically, and random usage of long-form references are removed in
   favour of the short-form version, unless the long-form style is justified, e.g., when lots of
   features are being used.
 - <csr-id-826dfa48cc7c73f19adcd67bb06c7464dba4921d/> fix sn/interface dep version
 - <csr-id-8d041a80b75bc773fcbe0e4c88940ade9bda4b9d/> remove unused deps after node/client split
 - <csr-id-2a731b990dbe67a700468865288585ee8dff0d71/> move examples/bench -> sn_client where appropriate
 - <csr-id-aad69387240b067604a3d54bcf631a726c9d0956/> safe_network->sn_node
 - <csr-id-0fc38442008ff62a6bf5398ff36cd67f99a6e172/> rename sn->sn_node now we have client extracted
 - <csr-id-6383f038449ebba5e7c5dec1d3f8cc1f7deca581/> remove olde node github workflows

### Chore

 - <csr-id-cbf5d45ec4522961fc7ef0860d86cc7d5e0ecca8/> sn_node-0.58.14

### New Features

 - <csr-id-5df610c93b76cfc3a6f09734476240313b16bee6/> compare against all nodes in section
   When calculating scores, we compare against the average of all the nodes in the section, rather than
   the 'neighbours' of the node. As a consequence, the `DysfunctionDetection` struct becomes simpler,
   as we don't need to keep track of what nodes are 'closer' to others as the set of nodes changes.
   
   In the course of this change, the Dysfunction API was updated.
   
   First, the `ScoreType` enum was renamed `IssueType`, which now details the three issue types, rather
   than have timed versus operational. Secondly, one `track_issue` function merges three
   `track_x_issue` functions for adding issues to the tracker.
   
   Both these decisions were influenced by property testing: there were properties that should have
   been true for each issue type, and it was easier to write tests for those properties if they all
   used a single API.
   
   It's worth noting, the `track_issue` function provides an `Option` argument for supplying an
   `op_id`. This value only applies when a `PendingRequestOperation` type is used. At first, the
   pending request entry was declared as `PendingRequestOperation([u8; 32])`, which makes sense
   initially. For adding the issue, you just pass the op ID along with the issue type. However, the
   problem comes when you later want to select the issues of this type. For example:
   ```
   let _ = op_scores.insert(
   *node,
   self.calculate_node_score(
   node,
   adults.clone(),
   IssueType::PendingRequestOperation,
   )
   .await,
   );
   ```
   
   If you have the `op_id` parameter on the enum entry, the code becomes very clunky:
   ```
   let _ = op_scores.insert(
   *node,
   self.calculate_node_score(
   node,
   adults.clone(),
   IssueType::PendingRequestOperation([1; 32])
   )
   .await,
   );
   ```
   
   As can be seen, you need to supply an ugly placeholder value that has no effect. For this reason, I
   decided to just supply the `op_id` as an optional argument on `track_issue`. Neither solution is
   completely ideal, but I prefer this one.
   
   Remaining changes were a bit more superficial. A `ScoreResults` helper struct was introduced to
   return all three types of scores from the refactored `calculate_scores` function, rather than
   returning a tuple of three hash tables, which would have been a bit cumbersome.

### Bug Fixes

 - <csr-id-9aa65d92e1d806150401f8bdefa1ead2e3cafd42/> use the config verbosity if no env var present
 - <csr-id-1e7c4ab6d56304f99d11396e0eee5109eb4dda04/> update some instances of safe_network->sn_node
 - <csr-id-ae4ee5c1a37dab7b5ca132d96d026bcdbac50be0/> use supported referencing style
   Currently smart-release doesn't support the `~` style of reference; the `^` style must be used. This
   caused the last nightly run to fail at version bumping.

### Other

 - <csr-id-5580cac3d7aeab7e809729697753a9a38e8f2270/> test valid nonce signature
 - <csr-id-a6cb9e6c5bd63d61c4114afdcc632532f48ba208/> remove test-publish step entirely.
   It doesnt buy us much and may fail if any dep of  has changed.
   Better to work on checking what we want (for git deps eg) rather than breaking CI
 - <csr-id-9945bf8fb5981c1a64b23d6ea1afba5089aa5c3a/> fix benchmark workflow for sn_node dir
 - <csr-id-54000b43cdd3688e6c691bef9dedc299da3c22aa/> test updates for sn_node and sn_client

### Refactor

 - <csr-id-1f3af46aea59ebeb1b6a4b736e80e86ce2f724d8/> remove op_id arg from track_issue
   Based on PR feedback, Yogesh pointed out we could change the `PendingRequestOperation` to use an
   `Option<OperationId>`. This solved the problem when performing a selection, because you can use
   `PendingRequestOperation(None)`. That's a lot better than using some placeholder value for the
   operation ID. This also tidies up `track_issue` to remove the optional `op_id` argument.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 19 commits contributed to the release over the course of 314 calendar days.
 - 18 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_node-0.58.14 ([`cbf5d45`](https://github.com/maidsafe/safe_network/commit/cbf5d45ec4522961fc7ef0860d86cc7d5e0ecca8))
    - use the config verbosity if no env var present ([`9aa65d9`](https://github.com/maidsafe/safe_network/commit/9aa65d92e1d806150401f8bdefa1ead2e3cafd42))
    - update some instances of safe_network->sn_node ([`1e7c4ab`](https://github.com/maidsafe/safe_network/commit/1e7c4ab6d56304f99d11396e0eee5109eb4dda04))
    - Merge #1128 ([`e49d382`](https://github.com/maidsafe/safe_network/commit/e49d38239b3a8c468616ad3782e1208316e9b5e0))
    - sn_interface-0.2.0/sn_dysfunction-0.1.2/sn_api-0.59.0/sn_cli-0.52.0 ([`2f4e7e6`](https://github.com/maidsafe/safe_network/commit/2f4e7e6305ba387f2e28945aee71df650ac1d3eb))
    - tidy references in cargo manifests ([`318ee1d`](https://github.com/maidsafe/safe_network/commit/318ee1d22970b5f06e93a99b6e8fff6da638c589))
    - use supported referencing style ([`ae4ee5c`](https://github.com/maidsafe/safe_network/commit/ae4ee5c1a37dab7b5ca132d96d026bcdbac50be0))
    - test valid nonce signature ([`5580cac`](https://github.com/maidsafe/safe_network/commit/5580cac3d7aeab7e809729697753a9a38e8f2270))
    - remove op_id arg from track_issue ([`1f3af46`](https://github.com/maidsafe/safe_network/commit/1f3af46aea59ebeb1b6a4b736e80e86ce2f724d8))
    - fix sn/interface dep version ([`826dfa4`](https://github.com/maidsafe/safe_network/commit/826dfa48cc7c73f19adcd67bb06c7464dba4921d))
    - compare against all nodes in section ([`5df610c`](https://github.com/maidsafe/safe_network/commit/5df610c93b76cfc3a6f09734476240313b16bee6))
    - remove test-publish step entirely. ([`a6cb9e6`](https://github.com/maidsafe/safe_network/commit/a6cb9e6c5bd63d61c4114afdcc632532f48ba208))
    - remove unused deps after node/client split ([`8d041a8`](https://github.com/maidsafe/safe_network/commit/8d041a80b75bc773fcbe0e4c88940ade9bda4b9d))
    - fix benchmark workflow for sn_node dir ([`9945bf8`](https://github.com/maidsafe/safe_network/commit/9945bf8fb5981c1a64b23d6ea1afba5089aa5c3a))
    - move examples/bench -> sn_client where appropriate ([`2a731b9`](https://github.com/maidsafe/safe_network/commit/2a731b990dbe67a700468865288585ee8dff0d71))
    - test updates for sn_node and sn_client ([`54000b4`](https://github.com/maidsafe/safe_network/commit/54000b43cdd3688e6c691bef9dedc299da3c22aa))
    - safe_network->sn_node ([`aad6938`](https://github.com/maidsafe/safe_network/commit/aad69387240b067604a3d54bcf631a726c9d0956))
    - rename sn->sn_node now we have client extracted ([`0fc3844`](https://github.com/maidsafe/safe_network/commit/0fc38442008ff62a6bf5398ff36cd67f99a6e172))
    - remove olde node github workflows ([`6383f03`](https://github.com/maidsafe/safe_network/commit/6383f038449ebba5e7c5dec1d3f8cc1f7deca581))
</details>

## v0.58.13 (2022-04-23)

<csr-id-7b8ce1c9d980015768a300ac99d07f69cc1f5ae3/>
<csr-id-bc6d861706e57d6cc80bfde2b876ba9ce57efb09/>
<csr-id-8494a01d9be3dddc0d0f4c2c37cdc4d6c3e54521/>
<csr-id-b62ad80298eb4b3e2f9810d20dd553aaf802408b/>
<csr-id-86ce41ca31508dbaf2de56fc81e1ca3146f863dc/>
<csr-id-1f3af46aea59ebeb1b6a4b736e80e86ce2f724d8/>
<csr-id-a6cb9e6c5bd63d61c4114afdcc632532f48ba208/>
<csr-id-9945bf8fb5981c1a64b23d6ea1afba5089aa5c3a/>
<csr-id-54000b43cdd3688e6c691bef9dedc299da3c22aa/>
<csr-id-318ee1d22970b5f06e93a99b6e8fff6da638c589/>
<csr-id-826dfa48cc7c73f19adcd67bb06c7464dba4921d/>
<csr-id-8d041a80b75bc773fcbe0e4c88940ade9bda4b9d/>
<csr-id-2a731b990dbe67a700468865288585ee8dff0d71/>
<csr-id-aad69387240b067604a3d54bcf631a726c9d0956/>
<csr-id-0fc38442008ff62a6bf5398ff36cd67f99a6e172/>
<csr-id-6383f038449ebba5e7c5dec1d3f8cc1f7deca581/>

### Chore

 - <csr-id-7b8ce1c9d980015768a300ac99d07f69cc1f5ae3/> remove unused sn_interface deps
 - <csr-id-bc6d861706e57d6cc80bfde2b876ba9ce57efb09/> fix bench types dep -> sn_interface
 - <csr-id-8494a01d9be3dddc0d0f4c2c37cdc4d6c3e54521/> split put messaging and types into top level crate

### Refactor

 - <csr-id-1f3af46aea59ebeb1b6a4b736e80e86ce2f724d8/> remove op_id arg from track_issue
   Based on PR feedback, Yogesh pointed out we could change the `PendingRequestOperation` to use an
   `Option<OperationId>`. This solved the problem when performing a selection, because you can use
   `PendingRequestOperation(None)`. That's a lot better than using some placeholder value for the
   operation ID. This also tidies up `track_issue` to remove the optional `op_id` argument.

### Other

 - <csr-id-a6cb9e6c5bd63d61c4114afdcc632532f48ba208/> remove test-publish step entirely.
   It doesnt buy us much and may fail if any dep of  has changed.
   Better to work on checking what we want (for git deps eg) rather than breaking CI
 - <csr-id-9945bf8fb5981c1a64b23d6ea1afba5089aa5c3a/> fix benchmark workflow for sn_node dir
 - <csr-id-54000b43cdd3688e6c691bef9dedc299da3c22aa/> test updates for sn_node and sn_client

### Bug Fixes

 - <csr-id-ae4ee5c1a37dab7b5ca132d96d026bcdbac50be0/> use supported referencing style
   Currently smart-release doesn't support the `~` style of reference; the `^` style must be used. This
   caused the last nightly run to fail at version bumping.

### New Features

 - <csr-id-5df610c93b76cfc3a6f09734476240313b16bee6/> compare against all nodes in section
   When calculating scores, we compare against the average of all the nodes in the section, rather than
   the 'neighbours' of the node. As a consequence, the `DysfunctionDetection` struct becomes simpler,
   as we don't need to keep track of what nodes are 'closer' to others as the set of nodes changes.
   
   In the course of this change, the Dysfunction API was updated.
   
   First, the `ScoreType` enum was renamed `IssueType`, which now details the three issue types, rather
   than have timed versus operational. Secondly, one `track_issue` function merges three
   `track_x_issue` functions for adding issues to the tracker.
   
   Both these decisions were influenced by property testing: there were properties that should have
   been true for each issue type, and it was easier to write tests for those properties if they all
   used a single API.
   
   It's worth noting, the `track_issue` function provides an `Option` argument for supplying an
   `op_id`. This value only applies when a `PendingRequestOperation` type is used. At first, the
   pending request entry was declared as `PendingRequestOperation([u8; 32])`, which makes sense
   initially. For adding the issue, you just pass the op ID along with the issue type. However, the
   problem comes when you later want to select the issues of this type. For example:
   ```
   let _ = op_scores.insert(
   *node,
   self.calculate_node_score(
   node,
   adults.clone(),
   IssueType::PendingRequestOperation,
   )
   .await,
   );
   ```
   
   If you have the `op_id` parameter on the enum entry, the code becomes very clunky:
   ```
   let _ = op_scores.insert(
   *node,
   self.calculate_node_score(
   node,
   adults.clone(),
   IssueType::PendingRequestOperation([1; 32])
   )
   .await,
   );
   ```
   
   As can be seen, you need to supply an ugly placeholder value that has no effect. For this reason, I
   decided to just supply the `op_id` as an optional argument on `track_issue`. Neither solution is
   completely ideal, but I prefer this one.
   
   Remaining changes were a bit more superficial. A `ScoreResults` helper struct was introduced to
   return all three types of scores from the refactored `calculate_scores` function, rather than
   returning a tuple of three hash tables, which would have been a bit cumbersome.

### Chore

 - <csr-id-318ee1d22970b5f06e93a99b6e8fff6da638c589/> tidy references in cargo manifests
   All references are organised alphabetically, and random usage of long-form references are removed in
   favour of the short-form version, unless the long-form style is justified, e.g., when lots of
   features are being used.
 - <csr-id-826dfa48cc7c73f19adcd67bb06c7464dba4921d/> fix sn/interface dep version
 - <csr-id-8d041a80b75bc773fcbe0e4c88940ade9bda4b9d/> remove unused deps after node/client split
 - <csr-id-2a731b990dbe67a700468865288585ee8dff0d71/> move examples/bench -> sn_client where appropriate
 - <csr-id-aad69387240b067604a3d54bcf631a726c9d0956/> safe_network->sn_node
 - <csr-id-0fc38442008ff62a6bf5398ff36cd67f99a6e172/> rename sn->sn_node now we have client extracted
 - <csr-id-6383f038449ebba5e7c5dec1d3f8cc1f7deca581/> remove olde node github workflows

### Other

 - <csr-id-b62ad80298eb4b3e2f9810d20dd553aaf802408b/> add test-utils feat to bench

### Test

 - <csr-id-86ce41ca31508dbaf2de56fc81e1ca3146f863dc/> adding more unit tests to wallet APIs

## v0.58.12 (2022-04-09)

<csr-id-0a719147ae567b41ba2fcbf4c3c0b44e6d1955d1/>
<csr-id-03258b3777644de7799e9563df35afc5e8531be2/>
<csr-id-c8f4eed0406253cc4c253292bc82e7320fdcbf70/>
<csr-id-3dac350598f863fc3d66669c9f8789db51573b96/>
<csr-id-c45f6e362257ba9378547a8f1fd508a5e680cb0a/>
<csr-id-c4e3de1d9715c6e3618a763fa857feca4258248f/>

### Chore

 - <csr-id-0a719147ae567b41ba2fcbf4c3c0b44e6d1955d1/> sn_cli-0.51.1
 - <csr-id-03258b3777644de7799e9563df35afc5e8531be2/> only log SendSus once per batch

### Chore

 - <csr-id-c4e3de1d9715c6e3618a763fa857feca4258248f/> safe_network-0.58.12/sn_api-0.58.1/sn_cli-0.51.2

### New Features

 - <csr-id-951d1bd87490ad8b3c3747cba952424416da013f/> watch status of individual msgs
 - <csr-id-20c33249fceea1c3d085de048f13388187a77ea5/> individual send rates

### Bug Fixes

 - <csr-id-351450ec6794d7933fd2b29359f820e87c0486db/> adult shall update to new SAP
 - <csr-id-14725d0353d797b9437033781e8ff295a7eacc34/> allow update message pass through
 - <csr-id-bd488cbc9d24324bec730f85bdcebccaec2e75c4/> exclude suspects from current
 - <csr-id-bbd6a2370a809a4d23a1df0a813cac2809e06690/> bump node suspicion event prio
 - <csr-id-63c5deae1a4c31c681c019846da95105c0ef7733/> improve success rate

### Other

 - <csr-id-c8f4eed0406253cc4c253292bc82e7320fdcbf70/> debug promotion, rename workflow

### Refactor

 - <csr-id-3dac350598f863fc3d66669c9f8789db51573b96/> use msgs per s

### Test

 - <csr-id-c45f6e362257ba9378547a8f1fd508a5e680cb0a/> run 40mb test as normal

## v0.58.11 (2022-04-01)

### New Features

 - <csr-id-951d1bd87490ad8b3c3747cba952424416da013f/> watch status of individual msgs
 - <csr-id-20c33249fceea1c3d085de048f13388187a77ea5/> individual send rates

### Bug Fixes

 - <csr-id-14725d0353d797b9437033781e8ff295a7eacc34/> allow update message pass through
 - <csr-id-bd488cbc9d24324bec730f85bdcebccaec2e75c4/> exclude suspects from current
 - <csr-id-bbd6a2370a809a4d23a1df0a813cac2809e06690/> bump node suspicion event prio
 - <csr-id-63c5deae1a4c31c681c019846da95105c0ef7733/> improve success rate

## v0.58.10 (2022-03-31)

### New Features

 - <csr-id-951d1bd87490ad8b3c3747cba952424416da013f/> watch status of individual msgs
 - <csr-id-20c33249fceea1c3d085de048f13388187a77ea5/> individual send rates

### Bug Fixes

 - <csr-id-bd488cbc9d24324bec730f85bdcebccaec2e75c4/> exclude suspects from current
 - <csr-id-bbd6a2370a809a4d23a1df0a813cac2809e06690/> bump node suspicion event prio
 - <csr-id-63c5deae1a4c31c681c019846da95105c0ef7733/> improve success rate
 - <csr-id-14725d0353d797b9437033781e8ff295a7eacc34/> allow update message pass through

## v0.58.9 (2022-03-26)

<csr-id-b471b5c9f539933dd12de7af3473d2b0f61d7f28/>
<csr-id-c0806e384d99b94480e8f8e0322a6f5a6bd3636a/>
<csr-id-dabdc555d70be79b910c5fe2b2647ca85f2319f9/>

### Chore

 - <csr-id-b471b5c9f539933dd12de7af3473d2b0f61d7f28/> sn_dysfunction-/safe_network-0.58.9
 - <csr-id-c0806e384d99b94480e8f8e0322a6f5a6bd3636a/> safe_network-0.58.8/sn_api-0.58.0/sn_cli-0.51.0
 - <csr-id-dabdc555d70be79b910c5fe2b2647ca85f2319f9/> check sus nodes periodically and not on query

## v0.58.8 (2022-03-25)

<csr-id-aa82ab7d691cca3990ece4e56d75cd63fda2fe15/>
<csr-id-196b32f348d3f5ce2a63c24877acb1a0a7f449e3/>
<csr-id-907c7d3ef4f65df5566627938154dfca1e2fdc05/>
<csr-id-90712c91368b4d88537acc65a3ccc5478fe38d2c/>
<csr-id-51d534d39caf3a4c029f7d1a9c87a3edf3192b2f/>
<csr-id-b239e6b38a99afda7a945a51d3f6e00841730a8f/>
<csr-id-aafb6d2a458fc4e2dc94ea3a08cb519fe52bc131/>
<csr-id-6e897d0bc93256f5ab72350c9774f9a33937da1b/>
<csr-id-224079f66832c0a914fd20af4fc2f9e90dc9c9c9/>
<csr-id-453b246c002f9e964896876c254e6c31f1f6045d/>
<csr-id-0cad4c981dbdf9eee58fc28b4637f136b817c384/>
<csr-id-43489f6170ce13ea05148a52422fbff6bdb91f19/>
<csr-id-1ea4b4413fee11fde2b69086caeb69cc191fe277/>
<csr-id-2ac9edab88602fa2aeb148baf6566cab876dc2af/>
<csr-id-e4a7c564180cb3b81601950c4f7623dfcdcd7650/>
<csr-id-80c2ea0c2863ba8cc3500a1880337285e40fdf4c/>
<csr-id-a49ea81658b15280c23e93b1945f67aeb43a5962/>
<csr-id-6b83f38f17c241c00b70480a18a47b04d9a51ee1/>

### Other

 - <csr-id-aa82ab7d691cca3990ece4e56d75cd63fda2fe15/> remove unneeded retry_backoff in reg tests
 - <csr-id-196b32f348d3f5ce2a63c24877acb1a0a7f449e3/> re-ignore 40/100mb tests for basic ci

### Chore

 - <csr-id-907c7d3ef4f65df5566627938154dfca1e2fdc05/> safe_network-0.58.8/sn_api-0.58.0/sn_cli-0.51.0
 - <csr-id-90712c91368b4d88537acc65a3ccc5478fe38d2c/> update deps
 - <csr-id-51d534d39caf3a4c029f7d1a9c87a3edf3192b2f/> add LogMarker for dysfunction agreement
 - <csr-id-b239e6b38a99afda7a945a51d3f6e00841730a8f/> remove unnecessary map_err call
   Removes a map_err which isn't needed for
 - <csr-id-aafb6d2a458fc4e2dc94ea3a08cb519fe52bc131/> rename dysfunction -> sn_dysfunction
 - <csr-id-6e897d0bc93256f5ab72350c9774f9a33937da1b/> remove retry_loop! from nrs tests
 - <csr-id-224079f66832c0a914fd20af4fc2f9e90dc9c9c9/> tweaking values and thresholds
   Client tests should not trigger suspect or dysfunctional nodes.
   Prior to this, we can see that heppening regularly in testnets
 - <csr-id-453b246c002f9e964896876c254e6c31f1f6045d/> refactor NodeQueryResponse handling at elder
 - <csr-id-0cad4c981dbdf9eee58fc28b4637f136b817c384/> use one func to detect nodes beyond a severity
 - <csr-id-43489f6170ce13ea05148a52422fbff6bdb91f19/> remove _ from dys_function crate name
 - <csr-id-1ea4b4413fee11fde2b69086caeb69cc191fe277/> enable 100mb test
 - <csr-id-2ac9edab88602fa2aeb148baf6566cab876dc2af/> reduce node cleanup frequency
 - <csr-id-e4a7c564180cb3b81601950c4f7623dfcdcd7650/> different logs for different data failures
 - <csr-id-80c2ea0c2863ba8cc3500a1880337285e40fdf4c/> log ProposeOffline
 - <csr-id-a49ea81658b15280c23e93b1945f67aeb43a5962/> split out deviant/unresponsive calculations
   This should just keep things a bit tidier
 - <csr-id-6b83f38f17c241c00b70480a18a47b04d9a51ee1/> deps, remove ~ restriction on major versioned deps
   tilde w/ a major version restricts us to path udpats only.
   we want caret, which is implicit frm v 1

### New Features

 - <csr-id-5d80122d51dcbe8241e85f27d23e34b053b77651/> optimise DataQuery cache
   Move the pending_requests to storing an Arc<DashSet<Peers>>
   
   This is due to cache providing a clone when we , which means
   we may not always be setting pending peers correctly.
   
   Now we also use the DATA_QUERY_TIMEOUT, set to 15s to decide if we should
   re-send a query. (Previously it was only sent if no peer was waiting; ).
   
   This means we have a hard limit on how often we may ask for a chunk if peers are waiting for
   that chunk.
 - <csr-id-8168be4ef68f467f1c4b35943442fd30102a34f2/> record time of conn/knowledge issues
   Remove older issues (currently set to 15 mins) from the tracker to keep
   node dysfunction detection relevant to current circumstance
 - <csr-id-73fcad5160127c9e4d3a808df229947fa6163ae0/> add knowledge dysfunction tracking and weighting
 - <csr-id-88e1800103ac8465bbfc377719ed5804853d833a/> use weighted averages for dysfunctional detection
 - <csr-id-1d390ee298293ffe7533b4f1a3b47cc1d11a2198/> initial move of liveness->dysfunctional detection crate in root

### Bug Fixes

 - <csr-id-a45a3bda7044f07b6ecd99569ec4c043330d7160/> Improve query handling, keep peers on DataNotFound
   Previously if we had DataNotFound, any waiting peers are not requeued.
   So this means that one bogus adult can effectively fail a query which
   might otherwise succeed.
   
   In doing so, we need to always resend queries to adults if clients retry
   as otherwise they may remain queued but wont actually requery adults
   ever again.
   
   This appears to improve test time, even after reducing the RETRY_COUNT
   on messages.
 - <csr-id-2619ca4b399cf8156b9fdf2e33abe2708e6673b9/> re-queue waiting peers on dud responses
 - <csr-id-6915006f070811eccc7aea075a707d15a20abb93/> add new querying peer to wait list if that already exists
 - <csr-id-dbcffd94114e36ac763e5718937d1228ba882baa/> throttle suspect node's preemptive replication.
   Previous every query response could trigger this flow.
   Now we only do it once every X time. (25 mins atm)

## v0.58.7 (2022-03-22)

<csr-id-a6e2e0c5eec5c2e88842d18167128991b76ecbe8/>
<csr-id-d3989bdd95129999996e58736ec2553242697f2c/>

### Chore

 - <csr-id-a6e2e0c5eec5c2e88842d18167128991b76ecbe8/> safe_network-0.58.7/sn_api-0.57.3/sn_cli-0.50.5
 - <csr-id-d3989bdd95129999996e58736ec2553242697f2c/> bump bls_dkg, self_encryption, xor_name
   This is a step towards integrating sn_dbc into safe_network.

## v0.58.6 (2022-03-17)

<csr-id-a741d930b906054d09f1311ddcf35479aa1aa3ee/>
<csr-id-20c057c72fdcacc0bcda6da1f97eb6fab1a0cf4c/>

### Chore

 - <csr-id-a741d930b906054d09f1311ddcf35479aa1aa3ee/> safe_network-0.58.6/sn_api-0.57.2
 - <csr-id-20c057c72fdcacc0bcda6da1f97eb6fab1a0cf4c/> make resource logging consistent


### New Features

 - <csr-id-d6e601a3c18dc2b7f60c297f5c794883952e1d14/> prune parent whenever a child inserted

## v0.58.5 (2022-03-09)

<csr-id-df330fa0de1e334e55863828fb743131ab629a18/>
<csr-id-784870abfdd5620da6839e7fd7df80702e0f3afa/>
<csr-id-df398e1017221c0027542b597c8f7c38c1828723/>
<csr-id-8c3bcc2bb64063e646d368d90fff98420ab22dce/>

### Other

 - <csr-id-df330fa0de1e334e55863828fb743131ab629a18/> discard DataReplicator module
   - DataReplicator was used to track replication flows in order to delete data that was not
   in a node's range after replication completes. This can now be removed since we hold on to data
   that is not in our range, even after replication ends.

### Chore

 - <csr-id-784870abfdd5620da6839e7fd7df80702e0f3afa/> safe_network-0.58.5
 - <csr-id-df398e1017221c0027542b597c8f7c38c1828723/> clarify comment regarding bootstrap
 - <csr-id-8c3bcc2bb64063e646d368d90fff98420ab22dce/> remove unneeded HandleNodeMsg command

### New Features

 - <csr-id-7a1065c46f5d72f6997a504c984a70493e197a5b/> impl throttled message sending
   - adds a new command that given sends messages in a throttled fashion

## v0.58.4 (2022-03-04)

<csr-id-96bf83f8635004fa23e6b5b870cac27bd5b42332/>
<csr-id-98a0eb988ab819ffce4727a9b823709efdc18dab/>
<csr-id-bf7de428cd1e091ef2a6e616c07fd6596d711992/>
<csr-id-ba0a26db12823bd2de51a4c75c5e7c4875c8f3f5/>
<csr-id-94d58d565d9a6870e334bb2ca249f26ac3f8a327/>
<csr-id-7cf7fd675beec5e7aa122f0f127402b636e659b7/>
<csr-id-43a42657ff18409dd83bba03135cd013c1298dc3/>
<csr-id-7cb5ffd03f9bfce6ebe74f66dcabddef661cf94d/>
<csr-id-000fc62a87dac3cd41bb6ced59596635f056ff29/>
<csr-id-0ff76c44aeddedd765fa4933c0841539feabaae5/>
<csr-id-6970533f288ff867a702d3dbc5424314b3639674/>

### Test

 - <csr-id-96bf83f8635004fa23e6b5b870cac27bd5b42332/> add unit tests to data storage and replicator modules
   - renames some of the internal APIs
   - fixed a bug in replicator module

### Refactor

 - <csr-id-98a0eb988ab819ffce4727a9b823709efdc18dab/> limit batch size to 50 chunks and remove ack message
 - <csr-id-bf7de428cd1e091ef2a6e616c07fd6596d711992/> do not delete data after finishing replication
 - <csr-id-ba0a26db12823bd2de51a4c75c5e7c4875c8f3f5/> remove entries from replicator if target already has data
 - <csr-id-94d58d565d9a6870e334bb2ca249f26ac3f8a327/> do not cache to-be-replicated data; fetch from storage

### Chore

 - <csr-id-7cf7fd675beec5e7aa122f0f127402b636e659b7/> safe_network-0.58.4/sn_cli-0.50.4
 - <csr-id-43a42657ff18409dd83bba03135cd013c1298dc3/> remove unneeded logs
 - <csr-id-7cb5ffd03f9bfce6ebe74f66dcabddef661cf94d/> dont delete chunks on republish
 - <csr-id-000fc62a87dac3cd41bb6ced59596635f056ff29/> only add liveness check when we're sending out reqs.
   Previously the liveness add was before we checked if this client was already awaiting this same data req.
   So one slow req could compound liveness checks
 - <csr-id-0ff76c44aeddedd765fa4933c0841539feabaae5/> rename we_are_not_a_holder for clarity
 - <csr-id-6970533f288ff867a702d3dbc5424314b3639674/> tweak logging msg prio and removed unneeded msgs

### New Features

 - <csr-id-076d5e3cfd64d7bd677c9e6d34baf93f2eb49b4a/> clients should retry message sending when Conn errors encountered
 - <csr-id-5b530fec3cd6d182f4dba89e2144826977404aa9/> add basic PeerLink cleanup
   prior to this client Peer Links were held forever. Now we trigger
   a Cmd to clean them up if they're not waiting on any other response from
   the network.
   
   We do this every X time
 - <csr-id-070c146ea287db9bf708b11fbaaa177422cea73d/> nodes retry link.send_with once blindly
 - <csr-id-65c3adc8dd45368260bf60ef83d44a99eb6ee7ca/> batch-up data for replication
 - <csr-id-d3fd698b756d100daa12230fb944129529b773fb/> impl pull model data replication
 - <csr-id-a3a28bd7c816582218bea5dbf0f7e141b3ae2c76/> init data replicator module

## v0.58.3 (2022-03-01)

<csr-id-a3a2bbfbaf3be846a1c1f22a74aee2f961341685/>
<csr-id-51b3d75fc7389de647f6df230bff32e8c7d7267c/>

### Other

 - <csr-id-a3a2bbfbaf3be846a1c1f22a74aee2f961341685/> use split example to start network for e2e-split run
   We use the split test to check data integrity, before continuing
   with standard client checks

### Chore

 - <csr-id-51b3d75fc7389de647f6df230bff32e8c7d7267c/> safe_network-0.58.3/sn_cli-0.50.3

## v0.58.2 (2022-02-27)

<csr-id-d8c57e082b52196cc538271bc25a88e3efd2a97c/>
<csr-id-634010fd79ce1487abbff5adf3d15da59709dd95/>
<csr-id-85e513670aa61f8acb3e3302ee4b39763ade036e/>
<csr-id-705995ef67b3d4c45c95689c4a675e1063467ec9/>
<csr-id-f8bfe9efe68593ceb4f968a6d2a396c431ad6429/>
<csr-id-61068aaf3e9cd1c7513b58c073c55004697fdf6f/>
<csr-id-ea2ba0afa036b6abab35db9a76488d052e7682d6/>
<csr-id-912a2f8d2da4159fcf40567666b3b14024e8c0da/>
<csr-id-d5e6f462615de830cd9c27dba49a34ba2da13b81/>
<csr-id-35a46f06e6233aff25d03350abaefacbe57ad25c/>
<csr-id-fc074ab28d3c8c011016e6598cf840fc38026418/>
<csr-id-b44ac353e254d8d67996c3185dc40e5e99c0e4c7/>
<csr-id-222742f7c57a4b451af354d33015974d0d7a3561/>
<csr-id-c086db96c09a77c43777783f614ca6a43eff7cdd/>

### Other

 - <csr-id-d8c57e082b52196cc538271bc25a88e3efd2a97c/> ignore cargo husky in udeps checks

### Chore

 - <csr-id-634010fd79ce1487abbff5adf3d15da59709dd95/> safe_network-0.58.2/sn_api-0.57.1/sn_cli-0.50.2
 - <csr-id-85e513670aa61f8acb3e3302ee4b39763ade036e/> remove pointless and long checks in reg batch
   Previously we needlessly attempt to check for a reg which
   didnt exist, to confirm that it didnt.
   
   This took a QUERY_TIMEOUT length of time, and was obviously slow.
   
   This speeds up the test from ~4 mins to 7secs
 - <csr-id-705995ef67b3d4c45c95689c4a675e1063467ec9/> changes to appease clippy 1.59
 - <csr-id-f8bfe9efe68593ceb4f968a6d2a396c431ad6429/> update deps.
 - <csr-id-61068aaf3e9cd1c7513b58c073c55004697fdf6f/> Add testing documentation
 - <csr-id-ea2ba0afa036b6abab35db9a76488d052e7682d6/> add read timing logs into put_get example
 - <csr-id-912a2f8d2da4159fcf40567666b3b14024e8c0da/> add put timing logs into put_get example
 - <csr-id-d5e6f462615de830cd9c27dba49a34ba2da13b81/> more general dep updates
 - <csr-id-35a46f06e6233aff25d03350abaefacbe57ad25c/> add put_get chunk soak example
   Adds an example to launch many clients uploading many files, and then
   verify those uploads.
   
   This is handy to test node load and client throughput.
   
   Currently it's not used in CI anywhere.
 - <csr-id-fc074ab28d3c8c011016e6598cf840fc38026418/> sn_cli-0.50.1
 - <csr-id-b44ac353e254d8d67996c3185dc40e5e99c0e4c7/> move testnet bin into its own crate
 - <csr-id-222742f7c57a4b451af354d33015974d0d7a3561/> update qp2p
 - <csr-id-c086db96c09a77c43777783f614ca6a43eff7cdd/> more log cmd inspector into its own crate.
   This removes some deps from sn, and log cmd inspector should work just
   fine there

### Bug Fixes

 - <csr-id-38fb057da44a0e243186410df0c39361a21ec46e/> introduce cohesive conn handling
 - <csr-id-ddd45b7cc73bbacea19f5c93519ae16a74cc01cc/> add MIN_PENDING_OPS threshold for liveness checks

## v0.58.1 (2022-02-20)

<csr-id-ea0387b43233f95d10f19d403d289f272f42336f/>

### Chore

 - <csr-id-ea0387b43233f95d10f19d403d289f272f42336f/> safe_network-0.58.1

### Bug Fixes

 - <csr-id-ddd45b7cc73bbacea19f5c93519ae16a74cc01cc/> add MIN_PENDING_OPS threshold for liveness checks

## v0.58.0 (2022-02-17)

<csr-id-7b213d7bb1eaf208f35fc4c0c3f4a71a1da5aca3/>
<csr-id-149665a53c00f62be0e8c8ec340b951a06346848/>
<csr-id-9fb73957fbba99929911296305bfd66dbaa4d15a/>
<csr-id-3a427eb33dd0a81f8dc77521a88eba2112bec778/>
<csr-id-c184c49cd9d71e657a4b5349940eb23810749132/>
<csr-id-ceaf43916dfac62213ea23bde35c3f931cdd8c37/>
<csr-id-b161e038bf30e0a50a7103eaad80ef2368b6689d/>
<csr-id-7e15db2de4d99d84126e7087d838047bae7a009b/>
<csr-id-bd19e9e6eae4ce6068e1bee2d89528d36fce5329/>
<csr-id-a56399864e18f2b2de7ba033497c5bbbe3e5394e/>

### Refactor

 - <csr-id-7b213d7bb1eaf208f35fc4c0c3f4a71a1da5aca3/> remove MIN_PENDING_OPS and always check for unresponsive nodes
   - also refactors the previous inversely proportionate ratio check to directly proportionate

### Chore

 - <csr-id-149665a53c00f62be0e8c8ec340b951a06346848/> safe_network-0.58.0/sn_api-0.57.0/sn_cli-0.50.0
 - <csr-id-9fb73957fbba99929911296305bfd66dbaa4d15a/> update NEIGHBOUR_COUNT to DEFAULT_ELDER_SIZE
   - NEIGHBOUR_COUNT must always be set relevant to section size
 - <csr-id-3a427eb33dd0a81f8dc77521a88eba2112bec778/> document Liveness consts
 - <csr-id-c184c49cd9d71e657a4b5349940eb23810749132/> minor fixes and refactor to active data republishing
 - <csr-id-ceaf43916dfac62213ea23bde35c3f931cdd8c37/> update dashmap for security adv -> 5.1.0
 - <csr-id-b161e038bf30e0a50a7103eaad80ef2368b6689d/> log insufficent adult err
 - <csr-id-7e15db2de4d99d84126e7087d838047bae7a009b/> log adults known about during storage calcs
 - <csr-id-bd19e9e6eae4ce6068e1bee2d89528d36fce5329/> easier to read logging for saps
 - <csr-id-a56399864e18f2b2de7ba033497c5bbbe3e5394e/> tweak command ack wait and interval looping.
   Increases wait for acks slightly.

### New Features

 - <csr-id-928600b13fdb1a31498cc27cba968a8e3eba598c/> republish data actively when deviant nodes are detected

### Bug Fixes

 - <csr-id-a787b7c003f0604383a3c47d731e6c2fc84f7583/> update MIN_PENDING_OPS value and liveness tests
 - <csr-id-5d1bae8cf0fa1aeb8d48c24bb4fc82bd705bc32e/> fixes data republish flow by routing messages via elders

### New Features (BREAKING)

 - <csr-id-4e7bc8bbb3f521324edf0e4a6e329271b7f854f6/> remove always-joinable feature flag

## v0.57.1 (2022-02-15)

<csr-id-07dc30b281f3c67cb5598aaaf72ba5c668353bf7/>
<csr-id-03cf556d1b73565d520e2c5b82ab1482b076e639/>
<csr-id-7068445cad25b0273c841c5072055833df9b8229/>
<csr-id-d3dd663cfd6f3d0b0943ff49b1eed8c6b37d6263/>
<csr-id-5fe289271fb0da28935b0b578928687be0dc4665/>
<csr-id-d6e788221f332c3ff3a22eec39af428eebf5e75f/>
<csr-id-283ff34f03dbf783b2299a3c7eb7a183c1e3c0a6/>
<csr-id-e9e249a674d4a64078a519b7e20baf6f0759c1c9/>

### Chore

 - <csr-id-07dc30b281f3c67cb5598aaaf72ba5c668353bf7/> safe_network-0.57.1/sn_cli-0.49.1
 - <csr-id-03cf556d1b73565d520e2c5b82ab1482b076e639/> begin sorting out messaging
 - <csr-id-7068445cad25b0273c841c5072055833df9b8229/> move capacity and liveness into data mod
 - <csr-id-d3dd663cfd6f3d0b0943ff49b1eed8c6b37d6263/> move chunk store to dbs
 - <csr-id-5fe289271fb0da28935b0b578928687be0dc4665/> co-locate related modules
 - <csr-id-d6e788221f332c3ff3a22eec39af428eebf5e75f/> rename core to node
 - <csr-id-283ff34f03dbf783b2299a3c7eb7a183c1e3c0a6/> rename node to nodeinfo
 - <csr-id-e9e249a674d4a64078a519b7e20baf6f0759c1c9/> move peer and prefix map into types

## v0.57.0 (2022-02-12)

<csr-id-a398c4f8d72828db0fc8c6d5825ead62ba85db64/>
<csr-id-8f580200ba5b8b67f36977fb59d6d48e7613e176/>
<csr-id-1f57e30a206998a03b2201f4b57b372ebe9ae828/>
<csr-id-1dfb651e72f38743645afbbee62c7e6e7fbb0fb2/>
<csr-id-5869662a29c4bd8dfe0d7bf07a30f10d89b450ad/>
<csr-id-74fc4ddf31af51e0036eb164e6b5c4f0864bd08c/>

### Chore

 - <csr-id-a398c4f8d72828db0fc8c6d5825ead62ba85db64/> safe_network-0.57.0/sn_api-0.56.0/sn_cli-0.49.0
 - <csr-id-8f580200ba5b8b67f36977fb59d6d48e7613e176/> Batch pending requests together per name
   Previously we were only allowing one operation id, which could be overwritten by new clients.
   This allows client requests to await one response together
 - <csr-id-1f57e30a206998a03b2201f4b57b372ebe9ae828/> only prioritise service cmd/query with permits.
   Previously we were holding permits in a RwLock struct for _any_ command.
   
   Now we remove both the conecpt of permits here, removing one potential
   locking loction, and the unused message prioritisation is removed to
   clean that up.
   
   Now we simply check if we have max allowed pending queries at any one
   time, and if so, we drop inbound queries.
 - <csr-id-1dfb651e72f38743645afbbee62c7e6e7fbb0fb2/> rename standard_wait to cmd_ack_wait, set default to 5s
   Previously we've had standard wait be more AE centric. But it was actually
   only being used to determine how long to wait for Cmd Acks.
   
   Here that is clarified, and a none-zero time set to be more useful
 - <csr-id-5869662a29c4bd8dfe0d7bf07a30f10d89b450ad/> simplify ae msg handling at clients
   Previously AE-retry/redirect did not both have a SAP. Now
   they do we can simplify that and directly use it in either case.
   
   We also can remove the AE caches, and just send on each incoming AE-msg
   to one elder (as opposed to `count` elders). This is deterministic based
   on sender name. Which means we can simply do it as each one comes in. No
   tracking of prior messages needed.
   
   We also remove the `make_contact` exemptions in case this interferes
   with initial network contact.
   
   Overall simpler and cleaner.

### New Features

 - <csr-id-0bc50ae33ccb934016ac425e7bb2eca90a4b06e3/> resolve nrs map container content
   The resolver can now return `NrsMapContainer` content, which can then be displayed by the CLI with
   the `cat` and `dog` commands. This functionality was unintentionally broken at some point.
   
   The first change introduced an `NrsEntry` field in `SafeData`, and modified the `NrsMapContainer` to
   remove its `resolve_into` and `public_name` fields. The intention is for the resolver to return
   `NrsMapContainer` data when a container XOR-URL is used, but when using an NRS URL, an `NrsEntry`
   will be returned. The `NrsMapContainer` data will have the NRS map, whereas the `NrsEntry` will only
   contain the target link and subname version. It's worth noting, the `NrsEntry` doesn't have an
   XOR-URL because the entries are still stored in the map. An NRS URL still has an `NrsMapContainer`
   content type and that content is retrieved during the resolution process.
   
   This brings us to the next change. The `nrs_get` API was modified to return an `Option<SafeUrl>`,
   where `None` will now be returned if the container XOR-URL is used. In this case, the resolver will
   know to return `NrsMapContainer` data, otherwise, it will return the `NrsEntry` with the target URL.
   One exception is worth mentioning: if the NRS URL uses the registered topname and that topname
   *doesn't* link to anything, `NrsMapContainer` data will also be returned. To make these extensions,
   small unit tests were added to the `NrsMap` and several tests were added to the resolver to cover
   these scenarios.
   
   With these changes in place, the CLI could then be updated. The `cat` and `dog` commands were
   modified to print the NRS map when `NrsMapContainer` data was returned. Previously, the map was
   printed as a table, but this isn't really suitable for presentation because the table crate doesn't
   have the ability to use multi-line cells and the target links are too large, so I changed it to
   print a list. Test cases were added for both commands, which should hopefully prevent us breaking
   the feature again.
   
   Finally, some usability changes were also made to `nrs` commands to give the user the XOR-URL of the
   container. This can be useful to them if they want to list all the entries in a map.

### Refactor (BREAKING)

 - <csr-id-74fc4ddf31af51e0036eb164e6b5c4f0864bd08c/> reorganise internal node messaging mod and remove redundant private APIs
   - Fixing missing AE update to be sent to sibling new Elders upon split.

## v0.56.0 (2022-02-08)

<csr-id-3f75bf8da770a6167c396080b3ad8b54cfeb27e2/>
<csr-id-471d910f2b6d8952569c3dc4b2dd31fe7aa30dfa/>
<csr-id-f1e0e4564c2b581352f6dc6a0ba32259452494c5/>

### Chore

 - <csr-id-3f75bf8da770a6167c396080b3ad8b54cfeb27e2/> safe_network-0.56.0/sn_api-0.55.0/sn_cli-0.48.0
 - <csr-id-471d910f2b6d8952569c3dc4b2dd31fe7aa30dfa/> improve acronym consistency
 - <csr-id-f1e0e4564c2b581352f6dc6a0ba32259452494c5/> ensure to use backoff.reset() before use
   otherwise the default settings appear to be used

### New Features (BREAKING)

 - <csr-id-0b5ebece1240deb56360b238b96e2aece4a6d314/> fix typo

## v0.55.5 (2022-02-07)

<csr-id-d56f3c7c0bfc7bd6d045eb80a68a885615e73115/>
<csr-id-473e4b0ca27767f4e1326629670f43ff5de5bc86/>
<csr-id-dab972dccdf968e706f0c7599154e188dc74bf48/>

### Chore

 - <csr-id-d56f3c7c0bfc7bd6d045eb80a68a885615e73115/> safe_network-0.55.5
 - <csr-id-473e4b0ca27767f4e1326629670f43ff5de5bc86/> tweak client commend retries and backoff
 - <csr-id-dab972dccdf968e706f0c7599154e188dc74bf48/> adding a trace log when updating a section member state

### New Features

 - <csr-id-0c04071e2184c4e74fa8c9f264a380585e84369e/> add `flame` arg to use cargo flamegraph
   uses the launcher option to enable flamegraph generation per node

## v0.55.4 (2022-02-05)

<csr-id-a0e9c52951e81018249a3bcf3b6300b3ad592136/>
<csr-id-1c78a27d31bb6530274d4ac5cecbce817fad5313/>

### Chore

 - <csr-id-a0e9c52951e81018249a3bcf3b6300b3ad592136/> safe_network-0.55.4
 - <csr-id-1c78a27d31bb6530274d4ac5cecbce817fad5313/> client errors when a SAP has less than expected elders

### New Features (BREAKING)

 - <csr-id-0b5ebece1240deb56360b238b96e2aece4a6d314/> fix typo

### Bug Fixes

 - <csr-id-e0e085273524f8e457887548cd68238060590a7b/> only send reg commands to a subset of elders

## v0.55.3 (2022-02-04)

<csr-id-86975f228f31303597a707e158005e44c86de1cc/>
<csr-id-064a7ed00ca84edd5b5f86640ae868c1cb202590/>

### Chore

 - <csr-id-86975f228f31303597a707e158005e44c86de1cc/> safe_network-0.55.3/sn_api-0.54.1/sn_cli-0.47.1
 - <csr-id-064a7ed00ca84edd5b5f86640ae868c1cb202590/> one more error msg on client send

### New Features

 - <csr-id-0c04071e2184c4e74fa8c9f264a380585e84369e/> add `flame` arg to use cargo flamegraph
   uses the launcher option to enable flamegraph generation per node

### New Features (BREAKING)

 - <csr-id-208e73c732ae5bac184bf5848c0490b98c9a0364/> move to q2p 0.28 / quinn 0.8

### Bug Fixes

 - <csr-id-df808e1d3c559408f5704590493d0aa97d9c2a19/> section_peers refactored to avoid dashmap deadlock.
   Also updates dashmap to latest version, which uses parking lot for locks,
   which is theoretically faster too

## v0.55.2 (2022-02-02)

<csr-id-b1d35af9c2ae6cb386c6726a432c02b9d44973c2/>
<csr-id-637ec03f9922e5d3dd0c8703eba5019256f4ec06/>
<csr-id-d42b34d54adfc16b7947fa5728fcde80ad4f49c7/>
<csr-id-7cd63377de16dd2962961f2dd936df3276fe8d6d/>
<csr-id-f73e00cc67fae6090b9c991ac4e06999ea28f22e/>
<csr-id-0c4c7d8c973bdb1e1d055798c17be2065f0334e2/>
<csr-id-2479daea6b05c7b680bf6062407f507ad8692d57/>
<csr-id-7f01106c8266abec07c9522cb69e4fbbb493f386/>

### Refactor

 - <csr-id-b1d35af9c2ae6cb386c6726a432c02b9d44973c2/> use the churn signature for deciding both the destination and members to relocate

### Chore

 - <csr-id-637ec03f9922e5d3dd0c8703eba5019256f4ec06/> safe_network-0.55.2
 - <csr-id-d42b34d54adfc16b7947fa5728fcde80ad4f49c7/> log chain update logic
 - <csr-id-7cd63377de16dd2962961f2dd936df3276fe8d6d/> all env var to override cmd_timeout
 - <csr-id-f73e00cc67fae6090b9c991ac4e06999ea28f22e/> session records all failed connection ids
 - <csr-id-0c4c7d8c973bdb1e1d055798c17be2065f0334e2/> minor reorganisation of code
 - <csr-id-2479daea6b05c7b680bf6062407f507ad8692d57/> failcmd with any error after retries
 - <csr-id-7f01106c8266abec07c9522cb69e4fbbb493f386/> simplify data replication cmds and flow

## v0.55.1 (2022-02-01)

<csr-id-25259b221120c8c9258ffdfae8883c65ead38677/>
<csr-id-2ec86e28246031084d603768ffa1fddf320a10a2/>
<csr-id-81298bb8ce1d93d7a418ff340d866fc00d9f60a4/>
<csr-id-fa6014e581f52615971701572a1635dfde922fb6/>

### Test

 - <csr-id-25259b221120c8c9258ffdfae8883c65ead38677/> unit test liveness tracker basics

### Chore

 - <csr-id-2ec86e28246031084d603768ffa1fddf320a10a2/> safe_network-0.55.1/sn_api-0.54.0/sn_cli-0.47.0
 - <csr-id-81298bb8ce1d93d7a418ff340d866fc00d9f60a4/> log data reorganisation

### Refactor (BREAKING)

 - <csr-id-fa6014e581f52615971701572a1635dfde922fb6/> remove the relocation promise step for Elders
   - Both Adults and Elders now receive the `Relocate` message with all the relocation details
   without any previous or additional steps required to proceed with their relocation.

### New Features

 - <csr-id-b2b0520630774d935aca1f2b602a1de9479ba6f9/> enable cmd retries
   Previously a command error would simply error out and fail.
   Now we use an exponential backoff to retry incase errors
   can be overcome

### Bug Fixes

 - <csr-id-16bd75af79708f88dcb7086d04fac8475f3b3190/> fix liveness tracking logic and keep track of newly joining adults
 - <csr-id-6aae745a4ffd7302fc74d5d548ca464066d5203f/> make log files rotate properly

## v0.55.0 (2022-01-28)

<csr-id-c52de7a75ce4bbf872b215a14258b5e7778bfb34/>
<csr-id-366eee25f4b982d5a20d90168368a1aa14aa3181/>
<csr-id-2cc7323ec8b62f89f2e4247c5d6ab56f78eda2ce/>
<csr-id-2746cff087fd33aff523bc2df07a3462d05c6de1/>
<csr-id-fa6014e581f52615971701572a1635dfde922fb6/>

### Refactor

 - <csr-id-c52de7a75ce4bbf872b215a14258b5e7778bfb34/> keep section Left/Relocated members in a separate archive container
   - Also remove an archived section member when its state is previous to last five Elder churn events

### Chore

 - <csr-id-366eee25f4b982d5a20d90168368a1aa14aa3181/> safe_network-0.55.0/sn_api-0.53.0/sn_cli-0.46.0
 - <csr-id-2cc7323ec8b62f89f2e4247c5d6ab56f78eda2ce/> clarify sap switch logic
 - <csr-id-2746cff087fd33aff523bc2df07a3462d05c6de1/> renaming apis for clarity of purpose
   Prior we had many 'send to target' type namings for send apis in node
   it wasn't always clear who/what could be sent throuh this and we've seen some enduser msgs
   being dropped there. (Perhaps only Backpressure reports... but still).
   
   This aims to clarify those APIs a bit, and provides a simple method to check
   if the src

### Bug Fixes

 - <csr-id-6aae745a4ffd7302fc74d5d548ca464066d5203f/> make log files rotate properly

### Refactor (BREAKING)

 - <csr-id-fa6014e581f52615971701572a1635dfde922fb6/> remove the relocation promise step for Elders
   - Both Adults and Elders now receive the `Relocate` message with all the relocation details
   without any previous or additional steps required to proceed with their relocation.

## v0.54.2 (2022-01-25)

<csr-id-77d45d7498facb18a611a8edcf608a3c0ff0b2c8/>
<csr-id-9d3a164efa7f38b7eeafc3936160458871956f5b/>
<csr-id-90cdbce964c8fd293d85b9a28f1b0d2d2a046b08/>
<csr-id-99df748624a36e1d2457cb81f74e8cd8accb8633/>

### Other

 - <csr-id-77d45d7498facb18a611a8edcf608a3c0ff0b2c8/> run many client test on ci

### Chore

 - <csr-id-9d3a164efa7f38b7eeafc3936160458871956f5b/> safe_network-0.54.2
 - <csr-id-90cdbce964c8fd293d85b9a28f1b0d2d2a046b08/> send queries to random elders in a section
   data holders are deterministic, which means any elder could
   successfully route to the relevant adults.
   
   This change should spread the load of popular data amongst
   all elders
 - <csr-id-99df748624a36e1d2457cb81f74e8cd8accb8633/> keep all logs by default

### Bug Fixes

 - <csr-id-501ce9838371b3fdcff6439a4a4f13a70423988f/> avoid infinite loop
 - <csr-id-16bd75af79708f88dcb7086d04fac8475f3b3190/> fix liveness tracking logic and keep track of newly joining adults

### New Features

 - <csr-id-67a3313105b31cee9ddd58de6e510384b8ae1397/> randomize elder contact on cmd.
   Priously we always took the first 3 from the SAP's elders vec. This should spread
   commands out over all elders more fairly.

## v0.54.1 (2022-01-24)

<csr-id-5a90ec673edcb36ae954b0af6144bae7d8243cd7/>
<csr-id-1ff0ab735994f8cbfae6abd58fea0e71a90f742c/>

### Chore

 - <csr-id-5a90ec673edcb36ae954b0af6144bae7d8243cd7/> safe_network-0.54.1
 - <csr-id-1ff0ab735994f8cbfae6abd58fea0e71a90f742c/> dont keep wiremsg in mem while sendin bytes

### Bug Fixes

 - <csr-id-151993ac6442224079dc02bfe476d2dfbc1a411b/> put connection ensurance and sending within the same spawned thread
 - <csr-id-39c8f5dceed5639daa226432f3d6f3c9bf19a852/> check Joined status on Info level

## v0.54.0 (2022-01-22)

<csr-id-8f58c5d900ce00d90bf7421f34122f4ca5ff5601/>
<csr-id-3b5ce194213a7090ee83c02b0043700cda230796/>
<csr-id-d84ac520676b83f39d5cf356e89ac3ec2d6478cc/>
<csr-id-0190f0305980bdaee30f9f2ab5eb5510149916db/>
<csr-id-d59999adcecd36380ed3a6855fdedfbef8107914/>
<csr-id-4e517a1fd57e63a3b5381ecd0cdf9db4f762e03f/>
<csr-id-3dc23278c6a4fabc250b27f4312f5c51f0f271a4/>
<csr-id-274cc12ca37da5ff536a7b4ab59f803546ccefe9/>
<csr-id-1b3c0eb4443cff3f4575164907a7a1fbf92cebe2/>
<csr-id-d96ded11ca74e75dde3dcc0d0b865712895d3dcc/>
<csr-id-7a7752f830785ec39d301e751dc75f228d43d595/>
<csr-id-959e909d8c61230ea143702858cb7b4c42caffbf/>

### chore (BREAKING)

 - <csr-id-8f58c5d900ce00d90bf7421f34122f4ca5ff5601/> making section members list non optional in AntiEntropyUpdate message type


### Refactor

 - <csr-id-3b5ce194213a7090ee83c02b0043700cda230796/> remove one layer of indirection

### Other

 - <csr-id-d84ac520676b83f39d5cf356e89ac3ec2d6478cc/> ignore many_client tests

### Chore

 - <csr-id-0190f0305980bdaee30f9f2ab5eb5510149916db/> safe_network-0.54.0/sn_api-0.52.0/sn_cli-0.45.0
 - <csr-id-d59999adcecd36380ed3a6855fdedfbef8107914/> make op_id u64, and use this for pending_query tracking
   Previously we were tracking pending queries using the data address, which with many queries from many clients for the same data would obviously be problematic.
   
   Here we use the operation_id And the peername to track returned data...
 - <csr-id-4e517a1fd57e63a3b5381ecd0cdf9db4f762e03f/> address some review comments
 - <csr-id-3dc23278c6a4fabc250b27f4312f5c51f0f271a4/> update remaining places
 - <csr-id-274cc12ca37da5ff536a7b4ab59f803546ccefe9/> resolve clippy failures after rebase
 - <csr-id-1b3c0eb4443cff3f4575164907a7a1fbf92cebe2/> wait before attempting to make_contact again
   Prior contact rounds did not between attempts. This gives the prev attempts more chance
   to succeed before we give up on them
 - <csr-id-d96ded11ca74e75dde3dcc0d0b865712895d3dcc/> dont fail client send w/ closed conn
   Closing a connection would cause a client error in send before this.
   Now we check if we had an application layer close... something intentional.
   If so we do not consider this a failure at the client and can
   continue thereafter.
   
   qp2p is upgraded to send a reason on close that we can check against.
 - <csr-id-7a7752f830785ec39d301e751dc75f228d43d595/> update year on files modified 2022

### Bug Fixes

 - <csr-id-8f5f7fd0c57db4a2cd29dfe2dc617b796fea97f4/> make client sending message non-blocking
 - <csr-id-36daaebd84e07c3ff55477e24598e6b9d0c0b314/> correct correlation_id for query response
 - <csr-id-56b6b11447010485c860a55e6dbd035d826091d1/> client wait longer for AE update got triggered
 - <csr-id-10123e6a3d18129fcb5c0090d409a02b2f762139/> using try_send to avoid blocking
 - <csr-id-1e4049fc944f8bcab0e51671219896854a3bffaa/> return CmdError to the caller
 - <csr-id-c0cc9807df988ca3a871720edadc4ff19e028f63/> sign to client msg with node's Ed key
 - <csr-id-cc38b212179645d99f5cf4f26cc5c43393d9db72/> JoinResponse::Redirect doesn't update prefix_map

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
 - <csr-id-65282654943bdf156f144e0449a85dc4e2956f58/> randomize elder contact on cmd.
   Priously we always took the first 3 from the SAP's elders vec. This should spread
   commands out over all elders more fairly.

### Refactor (BREAKING)

 - <csr-id-959e909d8c61230ea143702858cb7b4c42caffbf/> removing the internal routing layer

## v0.53.0 (2022-01-20)

<csr-id-8f58c5d900ce00d90bf7421f34122f4ca5ff5601/>
<csr-id-37d2522619e49212572aa37034e0ba1857679dc7/>
<csr-id-565711d4bc58f18181d279e5f29d9adb3acb051a/>
<csr-id-4eef4d3a9356d6a0724c0c670809e8d9fd1f11c0/>
<csr-id-c222370b52befaedebe895fcf7feb2c1da99aeaa/>
<csr-id-a018022e4eb298a9a66513c770c0edc1b415a886/>
<csr-id-893df83ec9a1c4231f09b4e7bad9c15ef2944036/>
<csr-id-08cde8586e018e19df6b35270bd999f45d30596e/>
<csr-id-fdda641b3874c425616352daf7db7429219bb858/>
<csr-id-923930acb3769cfa7047954a1fee1853ec9e3062/>
<csr-id-82f39977ec965429269a31639ce83be9749b5d80/>
<csr-id-b06bc6eb0fd210b6abc9039d1f20ab8d93befc16/>
<csr-id-f7f59369d09a4d25a94328d031f00cc61b187eed/>
<csr-id-96a955e5b124db3250c3d0fd09926cec10322632/>
<csr-id-57749b7d0671423fe205447bc84d9f8bfc99f54b/>
<csr-id-252f81e155dce2b8d774c8999b730e763674d93f/>
<csr-id-40b00cbd899c72889439b9f94b34b173ff3af837/>
<csr-id-941b83f3960c84cfee86a8c818233fbbc403c189/>
<csr-id-56bed01344ca5ec74a49c2a41116ef76fb33e3b4/>
<csr-id-026458c6afa9848bb58a694da6ae4b81196b8f19/>
<csr-id-f22e4dce22492079170cbaeb8c29b3911faaf89a/>

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


### Test

 - <csr-id-37d2522619e49212572aa37034e0ba1857679dc7/> send CmdAck for client cmd during test

### Refactor

 - <csr-id-565711d4bc58f18181d279e5f29d9adb3acb051a/> moving logic to check for split into a helper function
 - <csr-id-4eef4d3a9356d6a0724c0c670809e8d9fd1f11c0/> re-enable init with populated register
 - <csr-id-c222370b52befaedebe895fcf7feb2c1da99aeaa/> store cap within the register
 - <csr-id-a018022e4eb298a9a66513c770c0edc1b415a886/> only check permissions once

### Other

 - <csr-id-893df83ec9a1c4231f09b4e7bad9c15ef2944036/> fix client config query timeout check
 - <csr-id-08cde8586e018e19df6b35270bd999f45d30596e/> cleanup and cover both public/private
 - <csr-id-fdda641b3874c425616352daf7db7429219bb858/> use AE on all client tests

### Chore

 - <csr-id-923930acb3769cfa7047954a1fee1853ec9e3062/> safe_network-0.53.0/sn_api-0.51.0/sn_cli-0.44.0
 - <csr-id-82f39977ec965429269a31639ce83be9749b5d80/> moving relocation module within routing::Core where it belongs
 - <csr-id-b06bc6eb0fd210b6abc9039d1f20ab8d93befc16/> qp2p update/ clippy fixes and code comment cleanup
 - <csr-id-f7f59369d09a4d25a94328d031f00cc61b187eed/> allow 1gb uncompressed logs by default
 - <csr-id-96a955e5b124db3250c3d0fd09926cec10322632/> lessen nesting and indentation
 - <csr-id-57749b7d0671423fe205447bc84d9f8bfc99f54b/> solving new clippy findings
 - <csr-id-252f81e155dce2b8d774c8999b730e763674d93f/> Don't tie idle_timeout to query_timeout
 - <csr-id-40b00cbd899c72889439b9f94b34b173ff3af837/> remove unused old file
 - <csr-id-941b83f3960c84cfee86a8c818233fbbc403c189/> fix additional wrongly setup test cases
 - <csr-id-56bed01344ca5ec74a49c2a41116ef76fb33e3b4/> removing unused routing Command and API
 - <csr-id-026458c6afa9848bb58a694da6ae4b81196b8f19/> add checks to ensure all nodes have joined

### Refactor (BREAKING)

 - <csr-id-f22e4dce22492079170cbaeb8c29b3911faaf89a/> removing MIN_AGE and the concept of mature node
   - Also refactoring routing internal API for querying section peers from its network knowledge.

## v0.52.13 (2022-01-06)

<csr-id-155ee032ee56cbbb34928f2d14529273ccb69559/>

### Chore

 - <csr-id-155ee032ee56cbbb34928f2d14529273ccb69559/> safe_network-0.52.13/sn_api-0.50.6

### Bug Fixes

 - <csr-id-edefd2d7860e6f79c07060e078cdaa433da9e804/> use latest PK to send DataExchange messages

## v0.52.12 (2022-01-06)

<csr-id-81282318ae2da1793e66f28f0c8b3c0b2272a529/>
<csr-id-7928ce6411d237078f7ed3ba83823f438f3a991f/>
<csr-id-f1afc5933dc782bc6a7840cd12cebb32a189a5df/>
<csr-id-4f29c285a0b48220df1f1c6c52c4b487350eae08/>
<csr-id-db515397771f117b3bf095e1a4afb897eb4acafe/>
<csr-id-bebdae9d52d03bd13b679ee19446452990d1e2cf/>

### Refactor

 - <csr-id-81282318ae2da1793e66f28f0c8b3c0b2272a529/> clients and node read/write to a global prefix_map
 - <csr-id-7928ce6411d237078f7ed3ba83823f438f3a991f/> move to adults

### Other

 - <csr-id-f1afc5933dc782bc6a7840cd12cebb32a189a5df/> increase testnet startup interval on CI only.
   Lower the default value for smoother development networks

### Chore

 - <csr-id-4f29c285a0b48220df1f1c6c52c4b487350eae08/> safe_network-0.52.12
 - <csr-id-db515397771f117b3bf095e1a4afb897eb4acafe/> sn_cli-0.43.1
 - <csr-id-bebdae9d52d03bd13b679ee19446452990d1e2cf/> rename dest to dst

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

## v0.52.11 (2022-01-06)

<csr-id-99d012ef529df78ef4c84f5e6ea99d3a77414797/>

### Chore

 - <csr-id-99d012ef529df78ef4c84f5e6ea99d3a77414797/> safe_network-0.52.11/sn_api-0.50.5/sn_cli-0.43.2

### Bug Fixes

 - <csr-id-42d3b932f69c3680e8aaff488395406983728ed6/> only send data cmd to 3 elders

### Documentation

 - <csr-id-1d36f019159296db6d5a6bb4ebeec8699ea39f96/> add doc for usage of DKG in safe_network
   - also moves README from the .github folder so it doesn't show up in the
   repo root

## v0.52.10 (2022-01-05)

<csr-id-9c9a537ad12cc809540df321297c8552c52a8648/>
<csr-id-7b0cd4d183a9f82d1d78dbb56572e5bf36714a0a/>
<csr-id-233e64af3b2af63bbba06ba8f43d0a7becece913/>
<csr-id-9f1bd81b269ec06fd5d379ab4b07f18f814da865/>
<csr-id-bf16c5ea7051386064233443921438cbbd79d907/>
<csr-id-012e91a60e01cd2ce9155d5c56045f211865ff2c/>

### Refactor

 - <csr-id-9c9a537ad12cc809540df321297c8552c52a8648/> ties up the loose ends in unified data flow

### Chore

 - <csr-id-7b0cd4d183a9f82d1d78dbb56572e5bf36714a0a/> safe_network-0.52.10
 - <csr-id-233e64af3b2af63bbba06ba8f43d0a7becece913/> ignore GA failing test
 - <csr-id-9f1bd81b269ec06fd5d379ab4b07f18f814da865/> misc fixes
 - <csr-id-bf16c5ea7051386064233443921438cbbd79d907/> log EntryHash human readable
 - <csr-id-012e91a60e01cd2ce9155d5c56045f211865ff2c/> remove unused dep async recursion

### Bug Fixes

 - <csr-id-ad202c9d24da5a5e47d93a3793d665e1b844b38d/> dont .await on threads spawned for subtasks
 - <csr-id-e18c88019d37ab4f7618dde1a90e19ddf94db1c7/> VersioinHash use Display for encode

### New Features

 - <csr-id-c47aeca2618d54a8b3d7b21c82f6ac6e62acd10c/> refactor node to support tokio-console and resolve issues

## v0.52.9 (2022-01-04)

<csr-id-a64c7e0414b77f545cb8cdbf64af0fb7212d1f2e/>

### Chore

 - <csr-id-a64c7e0414b77f545cb8cdbf64af0fb7212d1f2e/> safe_network-0.52.9/sn_api-0.50.4

### Bug Fixes

 - <csr-id-edefd2d7860e6f79c07060e078cdaa433da9e804/> use latest PK to send DataExchange messages

### New Features

 - <csr-id-fff2d52b700dfe7ec9a8909a0d5adf176de4c5c7/> substract space from used_space on register delete

## v0.52.8 (2022-01-04)

<csr-id-c790077bebca691f974000278d5525f4b011b8a7/>
<csr-id-5214d5e7f84a3c1cf213097a5d55bfb293f03324/>
<csr-id-6ccb792c18481ffd8218cd7c27b28d8a10d1f528/>

### Refactor

 - <csr-id-c790077bebca691f974000278d5525f4b011b8a7/> rename blob to file

### Chore

 - <csr-id-5214d5e7f84a3c1cf213097a5d55bfb293f03324/> safe_network-0.52.8
 - <csr-id-6ccb792c18481ffd8218cd7c27b28d8a10d1f528/> some detailed logging for debugging

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

## v0.52.7 (2022-01-04)

<csr-id-da3bbe16084b71ec42343035087848c8f6996ec4/>
<csr-id-b35d0ccd0305e3e87a9070bc2a57287dbe6b2633/>
<csr-id-2ebd1b4fcab47bc86980860379891bb041ff2aa4/>
<csr-id-40d1844e0b28578e8b8c6b270151dbb86961a766/>
<csr-id-bd0382fef77947935584418ee91720001f5f269c/>

### Refactor

 - <csr-id-da3bbe16084b71ec42343035087848c8f6996ec4/> record it in an atomic usize

### Other

 - <csr-id-b35d0ccd0305e3e87a9070bc2a57287dbe6b2633/> reorder network startup
   - Remove unverifiable asserts around query msg counts
   * Alters order of testnet startup vs building tests. As yet unverified,
   but building the tests is CPU intensive and could possibly slow down
   section startup leading to some test failures as we dont have all elders
   yet... Just a theory. We shall see...
 - <csr-id-2ebd1b4fcab47bc86980860379891bb041ff2aa4/> use zsh to repeat e2e tests
   We've seen occasionally flakey e2e tests. This sets the basic e2e tests to run 5 times back to back, which should increase confidence in each PR being merged and reduce fluke ci-passes

### Chore

 - <csr-id-40d1844e0b28578e8b8c6b270151dbb86961a766/> safe_network-0.52.7
 - <csr-id-bd0382fef77947935584418ee91720001f5f269c/> remove unneeded retry_loop from reg tests
   Splits out batching into its own tests that takes significantly longer than the rest

### New Features

 - <csr-id-19d7d3ad04a428485738ffc916b4f14388ad10d5/> optimise disk space checks by doing them less often

## v0.52.6 (2022-01-04)

<csr-id-838c495c8858b85c693da1a3e45baafa57ba03ea/>
<csr-id-36ba7d5e85e304d6d0ff3210429923beed77d25b/>
<csr-id-0a70425fb314de4c165da54fdc29a127ae900d81/>
<csr-id-1caad35e0e744e50b2bd15dda8dbd3adbacb87c7/>
<csr-id-78e41a3d8387f9b53bfd5e078ae7aa44fe1ea6d4/>
<csr-id-04ee3e74e5573f903be29cd89416ce9e5758cf00/>
<csr-id-a9913a0f7140d302fcaf24264fc1982f2ad3d06b/>
<csr-id-97171142548772a466188f2e6d9f24072f28640d/>
<csr-id-a7e7908537d63e4071323a59cbbd036edcff41ab/>
<csr-id-18cee44f08aa4f83ad477cc82a29525e9d339e0c/>
<csr-id-1d19c02668dfa3739a350b15c7310daec93d9837/>
<csr-id-2492ea84e9fcba5d19022e171ec6b60c341ee59b/>
<csr-id-8884f9453a859bd63b378337aab326889d153768/>
<csr-id-690b24f14c3183640d04d048c2a7f4ac79f6e6c7/>
<csr-id-3b3a38130bd6943b7c53b1cf74321d89dd4af1da/>
<csr-id-8effd08c16cfd2e0715ee0d00092e267f72a8cf0/>

### Refactor

 - <csr-id-838c495c8858b85c693da1a3e45baafa57ba03ea/> rename + use glob const
   - Adds max_num_faulty_elders based on const assumption for
   max number of faulty Elders as 1/3.
   - Adds at_least_one_correct_elder based on max_num_faulty_elders.
   
   These numbers were made dependent on global consts, as was asked for in
   the todo comment prior to this change.
   
   Also, the number of Elders to whom a client sends a chunk is not based
   on the chunk copy count, but on our assumptions on max number of faulty
   Elders, hence renaming the variable.

### Other

 - <csr-id-36ba7d5e85e304d6d0ff3210429923beed77d25b/> add 5mb many client test
   This adds a test to check if many many client connections will result in high mem usage at nodes.

### Chore

 - <csr-id-0a70425fb314de4c165da54fdc29a127ae900d81/> safe_network-0.52.6/sn_api-0.50.2
 - <csr-id-1caad35e0e744e50b2bd15dda8dbd3adbacb87c7/> refactor wait until higher_prio
   Moves wait into its own function and keeps semaphore acquisition separate
 - <csr-id-78e41a3d8387f9b53bfd5e078ae7aa44fe1ea6d4/> reduce semaphore wait timeout
 - <csr-id-04ee3e74e5573f903be29cd89416ce9e5758cf00/> make unstable-command-prioritisation required for wait
   Otherwise no waits are performed and we just need to acquire the semaphore to proceed
 - <csr-id-a9913a0f7140d302fcaf24264fc1982f2ad3d06b/> disable waiting for higher prio messages before continuing
 - <csr-id-97171142548772a466188f2e6d9f24072f28640d/> change dkg interval
 - <csr-id-a7e7908537d63e4071323a59cbbd036edcff41ab/> improve formatting of priority match statements
 - <csr-id-18cee44f08aa4f83ad477cc82a29525e9d339e0c/> limit time waiting to acquire priority permit
 - <csr-id-1d19c02668dfa3739a350b15c7310daec93d9837/> tidy up some log messages
 - <csr-id-2492ea84e9fcba5d19022e171ec6b60c341ee59b/> put command prioritisation behind a feature flag.
   It has been noted that enabling prioritisation flat out may result in obscuring underlying bugs
   (such as those hidden behind the DkgUnderway flag). This way we can keep code and work out
   where/when to activate this thereafter (perhaps when ndoes are under stress eg).
 - <csr-id-8884f9453a859bd63b378337aab326889d153768/> add PermitInfo type
   This more clearly states what we're storing as we track Permit use for prioritisation.
 - <csr-id-690b24f14c3183640d04d048c2a7f4ac79f6e6c7/> stop everything if something more important is going on
 - <csr-id-3b3a38130bd6943b7c53b1cf74321d89dd4af1da/> ensure child commands dont remove root permit early
 - <csr-id-8effd08c16cfd2e0715ee0d00092e267f72a8cf0/> use constants for cmd priorities

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

## v0.52.5 (2022-01-04)

<csr-id-ab00eca916d6ed8a0a137004a6b9fd24e7217a70/>

### Chore

 - <csr-id-ab00eca916d6ed8a0a137004a6b9fd24e7217a70/> safe_network-0.52.5

### Bug Fixes

 - <csr-id-9f89966e02c3e0ba0297377b4efdf88a31ec1e87/> restore original behavior
 - <csr-id-22f6dbcba8067ef777b7bea0393673bb669893b4/> make all atomic ops relaxed

## v0.52.4 (2022-01-04)

<csr-id-4bb2adf52efdac6187fffc299018bf13f3398e14/>
<csr-id-0a5fca96af4d2627b842591775e77c09201ed655/>
<csr-id-3af8ddbee91f3403b86914d352a970e366d1fa40/>

### Chore

 - <csr-id-4bb2adf52efdac6187fffc299018bf13f3398e14/> safe_network-0.52.4/sn_api-0.50.1
 - <csr-id-0a5fca96af4d2627b842591775e77c09201ed655/> do not skip AE check for AE-Probe messages
 - <csr-id-3af8ddbee91f3403b86914d352a970e366d1fa40/> set testnet interval to 10s by default once again

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

## v0.52.3 (2022-01-03)

<csr-id-6b9b1590bce9423130210007ddd6e9c14b51819d/>
<csr-id-be8989971c68ac5aee43380223000a1f400252fc/>
<csr-id-292466119e2d99c36043e7f2247b1bde9ec9ced9/>
<csr-id-d54c955aa768ab08ef8193b7e36cb96822bc6cb8/>
<csr-id-72f79d46fc56fdda9215f8a9d6f95bcdf323a66f/>
<csr-id-e9a9cc1096e025d88f19390ad6ba7398f71bc800/>

### Refactor

 - <csr-id-6b9b1590bce9423130210007ddd6e9c14b51819d/> do not check if space available
 - <csr-id-be8989971c68ac5aee43380223000a1f400252fc/> remove repetitive code

### Chore

 - <csr-id-292466119e2d99c36043e7f2247b1bde9ec9ced9/> safe_network-0.52.3
 - <csr-id-d54c955aa768ab08ef8193b7e36cb96822bc6cb8/> fmt
 - <csr-id-72f79d46fc56fdda9215f8a9d6f95bcdf323a66f/> typos and shortcuts

### Refactor (BREAKING)

 - <csr-id-e9a9cc1096e025d88f19390ad6ba7398f71bc800/> removing unnecessary Error type definitions

### New Features

 - <csr-id-5fadc027b4b7dd942275ef6041b2bdb92b062bed/> remove unused kv store, cleanup chunk store
 - <csr-id-7f1ead2f0f33558583989a7314d2c121a6f1280a/> disk chunk store
 - <csr-id-eb30133e773124b46cdad6e6fa7f3c65f066946a/> make read and delete async

## v0.52.2 (2022-01-03)

<csr-id-d490127b17d53a7648f9e97aae690b232188b034/>
<csr-id-9d7e6843701465a13de7b528768273bad02e920e/>
<csr-id-e9a9cc1096e025d88f19390ad6ba7398f71bc800/>

### Chore

 - <csr-id-d490127b17d53a7648f9e97aae690b232188b034/> safe_network-0.52.2
 - <csr-id-9d7e6843701465a13de7b528768273bad02e920e/> fix docs to match parameters

### Refactor (BREAKING)

 - <csr-id-e9a9cc1096e025d88f19390ad6ba7398f71bc800/> removing unnecessary Error type definitions

## v0.52.1 (2022-01-03)

<csr-id-e38925e07d69432db310fc8ec9803200ea964ab2/>
<csr-id-36dbea03d879c07f922be36a124ad8d44c3c2d0e/>
<csr-id-48ef44e9db01d74119a2b1c9f7e7dae4ce988c57/>
<csr-id-619d142de8999d536e41ac5fe402a94d934689fb/>

### Chore

 - <csr-id-e38925e07d69432db310fc8ec9803200ea964ab2/> safe_network-0.52.1/sn_api-0.48.0/sn_cli-0.41.0
 - <csr-id-36dbea03d879c07f922be36a124ad8d44c3c2d0e/> further integrate and organize dir
 - <csr-id-48ef44e9db01d74119a2b1c9f7e7dae4ce988c57/> move routing into node dir
 - <csr-id-619d142de8999d536e41ac5fe402a94d934689fb/> remove expired queries from pending queries cache

### Bug Fixes

 - <csr-id-f00de3a9cbc43eabeb0d46804a92b88204a48ea4/> respond only once to client for every chunk query

## v0.52.0 (2021-12-22)

<csr-id-6b59ad852f89f033caf2b3c7dfcfa3019f8129e8/>
<csr-id-1188ed58eed443b4b8c65b591376f2f9a21acc0d/>

### Chore

 - <csr-id-6b59ad852f89f033caf2b3c7dfcfa3019f8129e8/> safe_network-0.52.0/sn_api-0.47.0/sn_cli-0.40.0

### Bug Fixes

 - <csr-id-f00de3a9cbc43eabeb0d46804a92b88204a48ea4/> respond only once to client for every chunk query

### Refactor (BREAKING)

 - <csr-id-1188ed58eed443b4b8c65b591376f2f9a21acc0d/> minor refactor to error types definitions

## v0.51.7 (2021-12-20)

<csr-id-c76c3ab638188cba38911f037829c209fcc45fc3/>
<csr-id-07e19b53cd8eaa777f4c83369d2ee1076c75fe4f/>
<csr-id-069013b032b7fd8d8a58ca0d75f6ea357abf5593/>

### Chore

 - <csr-id-c76c3ab638188cba38911f037829c209fcc45fc3/> safe_network-0.51.7
 - <csr-id-07e19b53cd8eaa777f4c83369d2ee1076c75fe4f/> reduce query attempts
 - <csr-id-069013b032b7fd8d8a58ca0d75f6ea357abf5593/> removing exponential backoff when retrying queries

### Bug Fixes

 - <csr-id-44e57667f48b1fd9bce154652ae2108603a35c11/> send_query_with_retry_count now uses that retry count in its calculations

## v0.51.6 (2021-12-17)

<csr-id-79b2d0a3f52de0335323773936dee9bdbe12a0cf/>

### Chore

 - <csr-id-79b2d0a3f52de0335323773936dee9bdbe12a0cf/> safe_network-0.51.6

### New Features

 - <csr-id-1078e59be3a58ffedcd3c1460385b4bf00f18f6b/> use upload_and_verify by default in safe_client

### Bug Fixes

 - <csr-id-f083ea9200a1fccfc7bd21117f34c118702a7a70/> fix: adult choice per chunk and handling db errors

## v0.51.5 (2021-12-16)

<csr-id-45df3d71cc4b3185602b9d27b8cb0f5bf65a4b43/>
<csr-id-9cf7c72a94386f2cbe6f803be970c6debfbcb99b/>

### Chore

 - <csr-id-45df3d71cc4b3185602b9d27b8cb0f5bf65a4b43/> safe_network-0.51.5
 - <csr-id-9cf7c72a94386f2cbe6f803be970c6debfbcb99b/> attempt to retrieve all bytes during upload_and_verify

### New Features

 - <csr-id-1078e59be3a58ffedcd3c1460385b4bf00f18f6b/> use upload_and_verify by default in safe_client
 - <csr-id-3e91a43676e1252a872a24872db8f91c729bfb15/> keeping Chunks wich were retrieved in a local client cache

### Bug Fixes

 - <csr-id-d8ad6a9b1d6a530b7f597bccf6a6bed6d8546ac0/> populate client sender success counter

## v0.51.4 (2021-12-16)

<csr-id-17d7906656bec401d6b39cc3551141112a3d77c4/>

### Chore

 - <csr-id-17d7906656bec401d6b39cc3551141112a3d77c4/> safe_network-0.51.4

### New Features

 - <csr-id-3e91a43676e1252a872a24872db8f91c729bfb15/> keeping Chunks wich were retrieved in a local client cache

### Bug Fixes

 - <csr-id-d8ad6a9b1d6a530b7f597bccf6a6bed6d8546ac0/> populate client sender success counter

## v0.51.3 (2021-12-16)

<csr-id-92cdb53391652651bfe9a47c5a0261ba10f38148/>
<csr-id-9be440b36db07e1c04ab688b44ef91e4a56ed576/>

### Refactor

 - <csr-id-92cdb53391652651bfe9a47c5a0261ba10f38148/> defining a const for Chunks cache size

### Chore

 - <csr-id-9be440b36db07e1c04ab688b44ef91e4a56ed576/> safe_network-0.51.3/sn_api-0.46.1

### New Features

 - <csr-id-3e91a43676e1252a872a24872db8f91c729bfb15/> keeping Chunks wich were retrieved in a local client cache

## v0.51.2 (2021-12-16)

<csr-id-595541b83284a5c5b60fbc00e47b1146117d7613/>
<csr-id-f522d9aad0071cd3b47b1a3e4c178b0100ec71d6/>

### Chore

 - <csr-id-595541b83284a5c5b60fbc00e47b1146117d7613/> safe_network-0.51.2
 - <csr-id-f522d9aad0071cd3b47b1a3e4c178b0100ec71d6/> add more logs regarding storage levels

## v0.51.1 (2021-12-15)

<csr-id-dcbb67fc699d7cb1f3a2c4632bcb8a5738916091/>
<csr-id-e52a0c063f747e0be1525f07f8f759f4b9d042a7/>
<csr-id-df87fcf42c46dc28e6926394c120fbf2c715e54a/>

### Chore

 - <csr-id-dcbb67fc699d7cb1f3a2c4632bcb8a5738916091/> safe_network-0.51.1
 - <csr-id-e52a0c063f747e0be1525f07f8f759f4b9d042a7/> add recursion limit for clippy
 - <csr-id-df87fcf42c46dc28e6926394c120fbf2c715e54a/> move Peer to root of sn
   This will let us use the same struct across routing and clients

### New Features

 - <csr-id-591ce5f6dfa14143114fbd16c1d632a8dbe4a2d1/> Use Peers to ensure connection reuse

## v0.51.0 (2021-12-15)

<csr-id-c685838d8f9c10b0f4e7541fe201862bb84e8555/>
<csr-id-7c34940401b0115105d9b818b9f93c39d7669eed/>

### Chore

 - <csr-id-c685838d8f9c10b0f4e7541fe201862bb84e8555/> safe_network-0.51.0
 - <csr-id-7c34940401b0115105d9b818b9f93c39d7669eed/> add in distinct client errors for comms failure
   Some situations were mislabelled as InsuffcientElderConnection, implying failed connections, but it was actually a failure in our network knowledge.
   
   This commit adds clarifying error types for this situation

### New Features (BREAKING)

 - <csr-id-134dfa29b3698fb233095194305d9bbbba2875c7/> different flavors of upload data API, with/without verifying successful upload

## v0.50.0 (2021-12-14)

<csr-id-653f653a775a101679904ab75c8012a72dfdedfb/>

### Chore

 - <csr-id-653f653a775a101679904ab75c8012a72dfdedfb/> safe_network-0.50.0

### New Features (BREAKING)

 - <csr-id-134dfa29b3698fb233095194305d9bbbba2875c7/> different flavors of upload data API, with/without verifying successful upload

## v0.49.3 (2021-12-14)

<csr-id-edb8de8b4d923e97d68eed40a7953f38461b0281/>
<csr-id-36ca20e606899ecbdea24d845c34ba11ab889cf7/>

### Test

 - <csr-id-edb8de8b4d923e97d68eed40a7953f38461b0281/> adding a test for retrieving Blob with range over data length

### Chore

 - <csr-id-36ca20e606899ecbdea24d845c34ba11ab889cf7/> safe_network-0.49.3

## v0.49.2 (2021-12-14)

<csr-id-62d747969b739172910aabca6fcb273d2827fc8a/>
<csr-id-dd50d1d860aa4ca60b6c0d5a525b45d88ddf432e/>

### Chore

 - <csr-id-62d747969b739172910aabca6fcb273d2827fc8a/> safe_network-0.49.2
 - <csr-id-dd50d1d860aa4ca60b6c0d5a525b45d88ddf432e/> set default AE_WAIT to be 0

### New Features

 - <csr-id-86ba4234a29137518c73b18becbf018993e104a8/> on initial contact put all known elders into the contact pool.
   Previously if we knew a sap, we only took 3 nodes
 - <csr-id-99add55c5ca5a3e3da2130797083dd449da2f7cd/> make contact via register get
   We previously use a chunk get, but this in itself will cause more network messaging than a simple register get which can be dealt with by elders only
 - <csr-id-2bdc03578f3d9144a097a947ab44d0c1286f6180/> use backoff during make contact instead of standard_wait.
   This should help aleviate any pressure on already struggling nodes, especially if a low wait was set to make tests run faster eg (where ae may not always be needed

## v0.49.1 (2021-12-13)

<csr-id-69ae8c20e91dd9959ebfa5456efdf9c218a9d66f/>
<csr-id-2e7bc0b782da6231f54edc440fa555fa754d294c/>

### Chore

 - <csr-id-69ae8c20e91dd9959ebfa5456efdf9c218a9d66f/> safe_network-0.49.1
 - <csr-id-2e7bc0b782da6231f54edc440fa555fa754d294c/> set DEFAULT_QUERY_TIMEOUT to 120s

### New Features

 - <csr-id-86ba4234a29137518c73b18becbf018993e104a8/> on initial contact put all known elders into the contact pool.
   Previously if we knew a sap, we only took 3 nodes
 - <csr-id-99add55c5ca5a3e3da2130797083dd449da2f7cd/> make contact via register get
   We previously use a chunk get, but this in itself will cause more network messaging than a simple register get which can be dealt with by elders only
 - <csr-id-2bdc03578f3d9144a097a947ab44d0c1286f6180/> use backoff during make contact instead of standard_wait.
   This should help aleviate any pressure on already struggling nodes, especially if a low wait was set to make tests run faster eg (where ae may not always be needed

## v0.49.0 (2021-12-13)

<csr-id-88c78e8129e5092bd120d0fc6c9696673550be9d/>
<csr-id-6f5516d8bb677462ea6def46aa65a1094767d68c/>
<csr-id-1a81c8f04f947d2b83d3cd726c00ad66927f5225/>
<csr-id-a569474a8be9c11ab73ec7ad1ad157f69827b4d3/>

### New Features

 - <csr-id-35467d3e886d2824c4f9e4586666cab7a7960e54/> limit the relocations at one time

### Bug Fixes

 - <csr-id-8955fcf9d69e869725177340d1de6b6b1e7a203b/> read_from client API was incorrectly using provided length value as an end index
   - Minor refactoring in sn_api moving the SafeData struct into its own file.

### chore (BREAKING)

 - <csr-id-88c78e8129e5092bd120d0fc6c9696673550be9d/> rename enum variants for improved clarity on flows

### Chore

 - <csr-id-6f5516d8bb677462ea6def46aa65a1094767d68c/> safe_network-0.49.0
 - <csr-id-1a81c8f04f947d2b83d3cd726c00ad66927f5225/> replace use of deprecated dalek function
   See the comment on the function for an explanation as to why this was changed.
 - <csr-id-a569474a8be9c11ab73ec7ad1ad157f69827b4d3/> tidy up some log messages

## v0.48.0 (2021-12-10)

<csr-id-7eda2760da82b3079d5eee2f97e2d15ac8da0d57/>
<csr-id-9eff03598ba09aa339180d7ecd57b50174180095/>
<csr-id-58632a27d271140fc4d777f25a76b0daea582426/>
<csr-id-0d4343c8fa56749d3ec9390e298d1d6384573a67/>
<csr-id-85709655b0ce38246515658b956aa9b8f67cb55a/>

### Test

 - <csr-id-7eda2760da82b3079d5eee2f97e2d15ac8da0d57/> checking Relocation during CI

### Chore

 - <csr-id-9eff03598ba09aa339180d7ecd57b50174180095/> safe_network-0.48.0
 - <csr-id-58632a27d271140fc4d777f25a76b0daea582426/> minor improvement to client log msgs related to configured timeouts
 - <csr-id-0d4343c8fa56749d3ec9390e298d1d6384573a67/> remove Generic client error
 - <csr-id-85709655b0ce38246515658b956aa9b8f67cb55a/> safe_network-0.47.0

### New Features

 - <csr-id-fd31bd2ef5ccc9149f8f0a2844c52af60bff3840/> use Vec of DataCmd instead of wrapper struct
 - <csr-id-84ebec0e1b2b3e1107d673734125aadbdb108472/> upgrade batch to write ahead log with index

### Bug Fixes

 - <csr-id-661e994cb452242a1d7c831ab88b9a66a244faff/> avoid change name by mistake when Join
 - <csr-id-58bd78e8a4fc34e7f76a9f449301b9216cb6a1d4/> complete the relocation flow

### New Features (BREAKING)

 - <csr-id-565e57619557e2b63f028eb214b59fc69b77fc37/> register op batching

## v0.47.0 (2021-12-08)

### New Features

 - <csr-id-fd31bd2ef5ccc9149f8f0a2844c52af60bff3840/> use Vec of DataCmd instead of wrapper struct
 - <csr-id-84ebec0e1b2b3e1107d673734125aadbdb108472/> upgrade batch to write ahead log with index

### Bug Fixes

 - <csr-id-58bd78e8a4fc34e7f76a9f449301b9216cb6a1d4/> complete the relocation flow

### New Features (BREAKING)

 - <csr-id-565e57619557e2b63f028eb214b59fc69b77fc37/> register op batching

## v0.46.6 (2021-12-07)

<csr-id-b5e9dcc5b13b1eda711d4760d9feb8dc929a0c43/>
<csr-id-05f6d98cf21f0158f4b5161484c7c15a0561b6f4/>

### Chore

 - <csr-id-b5e9dcc5b13b1eda711d4760d9feb8dc929a0c43/> safe_network-0.46.6
 - <csr-id-05f6d98cf21f0158f4b5161484c7c15a0561b6f4/> clippy tidyup for rust 1.57

### Bug Fixes

 - <csr-id-ddde0d6767f5230eca7a760423538fa5e4f640a2/> order of target length in ElderConn error corrected
 - <csr-id-1ea33e0c63fead5097b57496b2fd997201a2c531/> keep trying even with connection error w/ always-joinable flag
 - <csr-id-8e480e5eb5cd38f9bfb3b723aa1d0dd5e4be3122/> aovid overwrite pervious session by  accident
 - <csr-id-fdc4ba6bb36dff85126c8273cdfc67f9b07e2175/> multiple fixes for DKG-AE
   - discard DKG session info for older sessions

## v0.46.5 (2021-12-02)

<csr-id-86577846e845c110c49e15c95c6bd5595db51773/>

### Chore

 - <csr-id-86577846e845c110c49e15c95c6bd5595db51773/> safe_network-0.46.5

### Bug Fixes

<csr-id-01a5c961fc02f9ca8d6f60286306d5efba460e4e/>

 - <csr-id-fdc4ba6bb36dff85126c8273cdfc67f9b07e2175/> multiple fixes for DKG-AE
   - discard DKG session info for older sessions

## v0.46.4 (2021-12-02)

<csr-id-de3051e7e809a8f75507c54f3cf053a4244fdf19/>

### Chore

 - <csr-id-de3051e7e809a8f75507c54f3cf053a4244fdf19/> safe_network-0.46.4

### Bug Fixes

 - <csr-id-01a5c961fc02f9ca8d6f60286306d5efba460e4e/> avoid network_knowledge loopup when sending DkgRetry or DkgSessionInfo
 - <csr-id-27868bc51ba2dd12cc584396c53a158706d0c07b/> avoid network_knowledge lookup when sending DKGNotReady or DkgSessionUnknown

## v0.46.3 (2021-12-02)

<csr-id-69e9be2a1567bfa211af7e9d7595381d9a0a3b38/>

### Chore

 - <csr-id-69e9be2a1567bfa211af7e9d7595381d9a0a3b38/> safe_network-0.46.3

### New Features

 - <csr-id-5b003745ce3421b0554f35a6198d58cf67a1f4c6/> aggregate joins per section key
   Previously we only did one aggregation. But errors could arise if we had to resend and scrap aggregation.
   
   This means we can aggregate any number of churning section keys.

### Bug Fixes

 - <csr-id-27868bc51ba2dd12cc584396c53a158706d0c07b/> avoid network_knowledge lookup when sending DKGNotReady or DkgSessionUnknown

## v0.46.2 (2021-12-01)

<csr-id-260eaabd2d1b0c26dec9febc963929e65d7ec912/>
<csr-id-b4f0306fb945cce096de7f68c3cf6ece6905786d/>
<csr-id-204f927220c8bd1829ac89feaed1c48a8034e80b/>
<csr-id-2ef8e45f53ff6925e56321ed0ebc922d2d4dd9b9/>
<csr-id-1eea84476294c1dbfbfe72fe0e9acdb997762595/>

### Chore

 - <csr-id-260eaabd2d1b0c26dec9febc963929e65d7ec912/> safe_network-0.46.2
 - <csr-id-b4f0306fb945cce096de7f68c3cf6ece6905786d/> remove sig check on join ApprovalShare receipt
   Now we have a map of keys and aggregators, we dont need this check here.
   If keys dont match, we'll get an error and initiate join sending again below
 - <csr-id-204f927220c8bd1829ac89feaed1c48a8034e80b/> remove Joinrejection DkgUnderway
 - <csr-id-2ef8e45f53ff6925e56321ed0ebc922d2d4dd9b9/> leave longer wait on testnet startup
 - <csr-id-1eea84476294c1dbfbfe72fe0e9acdb997762595/> dont send dkg underway

### New Features

 - <csr-id-5b003745ce3421b0554f35a6198d58cf67a1f4c6/> aggregate joins per section key
   Previously we only did one aggregation. But errors could arise if we had to resend and scrap aggregation.
   
   This means we can aggregate any number of churning section keys.

### Bug Fixes

 - <csr-id-8de5ecf50008793546760621d906f23e8fe9791f/> make has_split.sh detect SplitSuccess correctly
 - <csr-id-8cfc5a4adf8a31bf6916273597cf4c18aa5adc46/> reduce backoff time; avoid start DKG shrink elders

## v0.46.1 (2021-12-01)

<csr-id-bf55f9b7e3b96319de4423e19333bf3b16fd1c78/>

### Chore

 - <csr-id-bf55f9b7e3b96319de4423e19333bf3b16fd1c78/> safe_network-0.46.1

### Bug Fixes

 - <csr-id-8de5ecf50008793546760621d906f23e8fe9791f/> make has_split.sh detect SplitSuccess correctly
 - <csr-id-8cfc5a4adf8a31bf6916273597cf4c18aa5adc46/> reduce backoff time; avoid start DKG shrink elders

## v0.46.0 (2021-12-01)

<csr-id-f3d3ab2b059040ff08b6239c8a6583c64eac160e/>
<csr-id-8ea94983b37b1d559358a62d6ca075b97c193f0d/>
<csr-id-3f77429e8bd659a5b2e7aa377437fac1b3d709c0/>
<csr-id-a7e058536ae6ae27228bd2254ea6465c5eface35/>

### New Features

 - <csr-id-24f1aa0208e3e474862c18b21ea9f048cb6abf25/> expose API for calculate Blob/Spot address without a network connection

### chore (BREAKING)

 - <csr-id-f3d3ab2b059040ff08b6239c8a6583c64eac160e/> include remote addrs in listerners threads log entries


### Chore

 - <csr-id-8ea94983b37b1d559358a62d6ca075b97c193f0d/> safe_network-0.46.0
 - <csr-id-3f77429e8bd659a5b2e7aa377437fac1b3d709c0/> unify skip port forwarding arguments
   We decided to rename the `--skip-igd` argument to `--skip-auto-port-forwarding`. I had initially
   renamed the arguments on the CLI and `sn_launch_tool` to `--disable-port-forwarding`, but it turned
   out that wasn't a completely accurate description of what's happening.
   
   We are now uniformly referring to this argument as `--skip-auto-port-forwarding`, which is quite an
   accurate description, as it skips (but not disables) the software-based port forwarding, leaving you
   to 'manually' setup port forwarding on your router, if need be.
   
   Also fixed some clippy warnings.
 - <csr-id-a7e058536ae6ae27228bd2254ea6465c5eface35/> safe_network-0.45.0

## v0.45.0 (2021-11-30)

<csr-id-f3d3ab2b059040ff08b6239c8a6583c64eac160e/>

### New Features

 - <csr-id-24f1aa0208e3e474862c18b21ea9f048cb6abf25/> expose API for calculate Blob/Spot address without a network connection

### chore (BREAKING)

 - <csr-id-f3d3ab2b059040ff08b6239c8a6583c64eac160e/> include remote addrs in listerners threads log entries


## v0.44.5 (2021-11-30)

<csr-id-d364a7e364827b2d71f196a9f897b7d613bbab94/>
<csr-id-0da3c99785e026075214b7cfa7933f64420aa00f/>
<csr-id-aaab10b3a5a44d9ec844757c71ac091016f51fd1/>
<csr-id-9deb9e7e2ae5d909eeb745e9021cf7660ed55dc3/>

### Other

 - <csr-id-d364a7e364827b2d71f196a9f897b7d613bbab94/> only check split count for health check for now
 - <csr-id-0da3c99785e026075214b7cfa7933f64420aa00f/> log desired elder count during health check

### Chore

 - <csr-id-aaab10b3a5a44d9ec844757c71ac091016f51fd1/> safe_network-0.44.5
 - <csr-id-9deb9e7e2ae5d909eeb745e9021cf7660ed55dc3/> add SN_AE_WAIT env var
   This unhooks the post-Put wait for AE messages from the query timeout.
   This should keep PUTs fast while allowing longer query timeouts as necessary, while
   also allowing it to be easily configured

## v0.44.4 (2021-11-30)

<csr-id-984c5f83e3f4d889dc4e0583b09571e540357cf9/>
<csr-id-e841ae0cf3caf852e8a48b559dcffb64b7fcecad/>

### Chore

 - <csr-id-984c5f83e3f4d889dc4e0583b09571e540357cf9/> safe_network-0.44.4
 - <csr-id-e841ae0cf3caf852e8a48b559dcffb64b7fcecad/> spawn writing prefix_map to disk as separate thread

### New Features

 - <csr-id-d2c352198c0e421cf4b7327a17c4343dc693b2dd/> start a fresh round of joins when aggregation is erroring.
   Aggregation may error, perhaps due to section churn. If we half or more bad signaturs that would prevent aggregation. We clean the slate and start a fresh round of aggregation and join messaging.

### Bug Fixes

 - <csr-id-758661d87edfef0fee6676993f1e96b3fd2d26bb/> don't crash node on failed signature aggregation on join
 - <csr-id-b79e8d6dbd18a4a0787d326a6b10146fa6b90f30/> retry joins on section key mismatch.
   We could sometimes get a key sent from nodes with a newer section_pk
   than we were expecting.
   
   We should then retry

## v0.44.3 (2021-11-30)

<csr-id-ec3dd4991535bb22235e2d1d413dd93489b8aedf/>
<csr-id-9885536afe337bf109e3ab18eec054100bf3fd82/>
<csr-id-cf67a374ae5aa5a9f95d39357fdbd937eb5e1a1a/>

### Chore

 - <csr-id-ec3dd4991535bb22235e2d1d413dd93489b8aedf/> safe_network-0.44.3
 - <csr-id-9885536afe337bf109e3ab18eec054100bf3fd82/> increase join node aggregatin expiration.
   Could be that some shares take time to come in under load/membership changes
 - <csr-id-cf67a374ae5aa5a9f95d39357fdbd937eb5e1a1a/> improve node logging

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

## v0.44.2 (2021-11-30)

<csr-id-51b0f0068c9a279da9a1edf45509cf80a90e663d/>
<csr-id-a3c5ef63f37ec0d2f98d45176e27da7de31baabc/>

### Chore

 - <csr-id-51b0f0068c9a279da9a1edf45509cf80a90e663d/> safe_network-0.44.2
 - <csr-id-a3c5ef63f37ec0d2f98d45176e27da7de31baabc/> move key_share check into network_knowledge_update

### Bug Fixes

 - <csr-id-5523d0fcca7bbbbd05b6d125692f5fbd1a8f50d7/> avoid use outdated keyshare status

## v0.44.1 (2021-11-29)

<csr-id-14c84c9db23557626e4889eff0ff403a574dccad/>
<csr-id-10e135b4f77dcd30de99eb7bc370ba1e15bbd148/>

### Chore

 - <csr-id-14c84c9db23557626e4889eff0ff403a574dccad/> safe_network-0.44.1
 - <csr-id-10e135b4f77dcd30de99eb7bc370ba1e15bbd148/> remove Core::is_dkg_underway flag which is not necessary

### Bug Fixes

 - <csr-id-0e505be2f57ab427cd3ed8c9564fd8b84909f6f3/> restore `ClientMsg` authority check
   The `AuthorityProof` struct is designed to be a proof of valid
   authority, by ensuring all possible constructors either generate or
   validate a signature. This can only be guaranteed if the field remains
   module-private. At some point it seems the field was made `pub(crate)`,
   which meant we were missing an authority check for some `ClientMsg`s,

## v0.44.0 (2021-11-26)

<csr-id-75a4b537573d4e5e8767e38fa7d1b1126dffe148/>

### Chore

 - <csr-id-75a4b537573d4e5e8767e38fa7d1b1126dffe148/> safe_network-0.44.0

### New Features

 - <csr-id-cc256bdf3f493f8841be07b9d7634c486e21a1cf/> avoid broadcasting DKG messages
 - <csr-id-60e6a5b1c1db4011c6bcdb473be3dbfea8858d6a/> revamp joins/rejoins to follow BRB

### Bug Fixes

 - <csr-id-0e505be2f57ab427cd3ed8c9564fd8b84909f6f3/> restore `ClientMsg` authority check
   The `AuthorityProof` struct is designed to be a proof of valid
   authority, by ensuring all possible constructors either generate or
   validate a signature. This can only be guaranteed if the field remains
   module-private. At some point it seems the field was made `pub(crate)`,
   which meant we were missing an authority check for some `ClientMsg`s,
 - <csr-id-23bd9b23243da1d20cee8e7f06adc90f0894c2e7/> fix node joining age for genesis section Adults
 - <csr-id-5dd0961c5172ed890b2453eb33332380e4757ad3/> refactor join flows to use the new Peer type
 - <csr-id-1822857e35b888df75bea652a2858cfd34b8c2fc/> remove redundant send_node_approval

### Bug Fixes (BREAKING)

 - <csr-id-94c91e16135e524202608b2dc6312102890eab94/> include the original message when requesting DkgSessionInfo
   - also early-returns when signature verification fails / the key is
   outdated

## v0.43.0 (2021-11-26)

<csr-id-0f2786483016adb2f44199cd4e5bf55e8c54adc3/>
<csr-id-ee6431e9509dd5b02ec2eca4c10ea17c7dddcfc9/>
<csr-id-d73deebbf94e65898d29455d6d4ed951ca59b814/>
<csr-id-3fa2957fe4f4a1f14b9001e101270dec4e386c14/>
<csr-id-612f8437429d4d9852f54fa0e297b059b8e86827/>
<csr-id-0ab3c35e424e60ffbed0edf8c1f71da48daa0978/>
<csr-id-c7b9d748b12e6d1e23d2a2972f2553af50343b22/>
<csr-id-d85d896ceaa68785797b1c525ae8653ef26f2708/>
<csr-id-1c5929bcc0638e8ff05dd495b3cedfdeff7540aa/>
<csr-id-e6df2ec1d2c8ab02bdaa95ed4c3288cfc0e532cc/>
<csr-id-ddd3726a0a2a131ae7c05a4190edb8bdc7fa3665/>
<csr-id-52a39b4c41b09ec33c60bc09c55fc63484e524cf/>
<csr-id-f69c0e319b639b678b46b73724a77ed7172cfec3/>
<csr-id-7ae741f93f39c437618bde150458eedd7663b512/>
<csr-id-c78f4703a970e8b7466b091ad331d0f2233aa9a3/>
<csr-id-2003e34cc26c450486151fa1df74ffea61c8c9ae/>
<csr-id-c051e02afdcbd2c21deec19b1baebfb8293d515c/>
<csr-id-016b7d47f708308490c1883db4ba239fc2add59b/>
<csr-id-580f2e26baf787bdee64c093a03ae9470668619d/>
<csr-id-da39113a7f97f6881643e66a48e19f2e06368e8d/>

### Refactor

 - <csr-id-0f2786483016adb2f44199cd4e5bf55e8c54adc3/> prefer existing connection when merging `Peer`s
   This is a probably-premature optimisation to minimise when we take write
   locks on `Peer::connection`. Previously when merging connections we
   would prioritise the source connection (e.g. we would overwrite an
   existing connection in the target). In particular, this would cause
   write-locking even if it was the same connection, which could lead to
   needless contention when trying to read connections.
   
   This commit changes to instead retain the existing connection if we have
   one, and does so in a way that minimises the likelihood that we need to
   take a write lock at all.
 - <csr-id-ee6431e9509dd5b02ec2eca4c10ea17c7dddcfc9/> prevent concurrent sends from opening many connections
   This is an optimisation in favour of reducing the number of connections
   we open, at the cost of reduced parallelism when sending multiple
   messages to the same unconnected `Peer` (which is fairly common when
   performing DKG rounds with new peers, e.g.).
   
   The implementation is based on giving `Peer` the responsibility of
   creating new connections if necessary, meaning it can take the write
   lock and see if any competing attempt already set a new connection - and
   if so, avoid creating another one. A 'fast path' is included that will
   return immediately if there's an existing valid connection.
 - <csr-id-d73deebbf94e65898d29455d6d4ed951ca59b814/> only recreate failed connections in `Comm::send`
   `Peer`s may hold connections that have been lost due to timeouts etc. As
   such, we want to handle connection loss when sending by trying again and
   forcing a reconnection. We would previously do this by passing a
   `force_reconnection` flag, however in the event of concurrent sends this
   might lead to more connections being opened than necessary.
   
   The new approach tests the existing connection to see if it's the same
   one that previously failed, and will only reconnect if so.
   
   This does not fully protect from 'stampedes' of new connections, since
   the same issue exists when the `Peer` has no connection (e.g. many
   concurrent sends for the same peer may lead to many connections being
   made), but this will be followed up separately.
 - <csr-id-3fa2957fe4f4a1f14b9001e101270dec4e386c14/> simplify `Comm::send` error handling
   There's now a dedicated error type for `Comm::send_to_one` that better
   represents the possible states. In particular, a connection error could
   never be from a reused connection, and we avoid having to wrap errors as
   much.
 - <csr-id-612f8437429d4d9852f54fa0e297b059b8e86827/> replace `Comm::send` closure with normal `fn`
   The closure was rather massive. Separating out makes it easier to see
   what's going on.
 - <csr-id-0ab3c35e424e60ffbed0edf8c1f71da48daa0978/> reuse system message sender connections
   Whenever we receive a valid system message, we now merge the sender's
   connection into our network knowledge, if applicable.
   
   An example of when this is useful is when a node uses their SAP to cold-
   call an elder who they have not yet spoken to. Without this change,
   there would be additional connections since the elder would reply over a
   new connection. With this change in place, they will instead be able to
   reply over the same sending connection.
 - <csr-id-c7b9d748b12e6d1e23d2a2972f2553af50343b22/> merge sender connection into SAP when joining
   This sets up joining nodes to reuse the connection they already have
   with the approving elder.
 - <csr-id-d85d896ceaa68785797b1c525ae8653ef26f2708/> remove connection pooling
   This completely removes `ConnectedPeers`. Prior changes ensure that
   connection usage remains fairly nominal (though still higher than
   optimal), and that clients are still reachable without reconnection.
 - <csr-id-1c5929bcc0638e8ff05dd495b3cedfdeff7540aa/> reuse more `Peer`s in DKG sessions
   We previously started to check our knowledge of section members for
   existing `Peer`s, however in some cases it seems that elders are not
   present in `members` (perhaps deliberately?). To ensure maximum reuse,
   we now also check the SAP for reusable connections.
 - <csr-id-e6df2ec1d2c8ab02bdaa95ed4c3288cfc0e532cc/> get section name from prefix and get section key from our
 - <csr-id-ddd3726a0a2a131ae7c05a4190edb8bdc7fa3665/> replace dkg message backlog with message exchange
   when a node has not received enough sig. shares to start processing a
 - <csr-id-52a39b4c41b09ec33c60bc09c55fc63484e524cf/> send a retry join message if DKG is underway
 - <csr-id-f69c0e319b639b678b46b73724a77ed7172cfec3/> adults join with age lesser than the youngest elder in genesis section

### Other

 - <csr-id-7ae741f93f39c437618bde150458eedd7663b512/> fixes node join/rejoin tests to support the BRB join process

### Chore

 - <csr-id-c78f4703a970e8b7466b091ad331d0f2233aa9a3/> safe_network-0.43.0
 - <csr-id-2003e34cc26c450486151fa1df74ffea61c8c9ae/> use bls-dkg 0.9.0
 - <csr-id-c051e02afdcbd2c21deec19b1baebfb8293d515c/> reduce the testnet node numbers and make name deduce more
 - <csr-id-016b7d47f708308490c1883db4ba239fc2add59b/> fixes to node joining and DKG flows
 - <csr-id-580f2e26baf787bdee64c093a03ae9470668619d/> add comments and use backoff for DKGUnderway on joining
 - <csr-id-da39113a7f97f6881643e66a48e19f2e06368e8d/> backoff BRB join response

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

## v0.42.0 (2021-11-25)

<csr-id-ca21d1e97fcd28ca351887636affffff78e3aeb3/>
<csr-id-bad083332fdcc3a0bc3c3f13628c00315f1b2519/>

### Chore

 - <csr-id-ca21d1e97fcd28ca351887636affffff78e3aeb3/> safe_network-0.42.0/sn_api-0.43.0
 - <csr-id-bad083332fdcc3a0bc3c3f13628c00315f1b2519/> remove url deps from sn

### New Features

 - <csr-id-a8c0e645d9bf11834557049045cca95f8d715a77/> deduce expected_prefix from expected_age

### New Features (BREAKING)

 - <csr-id-3fe9d7a6624fe5503f80395f6ed11426b131d3b1/> move Url to sn_api

## v0.41.4 (2021-11-25)

<csr-id-8b8a3616673405005d77868dc397bd7542ab3ea7/>

### Chore

 - <csr-id-8b8a3616673405005d77868dc397bd7542ab3ea7/> safe_network-0.41.4/sn_api-0.42.0

### New Features

 - <csr-id-a8c0e645d9bf11834557049045cca95f8d715a77/> deduce expected_prefix from expected_age
 - <csr-id-b96034796d479352340caaf3d6e6b6d7e6e425ad/> adapt registers to hold vec u8 instead of Url in sn

## v0.41.3 (2021-11-24)

<csr-id-4b72bfc9a6c3a0db4821e7ebf1f4b5daa7cc56d1/>
<csr-id-df25e4920c570771f6813ca03da02f6dfc8e59fb/>
<csr-id-9d657249f3d0391e7c3bbeae6f81909993802cc7/>

### Chore

 - <csr-id-4b72bfc9a6c3a0db4821e7ebf1f4b5daa7cc56d1/> safe_network-0.41.3
 - <csr-id-df25e4920c570771f6813ca03da02f6dfc8e59fb/> sn_api-0.41.0
 - <csr-id-9d657249f3d0391e7c3bbeae6f81909993802cc7/> add comment about vec size

### New Features

 - <csr-id-b96034796d479352340caaf3d6e6b6d7e6e425ad/> adapt registers to hold vec u8 instead of Url in sn

## v0.41.2 (2021-11-24)

<csr-id-bf0488d239fc52ce03c1f380ae0986810d753007/>
<csr-id-aca0fb7a451ffc25c6e34479cc9201fef42796be/>
<csr-id-c34c28dd8776196a6c1c475b7f3ec3be709c0b6d/>
<csr-id-a973039178af33b859d421cf36571de49cceff17/>

### Refactor

 - <csr-id-bf0488d239fc52ce03c1f380ae0986810d753007/> simplify `Proposal::as_signable_bytes`
   The `SignableView` struct isn't used anywhere else, and although it's
   annoying having to write `bincode::serialize` a bunch of times, it is
   simpler with less indirection.
 - <csr-id-aca0fb7a451ffc25c6e34479cc9201fef42796be/> replace `ProposalUtils` with inherent impl
   Now that `Proposal` lives outside of `messaging`, there's no point in
   having the functions in a trait.
 - <csr-id-c34c28dd8776196a6c1c475b7f3ec3be709c0b6d/> duplicate `Proposal` in `routing`
   This simplifies a bunch of conversion to/from the `messaging` and
   `routing` variants of `NodeState` and `SectionAuthorityProvider`.
   Additionally, this lets us reuse more connections when we handle a
   proposal ourselves, such as when responding to a `JoinRequest`.
   
   This had a surprisingly high impact on connection use, shaving off 500
   or so when forming an 11 node network.

### Chore

 - <csr-id-a973039178af33b859d421cf36571de49cceff17/> safe_network-0.41.2/sn_api-0.40.1

### New Features

 - <csr-id-b96034796d479352340caaf3d6e6b6d7e6e425ad/> adapt registers to hold vec u8 instead of Url in sn

## v0.41.1 (2021-11-23)

<csr-id-b456f2f610ea57e5d8a4811fbca5a26175434645/>
<csr-id-950a4eece1a11f70b2aef71cc11404603bb2ec5c/>
<csr-id-f73364efa66718b92e04f24d4546c1e248198ce8/>
<csr-id-ad1617f96954a810898484e5b00b5b8b12495f4e/>
<csr-id-d8ec5a81ae566e8d7068592e01cff4e808b1cad1/>
<csr-id-62aa668d5777058ae617f8952cfcb62be002abf3/>
<csr-id-63432eb2e528401ae67da8eea0c82837ab42fc18/>
<csr-id-7a875a88c911b5a9db59a55b81c94b995e2b95ae/>
<csr-id-5514d9c06ee400e983a258ba2eb37bff1abf0dd0/>
<csr-id-1848189aa780f2f6aabddb3564b816c72f9bee6e/>

### Refactor

 - <csr-id-b456f2f610ea57e5d8a4811fbca5a26175434645/> renaming some variables and functions for better readability
 - <csr-id-950a4eece1a11f70b2aef71cc11404603bb2ec5c/> reuse connections during join flow
   This only saves a few connections, but it proves out the machinery for
   carrying them around. Once additional mechanism was added,
 - <csr-id-f73364efa66718b92e04f24d4546c1e248198ce8/> replace `Sender` with `UnknownPeer`
   The `UnknownPeer` struct is more general, and will make it easier to
   propagate connections into more places (e.g. this already propagates
   connections into `HandleNodeMsg`). It's also generally easier to use
   than `Sender`, and doesn't need any special casing for test-only
   purposes.
   
   The downside is that we lose the 'sending to ourself' representation.
   This wasn't being used so far though, so it's only a hypothetical. The
   comment on `UnknownPeer` attempts to document that 'rough edge' (since
   we would otherwise expect to only know peers either through
   advertisement, where the name would be known, or through direct
   connection, where the connection would be present).

### Other

 - <csr-id-ad1617f96954a810898484e5b00b5b8b12495f4e/> revert "chore(release):"
   This reverts commit d794bc6862692004432699b8deae1a52a1ae1207.
   
   This release was a mistake and modified the changelog in an incorrect way.
 - <csr-id-d8ec5a81ae566e8d7068592e01cff4e808b1cad1/> revert "chore(release): safe_network-0.42.0/sn_api-0.41.0"
   This reverts commit 63432eb2e528401ae67da8eea0c82837ab42fc18.
   
   This release was duplicating everything that was in 0.41.0, probably because the tags weren't
   correct.

### Chore

 - <csr-id-62aa668d5777058ae617f8952cfcb62be002abf3/> safe_network-0.41.1
 - <csr-id-63432eb2e528401ae67da8eea0c82837ab42fc18/> safe_network-0.42.0/sn_api-0.41.0
 - <csr-id-7a875a88c911b5a9db59a55b81c94b995e2b95ae/> add 'connectedness' to `Peer`'s `Display`
 - <csr-id-5514d9c06ee400e983a258ba2eb37bff1abf0dd0/> inherit span on `Comm::send` incoming message listener
   Since we spawn a detached future, we need to explicitly inherit the
   span.
 - <csr-id-1848189aa780f2f6aabddb3564b816c72f9bee6e/> add a feature for increased `WireMsg` debug info
   When we serialize to a `WireMsg`, we lose visibility of the exact
   contents of the message. Serialization is necessary in order to obtain a
   signature for the message, so we can't simply hold the message
   unserialised until send time (at least, not without passing around a
   keypair, which we don't want to do).
   
   As a hacky alternative, an `unstable-wiremsg-debuginfo` feature has been
   added. When enabled, this adds a `payload_debug` field to `WireMsg`
   which can be set to contain a debug representation of the message
   contents. Since `WireMsg` is constructed with `Bytes`, its necessary for
   callers to set this themselves.

## v0.41.0 (2021-11-23)

<csr-id-b1297f2e42cdd7f7945c9fd5b4012086f8298b85/>
<csr-id-680f9431bc75efea455dd7c6985e41c11740dc3e/>
<csr-id-eb0291082e49d1114c853b214f55b8a25e18d1e1/>
<csr-id-93bffbf1343621a8d5187649d1bb6d2a81cba793/>
<csr-id-1268fb6e46e4ba061ba7f724917371d8e0db0092/>
<csr-id-b70a1053adc267780215636dd80a759c45d533d5/>
<csr-id-1d62fcefd5d44ef0df84b8126ba88de1127bd2bd/>
<csr-id-20c1d7bf22907a44be2cc9585dd2ac55dd2985bf/>
<csr-id-182c4db4531c77fa6c67b8267cd7895b49f26ae1/>
<csr-id-02f1325c18779ff02e6bb7d903fda4519ca87231/>
<csr-id-1bf17662ecae4e1f3e9969ac03a8f543f57f2cd0/>
<csr-id-90f2979a5bcb7f8e1786b5bdd868793d2fe924b4/>
<csr-id-14328b3d5b2579e4f038624c2353ec36c45fa9ed/>
<csr-id-4f5e84da96d7277dfc4e385ff03edf6c1d84991e/>
<csr-id-14fdaa6537619483e94424ead5751d5ab41c8a01/>
<csr-id-dbdda1392325e8a08a6a28988208769552d5e543/>
<csr-id-e8e44fb69dae7fe1cae3bbf39f7b03a90df44dd7/>
<csr-id-86c83bedf061b285b0d4f3effb54cc89a9fbd84d/>
<csr-id-590bfea538bb78fd20bb9574c3ea9ed6dcceced0/>
<csr-id-85f2b5fd2adbf1f57e184c9e687fd14929b911e7/>
<csr-id-14c6ab95da62a144a798665ade6da626fb5ee6f2/>
<csr-id-30d220c180c7586de56282fafe52b54c8b619860/>
<csr-id-6c0209187797bb27fa1529dd2f0ee8116975a02d/>
<csr-id-a328d70720a795b62ac05a68454abf1034e97b9d/>
<csr-id-a98cc810409a4ccc78e3c7a36b38b5f00cfcb23a/>
<csr-id-da1fe8dda6325e05b4d976d2b77d084c06d98cb0/>
<csr-id-078f45148af2da8d1690490720ef5f95555d42cd/>
<csr-id-4c9b20ee58be9ca3579c7d1f4edd09452d59d38c/>
<csr-id-49d3a298be19a46dcb6232ee8087c9bf0381db23/>
<csr-id-cdc31cb44af8b6dc2393a0f15c98c0be364db6ff/>
<csr-id-bc717073fde5fef59cbbe670ab6b7a9d66253d3b/>
<csr-id-189aeb6941d277f94e47ade0e851696206aaa146/>
<csr-id-9d5baf83c630e597407e5c5d4a47fc7f34805696/>
<csr-id-9aaccb8784afd03454cd69f599dc2b7822ca130c/>
<csr-id-8f1cc32b87a9e333f32399020339dd41e2d0b049/>
<csr-id-1fbc9b24d14460c6a88a064297c3a327ca9182aa/>
<csr-id-4e7da0aeddba7287c1556ca318d9cdd8459000d2/>
<csr-id-456c022ddf66af5f991e6aca2240d2ab73f8241d/>
<csr-id-ccb7efda1ee0c6303ac537ca9edd4b6c5cfcc5f2/>
<csr-id-d55978b38c9f69c0e31b335909ca650758c522c1/>
<csr-id-9a149113d6ffa4d4e5564ad8ba274117a4522685/>
<csr-id-f8de28a8e040439fcf83f217261facb934cacd51/>
<csr-id-231bde07064d6597867b5668a1e5ff319de3a202/>
<csr-id-3b7091bd8b45fc5bbfa6f1714ec56e123d76c647/>
<csr-id-3b4e2e33285f4b1a53bd52b147dc85c570ed2f6e/>
<csr-id-345033abe74a86c212e3b38fc0f6b655216b63b0/>
<csr-id-4a1349c5e99fdc45c038afdd2dac24486ee625ea/>
<csr-id-d6732833db0bb13d8414f99758e8767ab8ab2bdd/>
<csr-id-d75e5c65d04c60dca477cca4e704308198cfd6cf/>

### Refactor

 - <csr-id-b1297f2e42cdd7f7945c9fd5b4012086f8298b85/> store `Peer`s in `SectionAuthorityProvider`
   `SectionAuthorityProvider` contains details of the elders of a section,
   and is often used as the source of recipients when sending messages to
   elders. By storing a `Peer` inside the SAP, we ensure that connections
   will be reused if possible when messaging elders.
   
   Note that we wholesale replace the SAP in a few cases, which will
   immediately cause us to drop connections. In future we can be more
   careful to retain or selectively update existing `Peer`s to get more
   reuse.
 - <csr-id-680f9431bc75efea455dd7c6985e41c11740dc3e/> reuse known `Peer`s in DKG sessions
   Now that `NodeState` contains a `Peer`, which itself may contain an
   existing `Connection`, it makes sense to try and reuse the `Peer`s in
   network knowledge when initialising DKG sessions.
   
   This doesn't have much impact on the total number of connections yet,
   probably because we don't always maintain the `Peer`s in network
   knowledge.
 - <csr-id-eb0291082e49d1114c853b214f55b8a25e18d1e1/> clarify `connection` in `Peer`'s `Debug` output
   This makes it easier to see in logs whether a `Peer` was connected or
   not, which is useful for identifying where connections are not being
   maintained.
 - <csr-id-93bffbf1343621a8d5187649d1bb6d2a81cba793/> store a `Peer` in `NodeState`
   Since `Peer` can now hold a `connection`, we'd like to get `Peer`
   instances into as many places as possible to enable connection reuse.
   `NodeState` records are held by `SectionPeers`, which contains all known
   members of a node's section. If we can get a connection in there, we
   should be able to reuse it for most node<->node communication.
   
   The change was largely mechanical. The only interesting thing is that
 - <csr-id-1268fb6e46e4ba061ba7f724917371d8e0db0092/> set `Peer::connection` on send
   This changes the `Peer::connection` field to an `Arc<RwLock<...>>` so
   that `routing::Comm` can set the connection if one is opened. This
   shaves off ~ 4k `ConnectionOpened` log markers when forming an 11 node
   network.
   
   The main concern with this approach is that any existing connection is
   simply overwrote, and so will be disassociated from the `Peer`. This
   should currently be fine, since we do not rely on messages coming from
   particular connections.
 - <csr-id-b70a1053adc267780215636dd80a759c45d533d5/> correlate client connections without `ConnectedPeers`
   This is a first pass at introducing a mechanism to correlate client
   connections without using the `ConnectedPeers` cache/pool. The approach
   is centred on passing along the connected `Peer` that originated the
   request, which in most cases is enough to get the response to reply
   directly to the same `Peer` instance.
   
   Chunk queries need an additional mechanism, since the elder cannot reply
   right away, and instead must forward the query to nodes, and can only
   reply to the client once the nodes have themselves replied. For this we
   introduce a `pending_chunk_queries` field to `routing::Core`. When a
   chunk query is received from a client, a random `XorName` is generated
   as a correlation ID, and the client `Peer` is stored in the
   `pending_chunk_queries` cache under that `XorName`. The `XorName` is
   then passed as the 'end user' for the message to nodes, who send it back
   in their reply, allowing the receiving elder to correlate the response
   with the origin client `Peer`, and forward the response to the client
   over the `Peer`'s connection.
   
   There are some minor behavioural changes with this approach:
   
   1. Unlike the connection pooling approach, the client's exact connection
      must remain valid (e.g. if they reconnect while waiting for a
      response, they will never receive it). There is no way around this
      without a general-purpose connection cache, since we do not know on
      connection whether a peer has ever sent a client request. Thankfully
      it's not a big deal, since client operations are always idempotent,
      so client logic can handle any connection interruption by sending the
      same request(s) over a new connection.
   
   2. The `pending_chunk_queries` cache uses duration-based expiration,
      currently hard-coded to 5 minutes. Previously there was no timeout
      for this operation, and it would just depend on the connection
      remaining open in the pool. Since we cannot easily detect connection
      closure in `routing::Core`, and 5 mins is longer than the default
      client query timeout, this seems reasonable for now.
 - <csr-id-1d62fcefd5d44ef0df84b8126ba88de1127bd2bd/> use `qp2p::Connection` for connected senders
   This gets us closer to being able to reuse incoming connections, without
   `ConnectedPeers` (though there's no functional change yet).
   
   Sadly, a `cfg(test)` variant was necessary to satisfy routing tests,
   which don't make actual connections but do make assertions on the sender
   address.
 - <csr-id-20c1d7bf22907a44be2cc9585dd2ac55dd2985bf/> represent `HandleMessage::sender` as an enum
   This is a precursor to having `HandleMessage::sender` contain the
   originating `Connection` for connected peers. Since we use
   `HandleMessage` also when 'sending' messages to ourselves, we have to
   deal with the possibility that there may not be a connection. We could
   simply use `Option` for this, however the dedicated enum makes it
   clearer exactly what situation is represented.
   
   To facilitate later plumbing, `ConnectionEvent` now also uses `Sender`
   to represent the message sender.
   
   It's possible that we should make more use of the knowledge that a
   message was sent to ourselves, e.g. to short-circuit AE checks. For now,
   we more-or-less immediately lookup our own address, and proceed as if
   the message was actually sent from there.
 - <csr-id-182c4db4531c77fa6c67b8267cd7895b49f26ae1/> add an `Option<qp2p::Connection>` to `Peer`
   This allows the `Peer` struct to carry an existing connection to the
   peer, if one exists. Currently, the connection must be set on
   construction with `Peer::connected`. A reference to the connection can
   be retrieved with `Peer::connection`. When sending a message using
 - <csr-id-02f1325c18779ff02e6bb7d903fda4519ca87231/> add a log marker for reusing a connection
   This is mostly informational, but it gives a good indication that
   connection reuse is occurring, and how many new connection attempts have
   been saved.
 - <csr-id-1bf17662ecae4e1f3e9969ac03a8f543f57f2cd0/> add a feature to disable connection pooling
   The feature is named with `unstable-` to indicate that it may be removed
   at any time. This allows us to better test changes to connection
   management, since we know that all reuse must be coming from connection
   management rather than pooling.
 - <csr-id-90f2979a5bcb7f8e1786b5bdd868793d2fe924b4/> use a DAG to keep track of all sections chains within our NetworkKnowledge

### Other

 - <csr-id-14328b3d5b2579e4f038624c2353ec36c45fa9ed/> set node count to be 33
 - <csr-id-4f5e84da96d7277dfc4e385ff03edf6c1d84991e/> increase timeout for network forming

### Chore

 - <csr-id-14fdaa6537619483e94424ead5751d5ab41c8a01/> safe_network-0.41.0
 - <csr-id-dbdda1392325e8a08a6a28988208769552d5e543/> Remove always sending a message to ourself when we can handle it directly
 - <csr-id-e8e44fb69dae7fe1cae3bbf39f7b03a90df44dd7/> move ELDER_COUNT to root of sn
 - <csr-id-86c83bedf061b285b0d4f3effb54cc89a9fbd84d/> depend on ELDER_SIZE where it should be used
   Changes certain instances of  ">= 7" sort of thing, when we actually want to use ELDER_SIZE
 - <csr-id-590bfea538bb78fd20bb9574c3ea9ed6dcceced0/> increase join timeout when always-joinable set
 - <csr-id-85f2b5fd2adbf1f57e184c9e687fd14929b911e7/> increase join timeout before waiting to retry at bin/node
 - <csr-id-14c6ab95da62a144a798665ade6da626fb5ee6f2/> bye bye so much olde node code
 - <csr-id-30d220c180c7586de56282fafe52b54c8b619860/> readd routing member joined event for tests
 - <csr-id-6c0209187797bb27fa1529dd2f0ee8116975a02d/> log node age on relocation in routing
 - <csr-id-a328d70720a795b62ac05a68454abf1034e97b9d/> set joins allowed in routing code
 - <csr-id-a98cc810409a4ccc78e3c7a36b38b5f00cfcb23a/> burn down more of the /node dir
 - <csr-id-da1fe8dda6325e05b4d976d2b77d084c06d98cb0/> add StillElderAfterSplit LogMarker
   This makes it easier to distinguish nodes that continued being an aleder after split
 - <csr-id-078f45148af2da8d1690490720ef5f95555d42cd/> remove AdultsChanged event and ahndle everything inside routing
 - <csr-id-4c9b20ee58be9ca3579c7d1f4edd09452d59d38c/> update client listener error handling
 - <csr-id-49d3a298be19a46dcb6232ee8087c9bf0381db23/> cleanup test clippy
 - <csr-id-cdc31cb44af8b6dc2393a0f15c98c0be364db6ff/> dont always exit on client listener loop errors
 - <csr-id-bc717073fde5fef59cbbe670ab6b7a9d66253d3b/> fail query if we cannot send to more than 1 elder
 - <csr-id-189aeb6941d277f94e47ade0e851696206aaa146/> fix routing tests failing after we no longer emit MemerLeft event
 - <csr-id-9d5baf83c630e597407e5c5d4a47fc7f34805696/> simplify msg handling for NodeCmds
 - <csr-id-9aaccb8784afd03454cd69f599dc2b7822ca130c/> more node plumbing cleanup
 - <csr-id-8f1cc32b87a9e333f32399020339dd41e2d0b049/> handle set storage level in routing directly
 - <csr-id-1fbc9b24d14460c6a88a064297c3a327ca9182aa/> handle member left directly in routing code
   Remove related /node plumbing
 - <csr-id-4e7da0aeddba7287c1556ca318d9cdd8459000d2/> handle receiving dataExchange packet in routing.
   Removes more flows from the node code
 - <csr-id-456c022ddf66af5f991e6aca2240d2ab73f8241d/> remove node plumbing
 - <csr-id-ccb7efda1ee0c6303ac537ca9edd4b6c5cfcc5f2/> perform data exchange on split in routing
 - <csr-id-d55978b38c9f69c0e31b335909ca650758c522c1/> on split retain adult members only
 - <csr-id-9a149113d6ffa4d4e5564ad8ba274117a4522685/> fix demotion sendmessage test
 - <csr-id-f8de28a8e040439fcf83f217261facb934cacd51/> only fire SplitSuccess if elder
 - <csr-id-231bde07064d6597867b5668a1e5ff319de3a202/> send data updates to new elders only
 - <csr-id-3b7091bd8b45fc5bbfa6f1714ec56e123d76c647/> dont AE-update sibling elders on split.
   They cannot do anything with this info until they have their secret key, and with that they should get the SAP anyway. Or another AE flow can correct as needed
 - <csr-id-3b4e2e33285f4b1a53bd52b147dc85c570ed2f6e/> remove node plumbing
 - <csr-id-345033abe74a86c212e3b38fc0f6b655216b63b0/> perform data exchange on split in routing
 - <csr-id-4a1349c5e99fdc45c038afdd2dac24486ee625ea/> stop sending unnecessary AE-Update msgs to to-be-promoted candidates
 - <csr-id-d6732833db0bb13d8414f99758e8767ab8ab2bdd/> log an error when failing to add latest sibling key to proof chain
 - <csr-id-d75e5c65d04c60dca477cca4e704308198cfd6cf/> update launch tool and secured linked list deps

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

## v0.40.0 (2021-11-15)

<csr-id-1389ffa00762e126047a206abc475599c277930c/>
<csr-id-70015730c3e08881f803e9ce59be7ca16185ae11/>
<csr-id-ee165d41ca40be378423394b6422570d1d47727c/>
<csr-id-00acf0c0d8a65bdd5355ba909d73e74729a27044/>
<csr-id-7d5d5e11fef39a6dc1b89c972e42772db807374c/>
<csr-id-855b8ff87217e92a5f7d55fb78ab73c9d81f75a2/>
<csr-id-23b8a08e97fa415b9216caac5da18cb97ede980f/>

### Refactor

 - <csr-id-1389ffa00762e126047a206abc475599c277930c/> retry on connection loss of reused connection
   The `routing::Comm::send` method uses an instance of `ConnectedPeers` to
   reuse existing connections. This saves a lot of connections during busy
   exchanges between few nodes, such as DKG. However, should the connection
   retrieved for reuse have become invalid (e.g. closed by the peer or
   timed out), that recipient would be recorded as failed.
   
   Since connection timeouts are to be expected, particularly for stored
   connections with keep-alives disabled as in `ConnectedPeers`, it makes
   sense to retry the recipient with a fresh connection before declaring
   it failed.
   
   The implementation has been bolted on to the existing `send` mechanism.
   The `send` closure will return an additional boolean indicating whether
   or not the connection was reused. When evaluating the result, if it
   failed due to connection loss on a reused connection, we crate a new
   `send` task for the same recipient, but pass an additional flag to force
   a new connection. Thus, if the peer fails *again* for connection loss,
   the connection will not have been reused and the peer will be recorded
   as a failed recipient as normal.

### Chore

 - <csr-id-70015730c3e08881f803e9ce59be7ca16185ae11/> safe_network v0.40.0/sn_api v0.39.0

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

### Refactor (BREAKING)

 - <csr-id-ee165d41ca40be378423394b6422570d1d47727c/> duplicate `SectionAuthorityProvider` in `routing`
   This is part of the move towards passing connection handles through
   network knowledge. Whilst it would be possible to put a connection
   handle in the `messaging` struct, and just skip it when (de)serialising,
   this seems less clean than having a separate struct.
 - <csr-id-00acf0c0d8a65bdd5355ba909d73e74729a27044/> move `SectionPeers` from `messaging` to `routing`
   `SectionPeers` was only used in the `NodeMsg::AntiEntropyUpdate`
   message. Rather than simply inlining the `DashMap`, it's been replaced
   by `BTreeSet<SectionAuth<NodeState>>` since the map keys are redundant
   with the `name` field of `NodeState` (and `BTreeSet` specifically for
   deterministic ordering).
   
   With the move, it's also become `pub(crate)`, so from the perspective of
   the public API this type has been removed.
 - <csr-id-7d5d5e11fef39a6dc1b89c972e42772db807374c/> move `ElderCandidates` to `routing`
   The `ElderCandidate` type was in `messaging`, and was only used in the
   `NodeMsg::DkStart` message. It was more commonly used as network
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

## v0.39.0 (2021-11-12)

<csr-id-0a5027cd9b2c62833ccf70e2bcca5ab22625a840/>
<csr-id-645047b231eaf69f1299dee22ff2079feb7f5a95/>
<csr-id-0e2b01bb7e9c32f0bc5c1c6fd616acfcc5c45020/>
<csr-id-ab00cf08d217654c57449437348b73576a65e89f/>
<csr-id-5a3b70e9721fcdfdd809d2a6bd85968446b4e9a3/>
<csr-id-08629e583a03852d9f02b0e7ff66a829e3056d9b/>
<csr-id-5a81d874cf8040ff81a602bc8c5707a4ba6c64ff/>
<csr-id-8aa27b01f8043e98953971d1623aaf8b07d1596e/>
<csr-id-48a7d0062f259b362eb745e1bd59e77df514f1b8/>
<csr-id-008f36317f440579fe952325ccb97c9b5a547165/>
<csr-id-213cb39be8fbfdf614f3eb6248b14fe161927a14/>
<csr-id-ee165d41ca40be378423394b6422570d1d47727c/>
<csr-id-00acf0c0d8a65bdd5355ba909d73e74729a27044/>
<csr-id-7d5d5e11fef39a6dc1b89c972e42772db807374c/>
<csr-id-855b8ff87217e92a5f7d55fb78ab73c9d81f75a2/>
<csr-id-23b8a08e97fa415b9216caac5da18cb97ede980f/>

### Refactor

 - <csr-id-0a5027cd9b2c62833ccf70e2bcca5ab22625a840/> make `NodeState` not `Copy`
   At some point we might stick a connection handle in there, which would
   not be `Copy`, so unimplementing it now may save some changed lines in
   future.
 - <csr-id-645047b231eaf69f1299dee22ff2079feb7f5a95/> duplicate `NodeState` in `routing`
   This is part of the move towards passing connection handles through
   network knowledge. Whilst it would be possible to put a connection
   handle in the `messaging` struct, and just skip it when (de)serialising,
   this seems less clean than having a separate struct.
 - <csr-id-0e2b01bb7e9c32f0bc5c1c6fd616acfcc5c45020/> move `routing::Peer` into `network_knowledge`
   This seems like a better home for it. It's still re-exported from
   `routing` to minimise disruption.

### Chore

 - <csr-id-ab00cf08d217654c57449437348b73576a65e89f/> safe_network v0.39.0
 - <csr-id-5a3b70e9721fcdfdd809d2a6bd85968446b4e9a3/> remove unnecessary peer lagging check
   - All 'Proposal' messages are sent with current SAP's section key, same key
     as the Proposal's signature share public key, thus earlier AntiEntropy check
     would have caught a Proposal msg signed with an old share and this check
     is never reached.
 - <csr-id-08629e583a03852d9f02b0e7ff66a829e3056d9b/> update testnet default interval
 - <csr-id-5a81d874cf8040ff81a602bc8c5707a4ba6c64ff/> rename forced to skip section info agreement, tidy assignment
 - <csr-id-8aa27b01f8043e98953971d1623aaf8b07d1596e/> update bls_dkg to 0.7.1
 - <csr-id-48a7d0062f259b362eb745e1bd59e77df514f1b8/> stepped age with long interval
 - <csr-id-008f36317f440579fe952325ccb97c9b5a547165/> add more tracing around connections
   This is mostly peppering `.in_current_span` to some spawned futures.
   Additionally the traces for `ConnectionOpened` and `ConnectionClosed`
   now use fields for their context, since it makes JSON processing easier.
 - <csr-id-213cb39be8fbfdf614f3eb6248b14fe161927a14/> update bls_dkg and blsttc to 0.7 and 0.3.4 respectively

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

### Refactor (BREAKING)

 - <csr-id-ee165d41ca40be378423394b6422570d1d47727c/> duplicate `SectionAuthorityProvider` in `routing`
   This is part of the move towards passing connection handles through
   network knowledge. Whilst it would be possible to put a connection
   handle in the `messaging` struct, and just skip it when (de)serialising,
   this seems less clean than having a separate struct.
 - <csr-id-00acf0c0d8a65bdd5355ba909d73e74729a27044/> move `SectionPeers` from `messaging` to `routing`
   `SectionPeers` was only used in the `NodeMsg::AntiEntropyUpdate`
   message. Rather than simply inlining the `DashMap`, it's been replaced
   by `BTreeSet<SectionAuth<NodeState>>` since the map keys are redundant
   with the `name` field of `NodeState` (and `BTreeSet` specifically for
   deterministic ordering).
   
   With the move, it's also become `pub(crate)`, so from the perspective of
   the public API this type has been removed.
 - <csr-id-7d5d5e11fef39a6dc1b89c972e42772db807374c/> move `ElderCandidates` to `routing`
   The `ElderCandidate` type was in `messaging`, and was only used in the
   `NodeMsg::DkStart` message. It was more commonly used as network
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

## v0.38.0 (2021-11-10)

<csr-id-1568adb28a2a6a3cdf8a9737e098a5ea7bb2c419/>
<csr-id-cbdf0236f7cd241e7addb1afe75ed5e5cfac00ab/>
<csr-id-a7968dd17927e346b3d32bb5971ed6457aea6606/>

### Refactor

 - <csr-id-1568adb28a2a6a3cdf8a9737e098a5ea7bb2c419/> insert a connected peer for outgoing connections
   The `ConnectedPeers` struct serves as something of a 'cache' of existing
   connections with peers. It was introduced when connection pooling was
   removed from `qp2p`, in order to preserve connections from clients (who
   cannot be reconnected to). Since we can't discriminate between clients
   and nodes when they connect to us, we ended up inserting *all* incoming
   connections into `ConnectedPeers`. This is relatively harmless, since
   node<->node connections will eventually timeout (no keep-alive) and
   connections are removed from `ConnectedPeers` when they close for any
   reason.
   
   Unlike the `qp2p` connection pool that it replaces, outgoing connections
   were not added to `ConnectedPeers`. This was deliberate, on the basis
   that we would prefer more precise connection management, e.g. by passing
   connection handles around as part of commands/state. Sadly there are a
   few challenges to doing this:
   
   - The code paths are complicated and generally divergent. Ensuring
     connection reuse would likely involve plumbing a connection through a
     significant portion of the codebase.
   - Network state is often held in `messaging` types (e.g.
     `ElderCandidates`, `SectionPeers`, `NodeState`, etc.). Polluting the
     messaging types with connection handles that cannot be (de)serialised
     is undesirable, but keeping them out of state can involve non-trivial
     refactoring.
   - More generally, 'inertia' from having gone for a very long time with
     messaging being entirely address-based. This is closely related to the
     first point – most of the codebase is connection-unaware, and plumbing
     a connection through is slow work and typically only provides reuse
     for a single message flow.
   
   By contrast, inserting outgoing connections into `ConnectedPeers` is
   trivial, and massively cuts down the number of opened connections (from
   ~2.5k down to ~150 when forming an 11 node network, which is still far
   more connections than the minimum of one connection between each node
   and every other, 55, but better than nothing).
   
   It probably still makes sense to aim for tigher connection management,
   but some refactoring groundwork would make this a smoother process, and
   with everything else currently in flight it seems like a lower priority.

### Chore

 - <csr-id-cbdf0236f7cd241e7addb1afe75ed5e5cfac00ab/> safe_network v0.38.0
 - <csr-id-a7968dd17927e346b3d32bb5971ed6457aea6606/> reuse join permits for relocated nodes too.
   Also increases concurrent joins allowed

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

## v0.37.0 (2021-11-09)

<csr-id-1b7f5d6e198eb286034e12eed3fa4fc63b927e0f/>
<csr-id-9581fcf27765752f6b556d632e8396e93d0f15e0/>
<csr-id-a03d2cef55665759ddfeb40972676c87b17ccfa8/>
<csr-id-bde3cd5eac75cc41a3a9ffefb091584273575f68/>
<csr-id-e24763e604e6a25ee1b211af6cf3cdd388ec0978/>
<csr-id-7d5212a2b916a8e540403346e0770cee1a446884/>
<csr-id-109843c67ffd4ac4675cdfe56d7fcdaf97664007/>
<csr-id-6bb5e64b3b0eb518719581278551272ae8f2b2ed/>
<csr-id-e47e9ecb0c7f23209a9c1eb58c248fdf2facfd4a/>
<csr-id-3b728b65a07a33019e06dd6f3da9fd334e6da9e1/>
<csr-id-87c62d39d240afc01118135bc18d22fe23fc421c/>
<csr-id-d7972da6abd3001e75019bf72aae6a98919ed1db/>
<csr-id-49e2d14a1c5f8fd83aa4b9a5abe67e23fca9f966/>
<csr-id-5326de5f658dfe75d1f5c44224d8623123848b08/>
<csr-id-9a3cffc52589e8adf6dac75ae6aab4c184118648/>
<csr-id-c2df9db2fe9e99450cc16060ff034289ab683783/>
<csr-id-2fc2f0e6fbb5f7b05e61281e90992053ef5f0f5d/>
<csr-id-432e36de012695c6c5e20bd704dc184db9c5c4d6/>
<csr-id-9cc16296db7241819e17dd2673c7b3cb9fe2ead8/>
<csr-id-c49f9a16c9e0912bf581a2afef22ac4806898ade/>
<csr-id-9cd2d37dafb95a2765d5c7801a7bb0c58286c47c/>
<csr-id-b719b74abdd1cd84b3813ec7046f4fdf99cde6a2/>
<csr-id-3703819c7f0da220c8ff21169ca1e8161a20157b/>
<csr-id-d0134e870bb097e095e1c8a33e607cf7994e6491/>
<csr-id-b66444c7d7a10c9a26db30fe5ce8e91984031624/>
<csr-id-c5aae59a4eac6c3285bc45845433d23fc96d155f/>
<csr-id-723618d0e3ad77eb26a3c698f9545490166b7ee0/>
<csr-id-80ce4ca9376b99037b9377425c477ea3a7493e54/>
<csr-id-616c024f2280fc99de063026756a9c938f78b885/>
<csr-id-cf5a041742844b8b3d5e71c3e895018367b76013/>
<csr-id-ed6d2f09c12b72446f0f55ecacb5fc4f35278575/>
<csr-id-9350f273a5afec88120fe79fe85ceaf8027d691e/>
<csr-id-a54c4426caccf71d28b2697094135892ec4a5e16/>
<csr-id-8eedb660461b13b236545643fa21868eb6613826/>
<csr-id-abfe7378604a74119accd7b9f86bef5682b0784a/>
<csr-id-0399a9e76547a03f1e3902aec30ecbd57ed437c7/>
<csr-id-23f53a339a5fe0d2f2e5d415bfc01e646a81a5c8/>
<csr-id-5232bf51c738197339ac70ee4f46adee4fa87179/>
<csr-id-f9cd39b6d3b1877fa0f3fa73bd6e3a796cae3b08/>
<csr-id-208abdd108654ab83e8bee763793201e6f7b5eb2/>
<csr-id-fc10d037d64efc86796f1b1c6f255a4c7f91d3e1/>
<csr-id-12c673e7f52ed1c4cbe15c14fe6eb4e68b986e18/>
<csr-id-9d27749bc7d59ee499980044f57eab86d2e63d04/>
<csr-id-aaa0af4d1685c65a3c166070a590c10e9fd54765/>
<csr-id-73b405d1228244a2b984e1294d5e8542f8691cef/>
<csr-id-0c9c1f2edd9e872b9ba1642ac50f59a63f68b488/>
<csr-id-9bdd68f313b3b7881cb39db02026184bbab0bfb0/>
<csr-id-8b7b66450349b669e023539e92c77f9a3b830948/>
<csr-id-7fc29f8187a29c2eeff8bbb5e09f068414bb8b93/>
<csr-id-4415c9b1d166f7e53032a0100d829e8581255a1e/>
<csr-id-8eb1877effb8cf0bfc8986c23d49d727500087dd/>
<csr-id-0b8413998f018d7d577f2248e36e21f6c2744116/>
<csr-id-aaa6903e612178ce59481b2e81fe3bd0d1cc2617/>
<csr-id-2651423c61b160841557d279c9b706abdaab4cdf/>
<csr-id-20461a84dbc0fc373b184a3982a79affad0544f6/>
<csr-id-6a8f5a1f41e5a0f4c0cce6914d4b330b68f5e5d8/>
<csr-id-da70738ff0e24827b749a970f466f3983b70442c/>
<csr-id-7ddf2f850441e6596133ab7a596eb766380008c3/>
<csr-id-0387123114ff6ae42920577706497319c8a888cb/>
<csr-id-aa17309092211e8d1ba36d4aaf50c4677a461594/>
<csr-id-225432908839359800d301d9e5aa8274e4652ee1/>
<csr-id-7b430a54f50846a8475cec804bc24552043558b7/>
<csr-id-fe62ad9f590f151dd20a6832dfab81d34fc9c020/>
<csr-id-9e3cb4e7e4dc3d1b8fa23455151de60d5ea03d4d/>
<csr-id-37aec09a555d428c730cdd982d06cf5cb58b60b1/>
<csr-id-810fe0797a4f23a6bb2586d901ccac9272a9beb2/>
<csr-id-ca074b92b31add1ad6d0db50f2ba3b3d1ae25d5a/>
<csr-id-993d564f0ad5d8cac8d9a32b8c6c8d1dd00c3fd9/>
<csr-id-fd8e8f3fb22f8592a45db33e75f237ef22cd1f5b/>
<csr-id-c1c2bcac021dda4bf18a4d80ad2f86370d56efa7/>
<csr-id-e2727b91c836619dadf2e464ee7c57b338427f22/>
<csr-id-148140f1d932e6c4b30122ebcca3450ab6c84544/>
<csr-id-61dec0fd90b4df6b0695a7ba46da86999d199d4a/>
<csr-id-49c76d13a91474038bd8cb005959a37a7d4c6603/>
<csr-id-c5d2e31a5f0cea381bb60dc1f896dbbda5038506/>
<csr-id-0cb790f5c3712e357f685bfb88cd237c5b5f76c5/>
<csr-id-b3ce84012e6cdf4c87d6d4a3137ab6506264e949/>
<csr-id-ecf0bc9a9736167edb15db7ff4e3cf5dc388dd22/>

### Test

 - <csr-id-1b7f5d6e198eb286034e12eed3fa4fc63b927e0f/> fix healthcheck 0/1 summation.
 - <csr-id-9581fcf27765752f6b556d632e8396e93d0f15e0/> adapt routing unit tests to new network knowledge handling logic
 - <csr-id-a03d2cef55665759ddfeb40972676c87b17ccfa8/> update elder count check to be general one split health check
 - <csr-id-bde3cd5eac75cc41a3a9ffefb091584273575f68/> ignore demotion test for now
 - <csr-id-e24763e604e6a25ee1b211af6cf3cdd388ec0978/> add network elder count test, using testnet grep

### Refactor

 - <csr-id-7d5212a2b916a8e540403346e0770cee1a446884/> use the prefix map updating outcome to decide if current SAP and chain shall be updated
 - <csr-id-109843c67ffd4ac4675cdfe56d7fcdaf97664007/> drop JoinRetry responses for SAPs we've already resent join reqs to
 - <csr-id-6bb5e64b3b0eb518719581278551272ae8f2b2ed/> thread a `Peer` through more places
   Rather than using separate `XorName` and `SocketAddr` arguments/fields,
   we can use `Peer`. This doesn't buy us much currently, but in future we
   could store an open `Connection` to the peer inside the struct, letting
   us reuse connections to reply to incoming messages etc.
 - <csr-id-e47e9ecb0c7f23209a9c1eb58c248fdf2facfd4a/> simplify some iterator chains
 - <csr-id-3b728b65a07a33019e06dd6f3da9fd334e6da9e1/> use `peers()` instead of `Peer::new` when available
   This was only useful in a couple of places, but it makes sense to use
   the `peers` method when it exists.
 - <csr-id-87c62d39d240afc01118135bc18d22fe23fc421c/> replace `(XorName, SocketAddr)` with `Peer`
   In many places, we pass around `Peer`s which were then converted to
   `(XorName, SocketAddr)`. Most of the time this ends up at the
 - <csr-id-d7972da6abd3001e75019bf72aae6a98919ed1db/> storing all sections chains in routing network knowledge
 - <csr-id-49e2d14a1c5f8fd83aa4b9a5abe67e23fca9f966/> refactoring NetworkKnowledge private API
 - <csr-id-5326de5f658dfe75d1f5c44224d8623123848b08/> moving to a unified network knowledge for updating SAPs and section chain
 - <csr-id-9a3cffc52589e8adf6dac75ae6aab4c184118648/> make clients read prefix_map from disk
 - <csr-id-c2df9db2fe9e99450cc16060ff034289ab683783/> add `KvStore::flush` to avoid waiting in tests
   Some `KvStore` tests verify that used space adjusts appropriately when
   data is written (also when data is removed, but those tests are disabled
   since it's not predictable when sled will compact its log). We measure
   used space by measuring the size of the database files.
   
   sled flushes to disk asynchronously by default, and the tests were
   accounting for this by looping until the size changed. This could lead
   to hung tests in the case of bugs with used space calculation, which
   would be harder to debug than a concrete failure. As such, a test-only
   `flush` method has been added to `KvStore`, which is now used by the
   tests to force a flush to disk.
 - <csr-id-2fc2f0e6fbb5f7b05e61281e90992053ef5f0f5d/> remove redundant `K` type parameter from `KvStore`
   The `Value` trait already included an associated type for the key type,
   and this can be referenced directly in signatures without the need for
   an additional generic type parameter.
 - <csr-id-432e36de012695c6c5e20bd704dc184db9c5c4d6/> update `qp2p`, which removes `ConnectionPool`
   The latest release of `qp2p` has removed the `ConnectionPool`, meaning
   connections now close immediately when both sides of it (`Connection`
   and `ConnectionIncoming`). It's intended that we apply more deliberate
   connection management as a substitute, but as a stop-gap this commit
   implements a `ConnectedPeers` type, which behaves much like the `qp2p`
   `ConnectionPool`.
 - <csr-id-9cc16296db7241819e17dd2673c7b3cb9fe2ead8/> moving Proposal utilities into routing:Core
 - <csr-id-c49f9a16c9e0912bf581a2afef22ac4806898ade/> simplifying key shares cache
 - <csr-id-9cd2d37dafb95a2765d5c7801a7bb0c58286c47c/> encapsulate Section info to reduce SAP and chain cloning
 - <csr-id-b719b74abdd1cd84b3813ec7046f4fdf99cde6a2/> moving Section definition our of messaging onto routing

### Other

 - <csr-id-3703819c7f0da220c8ff21169ca1e8161a20157b/> update actions workflows for workspace refactor
   Now that we have multiple crates in the same repository, if we only want to do things when subsets
   of the source tree have been modified (e.g. only one crate particular crate has changed), it does
   make the CI situation a bit more cumbersome. We need to make much more frequent use the path filter
   action and only do things if certain files have been changed.
   
   It's also useful for the workflows to indicate the crate they relate to, so they've been renamed or
   added to accommodate that.
   
   Certain things have been moved to the level of the workspace because with these, you can't really
   discriminate based on the crate that's changed.
   
   An unused dependency for sn_api was also taken out here.
 - <csr-id-d0134e870bb097e095e1c8a33e607cf7994e6491/> update actions workflows for workspace refactor
   Now that we have multiple crates in the same repository, if we only want to do things when subsets
   of the source tree have been modified (e.g. only one crate particular crate has changed), it does
   make the CI situation a bit more cumbersome. We need to make much more frequent use the path filter
   action and only do things if certain files have been changed.
   
   It's also useful for the workflows to indicate the crate they relate to, so they've been renamed or
   added to accommodate that.
   
   Certain things have been moved to the level of the workspace because with these, you can't really
   discriminate based on the crate that's changed.
   
   An unused dependency for sn_api was also taken out here.

### Chore

 - <csr-id-b66444c7d7a10c9a26db30fe5ce8e91984031624/> some variables renaming and improving some comments
 - <csr-id-c5aae59a4eac6c3285bc45845433d23fc96d155f/> avoid rebuild on windows for testnet
   Windows OS has an issue of re-building as unable to remove testnet.exe.
 - <csr-id-723618d0e3ad77eb26a3c698f9545490166b7ee0/> add more log markers.
   For adding SKs to our cache as an elder
   For handlingElderAgreement
 - <csr-id-80ce4ca9376b99037b9377425c477ea3a7493e54/> update log messages and test errors for clarity
 - <csr-id-616c024f2280fc99de063026756a9c938f78b885/> remove blocking/non blocking msg handling distinction
 - <csr-id-cf5a041742844b8b3d5e71c3e895018367b76013/> clarify elder counts in health check test
 - <csr-id-ed6d2f09c12b72446f0f55ecacb5fc4f35278575/> reduce join backoff times
 - <csr-id-9350f273a5afec88120fe79fe85ceaf8027d691e/> change client timeouts
 - <csr-id-a54c4426caccf71d28b2697094135892ec4a5e16/> reorder blob tst log initialisation to _after_ initial AE triggering msgs sent
 - <csr-id-8eedb660461b13b236545643fa21868eb6613826/> update health check to test for Prefix(1/0), use this in CI
 - <csr-id-abfe7378604a74119accd7b9f86bef5682b0784a/> add env var for client query timeout
   rename ClientConfig
 - <csr-id-0399a9e76547a03f1e3902aec30ecbd57ed437c7/> reduce clone and lock during message handling
 - <csr-id-23f53a339a5fe0d2f2e5d415bfc01e646a81a5c8/> implement `Deref` for `SectionAuth`
   This lets us drop a lot of explicit `value` accessed in favour of
   automatic dereferencing. This may make future changes lower impact.
 - <csr-id-5232bf51c738197339ac70ee4f46adee4fa87179/> renaming section mod to network_knowledge
 - <csr-id-f9cd39b6d3b1877fa0f3fa73bd6e3a796cae3b08/> Section->NetworkKnowledge name change
 - <csr-id-208abdd108654ab83e8bee763793201e6f7b5eb2/> reduce the report check interval
 - <csr-id-fc10d037d64efc86796f1b1c6f255a4c7f91d3e1/> bump rust edition
   The few breaking changes in this edition did not affect us.
 - <csr-id-12c673e7f52ed1c4cbe15c14fe6eb4e68b986e18/> remove unnecessary `allow(unused)`
 - <csr-id-9d27749bc7d59ee499980044f57eab86d2e63d04/> remove unused `KvStore::store_batch`
   Although this was commented to indicate it will be used again soon, it
   is trivial to recover code from git history, but it may be non-trivial
   to maintain or fix unused code. That's the case for trying to fix used
   space handling in `store_batch`, since `sled::Tree::apply_batch` cannot
   perform compare-and-swap operations. As such, the whole operation
   probably needs a rethink, and since it's not currently used we can defer
   that until it's needed.
 - <csr-id-aaa0af4d1685c65a3c166070a590c10e9fd54765/> unignore routing demote test
 - <csr-id-73b405d1228244a2b984e1294d5e8542f8691cef/> tweak demotion test
 - <csr-id-0c9c1f2edd9e872b9ba1642ac50f59a63f68b488/> use prefixmap update as opposed to verify against redundant same chain
 - <csr-id-9bdd68f313b3b7881cb39db02026184bbab0bfb0/> trust any valid key for chain updates
   But only return if it was successfully inserted into prefixmap
 - <csr-id-8b7b66450349b669e023539e92c77f9a3b830948/> move known prefix log to verify and udpate
 - <csr-id-7fc29f8187a29c2eeff8bbb5e09f068414bb8b93/> add check to received sap on ae update
   other cleanup
 - <csr-id-4415c9b1d166f7e53032a0100d829e8581255a1e/> use constants for message priority
 - <csr-id-8eb1877effb8cf0bfc8986c23d49d727500087dd/> remove unused data exchange structs
 - <csr-id-0b8413998f018d7d577f2248e36e21f6c2744116/> minor refactor to prefix_map reading
 - <csr-id-aaa6903e612178ce59481b2e81fe3bd0d1cc2617/> try to update Section before updating Node info when relocating
 - <csr-id-2651423c61b160841557d279c9b706abdaab4cdf/> increase node resource proof difficulty
 - <csr-id-20461a84dbc0fc373b184a3982a79affad0544f6/> tweak join backoff
 - <csr-id-6a8f5a1f41e5a0f4c0cce6914d4b330b68f5e5d8/> remove core RwLock
 - <csr-id-da70738ff0e24827b749a970f466f3983b70442c/> upgrade `sn_launch_tool`
   The new version uses the upgraded versions of tracing which no longer
   depend on `chrono`.
 - <csr-id-7ddf2f850441e6596133ab7a596eb766380008c3/> tweak join retry backoff timing
 - <csr-id-0387123114ff6ae42920577706497319c8a888cb/> upgrade `tracing-appender` and `tracing-subscriber`
   These new versions have dropped their dependence on `chrono`, which has
   an active security advisory against it (RUSTSEC-2020-0159) which seems
   unlikely to be resolved.
   
   `chrono` is still being pulled in by `qp2p` (via `rcgen`), `sn_api`, and
   `sn_launch_tool`. This will be fixed in future commits.
 - <csr-id-aa17309092211e8d1ba36d4aaf50c4677a461594/> tweak join retry backoff timing
 - <csr-id-225432908839359800d301d9e5aa8274e4652ee1/> move safe_network code into sn directory
   Due to the fact that we're now using multiple crates, the safe_network code is moved into the `sn`
   directory.
   
   A Cargo.toml is added to the root directory to establish this repository as a workspace, currently
   with 2 members, sn and sn_api. If you now run a `cargo build` at the root directory, it will build
   both of these crates.
   
   The Github Actions workflows that were brought in from the `sn_api` merge were also removed here.
 - <csr-id-7b430a54f50846a8475cec804bc24552043558b7/> don't backoff when sending join resource challenge responses
 - <csr-id-fe62ad9f590f151dd20a6832dfab81d34fc9c020/> remove extraneous comment
 - <csr-id-9e3cb4e7e4dc3d1b8fa23455151de60d5ea03d4d/> cleanup comments
 - <csr-id-37aec09a555d428c730cdd982d06cf5cb58b60b1/> some clippy cleanup
   excuse the nonsense commit!
 - <csr-id-810fe0797a4f23a6bb2586d901ccac9272a9beb2/> remove unused debug
 - <csr-id-ca074b92b31add1ad6d0db50f2ba3b3d1ae25d5a/> add some DKG related markers
 - <csr-id-993d564f0ad5d8cac8d9a32b8c6c8d1dd00c3fd9/> move AE updat resend after retry to only happen for validated saps
 - <csr-id-fd8e8f3fb22f8592a45db33e75f237ef22cd1f5b/> add LogMarker for all different send msg types
 - <csr-id-c1c2bcac021dda4bf18a4d80ad2f86370d56efa7/> make elder agreemeent its own blocking command
   make more commands non blocking
 - <csr-id-e2727b91c836619dadf2e464ee7c57b338427f22/> flesh out far too many 'let _ = '
 - <csr-id-148140f1d932e6c4b30122ebcca3450ab6c84544/> make section peers concurrent by using Arc<DashMap>

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

### Refactor (BREAKING)

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

