# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## v0.17.2 (2023-02-03)

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 0 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Merge #2055 ([`d43f511`](https://github.com/maidsafe/safe_network/commit/d43f511a58dd34ce7ab1594f5e46beaea066aa10))
    - Merge branch 'main' into sap_change_force_dkg_termination ([`876d78a`](https://github.com/maidsafe/safe_network/commit/876d78a911e852b8cc1c33b2130e4cf9b28dd510))
</details>

## v0.17.1 (2023-02-02)

<csr-id-e706848522d6c52d6ed5eddf638376996cc947a9/>

### Chore

 - <csr-id-e706848522d6c52d6ed5eddf638376996cc947a9/> add clippy check for unused async

### Chore

 - <csr-id-3831dae3e34623ef252298645a43cbafcc923a13/> sn_interface-0.17.1/sn_fault_detection-0.15.3/sn_comms-0.2.1/sn_client-0.78.2/sn_node-0.73.3/sn_api-0.76.1

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
    - sn_interface-0.17.1/sn_fault_detection-0.15.3/sn_comms-0.2.1/sn_client-0.78.2/sn_node-0.73.3/sn_api-0.76.1 ([`3831dae`](https://github.com/maidsafe/safe_network/commit/3831dae3e34623ef252298645a43cbafcc923a13))
    - Merge #2061 ([`bab8208`](https://github.com/maidsafe/safe_network/commit/bab82087260ac4f1f44e688db2e67ca2387a7175))
    - add clippy check for unused async ([`e706848`](https://github.com/maidsafe/safe_network/commit/e706848522d6c52d6ed5eddf638376996cc947a9))
    - Merge branch 'main' into sap_change_force_dkg_termination ([`7d3665b`](https://github.com/maidsafe/safe_network/commit/7d3665bfe05f61d170229df9f4424c5663b116d5))
</details>

## v0.17.0 (2023-02-01)

<csr-id-69f8ade1ea8bb3e77c169b17ae21a40370bfab58/>
<csr-id-9ef9a2f2c8711895b62b82d25cb9d208c464cad6/>

### Chore

 - <csr-id-69f8ade1ea8bb3e77c169b17ae21a40370bfab58/> sn_interface-0.17.0/sn_comms-0.2.0/sn_client-0.78.0/sn_node-0.73.0/sn_api-0.76.0/sn_cli-0.69.0

### New Features

 - <csr-id-131ba75ab8155fe904771537823f2912dd0a3a97/> force DKG termination upon receiving SAP update

### Refactor (BREAKING)

 - <csr-id-9ef9a2f2c8711895b62b82d25cb9d208c464cad6/> implement new qp2p version
   This introduces quite some changes to the API that hopefully simplifies
   much of the internals of the node and client.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.17.0/sn_comms-0.2.0/sn_client-0.78.0/sn_node-0.73.0/sn_api-0.76.0/sn_cli-0.69.0 ([`69f8ade`](https://github.com/maidsafe/safe_network/commit/69f8ade1ea8bb3e77c169b17ae21a40370bfab58))
    - Merge #1996 ([`bb7b2db`](https://github.com/maidsafe/safe_network/commit/bb7b2dbcae9c0a67fc0a23c279537df49d88a07a))
    - implement new qp2p version ([`9ef9a2f`](https://github.com/maidsafe/safe_network/commit/9ef9a2f2c8711895b62b82d25cb9d208c464cad6))
    - force DKG termination upon receiving SAP update ([`131ba75`](https://github.com/maidsafe/safe_network/commit/131ba75ab8155fe904771537823f2912dd0a3a97))
</details>

## v0.16.20 (2023-01-31)

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

## v0.16.19 (2023-01-29)

<csr-id-4ea2b420b5c6390bd894505e3c71cb5e673244b8/>

### Chore

 - <csr-id-4ea2b420b5c6390bd894505e3c71cb5e673244b8/> sn_interface-0.16.19/sn_node-0.72.39

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
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

## v0.16.18 (2023-01-27)

<csr-id-6b923515f0f0cd1b0d1a4ed51d3ca418e41d9916/>
<csr-id-01ff2ccf45dfc9d45c5ad540144d7a4a640830fc/>

### Chore

 - <csr-id-6b923515f0f0cd1b0d1a4ed51d3ca418e41d9916/> fix issues reported by new clippy

### Chore

 - <csr-id-01ff2ccf45dfc9d45c5ad540144d7a4a640830fc/> sn_interface-0.16.18/sn_comms-0.1.4/sn_client-0.77.9/sn_node-0.72.34/sn_api-0.75.5/sn_cli-0.68.6

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
    - sn_interface-0.16.18/sn_comms-0.1.4/sn_client-0.77.9/sn_node-0.72.34/sn_api-0.75.5/sn_cli-0.68.6 ([`01ff2cc`](https://github.com/maidsafe/safe_network/commit/01ff2ccf45dfc9d45c5ad540144d7a4a640830fc))
    - Merge branch 'main' into chore-comms-remove-unused-async ([`e92dd49`](https://github.com/maidsafe/safe_network/commit/e92dd49f38f9b56c7276e86ba79f7fd8f816af76))
    - Merge branch 'main' into RevertDkgCache ([`24ff625`](https://github.com/maidsafe/safe_network/commit/24ff6257f85922090cfaa5fa83044082d3ef8dab))
    - fix issues reported by new clippy ([`6b92351`](https://github.com/maidsafe/safe_network/commit/6b923515f0f0cd1b0d1a4ed51d3ca418e41d9916))
</details>

## v0.16.17 (2023-01-25)

<csr-id-a4d295ccdddea3d4d11bca5eb0236a5447c75633/>

### Chore

 - <csr-id-a4d295ccdddea3d4d11bca5eb0236a5447c75633/> sn_interface-0.16.17/sn_comms-0.1.2/sn_node-0.72.30

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.17/sn_comms-0.1.2/sn_node-0.72.30 ([`a4d295c`](https://github.com/maidsafe/safe_network/commit/a4d295ccdddea3d4d11bca5eb0236a5447c75633))
    - Merge #2016 #2019 #2023 ([`c8e5746`](https://github.com/maidsafe/safe_network/commit/c8e574687ea74ed1adb69a722afe6bff734c19ad))
</details>

## v0.16.16 (2023-01-24)

<csr-id-6acbe4920d9d3a7db88e76a21e026bdee04e9584/>

### Chore

 - <csr-id-6acbe4920d9d3a7db88e76a21e026bdee04e9584/> sn_interface-0.16.16/sn_node-0.72.28

### New Features

 - <csr-id-908ee34d116e2a9e5250d3044f9dbe1c6d471ecc/> add retry for relocating node

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
    - sn_interface-0.16.16/sn_node-0.72.28 ([`6acbe49`](https://github.com/maidsafe/safe_network/commit/6acbe4920d9d3a7db88e76a21e026bdee04e9584))
    - Merge #2024 ([`fc0aa80`](https://github.com/maidsafe/safe_network/commit/fc0aa8062c0003a9ac3c263d4ea01111b5e6a8d3))
    - add retry for relocating node ([`908ee34`](https://github.com/maidsafe/safe_network/commit/908ee34d116e2a9e5250d3044f9dbe1c6d471ecc))
</details>

## v0.16.15 (2023-01-23)

<csr-id-e6ec500629844ad2d328d38fff7ebd0f52a8cb12/>
<csr-id-c94a953dddfcb20bf65d4bb34448dc2752a019c5/>

### Refactor

 - <csr-id-e6ec500629844ad2d328d38fff7ebd0f52a8cb12/> use existing join flow

### Bug Fixes

 - <csr-id-0f1ac79146aac0d0cea11644cca75b68012fa23d/> as elder request missing data on any membership update

### Chore

 - <csr-id-c94a953dddfcb20bf65d4bb34448dc2752a019c5/> sn_interface-0.16.15/sn_node-0.72.27

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 3 days passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.15/sn_node-0.72.27 ([`c94a953`](https://github.com/maidsafe/safe_network/commit/c94a953dddfcb20bf65d4bb34448dc2752a019c5))
    - Merge #2018 ([`1ee4f75`](https://github.com/maidsafe/safe_network/commit/1ee4f75af4dddb7b2bd18bb60317d3e977e356f7))
    - use existing join flow ([`e6ec500`](https://github.com/maidsafe/safe_network/commit/e6ec500629844ad2d328d38fff7ebd0f52a8cb12))
    - as elder request missing data on any membership update ([`0f1ac79`](https://github.com/maidsafe/safe_network/commit/0f1ac79146aac0d0cea11644cca75b68012fa23d))
</details>

## v0.16.14 (2023-01-20)

<csr-id-6f214e2abae8be935652e93edda6d7eec2354af6/>
<csr-id-7965789548e9e5deb64501a9a774532042ce6c16/>
<csr-id-3b9304bab92f0715a134ce68afbfd08a7ad31e61/>
<csr-id-21af053a5be2317be356e760c2b581c0f870a396/>
<csr-id-027b164f851209f0662e0a84ee839618d95af58d/>
<csr-id-a33d28e4bc3f11779eaf0bf6cafd67800dbc4e0d/>
<csr-id-6c0f451646ea5840c79f112868637facdd08293c/>
<csr-id-33a577f5e52029854757b9093e7b30535a7acabd/>
<csr-id-b2edff4b5b63b5d8a7905428b2c78b1d26598f07/>
<csr-id-9f5b5ffb5975810e22c634f171984fcc803062aa/>
<csr-id-82ac545c7c3bbf1941fe9d9a80dcc2f99ff58a2f/>
<csr-id-783d62461a65eb7c06b0d4f399b97216b6c75519/>

### Chore

 - <csr-id-6f214e2abae8be935652e93edda6d7eec2354af6/> remove unused priority
 - <csr-id-7965789548e9e5deb64501a9a774532042ce6c16/> skip section_tree update earlier for outdated sap
 - <csr-id-3b9304bab92f0715a134ce68afbfd08a7ad31e61/> some optimisations during AE probe
 - <csr-id-21af053a5be2317be356e760c2b581c0f870a396/> happy new year 2023
 - <csr-id-027b164f851209f0662e0a84ee839618d95af58d/> improve logging and comments
 - <csr-id-a33d28e4bc3f11779eaf0bf6cafd67800dbc4e0d/> tidying up
 - <csr-id-6c0f451646ea5840c79f112868637facdd08293c/> refactor away WireMsgUtils + make NodeJoin MsgKind
 - <csr-id-33a577f5e52029854757b9093e7b30535a7acabd/> lighten the CouldNotStoreData flow
   Dont return the full data, just the address. No
   need to fill up other nodes as removing this one will trigger
   that flow

### Chore

 - <csr-id-783d62461a65eb7c06b0d4f399b97216b6c75519/> sn_interface-0.16.14/sn_client-0.77.8/sn_node-0.72.25/sn_api-0.75.4/sn_cli-0.68.5

### New Features

 - <csr-id-ee90905abcf228d3a9d468ff0bb89a598cb0290d/> extra relocation criterias when relocate elder related node
   1, not to relocate elder to self section
   2, not counter join_as_relocated node as elder candidate immediately
 - <csr-id-17877a2cd0d712e7a773f54e3df05698f2f201bc/> require total participation for handover requests
 - <csr-id-8a8008cc668ef2578368ed74b96a75e0e03ce4e1/> remove unused node keypairs
 - <csr-id-4e9826c656e8763e888825ee2511e806a6e34928/> remove accounting of storage levels

### Bug Fixes

 - <csr-id-16526095eba5520325ff0fb4fcda5ff620ffbb49/> resolve issues cause the relocation failure
 - <csr-id-006b51e35435d61bac417674230e83d040814fac/> avoid reverifying for partial dag creation
   AE back and forth and other messaging can mean we make a good amount of
   partial_dags. Before this, we were pulling from a verified section_dag
   and verifying _every_ insert into our fresh partial dag.
   
   This was costly. So now we avoid it.

### Refactor

 - <csr-id-b2edff4b5b63b5d8a7905428b2c78b1d26598f07/> replace nodejoin type with flag
 - <csr-id-9f5b5ffb5975810e22c634f171984fcc803062aa/> proposal into multiple distinct messages
 - <csr-id-82ac545c7c3bbf1941fe9d9a80dcc2f99ff58a2f/> removing Mutex we hold around SendStream
   - Each SendStream is now moved into either `Cmd`s or functions
   instead of being shared using a Mutex around it.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 48 commits contributed to the release over the course of 23 calendar days.
 - 24 days passed between releases.
 - 18 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.14/sn_client-0.77.8/sn_node-0.72.25/sn_api-0.75.4/sn_cli-0.68.5 ([`783d624`](https://github.com/maidsafe/safe_network/commit/783d62461a65eb7c06b0d4f399b97216b6c75519))
    - Merge #1940 #1982 ([`3bddfdb`](https://github.com/maidsafe/safe_network/commit/3bddfdb6241116144e1e8869c192d20b89ae5534))
    - Merge #1981 ([`85da86c`](https://github.com/maidsafe/safe_network/commit/85da86cd0d4914489fc74125bb7a2655136f3508))
    - refactor(join): use existing msg-flow - Ae checks are made on TryJoin msg. - Elders drop msgs with invalid/unreachable parameters. - Unit tests more of `unit`-style than `e2e`. ([`8f596e9`](https://github.com/maidsafe/safe_network/commit/8f596e914f841839dffe89c67aa090f29bc03109))
    - extra relocation criterias when relocate elder related node ([`ee90905`](https://github.com/maidsafe/safe_network/commit/ee90905abcf228d3a9d468ff0bb89a598cb0290d))
    - Merge #1983 ([`c20b594`](https://github.com/maidsafe/safe_network/commit/c20b59440d148ec4b7c299bebae7f2030f511511))
    - remove unused priority ([`6f214e2`](https://github.com/maidsafe/safe_network/commit/6f214e2abae8be935652e93edda6d7eec2354af6))
    - Merge #1979 ([`6b8e25c`](https://github.com/maidsafe/safe_network/commit/6b8e25c6195c59d50ed61351e21570a594d209e8))
    - require total participation for handover requests ([`17877a2`](https://github.com/maidsafe/safe_network/commit/17877a2cd0d712e7a773f54e3df05698f2f201bc))
    - Merge #1974 ([`5afb1d0`](https://github.com/maidsafe/safe_network/commit/5afb1d064737daad6961ad290c2ff7c3ff2f1e38))
    - resolve issues cause the relocation failure ([`1652609`](https://github.com/maidsafe/safe_network/commit/16526095eba5520325ff0fb4fcda5ff620ffbb49))
    - Merge #1962 ([`61f7d98`](https://github.com/maidsafe/safe_network/commit/61f7d98c84df9d465d9e54c06e3d5569ceff097c))
    - Merge #1963 ([`1eedb03`](https://github.com/maidsafe/safe_network/commit/1eedb030f46157de68e4b12f45b74ccc4692f790))
    - feat(keypair): improves perf for smaller ranges - This becomes more important after a few splits. ([`43a06e0`](https://github.com/maidsafe/safe_network/commit/43a06e083f892dc56bbaa0485f1c0e76675428a4))
    - replace nodejoin type with flag ([`b2edff4`](https://github.com/maidsafe/safe_network/commit/b2edff4b5b63b5d8a7905428b2c78b1d26598f07))
    - Merge #1960 ([`2d52a72`](https://github.com/maidsafe/safe_network/commit/2d52a72d177653317d00655e772eee6a174b9716))
    - skip section_tree update earlier for outdated sap ([`7965789`](https://github.com/maidsafe/safe_network/commit/7965789548e9e5deb64501a9a774532042ce6c16))
    - Merge #1956 ([`d005784`](https://github.com/maidsafe/safe_network/commit/d005784be478c93a3e801e090f37ccf17a4acc19))
    - some optimisations during AE probe ([`3b9304b`](https://github.com/maidsafe/safe_network/commit/3b9304bab92f0715a134ce68afbfd08a7ad31e61))
    - Merge #1951 ([`24ca31f`](https://github.com/maidsafe/safe_network/commit/24ca31fd53c570c7c97849b74ded850c05273353))
    - happy new year 2023 ([`21af053`](https://github.com/maidsafe/safe_network/commit/21af053a5be2317be356e760c2b581c0f870a396))
    - Merge #1941 ([`a8227e8`](https://github.com/maidsafe/safe_network/commit/a8227e8b3bda5f51d1de8bd39e9d7bba5705a93a))
    - improve logging and comments ([`027b164`](https://github.com/maidsafe/safe_network/commit/027b164f851209f0662e0a84ee839618d95af58d))
    - tidying up ([`a33d28e`](https://github.com/maidsafe/safe_network/commit/a33d28e4bc3f11779eaf0bf6cafd67800dbc4e0d))
    - proposal into multiple distinct messages ([`9f5b5ff`](https://github.com/maidsafe/safe_network/commit/9f5b5ffb5975810e22c634f171984fcc803062aa))
    - Merge #1948 ([`bc2d0c1`](https://github.com/maidsafe/safe_network/commit/bc2d0c1d6672b86c05be2dd08567531149ad7355))
    - avoid reverifying for partial dag creation ([`006b51e`](https://github.com/maidsafe/safe_network/commit/006b51e35435d61bac417674230e83d040814fac))
    - Merge #1945 ([`b4fa062`](https://github.com/maidsafe/safe_network/commit/b4fa062f39a6617d0998efbd6dace72e6ae265bf))
    - feat(ae): target 1 rand elder in 3 rand sections - Maintains the global network knowledge of all sections. ([`bf17cd2`](https://github.com/maidsafe/safe_network/commit/bf17cd21ca9dce28025a583c2bd6b8dcda477b2d))
    - Merge #1942 ([`28d5b96`](https://github.com/maidsafe/safe_network/commit/28d5b967404b1c28406328a18d88bd4c85f7a335))
    - refactor away WireMsgUtils + make NodeJoin MsgKind ([`6c0f451`](https://github.com/maidsafe/safe_network/commit/6c0f451646ea5840c79f112868637facdd08293c))
    - Merge #1891 ([`716717c`](https://github.com/maidsafe/safe_network/commit/716717c1b3db9a881858bf8d2570f7fb9f4979f0))
    - removing Mutex we hold around SendStream ([`82ac545`](https://github.com/maidsafe/safe_network/commit/82ac545c7c3bbf1941fe9d9a80dcc2f99ff58a2f))
    - Merge #1926 #1936 ([`acc88c5`](https://github.com/maidsafe/safe_network/commit/acc88c5d94900c840cb6c3111ef92fc24b0f3a3d))
    - Merge branch 'main' into proposal_refactor ([`0bc7f94`](https://github.com/maidsafe/safe_network/commit/0bc7f94c72c374d667a9b455c4f4f1830366e4a4))
    - remove unused node keypairs ([`8a8008c`](https://github.com/maidsafe/safe_network/commit/8a8008cc668ef2578368ed74b96a75e0e03ce4e1))
    - feat(storage): use nodes where adults were used - This continues the move over to also using elders for storage. ([`250da72`](https://github.com/maidsafe/safe_network/commit/250da72ea38b82037ae928ac0eeb8c4b91568448))
    - remove accounting of storage levels ([`4e9826c`](https://github.com/maidsafe/safe_network/commit/4e9826c656e8763e888825ee2511e806a6e34928))
    - refactor(cmd): remove disambiguation - Renames `ReplicateOneData` to `StoreData`, which is the cmd used when a client stores data. - This leaves `ReplicateDataBatch` as the unambiguous cmd, already exclusively used for data replication between nodes. ([`29ee1d5`](https://github.com/maidsafe/safe_network/commit/29ee1d5205c1f4d079160f133e27e1cd1039b406))
    - feat(storage): report threshold reached only - This removes the book keeping of storage level on Elders. - Makes Adults report only when threshold reached. - Makes Elders allow joins until split when majority of Adults full. ([`a216003`](https://github.com/maidsafe/safe_network/commit/a216003b6275d36f1b419ad3cc2be30adb72700d))
    - Merge #1873 ([`8be1563`](https://github.com/maidsafe/safe_network/commit/8be1563fcddde2323ae2f892687dc76f253f3fb2))
    - chore(naming): rename dysfunction - Uses the more common vocabulary in fault tolerance area. ([`f68073f`](https://github.com/maidsafe/safe_network/commit/f68073f2897894375f5a09b870e2bfe4e03c3b10))
    - Merge #1933 ([`b408f59`](https://github.com/maidsafe/safe_network/commit/b408f597cbb7f5ea4737af2b06f2fd1dbe3f1786))
    - lighten the CouldNotStoreData flow ([`33a577f`](https://github.com/maidsafe/safe_network/commit/33a577f5e52029854757b9093e7b30535a7acabd))
    - Merge #1931 ([`964a1bf`](https://github.com/maidsafe/safe_network/commit/964a1bfde64969a2335b1f6f4558d0ea917474b2))
    - fix(name): rename from interrogative to imperative - The naming was confusing as to what the method actually did. ([`f2327d7`](https://github.com/maidsafe/safe_network/commit/f2327d7072612d02218ba3a55b181539f78cdf6b))
    - chore(errors): fix naming - There was an aiming error in the sentence structure. It's not the proof chain that is not covered, it's the key that is not covered. ([`a9b45f1`](https://github.com/maidsafe/safe_network/commit/a9b45f10bb7086cf1a65311d6577f846fa6e2c02))
    - Merge #1920 ([`71f2373`](https://github.com/maidsafe/safe_network/commit/71f2373f6ccc75e35a92b0a8736fc5ff72f28655))
</details>

## v0.16.13 (2022-12-27)

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
 - 4 days passed between releases.
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

## v0.16.12 (2022-12-22)

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

## v0.16.11 (2022-12-22)

<csr-id-3f094260e46e52e7293315cd772000617233d53e/>
<csr-id-6bef36cadd09bba0bff9171a352813e3e860ee2c/>

### Chore

 - <csr-id-3f094260e46e52e7293315cd772000617233d53e/> rename to ReplicateDataBatch
   to be more clearly distinct from the single replication flow

### Chore

 - <csr-id-6bef36cadd09bba0bff9171a352813e3e860ee2c/> sn_interface-0.16.11/sn_client-0.77.6/sn_node-0.72.19

### Bug Fixes

 - <csr-id-c4b47f1fa7b3d814a0de236f8a50b2c9f89750f2/> dont bail on join if sap update errors
 - <csr-id-386bf375395ace0acf140ae6a8ea42df2457daa4/> remove async call and LogCtx
   The readlock in here could have been causing a deadlock

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.11/sn_client-0.77.6/sn_node-0.72.19 ([`6bef36c`](https://github.com/maidsafe/safe_network/commit/6bef36cadd09bba0bff9171a352813e3e860ee2c))
    - Merge #1917 ([`94fecdf`](https://github.com/maidsafe/safe_network/commit/94fecdff1270a7f215095f7419cfa1bb649213ce))
    - dont bail on join if sap update errors ([`c4b47f1`](https://github.com/maidsafe/safe_network/commit/c4b47f1fa7b3d814a0de236f8a50b2c9f89750f2))
    - remove async call and LogCtx ([`386bf37`](https://github.com/maidsafe/safe_network/commit/386bf375395ace0acf140ae6a8ea42df2457daa4))
    - rename to ReplicateDataBatch ([`3f09426`](https://github.com/maidsafe/safe_network/commit/3f094260e46e52e7293315cd772000617233d53e))
</details>

## v0.16.10 (2022-12-21)

<csr-id-bf159dc0477417bfd35b0f778822dbdeb3dd0023/>
<csr-id-5ca4e906c3ff3a55cdedcff1203df57f9f5d4767/>
<csr-id-73e2c106ae61177617f0fbb1ce1306ad3102bf5f/>

### Refactor

 - <csr-id-bf159dc0477417bfd35b0f778822dbdeb3dd0023/> serialise the msg header only once when replicating data to holders
   - Also removing some minor but unnecessary WireMsg caching, and some objs cloning.

### Chore (BREAKING)

 - <csr-id-73e2c106ae61177617f0fbb1ce1306ad3102bf5f/> removing unused Cargo features

### Chore

 - <csr-id-5ca4e906c3ff3a55cdedcff1203df57f9f5d4767/> sn_interface-0.16.10/sn_node-0.72.18

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
    - sn_interface-0.16.10/sn_node-0.72.18 ([`5ca4e90`](https://github.com/maidsafe/safe_network/commit/5ca4e906c3ff3a55cdedcff1203df57f9f5d4767))
    - Merge #1921 ([`c3b09c5`](https://github.com/maidsafe/safe_network/commit/c3b09c5a851ce23ae4628455c7c7f3f17b58ed8c))
    - serialise the msg header only once when replicating data to holders ([`bf159dc`](https://github.com/maidsafe/safe_network/commit/bf159dc0477417bfd35b0f778822dbdeb3dd0023))
    - removing unused Cargo features ([`73e2c10`](https://github.com/maidsafe/safe_network/commit/73e2c106ae61177617f0fbb1ce1306ad3102bf5f))
</details>

## v0.16.9 (2022-12-20)

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

## v0.16.8 (2022-12-20)

<csr-id-bb11b8369c36d20eb926d11fd7fbaa41ff37f011/>

### Chore

 - <csr-id-bb11b8369c36d20eb926d11fd7fbaa41ff37f011/> sn_interface-0.16.8/sn_node-0.72.12

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.8/sn_node-0.72.12 ([`bb11b83`](https://github.com/maidsafe/safe_network/commit/bb11b8369c36d20eb926d11fd7fbaa41ff37f011))
    - Merge #1910 ([`f9cd9d6`](https://github.com/maidsafe/safe_network/commit/f9cd9d61a7b9229c14ea284c8aa9bf10a9f78bbd))
    - Revert "feat(join): prevent joins from nodes behind NAT" ([`c46bb99`](https://github.com/maidsafe/safe_network/commit/c46bb9934d7c12881dcac887ae55fe796027525d))
</details>

## v0.16.7 (2022-12-20)

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

## v0.16.6 (2022-12-19)

<csr-id-8fcbf73821b9cbde8ed2d87910842134e179fdbf/>

### Chore

 - <csr-id-8fcbf73821b9cbde8ed2d87910842134e179fdbf/> sn_interface-0.16.6/sn_node-0.72.10

### New Features

 - <csr-id-6fa35bc5b094583b728d8d068d9ae21df12d40b9/> bundle messages according to size and number

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
    - sn_interface-0.16.6/sn_node-0.72.10 ([`8fcbf73`](https://github.com/maidsafe/safe_network/commit/8fcbf73821b9cbde8ed2d87910842134e179fdbf))
    - Merge #1907 ([`8ebd0a6`](https://github.com/maidsafe/safe_network/commit/8ebd0a67d548169fc4cbf716f0c940425096264f))
    - bundle messages according to size and number ([`6fa35bc`](https://github.com/maidsafe/safe_network/commit/6fa35bc5b094583b728d8d068d9ae21df12d40b9))
</details>

## v0.16.5 (2022-12-17)

<csr-id-a8d7efe0b55280756811c571525b2947ca268bfc/>

### Chore

 - <csr-id-a8d7efe0b55280756811c571525b2947ca268bfc/> sn_interface-0.16.5/sn_node-0.72.7

### New Features

 - <csr-id-8aa694171b1dd5c2505259e67d6e3434ee94d213/> prevent joins from nodes behind NAT

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.5/sn_node-0.72.7 ([`a8d7efe`](https://github.com/maidsafe/safe_network/commit/a8d7efe0b55280756811c571525b2947ca268bfc))
    - Merge #1900 ([`9650289`](https://github.com/maidsafe/safe_network/commit/96502896245fc41a3ef619d3959f4938413e938c))
    - prevent joins from nodes behind NAT ([`8aa6941`](https://github.com/maidsafe/safe_network/commit/8aa694171b1dd5c2505259e67d6e3434ee94d213))
    - Merge branch 'main' into AlternateNetworkLimitation ([`5354f5e`](https://github.com/maidsafe/safe_network/commit/5354f5e9a0c0ac2145c5c5063b28d48f7bc3a30d))
</details>

## v0.16.4 (2022-12-16)

<csr-id-e0f052e46dcfb2beda4edc414fa7f560726fcd73/>
<csr-id-837c70af642b904f42121aa0a08f697eba551826/>

### Chore

 - <csr-id-e0f052e46dcfb2beda4edc414fa7f560726fcd73/> revert change split detection instead of size
   This reverts commit 38ebca089ed7134a63d9fefbf69f4f791b5858fb.

### Chore

 - <csr-id-837c70af642b904f42121aa0a08f697eba551826/> sn_interface-0.16.4/sn_node-0.72.4

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.4/sn_node-0.72.4 ([`837c70a`](https://github.com/maidsafe/safe_network/commit/837c70af642b904f42121aa0a08f697eba551826))
    - Merge #1895 ([`266a11a`](https://github.com/maidsafe/safe_network/commit/266a11aba08c7a7a0673499cf94144273dd48111))
    - revert change split detection instead of size ([`e0f052e`](https://github.com/maidsafe/safe_network/commit/e0f052e46dcfb2beda4edc414fa7f560726fcd73))
</details>

## v0.16.3 (2022-12-16)

<csr-id-540cd9fe6a95ab32894d3035e04a812de33de326/>
<csr-id-01dc60676d5740dc7dd6250edb130b46a33cc168/>
<csr-id-119ae2d7661d162371749b8466cfd2e9b85d910f/>

### Chore

 - <csr-id-540cd9fe6a95ab32894d3035e04a812de33de326/> fix rustdoc warnings
 - <csr-id-01dc60676d5740dc7dd6250edb130b46a33cc168/> fix new clippy warnings

### Chore

 - <csr-id-119ae2d7661d162371749b8466cfd2e9b85d910f/> sn_interface-0.16.3/sn_client-0.77.2/sn_api-0.75.2/sn_cli-0.68.1

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.3/sn_client-0.77.2/sn_api-0.75.2/sn_cli-0.68.1 ([`119ae2d`](https://github.com/maidsafe/safe_network/commit/119ae2d7661d162371749b8466cfd2e9b85d910f))
    - fix rustdoc warnings ([`540cd9f`](https://github.com/maidsafe/safe_network/commit/540cd9fe6a95ab32894d3035e04a812de33de326))
    - fix new clippy warnings ([`01dc606`](https://github.com/maidsafe/safe_network/commit/01dc60676d5740dc7dd6250edb130b46a33cc168))
</details>

## v0.16.2 (2022-12-15)

<csr-id-c42f6361cd6366c91d2e0c232abf0c070ab27ab7/>

### Chore

 - <csr-id-c42f6361cd6366c91d2e0c232abf0c070ab27ab7/> sn_interface-0.16.2/sn_node-0.72.2

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.2/sn_node-0.72.2 ([`c42f636`](https://github.com/maidsafe/safe_network/commit/c42f6361cd6366c91d2e0c232abf0c070ab27ab7))
    - Merge #1888 ([`fc0be25`](https://github.com/maidsafe/safe_network/commit/fc0be25da404d64a33b1addb6499033883f5035a))
</details>

## v0.16.1 (2022-12-15)

<csr-id-7620ede57d6f01a63380ac144684b5d504ae4fb4/>
<csr-id-80201067111349306a651a3f42a8ca740f48abaa/>
<csr-id-82c0cf683f8052374eafbb859176c69d52956c72/>
<csr-id-6e84b0aa25bfd6ecff45812cc32e189245b8ec3a/>

### Chore

 - <csr-id-7620ede57d6f01a63380ac144684b5d504ae4fb4/> removing unused 'url' dependency
 - <csr-id-80201067111349306a651a3f42a8ca740f48abaa/> use latest 0.33 qp2p

### Chore

 - <csr-id-82c0cf683f8052374eafbb859176c69d52956c72/> sn_interface-0.16.1/sn_client-0.77.1/sn_node-0.72.1/sn_api-0.75.1
 - <csr-id-6e84b0aa25bfd6ecff45812cc32e189245b8ec3a/> removing unused payload_debug field from msgs

### New Features

 - <csr-id-38ebca089ed7134a63d9fefbf69f4f791b5858fb/> change split detection instead of size
 - <csr-id-64742eeaa2b21646820537c1b8b703ab50b09906/> prevent splits for next test-net

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 8 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 6 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.16.1/sn_client-0.77.1/sn_node-0.72.1/sn_api-0.75.1 ([`82c0cf6`](https://github.com/maidsafe/safe_network/commit/82c0cf683f8052374eafbb859176c69d52956c72))
    - removing unused payload_debug field from msgs ([`6e84b0a`](https://github.com/maidsafe/safe_network/commit/6e84b0aa25bfd6ecff45812cc32e189245b8ec3a))
    - Merge #1885 ([`79439fb`](https://github.com/maidsafe/safe_network/commit/79439fb7c2d3ec01115960a893fcd8ce03da1790))
    - removing unused 'url' dependency ([`7620ede`](https://github.com/maidsafe/safe_network/commit/7620ede57d6f01a63380ac144684b5d504ae4fb4))
    - use latest 0.33 qp2p ([`8020106`](https://github.com/maidsafe/safe_network/commit/80201067111349306a651a3f42a8ca740f48abaa))
    - Merge #1884 ([`3f3175e`](https://github.com/maidsafe/safe_network/commit/3f3175ed7e006d68176670b31ddded2cef024b15))
    - change split detection instead of size ([`38ebca0`](https://github.com/maidsafe/safe_network/commit/38ebca089ed7134a63d9fefbf69f4f791b5858fb))
    - prevent splits for next test-net ([`64742ee`](https://github.com/maidsafe/safe_network/commit/64742eeaa2b21646820537c1b8b703ab50b09906))
</details>

## v0.16.0 (2022-12-13)

<csr-id-8bd83c64ca8cccc78dfe4641e522b4a02f03cbb8/>
<csr-id-e5b0dda1315a5299131cacd135b1d1ab66ed7073/>
<csr-id-f06b3e75ce97e7c749d2969276ad6533369806bb/>
<csr-id-7ac8d43bb3f559d01d9eac829a19e171a401e1a8/>
<csr-id-2691c53daa36b82185a664482a55d9c893dc8439/>
<csr-id-379dd724ccd0b16c8c90e9885eb727b80c8d43da/>
<csr-id-7f288b389175f3165fdca383dfe5f51097cc591f/>
<csr-id-1cedb64746cd42a037856403c1f579e6b1a628fe/>
<csr-id-38b8f55121d8b7c461efa6dd0c0407c4fae93418/>
<csr-id-a55b74b4c8f9bede3c91a9426d4687df01138257/>
<csr-id-667009dc02e6bb17bfaa60e2374d5ab7b75a7be5/>
<csr-id-ee824e7785b8da770b5aa6bba3415a274a4e0d68/>
<csr-id-317fdb18ce227bc383f5637e6dd300ec94af20ff/>
<csr-id-6351fb878e1d31d36b790eb0c0fee1e16b7cbbc8/>
<csr-id-98abbbe7af8c870faa22d62819691054e07df718/>
<csr-id-428d2e9528c567e5ac46256100ecadcf496dd8e1/>
<csr-id-033d17f355bea910939e094770af73be89e642ad/>
<csr-id-e57d83235f60a16bd7e1ee801f35a599113dc71a/>
<csr-id-a84fb5a3b1cf4a2c0c02dc20077f9d70fdb9e70d/>
<csr-id-5cbae9c10e9f1d4302d041a864bfee83d47834e0/>
<csr-id-ad8760eb7aa74b7055d0ca2a4ae66c8369865e70/>
<csr-id-51425951e8a66a8fd938a8dd2378b583cc80fb94/>
<csr-id-70d848a43b6df02812195845434849b98f409367/>
<csr-id-a0b2df5a0b12c70872dfc854d660afd0cf8b21aa/>
<csr-id-9a1cdf6f0135ce53f43a48c4346aff9023ccad33/>
<csr-id-9992d9701ecadff2b7682e47387014b9d11dba63/>
<csr-id-85a64394a70b2d69033fc2f175726afec1afb092/>
<csr-id-100e2ae70d21e141e1ebbc324f8b06e3d3f1a01c/>
<csr-id-e973eee96c9065ce87a1fa65ae45d9be8d6f940c/>
<csr-id-03da7f67fff1fa5bb06d60a66dfdb531506dec4c/>
<csr-id-71d992ba1405a48a8593f8f5aa3288296bd59af6/>
<csr-id-859fc30fa70ce41ceb910e0352c71dda5c5501ce/>
<csr-id-e01a964b7aee8fde125dd56b3cfa78498c4cbb41/>
<csr-id-65e03eba133a9cc19ff5460039478c7cc337fd81/>
<csr-id-abeaf725f2085ba86c34a81ef02f46193f239d1b/>
<csr-id-af3228c5eb6dc68cb87a0ca72ceb578a5fc7c88e/>
<csr-id-941ffb730776ee82040b3880210fdfb465f70c7f/>
<csr-id-b6474691ea6af5ee441b02f6cb9c3cf2b8f97459/>
<csr-id-c7de08209c659ec93557d6ea10e0bcd8c3b74d8b/>
<csr-id-230a6ed7f1f4193fa36b2fbb83bea072f4944c1d/>
<csr-id-994b1ef49ffee7c1b02b6361297bacd190e9b5e2/>
<csr-id-5179cf2dec47295f9673212efa6e23e9531e5ea3/>
<csr-id-6ca7f4377308a0dd47dbd17a3d01b07321d9b8a9/>
<csr-id-e8ab025a3454005890418b10a50560b3c65fd68f/>
<csr-id-3dc0bb1f0d8d04c8a92a75eab73e10721b105a10/>
<csr-id-bc2c4ee21335b627e3e998dd56209f72f20aac90/>
<csr-id-4a466e5a14b61f0dcf5467298d11d831a9a8d7e2/>
<csr-id-9f539e9a8dd9c22e7440539114b2fbdaaeb34515/>
<csr-id-3353ab617b438ca12cdac68a1f7956e3050eefcf/>
<csr-id-093ea5bfc200f940662c5c0e458c38c5c77294a9/>
<csr-id-4b6569ab2a9face420385d29d7baab31d8ca4d1e/>
<csr-id-9f8ecf90470ac18de31a956c1eee5f9f2d4c77a7/>
<csr-id-e97169b8eb41525b21603513dafc0f8c79fa19b5/>
<csr-id-9fad752ce1849763ae16cdb42159b9dccf1a13d0/>
<csr-id-633dfc836c10eafc54dedefc53b2cbc9526970bb/>
<csr-id-ab22c6989f55994065f0d859b91e141f7489a722/>
<csr-id-32744bcf6c94d9a7cda81813646560b70be53eb2/>
<csr-id-ba78a41f509a3015a5319e09e1e748ac91952b70/>
<csr-id-72abbfbc583b5b0dc99a0f7d90cb4d7eb72bd8c4/>
<csr-id-3302aee7a41abd58b6deba18cc690c5e74aabff4/>
<csr-id-3215110b021aaa7d3b755b7e80432aeed1e0b436/>
<csr-id-acaa90a13d598915bafc3584c70826f233d89881/>
<csr-id-07d0991fed28d49c9be85d44a3343b66fac076d9/>
<csr-id-f289de53f3894a58a6e4db51ce81aaf34f276490/>
<csr-id-452ef9c5778ad88270f4e251adc49ccbc9b3cb09/>
<csr-id-85f4d00e81ac5bf67b6be89d7ff51b7bb1060ed6/>
<csr-id-dd45c8f42b4f8674eeaea90aa27a465bd3bae0a2/>
<csr-id-072c5d4c5de7810a0837144853435e2ff2d091d0/>
<csr-id-610880711da814c7717c665e9cb34a729bda5797/>
<csr-id-1152b2764e955edd80fb33921a8d8fe52654a896/>
<csr-id-60e333d4ced688f3382cde513300d38790613692/>
<csr-id-6343b6fd21fe3bf81412d922da5e14b2c8b6f3c5/>
<csr-id-73f5531790ef8817ed3551fd9e4bcbcc7fc6f4f9/>
<csr-id-301aeeb687561106f8e2fd6bab1133159d22a6f1/>
<csr-id-6be0ea16b0ffe2c153c6a13f36916a91fb58cd05/>
<csr-id-fc0c7512144c0c42184b6ae1b5a64e4d894d0eab/>
<csr-id-b98d8f6b11a19a72187535b188faef0caf8ba578/>
<csr-id-80917f19125222ce6892e45487f2abe098fefd7a/>
<csr-id-bdf50e7ad1214ef4bb48c0a12db8a7700193bb2a/>
<csr-id-a973b62a8ef48acc92af8735e7e7bcac94e0092f/>
<csr-id-d550b553acbd70d4adb830a0600f7da7b833ee18/>
<csr-id-d67e502aab180f79869ffc240d94df4812f95a5e/>
<csr-id-ea1d0490f0b67a9f39bd98b2bd5830a0f63fbf6e/>
<csr-id-e3bb817e20843f68ee21e9a5dd7e52c8a6e92b88/>

### Chore

 - <csr-id-8bd83c64ca8cccc78dfe4641e522b4a02f03cbb8/> remove `network_utils` module
 - <csr-id-e5b0dda1315a5299131cacd135b1d1ab66ed7073/> minor logging improvements to help debug msgs arriving/processing on client and nodes
 - <csr-id-f06b3e75ce97e7c749d2969276ad6533369806bb/> upgrading qp2p to version 0.31.0
 - <csr-id-7ac8d43bb3f559d01d9eac829a19e171a401e1a8/> connect to relevant nodes first
 - <csr-id-2691c53daa36b82185a664482a55d9c893dc8439/> remove force_new_link as unused
 - <csr-id-379dd724ccd0b16c8c90e9885eb727b80c8d43da/> log which peer we're getting locks for
 - <csr-id-7f288b389175f3165fdca383dfe5f51097cc591f/> address review comments
 - <csr-id-1cedb64746cd42a037856403c1f579e6b1a628fe/> short-circuit conn creation after write lock
   If we've been waiting on the write lock, there's a high chance that a new connection exists, which we should then use. Otherwise we're repeatedly creating connections.
 - <csr-id-38b8f55121d8b7c461efa6dd0c0407c4fae93418/> Pass around MyNodeState to avoid holding locks
   For longer running message handling, we now pass around the inital
   MyNodeState. This avoids a tonnnn of read locks and therefore hopefully
   prevents holding up write and reads needlessly.
 - <csr-id-a55b74b4c8f9bede3c91a9426d4687df01138257/> replace `TestSAP` with `TestSapBuilder`
 - <csr-id-667009dc02e6bb17bfaa60e2374d5ab7b75a7be5/> remove duplicate strum/strum_macros/heck deps
 - <csr-id-ee824e7785b8da770b5aa6bba3415a274a4e0d68/> bump blsttc to 8.0.0
 - <csr-id-317fdb18ce227bc383f5637e6dd300ec94af20ff/> tidy up some error types
 - <csr-id-6351fb878e1d31d36b790eb0c0fee1e16b7cbbc8/> remove intermediate function
 - <csr-id-98abbbe7af8c870faa22d62819691054e07df718/> remove ExpiringConnection struct
   dont disconnect link when sutting down channel
   
   Allow dropping of link to do all cleanup
 - <csr-id-428d2e9528c567e5ac46256100ecadcf496dd8e1/> deem spentproof shares < 5 data not found
 - <csr-id-033d17f355bea910939e094770af73be89e642ad/> rejig retry loop, use max retries and keep querying if data not found until timeout
   * rejigs data_not_found() for query response to take into account
   EntryNotFound errors too. (This should hopefully stabilise some
   register permission tests)
 - <csr-id-e57d83235f60a16bd7e1ee801f35a599113dc71a/> write query responses as they come in
   as opposed to requiring a channel to be in place.
 - <csr-id-a84fb5a3b1cf4a2c0c02dc20077f9d70fdb9e70d/> impl custom `Debug` trait to compact the logs
 - <csr-id-5cbae9c10e9f1d4302d041a864bfee83d47834e0/> remove dashmap dep in sn_{dysfunction, interface}
 - <csr-id-ad8760eb7aa74b7055d0ca2a4ae66c8369865e70/> use or_default instead of wasteful or_insert
 - <csr-id-51425951e8a66a8fd938a8dd2378b583cc80fb94/> use gen_section_tree_update test utility
 - <csr-id-70d848a43b6df02812195845434849b98f409367/> rename SectionAuth to SectionSigned
 - <csr-id-a0b2df5a0b12c70872dfc854d660afd0cf8b21aa/> improve namings
 - <csr-id-9a1cdf6f0135ce53f43a48c4346aff9023ccad33/> compile after rebase
 - <csr-id-9992d9701ecadff2b7682e47387014b9d11dba63/> compile after rebase
 - <csr-id-85a64394a70b2d69033fc2f175726afec1afb092/> compile after rebase
 - <csr-id-100e2ae70d21e141e1ebbc324f8b06e3d3f1a01c/> remove spend retry on client
   The spend retry depends on providing new network knowledge. We will be using another mechanism to
   obtain this knowledge, which is not available at the moment. Once it's available, we'll add the
   retry again.
   
   For now we decided it's best to remove it and only merge the node-side changes.
   
   This also fixes up various changes after the merge of the new SectionsDAG that replaced the
   SecuredLinkedList.
 - <csr-id-e973eee96c9065ce87a1fa65ae45d9be8d6f940c/> remove redundant genesis_key argument in `NetworkKnowledge` constructor
 - <csr-id-03da7f67fff1fa5bb06d60a66dfdb531506dec4c/> optimizations and code cleanup
 - <csr-id-71d992ba1405a48a8593f8f5aa3288296bd59af6/> show parent idx while debugging
 - <csr-id-859fc30fa70ce41ceb910e0352c71dda5c5501ce/> enable `SectionTree` proptest
 - <csr-id-e01a964b7aee8fde125dd56b3cfa78498c4cbb41/> provide custom rng to generate random_sap, SecretKeySet

### Test

 - <csr-id-d67e502aab180f79869ffc240d94df4812f95a5e/> check excessive reg write error

### Chore

 - <csr-id-ea1d0490f0b67a9f39bd98b2bd5830a0f63fbf6e/> upgrade sn_dbc and blsttc
   Upgrade both of these crates to resolve a publishing issue regarding a crate that had been yanked
   being pulled in to the dependency graph.
 - <csr-id-e3bb817e20843f68ee21e9a5dd7e52c8a6e92b88/> sn_interface-0.16.0/sn_dysfunction-0.15.0/sn_client-0.77.0/sn_node-0.72.0/sn_api-0.75.0/sn_cli-0.68.0

### New Features

<csr-id-057ce1ce1e174102e23d96cfcd2ab1d090a6f1dc/>
<csr-id-2020ef1a91c8520abc4bb74d3de6385b8cd283b4/>
<csr-id-864c023e26697a609a9ad230c04e7aef7416650c/>
<csr-id-0cd47ad56e0d93e3e99feb0dfcea8094f871ff6f/>

 - <csr-id-fc9e4feab2a504168221fe2bd893d9327a45ae6f/> update sibling on split
 - <csr-id-bcdb4fc8035c108f2e24c14983af30ddfb54b8fd/> remove AE hold back
 - <csr-id-5a39a843c5570993b0e27780a1c2887bbf7a3212/> cmd responses sent from adults over stream
   Add in the stream initialisation from elders to adults.
 - <csr-id-3089b2c8d3f3ee099ff9e0880a96720b319e52a4/> try to reconnect once when the client lost a connection to a peer
 - <csr-id-3fd0a00bad2f9ca266a56de2086b54088459e153/> use bi stream from client; process in Node
 - <csr-id-95436a1f722bfd02a735dc3cf2f171a8b70de552/> move to event driven msg handling
   put incoming msgs and cmd handling into their own threads
 - <csr-id-a5bf211daa0272597f1a2d852a17592258a2115a/> force retries to use fresh connection
 - <csr-id-14ba1a8a034e488728762df09398347f8b909d65/> remove age stepping in genesis sections
 - <csr-id-8c348b2286925edf319daede9a064399b41f6ec1/> remove NodeMsgAuthority altogether
 - <csr-id-5019dd3896d278227878ffd91ce14d0cecb2b9dd/> remove node auth
 - <csr-id-a60eef81e20bee599f9b861822c8e8c3424073af/> get rid of InvalidState Error altogether
 - <csr-id-50e877d9ac75b62dfa9e851564b6fd6b60167ca3/> section peers uses BTreeMaps instead of DashMap
 - <csr-id-266f3120b574133fccc39405d3a5a02d05806dfc/> remove section share auth
 - <csr-id-da2833bc03e2b027294bec6a4fcc2f4d8970f5b8/> fix tests, restore aggregator reset uppon sig completion
 - <csr-id-2bcc1a0735d9cd727f71c93b58de85cd01c8a603/> universal aggregator without timers
 - <csr-id-8f2c41406165351796ca8df36d2ae3457e000ab9/> adapt to empty vec change in sn_sdkg
 - <csr-id-b30459ffc565bb38ce7975c443b8df8139b77752/> integrate recursive sn_sdkg changes
 - <csr-id-b9d39b09e39b7d91fd556abeb385310f50a0eee0/> use separate genesis dbc and section keys
   For more information, see the doc comments on the diff.
   
   The generation of the DBC was also updated to use non-zero values for the `alpha` and `r` fields of
   `MlsagMaterial`, because zeroes and ones are special values in elliptic curve cryptography.
   
   Also fixed a Clippy warning.
 - <csr-id-f4b5d6b207a0874621d777582ca5906e69196e06/> gossip DKG termination to trigger handover
 - <csr-id-3ed0e0167d5bec04d6c57d94ce1a63d1f043a1a0/> dkg start miss handling
 - <csr-id-ba859ae5f064d6dc15aa563ee956a26e85df1d45/> some necessary cleanup
 - <csr-id-f5d53c188a03150a06bdee97fb585f7900b7c251/> compiling sdkg integration
 - <csr-id-5c8b1f50d1bf346d45bd2a9faf92bbf33cb448da/> client retry spend on unknown section key
   When the client receives an unknown section key error, it will obtain the proof chain and SAP for
   the unknown section and resubmit the request with this additional information.
   
   There is no automated client test for this scenario. We went to great lengths to try, but it proved
   not worth the effort. It was too difficult to modify the network knowledge with a fake section and
   still have things function correctly. The scenario is unit tested on the node side, and we done
   enough testing to know that the retry loop does what's intended.
   
   There are a few misc changes along with this commit:
   
   * Debugging message to indicate a spend request being processed correctly, which proved useful when
   trying to get the automated test working.
* Remove the current section key from the unknown section key error. It's not necessary to include
     this.
* When running the baby fleming network with the Makefile, include log messages from `sn_interface`.
* Fix up git-based references to `sn_dbc` crate.
 - <csr-id-6010f2c369911876cb224a1a5c08fc38d428288b/> limit registers to 1024 entries
   With the 1024 bytes per entry, that's 1mb per reg tops

### Bug Fixes

<csr-id-4c0a0c1b1419b44a8ef48a43f7f5bbd666eb1202/>
<csr-id-4c039335707851c8f7ec71703acfb646184fa30a/>
<csr-id-20fc9c2cc3a039cd99aeeae9d0f575a1c838c939/>
<csr-id-652161a92c6c6285c75b969f4e2f742ff283efe1/>
<csr-id-e71eaa8467b244f875726b40e09ea255b3811c40/>
<csr-id-93fdda0505671426119f300a0444f7c6e51756a8/>
<csr-id-9acc41d5884ce4e6f647937fe56df906a7f86452/>
<csr-id-c73b83b0a4ac0e3072b41e1af5b42b90c8a54177/>
<csr-id-951e67cc7e06a78270dad363d7085af4cd136f65/>
<csr-id-23d98ebf05406f7addb22c9a939700c0683c4a2e/>
<csr-id-e92400023cf77c323d228b9e1fa18d85c33040d1/>
<csr-id-ffa2cf3c9b49aaa3f5f64b88e0800a4047948378/>
<csr-id-bb7a65517c2b1783f3f8b7865fff8d0c5f7bc705/>
<csr-id-42c43320c581df1b315fd2f199ebe74adcfbb803/>
<csr-id-22a0de0e3bd8478a10729112ec1b3bce9ba5cb90/>
<csr-id-4846aec802200b60c13a78aa092e5eed41642da5/>
<csr-id-4884c511d302522aa408ebf9350a7ff6cefeecb7/>

 - <csr-id-a5f5deca04a21ddc7ae691cd1da3ca598dae05b0/> adapting tests to work with the signed-sap logic changes
 - <csr-id-de8ed40b9f1ad353c9a8ded58db5de76acee21e1/> reconnect upon any LinkError::Connection(_) error when sending a msg on a bi-stream
   - Upgrading qp2p to v0.32.0.

### Other

 - <csr-id-65e03eba133a9cc19ff5460039478c7cc337fd81/> get `SecretKeySet` from `SecretKeyShare`
 - <csr-id-abeaf725f2085ba86c34a81ef02f46193f239d1b/> add unit tests
 - <csr-id-af3228c5eb6dc68cb87a0ca72ceb578a5fc7c88e/> add unit tests for `SectionPeers::update`
 - <csr-id-941ffb730776ee82040b3880210fdfb465f70c7f/> add unit tests for `SectionTree::update`
 - <csr-id-b6474691ea6af5ee441b02f6cb9c3cf2b8f97459/> sn_dkg integration
 - <csr-id-c7de08209c659ec93557d6ea10e0bcd8c3b74d8b/> minor refactoring and fixing issue reported by new clippy version
 - <csr-id-230a6ed7f1f4193fa36b2fbb83bea072f4944c1d/> spend with updated network knowledge
   Previously I had a placeholder in for this case, but now have something working.
   
   The test requires having two network sections and one of the input DBCs for a transaction being
   signed by the other section key.
   
   The `TestNodeBuilder` was extended with a function that creates a section without a creating a node,
   and this included being able to provide a section chain and tree.

### Refactor

 - <csr-id-994b1ef49ffee7c1b02b6361297bacd190e9b5e2/> remove extra logic around SAP update
 - <csr-id-5179cf2dec47295f9673212efa6e23e9531e5ea3/> move to sn_interfaces
 - <csr-id-6ca7f4377308a0dd47dbd17a3d01b07321d9b8a9/> mark redirect code with TODO to replace w/ retry + AEProbe
 - <csr-id-e8ab025a3454005890418b10a50560b3c65fd68f/> remove unnecessary Box around JoinResponse
 - <csr-id-3dc0bb1f0d8d04c8a92a75eab73e10721b105a10/> remove section_tree_updates from within join messages
 - <csr-id-bc2c4ee21335b627e3e998dd56209f72f20aac90/> use send_join_response helper where it makes sense
 - <csr-id-4a466e5a14b61f0dcf5467298d11d831a9a8d7e2/> make Proposal saner and add docs
 - <csr-id-9f539e9a8dd9c22e7440539114b2fbdaaeb34515/> provide age pattern to generate `NodeInfo`
 - <csr-id-3353ab617b438ca12cdac68a1f7956e3050eefcf/> organize more network knowledge utils
 - <csr-id-093ea5bfc200f940662c5c0e458c38c5c77294a9/> organize key related test utilites
 - <csr-id-4b6569ab2a9face420385d29d7baab31d8ca4d1e/> organize network_knowledge test utilites
 - <csr-id-9f8ecf90470ac18de31a956c1eee5f9f2d4c77a7/> remove redundant `bls::SecretKeySet` wrapper
 - <csr-id-e97169b8eb41525b21603513dafc0f8c79fa19b5/> remove from sn_interface
 - <csr-id-9fad752ce1849763ae16cdb42159b9dccf1a13d0/> remove some ? noise in tests
 - <csr-id-633dfc836c10eafc54dedefc53b2cbc9526970bb/> AuthKind into MsgKind without node sig
 - <csr-id-ab22c6989f55994065f0d859b91e141f7489a722/> assert_lists asserts instead of returning Result
 - <csr-id-32744bcf6c94d9a7cda81813646560b70be53eb2/> remove `SectionAuthorityProvider`, `SectionTreeUpdate` messages
 - <csr-id-ba78a41f509a3015a5319e09e1e748ac91952b70/> move `MembershipState`, `RelocateDetails` out from messaging
 - <csr-id-72abbfbc583b5b0dc99a0f7d90cb4d7eb72bd8c4/> remove `NodeState` message
 - <csr-id-3302aee7a41abd58b6deba18cc690c5e74aabff4/> move elders sig trust within DkgStart instead of deep within messaging
 - <csr-id-3215110b021aaa7d3b755b7e80432aeed1e0b436/> fmt
 - <csr-id-acaa90a13d598915bafc3584c70826f233d89881/> Remove resource proof
   We have dysfunction
 - <csr-id-07d0991fed28d49c9be85d44a3343b66fac076d9/> adapt confusing auth related code
 - <csr-id-f289de53f3894a58a6e4db51ce81aaf34f276490/> various more renamings
 - <csr-id-452ef9c5778ad88270f4e251adc49ccbc9b3cb09/> rename a bunch of auth to sig
 - <csr-id-85f4d00e81ac5bf67b6be89d7ff51b7bb1060ed6/> simplify section sig
 - <csr-id-dd45c8f42b4f8674eeaea90aa27a465bd3bae0a2/> Looking at change to NodeSig
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

### Style

 - <csr-id-73f5531790ef8817ed3551fd9e4bcbcc7fc6f4f9/> applied clippy nightly
   Mainly lints `iter_kv_map` (e.g. use `keys()`, instead of `iter()`, which
   iterates over both keys and values where the value is not used) and `needless_borrow`.

### Test

 - <csr-id-301aeeb687561106f8e2fd6bab1133159d22a6f1/> verify entry_hash generation

### Chore (BREAKING)

 - <csr-id-6be0ea16b0ffe2c153c6a13f36916a91fb58cd05/> attempt to reduce allocations

 - <csr-id-fc0c7512144c0c42184b6ae1b5a64e4d894d0eab/> removing unnecessary error types, plus some sn_node log msg improvements

### New Features (BREAKING)

 - <csr-id-7afd7a95d39098fb5166785c215881233bab528a/> retry once if connection was lost when trying to send on a bi-stream
 - <csr-id-7106b7533e119dc94bbf19fa304f3eb1f8dc9425/> making AE msg for clients to be a variant of client response msg type

### Refactor (BREAKING)

 - <csr-id-b98d8f6b11a19a72187535b188faef0caf8ba578/> moving the `sn_interface::types::connections` module to the `sn_client` crate
   - This module doesn't belong to the `sn_interface` crate since it's not
   part of the network's protocol/API.
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

 - 181 commits contributed to the release over the course of 85 calendar days.
 - 85 days passed between releases.
 - 133 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Merge #1882 ([`16e82d1`](https://github.com/maidsafe/safe_network/commit/16e82d13cfeee993c85c04f1c6f90e4305c90487))
    - upgrade sn_dbc and blsttc ([`ea1d049`](https://github.com/maidsafe/safe_network/commit/ea1d0490f0b67a9f39bd98b2bd5830a0f63fbf6e))
    - Merge #1876 ([`adc5338`](https://github.com/maidsafe/safe_network/commit/adc5338724e966563d1d3603e4f3a33839c2becb))
    - check excessive reg write error ([`d67e502`](https://github.com/maidsafe/safe_network/commit/d67e502aab180f79869ffc240d94df4812f95a5e))
    - limit registers to 1024 entries ([`6010f2c`](https://github.com/maidsafe/safe_network/commit/6010f2c369911876cb224a1a5c08fc38d428288b))
    - sn_interface-0.16.0/sn_dysfunction-0.15.0/sn_client-0.77.0/sn_node-0.72.0/sn_api-0.75.0/sn_cli-0.68.0 ([`e3bb817`](https://github.com/maidsafe/safe_network/commit/e3bb817e20843f68ee21e9a5dd7e52c8a6e92b88))
    - Merge #1846 ([`56df839`](https://github.com/maidsafe/safe_network/commit/56df8392897e5d1641570942a3852644e4cce427))
    - adapting tests to work with the signed-sap logic changes ([`a5f5dec`](https://github.com/maidsafe/safe_network/commit/a5f5deca04a21ddc7ae691cd1da3ca598dae05b0))
    - remove extra logic around SAP update ([`994b1ef`](https://github.com/maidsafe/safe_network/commit/994b1ef49ffee7c1b02b6361297bacd190e9b5e2))
    - Merge #1855 ([`233fc5f`](https://github.com/maidsafe/safe_network/commit/233fc5f7ed26623c1bec7442503cb1fee81179ef))
    - Merge #1845 ([`9d0f958`](https://github.com/maidsafe/safe_network/commit/9d0f958a0d2bceb9aad7b93b51aa17acf3394b30))
    - reconnect upon any LinkError::Connection(_) error when sending a msg on a bi-stream ([`de8ed40`](https://github.com/maidsafe/safe_network/commit/de8ed40b9f1ad353c9a8ded58db5de76acee21e1))
    - chore(style): declare error once - Removes the two added declarations and reuses a local fn instead. ([`298690f`](https://github.com/maidsafe/safe_network/commit/298690fb99a0ef41e7ff6b091a4ec2df2c8349a6))
    - refactor(proposal): use HandoverCompleted - This replaces both NewElders and NewSections. Also - Adds/updates verify serialize for signing test case. - Updates split tests, giving that HandleNewSectionsAgreement cmd flow is now also tested. ([`d020ce2`](https://github.com/maidsafe/safe_network/commit/d020ce28926011e7135c51e1e9395ef0c4084fc2))
    - update sibling on split ([`fc9e4fe`](https://github.com/maidsafe/safe_network/commit/fc9e4feab2a504168221fe2bd893d9327a45ae6f))
    - Merge #1797 ([`d16ebf1`](https://github.com/maidsafe/safe_network/commit/d16ebf1efd9fb7199891e46e2114b40fa7cc8687))
    - remove `network_utils` module ([`8bd83c6`](https://github.com/maidsafe/safe_network/commit/8bd83c64ca8cccc78dfe4641e522b4a02f03cbb8))
    - Merge #1830 ([`dee4ffc`](https://github.com/maidsafe/safe_network/commit/dee4ffc9544b5c979a240bc547ecdef21b5801ca))
    - moving the `sn_interface::types::connections` module to the `sn_client` crate ([`b98d8f6`](https://github.com/maidsafe/safe_network/commit/b98d8f6b11a19a72187535b188faef0caf8ba578))
    - Merge #1820 ([`1bfbdd3`](https://github.com/maidsafe/safe_network/commit/1bfbdd31ce1b132bc468a433a2211c621b95291e))
    - retry once if connection was lost when trying to send on a bi-stream ([`7afd7a9`](https://github.com/maidsafe/safe_network/commit/7afd7a95d39098fb5166785c215881233bab528a))
    - Merge #1827 ([`4ec86e9`](https://github.com/maidsafe/safe_network/commit/4ec86e9f398eb905285a5d378278fff2fb122671))
    - log cleanup ([`4c0a0c1`](https://github.com/maidsafe/safe_network/commit/4c0a0c1b1419b44a8ef48a43f7f5bbd666eb1202))
    - Merge #1824 ([`9494582`](https://github.com/maidsafe/safe_network/commit/949458280b567aa6dce387b276c06c2cb55d7ca4))
    - applied clippy nightly ([`73f5531`](https://github.com/maidsafe/safe_network/commit/73f5531790ef8817ed3551fd9e4bcbcc7fc6f4f9))
    - Merge #1819 ([`ec37ad6`](https://github.com/maidsafe/safe_network/commit/ec37ad6142930c59d1aad2325ac09b8d6383484d))
    - make client receive stream log clearer ([`4c03933`](https://github.com/maidsafe/safe_network/commit/4c039335707851c8f7ec71703acfb646184fa30a))
    - Merge #1796 ([`e15180b`](https://github.com/maidsafe/safe_network/commit/e15180b53d1daaec76b7eba4637ffc16076c80af))
    - Merge #1809 ([`9042bd2`](https://github.com/maidsafe/safe_network/commit/9042bd2cba7466b2a21592488d5765e27d05eda5))
    - test(spentbook): add test 3 - spent proofs do not relate to input dbcs should return spentbook error - NB: this can probably be simplified ([`aa23604`](https://github.com/maidsafe/safe_network/commit/aa23604f2dd26a4ce51735796a9822152059f15f))
    - move to sn_interfaces ([`5179cf2`](https://github.com/maidsafe/safe_network/commit/5179cf2dec47295f9673212efa6e23e9531e5ea3))
    - refactor(msgs): remove one layer of indirection - ValidateMsg can be replaced with HandleMsg. ([`fcc72d9`](https://github.com/maidsafe/safe_network/commit/fcc72d9cf97d9a9ed3529ab0193aafef65540c70))
    - Merge #1793 ([`c5ab10f`](https://github.com/maidsafe/safe_network/commit/c5ab10f2831cc1f6978dfa518293649f08033e03))
    - attempt to reduce allocations ([`6be0ea1`](https://github.com/maidsafe/safe_network/commit/6be0ea16b0ffe2c153c6a13f36916a91fb58cd05))
    - Merge #1651 ([`aaaa01b`](https://github.com/maidsafe/safe_network/commit/aaaa01b32f40c4a5ca7618cd7f820efb14551440))
    - Merge #1744 #1792 ([`ea83392`](https://github.com/maidsafe/safe_network/commit/ea83392ccc9cbb79b175c29ba77c4a7e27a5398f))
    - minor logging improvements to help debug msgs arriving/processing on client and nodes ([`e5b0dda`](https://github.com/maidsafe/safe_network/commit/e5b0dda1315a5299131cacd135b1d1ab66ed7073))
    - mark redirect code with TODO to replace w/ retry + AEProbe ([`6ca7f43`](https://github.com/maidsafe/safe_network/commit/6ca7f4377308a0dd47dbd17a3d01b07321d9b8a9))
    - add test for new err case for SectionsDag::partial_dag ([`20fc9c2`](https://github.com/maidsafe/safe_network/commit/20fc9c2cc3a039cd99aeeae9d0f575a1c838c939))
    - remove unnecessary Box around JoinResponse ([`e8ab025`](https://github.com/maidsafe/safe_network/commit/e8ab025a3454005890418b10a50560b3c65fd68f))
    - remove section_tree_updates from within join messages ([`3dc0bb1`](https://github.com/maidsafe/safe_network/commit/3dc0bb1f0d8d04c8a92a75eab73e10721b105a10))
    - remove genesis special case from SectionTree::partial_dag ([`652161a`](https://github.com/maidsafe/safe_network/commit/652161a92c6c6285c75b969f4e2f742ff283efe1))
    - adapt the join disallowed test work with the new flow ([`e71eaa8`](https://github.com/maidsafe/safe_network/commit/e71eaa8467b244f875726b40e09ea255b3811c40))
    - use send_join_response helper where it makes sense ([`bc2c4ee`](https://github.com/maidsafe/safe_network/commit/bc2c4ee21335b627e3e998dd56209f72f20aac90))
    - upgrading qp2p to version 0.31.0 ([`f06b3e7`](https://github.com/maidsafe/safe_network/commit/f06b3e75ce97e7c749d2969276ad6533369806bb))
    - Merge #1789 ([`7fa2ab8`](https://github.com/maidsafe/safe_network/commit/7fa2ab88ddefaad9f157b70b8a700824ce986f31))
    - Merge branch 'main' into remove-dataaddress-from-ack ([`ac2548b`](https://github.com/maidsafe/safe_network/commit/ac2548b1890935eb94e8802902d8bb1df0aae8fd))
    - Merge #1785 ([`018eaab`](https://github.com/maidsafe/safe_network/commit/018eaab8bef81f4105318c34dc05c4d58412a60d))
    - feat(ack): remove dataaddress from ack Client should keep track of this, no need to burdon the network with more data transmission. ([`ead623b`](https://github.com/maidsafe/safe_network/commit/ead623bf50bfc3a5cb6539159ed2c863356d6f8c))
    - when failed to store data the Adult was returning a success response to Elder ([`93fdda0`](https://github.com/maidsafe/safe_network/commit/93fdda0505671426119f300a0444f7c6e51756a8))
    - Merge #1780 ([`5a35817`](https://github.com/maidsafe/safe_network/commit/5a35817c9f928fe66a745ac645d9560964e05e8b))
    - make Proposal saner and add docs ([`4a466e5`](https://github.com/maidsafe/safe_network/commit/4a466e5a14b61f0dcf5467298d11d831a9a8d7e2))
    - Merge #1776 ([`bb65746`](https://github.com/maidsafe/safe_network/commit/bb657464f8217aa1a41501c4025ceb5dc6d0aca7))
    - connect to relevant nodes first ([`7ac8d43`](https://github.com/maidsafe/safe_network/commit/7ac8d43bb3f559d01d9eac829a19e171a401e1a8))
    - remove force_new_link as unused ([`2691c53`](https://github.com/maidsafe/safe_network/commit/2691c53daa36b82185a664482a55d9c893dc8439))
    - log which peer we're getting locks for ([`379dd72`](https://github.com/maidsafe/safe_network/commit/379dd724ccd0b16c8c90e9885eb727b80c8d43da))
    - Merge #1765 ([`90a870e`](https://github.com/maidsafe/safe_network/commit/90a870ebe1ce5110b4b264e8e317acc30152ceb1))
    - making AE msg for clients to be a variant of client response msg type ([`7106b75`](https://github.com/maidsafe/safe_network/commit/7106b7533e119dc94bbf19fa304f3eb1f8dc9425))
    - Merge #1766 ([`19ffd04`](https://github.com/maidsafe/safe_network/commit/19ffd04ac02fe98c72c0c4d497c29bdf961e9201))
    - address review comments ([`7f288b3`](https://github.com/maidsafe/safe_network/commit/7f288b389175f3165fdca383dfe5f51097cc591f))
    - refactor(responses): return correct cmd response - Returns the ack corresponding to the cmd. - Renames `ClientMsgResponse` to `ClientDataResponse`. - Makes `NodeDataResponse` be handled like `ClientDataResponse`. - Moves data write acks to `NodeDataReponse`. - Makes `NodeEvent` only be Adult to Elder notifications. ([`bd3b46e`](https://github.com/maidsafe/safe_network/commit/bd3b46e686a6f47cc006ce1f5da2f3041a614b2d))
    - Merge #1761 ([`aa6b24a`](https://github.com/maidsafe/safe_network/commit/aa6b24adb1790bc97eca1ffdc4d265247ec4953e))
    - removing unnecessary error types, plus some sn_node log msg improvements ([`fc0c751`](https://github.com/maidsafe/safe_network/commit/fc0c7512144c0c42184b6ae1b5a64e4d894d0eab))
    - Merge #1749 ([`ad2574c`](https://github.com/maidsafe/safe_network/commit/ad2574cb7fad692c2f9924fd87130f0b0bb9e2c2))
    - short-circuit conn creation after write lock ([`1cedb64`](https://github.com/maidsafe/safe_network/commit/1cedb64746cd42a037856403c1f579e6b1a628fe))
    - Pass around MyNodeState to avoid holding locks ([`38b8f55`](https://github.com/maidsafe/safe_network/commit/38b8f55121d8b7c461efa6dd0c0407c4fae93418))
    - replace `TestSAP` with `TestSapBuilder` ([`a55b74b`](https://github.com/maidsafe/safe_network/commit/a55b74b4c8f9bede3c91a9426d4687df01138257))
    - feat(test_utils): introduce `TestSapBuilder` to generate custom SAPs Replaces the standalone utils into a single one to generate random SAPs for testing. ([`25b0753`](https://github.com/maidsafe/safe_network/commit/25b0753c4338d1360fe2caae90566750ad962a97))
    - Merge #1637 ([`45903a9`](https://github.com/maidsafe/safe_network/commit/45903a9988528f543b09afbb56a89d21effbb929))
    - breaking up client msg type separating requests from responses ([`80917f1`](https://github.com/maidsafe/safe_network/commit/80917f19125222ce6892e45487f2abe098fefd7a))
    - remove AE hold back ([`bcdb4fc`](https://github.com/maidsafe/safe_network/commit/bcdb4fc8035c108f2e24c14983af30ddfb54b8fd))
    - small changes ([`9acc41d`](https://github.com/maidsafe/safe_network/commit/9acc41d5884ce4e6f647937fe56df906a7f86452))
    - provide age pattern to generate `NodeInfo` ([`9f539e9`](https://github.com/maidsafe/safe_network/commit/9f539e9a8dd9c22e7440539114b2fbdaaeb34515))
    - organize more network knowledge utils ([`3353ab6`](https://github.com/maidsafe/safe_network/commit/3353ab617b438ca12cdac68a1f7956e3050eefcf))
    - get `SecretKeySet` from `SecretKeyShare` ([`65e03eb`](https://github.com/maidsafe/safe_network/commit/65e03eba133a9cc19ff5460039478c7cc337fd81))
    - organize key related test utilites ([`093ea5b`](https://github.com/maidsafe/safe_network/commit/093ea5bfc200f940662c5c0e458c38c5c77294a9))
    - organize network_knowledge test utilites ([`4b6569a`](https://github.com/maidsafe/safe_network/commit/4b6569ab2a9face420385d29d7baab31d8ca4d1e))
    - remove redundant `bls::SecretKeySet` wrapper ([`9f8ecf9`](https://github.com/maidsafe/safe_network/commit/9f8ecf90470ac18de31a956c1eee5f9f2d4c77a7))
    - remove from sn_interface ([`e97169b`](https://github.com/maidsafe/safe_network/commit/e97169b8eb41525b21603513dafc0f8c79fa19b5))
    - remove duplicate strum/strum_macros/heck deps ([`667009d`](https://github.com/maidsafe/safe_network/commit/667009dc02e6bb17bfaa60e2374d5ab7b75a7be5))
    - bump blsttc to 8.0.0 ([`ee824e7`](https://github.com/maidsafe/safe_network/commit/ee824e7785b8da770b5aa6bba3415a274a4e0d68))
    - tidy up some error types ([`317fdb1`](https://github.com/maidsafe/safe_network/commit/317fdb18ce227bc383f5637e6dd300ec94af20ff))
    - cmd responses sent from adults over stream ([`5a39a84`](https://github.com/maidsafe/safe_network/commit/5a39a843c5570993b0e27780a1c2887bbf7a3212))
    - do not consider as a data-not-found case when not enough spent-proof-shares were retrieved from SpentBook ([`c73b83b`](https://github.com/maidsafe/safe_network/commit/c73b83b0a4ac0e3072b41e1af5b42b90c8a54177))
    - removing unused Error types and adding context info to a couple of them ([`bdf50e7`](https://github.com/maidsafe/safe_network/commit/bdf50e7ad1214ef4bb48c0a12db8a7700193bb2a))
    - try to reconnect once when the client lost a connection to a peer ([`3089b2c`](https://github.com/maidsafe/safe_network/commit/3089b2c8d3f3ee099ff9e0880a96720b319e52a4))
    - refactor(cmds): replace ack+error with response BREAKING CHANGE: ClientMsg enum variants changed. ([`df19b12`](https://github.com/maidsafe/safe_network/commit/df19b120bd769d0b375a27162f07a4a421f97ec0))
    - use existing conns when available ([`951e67c`](https://github.com/maidsafe/safe_network/commit/951e67cc7e06a78270dad363d7085af4cd136f65))
    - doctest fix ([`23d98eb`](https://github.com/maidsafe/safe_network/commit/23d98ebf05406f7addb22c9a939700c0683c4a2e))
    - use bi stream from client; process in Node ([`3fd0a00`](https://github.com/maidsafe/safe_network/commit/3fd0a00bad2f9ca266a56de2086b54088459e153))
    - apply clippy suggestion for iterator ([`e924000`](https://github.com/maidsafe/safe_network/commit/e92400023cf77c323d228b9e1fa18d85c33040d1))
    - remove intermediate function ([`6351fb8`](https://github.com/maidsafe/safe_network/commit/6351fb878e1d31d36b790eb0c0fee1e16b7cbbc8))
    - move to event driven msg handling ([`95436a1`](https://github.com/maidsafe/safe_network/commit/95436a1f722bfd02a735dc3cf2f171a8b70de552))
    - remove ExpiringConnection struct ([`98abbbe`](https://github.com/maidsafe/safe_network/commit/98abbbe7af8c870faa22d62819691054e07df718))
    - removing op id from query response ([`a973b62`](https://github.com/maidsafe/safe_network/commit/a973b62a8ef48acc92af8735e7e7bcac94e0092f))
    - deem spentproof shares < 5 data not found ([`428d2e9`](https://github.com/maidsafe/safe_network/commit/428d2e9528c567e5ac46256100ecadcf496dd8e1))
    - rejig retry loop, use max retries and keep querying if data not found until timeout ([`033d17f`](https://github.com/maidsafe/safe_network/commit/033d17f355bea910939e094770af73be89e642ad))
    - write query responses as they come in ([`e57d832`](https://github.com/maidsafe/safe_network/commit/e57d83235f60a16bd7e1ee801f35a599113dc71a))
    - force retries to use fresh connection ([`a5bf211`](https://github.com/maidsafe/safe_network/commit/a5bf211daa0272597f1a2d852a17592258a2115a))
    - chore(clippy) ([`ee6f761`](https://github.com/maidsafe/safe_network/commit/ee6f76119f78f5e49633f8f1e6c58d6b72925fcb))
    - remove some ? noise in tests ([`9fad752`](https://github.com/maidsafe/safe_network/commit/9fad752ce1849763ae16cdb42159b9dccf1a13d0))
    - impl custom `Debug` trait to compact the logs ([`a84fb5a`](https://github.com/maidsafe/safe_network/commit/a84fb5a3b1cf4a2c0c02dc20077f9d70fdb9e70d))
    - remove age stepping in genesis sections ([`14ba1a8`](https://github.com/maidsafe/safe_network/commit/14ba1a8a034e488728762df09398347f8b909d65))
    - Merge #1703 ([`297004f`](https://github.com/maidsafe/safe_network/commit/297004fe04bba05765eb4d02394210024dfcf559))
    - AuthKind into MsgKind without node sig ([`633dfc8`](https://github.com/maidsafe/safe_network/commit/633dfc836c10eafc54dedefc53b2cbc9526970bb))
    - Merge #1685 ([`992f917`](https://github.com/maidsafe/safe_network/commit/992f917830c6d7b10fbd4d1f03a81eb5e8a64fdc))
    - remove NodeMsgAuthority altogether ([`8c348b2`](https://github.com/maidsafe/safe_network/commit/8c348b2286925edf319daede9a064399b41f6ec1))
    - remove node auth ([`5019dd3`](https://github.com/maidsafe/safe_network/commit/5019dd3896d278227878ffd91ce14d0cecb2b9dd))
    - get rid of InvalidState Error altogether ([`a60eef8`](https://github.com/maidsafe/safe_network/commit/a60eef81e20bee599f9b861822c8e8c3424073af))
    - remove dashmap dep in sn_{dysfunction, interface} ([`5cbae9c`](https://github.com/maidsafe/safe_network/commit/5cbae9c10e9f1d4302d041a864bfee83d47834e0))
    - section peers uses BTreeMaps instead of DashMap ([`50e877d`](https://github.com/maidsafe/safe_network/commit/50e877d9ac75b62dfa9e851564b6fd6b60167ca3))
    - assert_lists asserts instead of returning Result ([`ab22c69`](https://github.com/maidsafe/safe_network/commit/ab22c6989f55994065f0d859b91e141f7489a722))
    - remove `SectionAuthorityProvider`, `SectionTreeUpdate` messages ([`32744bc`](https://github.com/maidsafe/safe_network/commit/32744bcf6c94d9a7cda81813646560b70be53eb2))
    - move `MembershipState`, `RelocateDetails` out from messaging ([`ba78a41`](https://github.com/maidsafe/safe_network/commit/ba78a41f509a3015a5319e09e1e748ac91952b70))
    - remove `NodeState` message ([`72abbfb`](https://github.com/maidsafe/safe_network/commit/72abbfbc583b5b0dc99a0f7d90cb4d7eb72bd8c4))
    - remove section share auth ([`266f312`](https://github.com/maidsafe/safe_network/commit/266f3120b574133fccc39405d3a5a02d05806dfc))
    - use or_default instead of wasteful or_insert ([`ad8760e`](https://github.com/maidsafe/safe_network/commit/ad8760eb7aa74b7055d0ca2a4ae66c8369865e70))
    - fix tests, restore aggregator reset uppon sig completion ([`da2833b`](https://github.com/maidsafe/safe_network/commit/da2833bc03e2b027294bec6a4fcc2f4d8970f5b8))
    - universal aggregator without timers ([`2bcc1a0`](https://github.com/maidsafe/safe_network/commit/2bcc1a0735d9cd727f71c93b58de85cd01c8a603))
    - use gen_section_tree_update test utility ([`5142595`](https://github.com/maidsafe/safe_network/commit/51425951e8a66a8fd938a8dd2378b583cc80fb94))
    - chore(section_peers): remove unusued match arm in update function The match arm will never be triggered since a node with a new age is considered as a new node (new XorName). ([`438f181`](https://github.com/maidsafe/safe_network/commit/438f18116ba5c7fce50a8113a3a98724deea4ae8))
    - add unit tests ([`abeaf72`](https://github.com/maidsafe/safe_network/commit/abeaf725f2085ba86c34a81ef02f46193f239d1b))
    - add unit tests for `SectionPeers::update` ([`af3228c`](https://github.com/maidsafe/safe_network/commit/af3228c5eb6dc68cb87a0ca72ceb578a5fc7c88e))
    - add unit tests for `SectionTree::update` ([`941ffb7`](https://github.com/maidsafe/safe_network/commit/941ffb730776ee82040b3880210fdfb465f70c7f))
    - move elders sig trust within DkgStart instead of deep within messaging ([`3302aee`](https://github.com/maidsafe/safe_network/commit/3302aee7a41abd58b6deba18cc690c5e74aabff4))
    - fmt ([`3215110`](https://github.com/maidsafe/safe_network/commit/3215110b021aaa7d3b755b7e80432aeed1e0b436))
    - Remove resource proof ([`acaa90a`](https://github.com/maidsafe/safe_network/commit/acaa90a13d598915bafc3584c70826f233d89881))
    - adapt confusing auth related code ([`07d0991`](https://github.com/maidsafe/safe_network/commit/07d0991fed28d49c9be85d44a3343b66fac076d9))
    - refactor: Node-> MyNode & MyNodeInfo ([`17baaf4`](https://github.com/maidsafe/safe_network/commit/17baaf4c27442273c238d09ebb240e65be85a582))
    - removing dst_address fn from ClientMsg as it doesn't always contain that info ([`d550b55`](https://github.com/maidsafe/safe_network/commit/d550b553acbd70d4adb830a0600f7da7b833ee18))
    - various more renamings ([`f289de5`](https://github.com/maidsafe/safe_network/commit/f289de53f3894a58a6e4db51ce81aaf34f276490))
    - rename a bunch of auth to sig ([`452ef9c`](https://github.com/maidsafe/safe_network/commit/452ef9c5778ad88270f4e251adc49ccbc9b3cb09))
    - simplify section sig ([`85f4d00`](https://github.com/maidsafe/safe_network/commit/85f4d00e81ac5bf67b6be89d7ff51b7bb1060ed6))
    - rename SectionAuth to SectionSigned ([`70d848a`](https://github.com/maidsafe/safe_network/commit/70d848a43b6df02812195845434849b98f409367))
    - Looking at change to NodeSig ([`dd45c8f`](https://github.com/maidsafe/safe_network/commit/dd45c8f42b4f8674eeaea90aa27a465bd3bae0a2))
    - refactor: NodeAuth -> NodeEvidence ([`cdc126b`](https://github.com/maidsafe/safe_network/commit/cdc126be934229198959eb3da317e5da92b16ac3))
    - adapt to empty vec change in sn_sdkg ([`8f2c414`](https://github.com/maidsafe/safe_network/commit/8f2c41406165351796ca8df36d2ae3457e000ab9))
    - integrate recursive sn_sdkg changes ([`b30459f`](https://github.com/maidsafe/safe_network/commit/b30459ffc565bb38ce7975c443b8df8139b77752))
    - join_invalid_retry_prefix test uses empty genesis prefix ([`ffa2cf3`](https://github.com/maidsafe/safe_network/commit/ffa2cf3c9b49aaa3f5f64b88e0800a4047948378))
    - refactor ([`4beec97`](https://github.com/maidsafe/safe_network/commit/4beec978b1f2eae2198bcd85e3e0bf377d97575c))
    - refactor: ([`444ed16`](https://github.com/maidsafe/safe_network/commit/444ed16e55d8e962404c8c7b643b00f0685eed18))
    - Clippy doc error ([`bb7a655`](https://github.com/maidsafe/safe_network/commit/bb7a65517c2b1783f3f8b7865fff8d0c5f7bc705))
    - refactor: ([`50d48bf`](https://github.com/maidsafe/safe_network/commit/50d48bfc4fcc54266125bc0f1a3369097376497c))
    - refactor: ServiceAuth -> ClientAuth Service -> Client NodeBlsShare -> SectionPart BlsShareAuth -> SectionAuthPart SystemMsg -> Node2NodeMsg OutgoingMsg::System -> OutgoingMsg::Node2Node + fmt / fix ([`0b9d08b`](https://github.com/maidsafe/safe_network/commit/0b9d08bf88b6892b53dabf82fa988674fdd9992a))
    - use separate genesis dbc and section keys ([`b9d39b0`](https://github.com/maidsafe/safe_network/commit/b9d39b09e39b7d91fd556abeb385310f50a0eee0))
    - Merge #1550 ([`c6f2e2f`](https://github.com/maidsafe/safe_network/commit/c6f2e2fb98e29911336f86f54c1d9b9605037b57))
    - improve namings ([`a0b2df5`](https://github.com/maidsafe/safe_network/commit/a0b2df5a0b12c70872dfc854d660afd0cf8b21aa))
    - compile after rebase ([`9a1cdf6`](https://github.com/maidsafe/safe_network/commit/9a1cdf6f0135ce53f43a48c4346aff9023ccad33))
    - gossip DKG termination to trigger handover ([`f4b5d6b`](https://github.com/maidsafe/safe_network/commit/f4b5d6b207a0874621d777582ca5906e69196e06))
    - compile after rebase ([`9992d97`](https://github.com/maidsafe/safe_network/commit/9992d9701ecadff2b7682e47387014b9d11dba63))
    - dkg start miss handling ([`3ed0e01`](https://github.com/maidsafe/safe_network/commit/3ed0e0167d5bec04d6c57d94ce1a63d1f043a1a0))
    - make session id sum u16 to reduce collision chance in logs ([`42c4332`](https://github.com/maidsafe/safe_network/commit/42c43320c581df1b315fd2f199ebe74adcfbb803))
    - bls key upgrade issue, more logs ([`22a0de0`](https://github.com/maidsafe/safe_network/commit/22a0de0e3bd8478a10729112ec1b3bce9ba5cb90))
    - compile after rebase ([`85a6439`](https://github.com/maidsafe/safe_network/commit/85a64394a70b2d69033fc2f175726afec1afb092))
    - some necessary cleanup ([`ba859ae`](https://github.com/maidsafe/safe_network/commit/ba859ae5f064d6dc15aa563ee956a26e85df1d45))
    - compiling sdkg integration ([`f5d53c1`](https://github.com/maidsafe/safe_network/commit/f5d53c188a03150a06bdee97fb585f7900b7c251))
    - sn_dkg integration ([`b647469`](https://github.com/maidsafe/safe_network/commit/b6474691ea6af5ee441b02f6cb9c3cf2b8f97459))
    - client retry spend on unknown section key ([`5c8b1f5`](https://github.com/maidsafe/safe_network/commit/5c8b1f50d1bf346d45bd2a9faf92bbf33cb448da))
    - move test_utils module ([`072c5d4`](https://github.com/maidsafe/safe_network/commit/072c5d4c5de7810a0837144853435e2ff2d091d0))
    - move build_spent_proof_share to sn_interface ([`6108807`](https://github.com/maidsafe/safe_network/commit/610880711da814c7717c665e9cb34a729bda5797))
    - get public commitments from sn_dbc ([`1152b27`](https://github.com/maidsafe/safe_network/commit/1152b2764e955edd80fb33921a8d8fe52654a896))
    - Merge #1607 ([`ec61fcc`](https://github.com/maidsafe/safe_network/commit/ec61fcc7c2241c65e00388dfdbbde5cb9c571158))
    - return err from NetworkKnowledge constructor ([`4846aec`](https://github.com/maidsafe/safe_network/commit/4846aec802200b60c13a78aa092e5eed41642da5))
    - minor refactoring and fixing issue reported by new clippy version ([`c7de082`](https://github.com/maidsafe/safe_network/commit/c7de08209c659ec93557d6ea10e0bcd8c3b74d8b))
    - Merge #1557 ([`6cac22a`](https://github.com/maidsafe/safe_network/commit/6cac22af4994651719f64bc76391d729a3efb656))
    - remove spend retry on client ([`100e2ae`](https://github.com/maidsafe/safe_network/commit/100e2ae70d21e141e1ebbc324f8b06e3d3f1a01c))
    - spend with updated network knowledge ([`230a6ed`](https://github.com/maidsafe/safe_network/commit/230a6ed7f1f4193fa36b2fbb83bea072f4944c1d))
    - remove redundant genesis_key argument in `NetworkKnowledge` constructor ([`e973eee`](https://github.com/maidsafe/safe_network/commit/e973eee96c9065ce87a1fa65ae45d9be8d6f940c))
    - bundle proof chain, SAP into `SectionTreeUpdate` ([`60e333d`](https://github.com/maidsafe/safe_network/commit/60e333d4ced688f3382cde513300d38790613692))
    - retry dbc spend on unknown section key ([`057ce1c`](https://github.com/maidsafe/safe_network/commit/057ce1ce1e174102e23d96cfcd2ab1d090a6f1dc))
    - dbc spend can update network knowledge ([`2020ef1`](https://github.com/maidsafe/safe_network/commit/2020ef1a91c8520abc4bb74d3de6385b8cd283b4))
    - pull out will-be-elder check into node ([`6343b6f`](https://github.com/maidsafe/safe_network/commit/6343b6fd21fe3bf81412d922da5e14b2c8b6f3c5))
    - Merge #1527 ([`1f06d6e`](https://github.com/maidsafe/safe_network/commit/1f06d6e90da6f889221f37cc8eac32b6933a94ba))
    - optimizations and code cleanup ([`03da7f6`](https://github.com/maidsafe/safe_network/commit/03da7f67fff1fa5bb06d60a66dfdb531506dec4c))
    - ignore update if we don't have KeyShare ([`4884c51`](https://github.com/maidsafe/safe_network/commit/4884c511d302522aa408ebf9350a7ff6cefeecb7))
    - show parent idx while debugging ([`71d992b`](https://github.com/maidsafe/safe_network/commit/71d992ba1405a48a8593f8f5aa3288296bd59af6))
    - custom Serializer, Deserializer for `SectionsDAG` ([`864c023`](https://github.com/maidsafe/safe_network/commit/864c023e26697a609a9ad230c04e7aef7416650c))
    - enable `SectionTree` proptest ([`859fc30`](https://github.com/maidsafe/safe_network/commit/859fc30fa70ce41ceb910e0352c71dda5c5501ce))
    - provide custom rng to generate random_sap, SecretKeySet ([`e01a964`](https://github.com/maidsafe/safe_network/commit/e01a964b7aee8fde125dd56b3cfa78498c4cbb41))
    - replace `SecuredLinkedList` with `SectionsDAG` ([`0cd47ad`](https://github.com/maidsafe/safe_network/commit/0cd47ad56e0d93e3e99feb0dfcea8094f871ff6f))
    - verify entry_hash generation ([`301aeeb`](https://github.com/maidsafe/safe_network/commit/301aeeb687561106f8e2fd6bab1133159d22a6f1))
</details>

## v0.15.0 (2022-09-19)

<csr-id-a8a9fb90791b29496e8559090dca4161e04054da/>
<csr-id-a0bc2562df4f427752ec0f3ab85d9befe2d20050/>
<csr-id-84cedf30fff0cc298f9f658d2c58499990967fe4/>
<csr-id-3e59963094f93528404af34efa9cf9900640702f/>
<csr-id-2d1221999b959bf4d0879cf42050d5e1e3119445/>
<csr-id-d5cc996e5ca0a34bfad3ed16760a44a93d3264a2/>

### Chore

 - <csr-id-a8a9fb90791b29496e8559090dca4161e04054da/> sn_interface-0.15.0/sn_dysfunction-0.14.0/sn_client-0.76.0/sn_node-0.71.0/sn_api-0.74.0/sn_cli-0.67.0
 - <csr-id-a0bc2562df4f427752ec0f3ab85d9befe2d20050/> cleanup unused deps
 - <csr-id-84cedf30fff0cc298f9f658d2c58499990967fe4/> remove unused back-pressure code

### New Features

 - <csr-id-32577f2e5c158db2420bbf173e84aef7f4175fd7/> add API to retrieve a single-branch partial DAG containing a given key.
   - Adding `single_branch_dag_for_key` API to get a partial `SectionsDAG` with a single branch
   which contains the given `key`, from the genesis to the last key of any of its children branches.

### Test

 - <csr-id-3e59963094f93528404af34efa9cf9900640702f/> RegisterCrdt generates entry hash properly

### Refactor (BREAKING)

 - <csr-id-2d1221999b959bf4d0879cf42050d5e1e3119445/> flattening up ServiceMsg::ServiceError and ServiceMsg::CmdError types
 - <csr-id-d5cc996e5ca0a34bfad3ed16760a44a93d3264a2/> operation id to be generated merely by bincode serialisation without any encoding

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release over the course of 6 calendar days.
 - 9 days passed between releases.
 - 7 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.15.0/sn_dysfunction-0.14.0/sn_client-0.76.0/sn_node-0.71.0/sn_api-0.74.0/sn_cli-0.67.0 ([`a8a9fb9`](https://github.com/maidsafe/safe_network/commit/a8a9fb90791b29496e8559090dca4161e04054da))
    - flattening up ServiceMsg::ServiceError and ServiceMsg::CmdError types ([`2d12219`](https://github.com/maidsafe/safe_network/commit/2d1221999b959bf4d0879cf42050d5e1e3119445))
    - cleanup unused deps ([`a0bc256`](https://github.com/maidsafe/safe_network/commit/a0bc2562df4f427752ec0f3ab85d9befe2d20050))
    - remove unused back-pressure code ([`84cedf3`](https://github.com/maidsafe/safe_network/commit/84cedf30fff0cc298f9f658d2c58499990967fe4))
    - RegisterCrdt generates entry hash properly ([`3e59963`](https://github.com/maidsafe/safe_network/commit/3e59963094f93528404af34efa9cf9900640702f))
    - operation id to be generated merely by bincode serialisation without any encoding ([`d5cc996`](https://github.com/maidsafe/safe_network/commit/d5cc996e5ca0a34bfad3ed16760a44a93d3264a2))
    - add API to retrieve a single-branch partial DAG containing a given key. ([`32577f2`](https://github.com/maidsafe/safe_network/commit/32577f2e5c158db2420bbf173e84aef7f4175fd7))
</details>

## v0.14.0 (2022-09-09)

<csr-id-448694176dd3b40a12bd8ecc16d9bb66fd171a37/>
<csr-id-7d4a15a7855429d604c0216f67e46620fea80e6f/>
<csr-id-927931c9eb833df3e589d72affc4839ba57b5cc2/>

### Chore

 - <csr-id-448694176dd3b40a12bd8ecc16d9bb66fd171a37/> sn_interface-0.14.0/sn_dysfunction-0.13.0/sn_client-0.75.0/sn_node-0.70.0/sn_api-0.73.0/sn_cli-0.66.0
 - <csr-id-7d4a15a7855429d604c0216f67e46620fea80e6f/> loop upload verification to avoid early NoDataFound
   The underlying API now returns NoDataFound once it ahs queried all adults... So we loop atop this to ensure we don't hit this too early as not all chunks may have been uploaded yet

### Chore (BREAKING)

 - <csr-id-927931c9eb833df3e589d72affc4839ba57b5cc2/> removing unused SystemMsg::NodeMsgError msg type

### New Features (BREAKING)

 - <csr-id-7bedb7bb7614a8af05f5892a28ff4732e87d4796/> return an error to the client when it cannot accept a query

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release over the course of 1 calendar day.
 - 1 day passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.14.0/sn_dysfunction-0.13.0/sn_client-0.75.0/sn_node-0.70.0/sn_api-0.73.0/sn_cli-0.66.0 ([`4486941`](https://github.com/maidsafe/safe_network/commit/448694176dd3b40a12bd8ecc16d9bb66fd171a37))
    - removing unused SystemMsg::NodeMsgError msg type ([`927931c`](https://github.com/maidsafe/safe_network/commit/927931c9eb833df3e589d72affc4839ba57b5cc2))
    - return an error to the client when it cannot accept a query ([`7bedb7b`](https://github.com/maidsafe/safe_network/commit/7bedb7bb7614a8af05f5892a28ff4732e87d4796))
    - Merge #1556 ([`d3d6593`](https://github.com/maidsafe/safe_network/commit/d3d6593989d9d16148b8490a6227acbe0871d267))
    - Merge branch 'main' into Chore-ClientRetriesOnDataNotFound ([`bbca976`](https://github.com/maidsafe/safe_network/commit/bbca97680840e1069c88278fe14ddee153b97dbb))
    - loop upload verification to avoid early NoDataFound ([`7d4a15a`](https://github.com/maidsafe/safe_network/commit/7d4a15a7855429d604c0216f67e46620fea80e6f))
</details>

## v0.13.0 (2022-09-07)

<csr-id-fe659c5685289fe0071b54298dcac394e83c0dce/>
<csr-id-b1329158b3c2427a7c1939060ba1fe3ef9e72bf9/>
<csr-id-84bfdaaf5b0df86912fef806dcb04f353e828b69/>
<csr-id-638bcdfea4cbc713d8a4faecec7ed8538317fa29/>
<csr-id-0c49daf5dbfad2593ccf13cb114841045688ffed/>

### Chore

 - <csr-id-fe659c5685289fe0071b54298dcac394e83c0dce/> sn_interface-0.13.0/sn_dysfunction-0.12.0/sn_client-0.74.0/sn_node-0.69.0/sn_api-0.72.0/sn_cli-0.65.0
 - <csr-id-b1329158b3c2427a7c1939060ba1fe3ef9e72bf9/> retry DataNotFound errors for data_copy_count * 2
   We do this twice in case of connection issues during prev run
 - <csr-id-84bfdaaf5b0df86912fef806dcb04f353e828b69/> pass by reference instead of by value

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
    - retry DataNotFound errors for data_copy_count * 2 ([`b132915`](https://github.com/maidsafe/safe_network/commit/b1329158b3c2427a7c1939060ba1fe3ef9e72bf9))
    - minor refactor to Capacity functions impl, plus removing unused fns ([`638bcdf`](https://github.com/maidsafe/safe_network/commit/638bcdfea4cbc713d8a4faecec7ed8538317fa29))
    - pass by reference instead of by value ([`84bfdaa`](https://github.com/maidsafe/safe_network/commit/84bfdaaf5b0df86912fef806dcb04f353e828b69))
    - removing unused Error types ([`0c49daf`](https://github.com/maidsafe/safe_network/commit/0c49daf5dbfad2593ccf13cb114841045688ffed))
    - report any error occurred when handling a service msg back to the client ([`d671f4e`](https://github.com/maidsafe/safe_network/commit/d671f4ee4c76b42187d266aee99351114acf6cd7))
</details>

## v0.12.0 (2022-09-06)

<csr-id-d28fdf3ddd0a39f7bbc6426e1e71d990319b0ec7/>
<csr-id-997517764347857f71674567364b7dbb852d8b10/>
<csr-id-921438659ccaf65b2ea8cc00efb61d8146ef71ef/>
<csr-id-f5d436fba99e0e9c258c7ab3c3a256be3be58f84/>
<csr-id-b040ea14e53247094838de6f1fa9af2830b051fa/>
<csr-id-3a718d8c0957957a75250b044c9d1ad1b5874ab0/>
<csr-id-183d7f83985a36deeb5933ae9b1880df21da2866/>
<csr-id-63958a8629c9fbca8e6604edb17d9b61ca92a4ee/>
<csr-id-62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2/>
<csr-id-a6685348578fe546576bd13405e6485d984b3487/>
<csr-id-ed9f627d0e2c42ab1b7386888cced751ae28f98a/>
<csr-id-5b73b33b683991be9e9f6440c3d8d568edab3ad6/>
<csr-id-b7530feb40987f433ff12c5176cfdbc375359dc6/>
<csr-id-1b9e0a6564e9564201ef3a3e04adb0bfbef6ac14/>

### Chore

 - <csr-id-d28fdf3ddd0a39f7bbc6426e1e71d990319b0ec7/> sn_interface-0.11.0/sn_dysfunction-0.10.0/sn_client-0.72.0/sn_node-0.67.0/sn_api-0.70.0/sn_cli-0.63.0
 - <csr-id-997517764347857f71674567364b7dbb852d8b10/> more idiomatic time code
 - <csr-id-921438659ccaf65b2ea8cc00efb61d8146ef71ef/> unneeded iter methods removal
 - <csr-id-f5d436fba99e0e9c258c7ab3c3a256be3be58f84/> applied use_self lint
 - <csr-id-b040ea14e53247094838de6f1fa9af2830b051fa/> sn_interface lints and fixes
   Apply lints used in other crates, as far as they can easily be applied.
   The `unused_results` lint has been left out, as that is too much
   cleaning up to do, just like adding documentation to all the public
   interfaces.
 - <csr-id-3a718d8c0957957a75250b044c9d1ad1b5874ab0/> switch on clippy::unwrap_used as a warning


### Chore

 - <csr-id-1b9e0a6564e9564201ef3a3e04adb0bfbef6ac14/> sn_interface-0.12.0/sn_dysfunction-0.11.0/sn_client-0.73.0/sn_node-0.68.0/sn_api-0.71.0/sn_cli-0.64.0

### New Features

 - <csr-id-19abbe20724770e618e5d038f238bdf4b3de6ea2/> rename NodeMsg to BackPressure
 - <csr-id-08a0a8eb75a0ca9d51fa321686d17dbcf97fc04e/> fix time alignment; more states; mv to sn_interface

### Bug Fixes

 - <csr-id-6bdc82295dfdcaa617c7c1e36d2b72f085e50042/> update qp2p for unique ids
   Latest qp2p should provide global unique connection id
   
   previously duplication of ids could have been breaking
   connection management
 - <csr-id-95930d61dcb191d18ae417db4bf8a223b13824db/> add back-pressure system msg

### Refactor

 - <csr-id-183d7f83985a36deeb5933ae9b1880df21da2866/> skip spentbook register creation if it already exists
 - <csr-id-63958a8629c9fbca8e6604edb17d9b61ca92a4ee/> move probe creation to network knowledge
 - <csr-id-62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2/> separating internal chunk from register store implementation layer

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

 - 24 commits contributed to the release over the course of 8 calendar days.
 - 8 days passed between releases.
 - 19 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.12.0/sn_dysfunction-0.11.0/sn_client-0.73.0/sn_node-0.68.0/sn_api-0.71.0/sn_cli-0.64.0 ([`1b9e0a6`](https://github.com/maidsafe/safe_network/commit/1b9e0a6564e9564201ef3a3e04adb0bfbef6ac14))
    - update qp2p to use UsrMsgBytes and avoid reserializing bytes ([`f5361d9`](https://github.com/maidsafe/safe_network/commit/f5361d91f8215585651229eb6dc2535f2ecb631c))
    - Merge #1544 ([`e8202a6`](https://github.com/maidsafe/safe_network/commit/e8202a6ea8c07f8ae0a04273b2cda350758352ab))
    - update qp2p for unique ids ([`6bdc822`](https://github.com/maidsafe/safe_network/commit/6bdc82295dfdcaa617c7c1e36d2b72f085e50042))
    - sn_interface-0.11.0/sn_dysfunction-0.10.0/sn_client-0.72.0/sn_node-0.67.0/sn_api-0.70.0/sn_cli-0.63.0 ([`d28fdf3`](https://github.com/maidsafe/safe_network/commit/d28fdf3ddd0a39f7bbc6426e1e71d990319b0ec7))
    - more idiomatic time code ([`9975177`](https://github.com/maidsafe/safe_network/commit/997517764347857f71674567364b7dbb852d8b10))
    - unneeded iter methods removal ([`9214386`](https://github.com/maidsafe/safe_network/commit/921438659ccaf65b2ea8cc00efb61d8146ef71ef))
    - applied use_self lint ([`f5d436f`](https://github.com/maidsafe/safe_network/commit/f5d436fba99e0e9c258c7ab3c3a256be3be58f84))
    - Merge #1525 ([`6884257`](https://github.com/maidsafe/safe_network/commit/6884257ae51616949b0dfaefaa47fcdd090a7d54))
    - skip spentbook register creation if it already exists ([`183d7f8`](https://github.com/maidsafe/safe_network/commit/183d7f83985a36deeb5933ae9b1880df21da2866))
    - improving internal helpers in register storage mod to reuse some logic/code ([`a668534`](https://github.com/maidsafe/safe_network/commit/a6685348578fe546576bd13405e6485d984b3487))
    - Merge #1535 ([`7327112`](https://github.com/maidsafe/safe_network/commit/7327112da76871d52b5039546419ab18e41982f8))
    - rename SystemMsg to BackPressure ([`19abbe2`](https://github.com/maidsafe/safe_network/commit/19abbe20724770e618e5d038f238bdf4b3de6ea2))
    - add back-pressure system msg ([`95930d6`](https://github.com/maidsafe/safe_network/commit/95930d61dcb191d18ae417db4bf8a223b13824db))
    - fix time alignment; more states; mv to sn_interface ([`08a0a8e`](https://github.com/maidsafe/safe_network/commit/08a0a8eb75a0ca9d51fa321686d17dbcf97fc04e))
    - Merge #1536 ([`5194123`](https://github.com/maidsafe/safe_network/commit/519412319c9b7504c97cbeae6e398a210226d14e))
    - move probe creation to network knowledge ([`63958a8`](https://github.com/maidsafe/safe_network/commit/63958a8629c9fbca8e6604edb17d9b61ca92a4ee))
    - sn_interface lints and fixes ([`b040ea1`](https://github.com/maidsafe/safe_network/commit/b040ea14e53247094838de6f1fa9af2830b051fa))
    - Merge branch 'main' into avoid_testing_data_collision ([`60c368b`](https://github.com/maidsafe/safe_network/commit/60c368b8494eaeb219572c2304bf787a168cfee0))
    - switch on clippy::unwrap_used as a warning ([`3a718d8`](https://github.com/maidsafe/safe_network/commit/3a718d8c0957957a75250b044c9d1ad1b5874ab0))
    - separating internal chunk from register store implementation layer ([`62bc8d6`](https://github.com/maidsafe/safe_network/commit/62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2))
    - removing unnecessary ReplicatedDataAddress type ([`ed9f627`](https://github.com/maidsafe/safe_network/commit/ed9f627d0e2c42ab1b7386888cced751ae28f98a))
    - removing unnecessary types ([`5b73b33`](https://github.com/maidsafe/safe_network/commit/5b73b33b683991be9e9f6440c3d8d568edab3ad6))
    - moving encoding/decoding utilities of data addresses types to storage impl ([`b7530fe`](https://github.com/maidsafe/safe_network/commit/b7530feb40987f433ff12c5176cfdbc375359dc6))
</details>

## v0.11.0 (2022-09-04)

<csr-id-997517764347857f71674567364b7dbb852d8b10/>
<csr-id-921438659ccaf65b2ea8cc00efb61d8146ef71ef/>
<csr-id-f5d436fba99e0e9c258c7ab3c3a256be3be58f84/>
<csr-id-b040ea14e53247094838de6f1fa9af2830b051fa/>
<csr-id-3a718d8c0957957a75250b044c9d1ad1b5874ab0/>
<csr-id-183d7f83985a36deeb5933ae9b1880df21da2866/>
<csr-id-63958a8629c9fbca8e6604edb17d9b61ca92a4ee/>
<csr-id-62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2/>
<csr-id-a6685348578fe546576bd13405e6485d984b3487/>
<csr-id-ed9f627d0e2c42ab1b7386888cced751ae28f98a/>
<csr-id-5b73b33b683991be9e9f6440c3d8d568edab3ad6/>
<csr-id-b7530feb40987f433ff12c5176cfdbc375359dc6/>

### Chore

 - <csr-id-997517764347857f71674567364b7dbb852d8b10/> more idiomatic time code
 - <csr-id-921438659ccaf65b2ea8cc00efb61d8146ef71ef/> unneeded iter methods removal
 - <csr-id-f5d436fba99e0e9c258c7ab3c3a256be3be58f84/> applied use_self lint
 - <csr-id-b040ea14e53247094838de6f1fa9af2830b051fa/> sn_interface lints and fixes
   Apply lints used in other crates, as far as they can easily be applied.
   The `unused_results` lint has been left out, as that is too much
   cleaning up to do, just like adding documentation to all the public
   interfaces.
 - <csr-id-3a718d8c0957957a75250b044c9d1ad1b5874ab0/> switch on clippy::unwrap_used as a warning


### New Features

 - <csr-id-19abbe20724770e618e5d038f238bdf4b3de6ea2/> rename NodeMsg to BackPressure
 - <csr-id-08a0a8eb75a0ca9d51fa321686d17dbcf97fc04e/> fix time alignment; more states; mv to sn_interface

### Bug Fixes

 - <csr-id-95930d61dcb191d18ae417db4bf8a223b13824db/> add back-pressure system msg

### Refactor

 - <csr-id-183d7f83985a36deeb5933ae9b1880df21da2866/> skip spentbook register creation if it already exists
 - <csr-id-63958a8629c9fbca8e6604edb17d9b61ca92a4ee/> move probe creation to network knowledge
 - <csr-id-62bc8d6d24b7c82bd3a27ceb43cd53d8077ff6b2/> separating internal chunk from register store implementation layer

### Refactor (BREAKING)

 - <csr-id-a6685348578fe546576bd13405e6485d984b3487/> improving internal helpers in register storage mod to reuse some logic/code
   - Removing some storage Error types, while adding more context information to others.
   - Allowing the Register storage to store 'Register edit' cmds even when the 'Register create' cmd
   is not found in the local replica/store yet.
 - <csr-id-ed9f627d0e2c42ab1b7386888cced751ae28f98a/> removing unnecessary ReplicatedDataAddress type
 - <csr-id-5b73b33b683991be9e9f6440c3d8d568edab3ad6/> removing unnecessary types
 - <csr-id-b7530feb40987f433ff12c5176cfdbc375359dc6/> moving encoding/decoding utilities of data addresses types to storage impl

## v0.10.2 (2022-08-28)

<csr-id-b587893737bc51aee483f7cd53da782036dd6c5e/>
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

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 2 calendar days.
 - 3 days passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
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
</details>

## v0.10.1 (2022-08-25)

<csr-id-401bc416c7aea65ae55e9adee2cbecf782c999cf/>
<csr-id-d58f1c55e9502fd6e8a99509f7ca30640835458b/>
<csr-id-fd6b97b37bb875404ef2ba7f5f35d5675c122ea0/>

### Chore

 - <csr-id-401bc416c7aea65ae55e9adee2cbecf782c999cf/> sn_interface-0.10.1/sn_client-0.71.0/sn_node-0.66.1/sn_api-0.69.0/sn_cli-0.62.0
 - <csr-id-d58f1c55e9502fd6e8a99509f7ca30640835458b/> make RegisterCmdId a hex-encodedstring
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
 - <csr-id-4da782096826f2074dac2a5628f9c9d9a85fcf1f/> paths for read/write RegisterCmd ops and support any order for reading them

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 1 day passed between releases.
 - 6 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.10.1/sn_client-0.71.0/sn_node-0.66.1/sn_api-0.69.0/sn_cli-0.62.0 ([`401bc41`](https://github.com/maidsafe/safe_network/commit/401bc416c7aea65ae55e9adee2cbecf782c999cf))
    - append RegsiterId as hex for storage folder ([`175011e`](https://github.com/maidsafe/safe_network/commit/175011ea4a14ef0ce2538ce9e69a6ffc8d47f2ac))
    - Decode ReplicatedDataAddress from chunk filename ([`604556e`](https://github.com/maidsafe/safe_network/commit/604556e670d5fe0a9408bbd0d586363c7b4c0d6c))
    - make RegisterCmdId a hex-encodedstring ([`d58f1c5`](https://github.com/maidsafe/safe_network/commit/d58f1c55e9502fd6e8a99509f7ca30640835458b))
    - paths for read/write RegisterCmd ops and support any order for reading them ([`4da7820`](https://github.com/maidsafe/safe_network/commit/4da782096826f2074dac2a5628f9c9d9a85fcf1f))
    - make RegisterCmds be stored under deterministic id ([`fd6b97b`](https://github.com/maidsafe/safe_network/commit/fd6b97b37bb875404ef2ba7f5f35d5675c122ea0))
</details>

## v0.10.0 (2022-08-23)

<csr-id-2c8cbdf06993e86f7e5575c5dc856721a5ed08b7/>
<csr-id-c8517a481e39bf688041cd8f8661bc663ee7bce7/>
<csr-id-589f03ce8670544285f329fe35c19897d4bfced8/>
<csr-id-9f64d681e285de57a54f571e98ff68f1bf39b6f1/>
<csr-id-836c1ba8d17d380e8504325e14f46739e2688bb3/>
<csr-id-1618cf6a93117942946d152efee24fe3c7020e55/>
<csr-id-11b8182a3de636a760d899cb15d7184d8153545a/>
<csr-id-e52028f1e9d7fcf19962a7643b272ba3a786c7c4/>
<csr-id-28d95a2e959e32ee69a70bdc855cba1fff1fc8d8/>
<csr-id-d3f66d6cfa838a5c65fb8f31fa68d48794b33dea/>
<csr-id-f0fbe5fd9bec0b2865271bb139c9fcb4ec225884/>
<csr-id-43fcc7c517f95eab0e27ddc79cd9c6de3631c7c6/>

### Chore

 - <csr-id-2c8cbdf06993e86f7e5575c5dc856721a5ed08b7/> update tokio
 - <csr-id-c8517a481e39bf688041cd8f8661bc663ee7bce7/> fix clippy some/none issues
 - <csr-id-589f03ce8670544285f329fe35c19897d4bfced8/> upgrading sn_dbc to v8.0
 - <csr-id-9f64d681e285de57a54f571e98ff68f1bf39b6f1/> increase data query limit
   Now we differentiate queries per adult/index, we may need more queries.
 - <csr-id-836c1ba8d17d380e8504325e14f46739e2688bb3/> check members need updating before verifying.
   During merge members we were spending a lot of CPU verifying, when we may not actually need the udpate at all

### Chore

 - <csr-id-43fcc7c517f95eab0e27ddc79cd9c6de3631c7c6/> sn_interface-0.10.0/sn_dysfunction-0.9.0/sn_client-0.70.0/sn_node-0.66.0/sn_api-0.68.0/sn_cli-0.61.0

### New Features

 - <csr-id-f0f860efcf89cb7bf51bddd6364a9bec33bbf3c3/> remove ConnectivityCheck
   Now we have periodic health checks and dysfunciton, this
   check should not be needed, and can cause network strain
   with the frequent DKG we have now
 - <csr-id-e97ab2220d150706741549944c6e4bf77f2a5bae/> new cmd to display detailed information about a configured network
 - <csr-id-438716cf9cfc11685968b10ccba8deffff96e56e/> include span in module path
 - <csr-id-108997c0ca4c291b8628b4a349732b3a23802d5a/> simplify log format to '<timestamp> [module] LEVEL <msg>'
 - <csr-id-1e2a0a122f8c53d669916cded16876aa16d8ebfb/> make AntiEntropyProbe carry a current known section key for response

### Bug Fixes

 - <csr-id-d529bd61de83795b2b10cce12549374cd9521a4f/> add fallback if only single prefix

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

 - 19 commits contributed to the release over the course of 8 calendar days.
 - 9 days passed between releases.
 - 19 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.10.0/sn_dysfunction-0.9.0/sn_client-0.70.0/sn_node-0.66.0/sn_api-0.68.0/sn_cli-0.61.0 ([`43fcc7c`](https://github.com/maidsafe/safe_network/commit/43fcc7c517f95eab0e27ddc79cd9c6de3631c7c6))
    - remove ConnectivityCheck ([`f0f860e`](https://github.com/maidsafe/safe_network/commit/f0f860efcf89cb7bf51bddd6364a9bec33bbf3c3))
    - removing unused CreateRegister::Populated msg type ([`28d95a2`](https://github.com/maidsafe/safe_network/commit/28d95a2e959e32ee69a70bdc855cba1fff1fc8d8))
    - removing unused sn_node::dbs::Error variants and RegisterExtend cmd ([`d3f66d6`](https://github.com/maidsafe/safe_network/commit/d3f66d6cfa838a5c65fb8f31fa68d48794b33dea))
    - update tokio ([`2c8cbdf`](https://github.com/maidsafe/safe_network/commit/2c8cbdf06993e86f7e5575c5dc856721a5ed08b7))
    - fix clippy some/none issues ([`c8517a4`](https://github.com/maidsafe/safe_network/commit/c8517a481e39bf688041cd8f8661bc663ee7bce7))
    - new cmd to display detailed information about a configured network ([`e97ab22`](https://github.com/maidsafe/safe_network/commit/e97ab2220d150706741549944c6e4bf77f2a5bae))
    - adding more context information to sn_client::Error types ([`991ccd4`](https://github.com/maidsafe/safe_network/commit/991ccd452119137d9da046b7f222f091177e28f1))
    - upgrading sn_dbc to v8.0 ([`589f03c`](https://github.com/maidsafe/safe_network/commit/589f03ce8670544285f329fe35c19897d4bfced8))
    - renaming NetworkPrefixMap to SectionTree ([`f0fbe5f`](https://github.com/maidsafe/safe_network/commit/f0fbe5fd9bec0b2865271bb139c9fcb4ec225884))
    - include span in module path ([`438716c`](https://github.com/maidsafe/safe_network/commit/438716cf9cfc11685968b10ccba8deffff96e56e))
    - simplify log format to '<timestamp> [module] LEVEL <msg>' ([`108997c`](https://github.com/maidsafe/safe_network/commit/108997c0ca4c291b8628b4a349732b3a23802d5a))
    - expose serialisation/deserialisation utilities as public methods instead ([`1618cf6`](https://github.com/maidsafe/safe_network/commit/1618cf6a93117942946d152efee24fe3c7020e55))
    - increase data query limit ([`9f64d68`](https://github.com/maidsafe/safe_network/commit/9f64d681e285de57a54f571e98ff68f1bf39b6f1))
    - add fallback if only single prefix ([`d529bd6`](https://github.com/maidsafe/safe_network/commit/d529bd61de83795b2b10cce12549374cd9521a4f))
    - clean up unused functionality ([`11b8182`](https://github.com/maidsafe/safe_network/commit/11b8182a3de636a760d899cb15d7184d8153545a))
    - SAP reference instead of clone ([`e52028f`](https://github.com/maidsafe/safe_network/commit/e52028f1e9d7fcf19962a7643b272ba3a786c7c4))
    - check members need updating before verifying. ([`836c1ba`](https://github.com/maidsafe/safe_network/commit/836c1ba8d17d380e8504325e14f46739e2688bb3))
    - make AntiEntropyProbe carry a current known section key for response ([`1e2a0a1`](https://github.com/maidsafe/safe_network/commit/1e2a0a122f8c53d669916cded16876aa16d8ebfb))
</details>

## v0.9.0 (2022-08-14)

<csr-id-66c26782759be707edb922daa548e3f0a3f9be8c/>
<csr-id-6d60525874dc4efeb658433f1f253d54e0cba2d4/>
<csr-id-29de67f1e3583eab867d517cb50ed2e404bd63fd/>
<csr-id-8242f2f1035b1c0718e53954951badffa30f3393/>
<csr-id-848dba48e5959d0b9cfe182fde2f12ede71ba9c2/>
<csr-id-35483b3f322eeea2c10427e94e4750a8269811c0/>
<csr-id-820fcc9a77f756fca308f247c3ea1b82f65d30b9/>
<csr-id-afcf083469c732f10c7c80f4a45e4c33ab111101/>
<csr-id-17f0e8a08c9543d380c16a35d3d7bfe7834a9e5a/>
<csr-id-aafc560d3b3b1e375f7be224e0e63a3b567bbd86/>
<csr-id-7394030fe5aeeb88f4524d2da2a71e36334c831d/>
<csr-id-73dc9b4a1757393270e62d265328bab0c0aa3b35/>
<csr-id-0a653e4becc4a8e14ffd6d0752cf035430067ce9/>
<csr-id-9789797e3f773285f23bd22957fe45a67aabec24/>
<csr-id-08af2a6ac3485a696d2a1e799af588943f207e6b/>
<csr-id-702c33b0d78f4a459725ed0c4538819c949978ce/>
<csr-id-2ea069543dbe6ffebac663d4d8d7e0bc33cfc566/>
<csr-id-322c69845e2e14eb029fdbebb24e08063a2323b0/>
<csr-id-ea490ddf749ac9e0c7962c3c21c053663e6b6ee7/>
<csr-id-bf2902c18b900b8b4a8abae5f966d1e08d547910/>
<csr-id-6f03b93bd2d02f0ffe54b69fbf25070fbe64eab0/>
<csr-id-8b3c4eb06fa988dc97b0cb75ed615ec69af29a48/>
<csr-id-214adedc31bca576c7f28ff52a1f4ff0a2676757/>
<csr-id-39c3fdf4128462e5f7c5fec3c628d394f505e2f2/>
<csr-id-1e8180c23fab27ac92c93f201efd050cff00db10/>
<csr-id-44cea00f54b39eaea0936ec187a8fa9ccdb61661/>
<csr-id-847db2c487cd102af0cf9a477b4c1b65fc2c8aa6/>
<csr-id-0a5593b0512d6f059c6a8003634b07e7d2d3e514/>
<csr-id-707b80c3526ae727a7e91330dc386cdb41c51f4c/>
<csr-id-9bd6ae20c1207f99420093fd5c9f4eb53836e3c1/>
<csr-id-31d9f9f99b4e166986b8e51c3d41e0eac55621a4/>
<csr-id-30a7028dd702e2f6575e299a609a2416439cbaed/>
<csr-id-dedec486f85c1cf6cf2d538238f32e826e08da0a/>
<csr-id-879678e986a722d216ee9a4f37e8ae398221a394/>
<csr-id-629a5873dd3bdf138649360222c00e3e0a76e097/>
<csr-id-12360a6dcc204153a81adbf842a64dc018c750f9/>
<csr-id-27ba2a63dcfa272cf7ef8c5301987fc6bfe18ed0/>
<csr-id-6e65ed8e6c5872bd2c49a1ed2837b1fb16523af1/>
<csr-id-6b1fee8cf3d0b2995f4b81e59dd684547593b5fa/>
<csr-id-ed37bb56e5e17d4cba7c1b2165746c193241d618/>
<csr-id-a0c89ff0e451d2e5dd13fc29635075097f2c7b94/>
<csr-id-0f07efd9ef0b75de79f27772566b013bc886bcc8/>
<csr-id-db4f4d07b155d732ad76d263563d81b5fee535f7/>
<csr-id-ff1a10b4aa2b41b7028949101504a29b52927e71/>
<csr-id-e0fb940b24e87d86fe920095176362f73503ce79/>
<csr-id-35ebd8e872f9d9db16c42cbe8d61702f9660aece/>
<csr-id-3f577d2a6fe70792d7d02e231b599ca3d44a5ed2/>
<csr-id-5c82df633e7c062fdf761a8e6e0a7ae8d26cc73a/>
<csr-id-24676dadb771bbd966b6a3e1aa53d1c736c90627/>
<csr-id-93614b18b4316af04ab8c74358a5c86510590b85/>
<csr-id-116b109ecdaaf0f1f10ede96cd68977782ab9ea3/>
<csr-id-f5f73acb035159718780a08f3d10512b11558e59/>
<csr-id-d3a05a728be8752ea9ebff4e38e7c4c85e5db09b/>
<csr-id-96da1171d0cac240f772e5d6a15c56f63441b4b3/>
<csr-id-dd2eb21352223f6340064e0021f4a7df402cd5c9/>
<csr-id-53f60c2327f8a69f0b2ef6d1a4e96644c10aa358/>

### Chore

 - <csr-id-66c26782759be707edb922daa548e3f0a3f9be8c/> add partial eq for rust 1.63; dep updates
 - <csr-id-6d60525874dc4efeb658433f1f253d54e0cba2d4/> remove wiremsg.priority as uneeded
 - <csr-id-29de67f1e3583eab867d517cb50ed2e404bd63fd/> serialize NetworkPrefixMap into JSON
 - <csr-id-8242f2f1035b1c0718e53954951badffa30f3393/> organise usings, cleanup
 - <csr-id-848dba48e5959d0b9cfe182fde2f12ede71ba9c2/> use matches! macros, minor refactoring
 - <csr-id-35483b3f322eeea2c10427e94e4750a8269811c0/> remove unused async/await
 - <csr-id-820fcc9a77f756fca308f247c3ea1b82f65d30b9/> remove NetworkPrefxiMap::genesis_key, NetworkKnowledge::genesis_key
   Remove the feilds as they can be obtained from NetworkPrefixMap::sections_dag
 - <csr-id-afcf083469c732f10c7c80f4a45e4c33ab111101/> remove RwLock from NetworkPrefixMap
 - <csr-id-17f0e8a08c9543d380c16a35d3d7bfe7834a9e5a/> make NetowrkPrefixMap::sections_dag private
 - <csr-id-aafc560d3b3b1e375f7be224e0e63a3b567bbd86/> rename traceroute fns
 - <csr-id-7394030fe5aeeb88f4524d2da2a71e36334c831d/> traceroute update after cmds flow rebase
 - <csr-id-73dc9b4a1757393270e62d265328bab0c0aa3b35/> make traceroute a default feature
 - <csr-id-0a653e4becc4a8e14ffd6d0752cf035430067ce9/> improve Display, Debug impl for Traceroute
 - <csr-id-9789797e3f773285f23bd22957fe45a67aabec24/> improve traceroute readability and other improvements
   - simplfies creating identites for traceroute to avoid locking
   - implements Display and Debug for traceroute
   - add clearer logs for traceroute
 - <csr-id-08af2a6ac3485a696d2a1e799af588943f207e6b/> clarify fn signatures
 - <csr-id-702c33b0d78f4a459725ed0c4538819c949978ce/> cleanup cache interface let binding

 - <csr-id-2ea069543dbe6ffebac663d4d8d7e0bc33cfc566/> remove RwLock over Cache type
 - <csr-id-322c69845e2e14eb029fdbebb24e08063a2323b0/> remove write lock around non query service msg handling
 - <csr-id-ea490ddf749ac9e0c7962c3c21c053663e6b6ee7/> reflect the semantics not the type
 - <csr-id-bf2902c18b900b8b4a8abae5f966d1e08d547910/> whitespace + typo fix
 - <csr-id-6f03b93bd2d02f0ffe54b69fbf25070fbe64eab0/> upgrade blsttc to 7.0.0
   This version has a more helpful error message for the shares interpolation problem.
 - <csr-id-8b3c4eb06fa988dc97b0cb75ed615ec69af29a48/> add traceroute feature to testnet bin
 - <csr-id-214adedc31bca576c7f28ff52a1f4ff0a2676757/> improve traceroute redability and resolve clippy
 - <csr-id-39c3fdf4128462e5f7c5fec3c628d394f505e2f2/> remove unused console-subscriber
 - <csr-id-1e8180c23fab27ac92c93f201efd050cff00db10/> re-enable registers benchmark and tidy sled residue
 - <csr-id-44cea00f54b39eaea0936ec187a8fa9ccdb61661/> refactor WireMsg.serialize to use BytesMut and avoid some Vec allocations
 - <csr-id-847db2c487cd102af0cf9a477b4c1b65fc2c8aa6/> remove locking from section key cache
 - <csr-id-0a5593b0512d6f059c6a8003634b07e7d2d3e514/> remove refcell on NetworkKnowledge::all_section_chains
 - <csr-id-707b80c3526ae727a7e91330dc386cdb41c51f4c/> remove refcell around NetworkKnowledge::signed_sap
 - <csr-id-9bd6ae20c1207f99420093fd5c9f4eb53836e3c1/> remove refcell on NetworkKnowledge::chain
 - <csr-id-31d9f9f99b4e166986b8e51c3d41e0eac55621a4/> remove awaits from tests as well
 - <csr-id-30a7028dd702e2f6575e299a609a2416439cbaed/> remove locking around signature aggregation
 - <csr-id-dedec486f85c1cf6cf2d538238f32e826e08da0a/> remove unused async
 - <csr-id-879678e986a722d216ee9a4f37e8ae398221a394/> logging sn_consensus in CI, tweak section min and max elder age

### Chore

 - <csr-id-53f60c2327f8a69f0b2ef6d1a4e96644c10aa358/> sn_interface-0.9.0/sn_dysfunction-0.8.0/sn_client-0.69.0/sn_node-0.65.0/sn_api-0.67.0/sn_cli-0.60.0

### Documentation

 - <csr-id-e3c90998e1abd10768e861370a65a934f52e2ec3/> broken links

### New Features

 - <csr-id-df8ea32f9218d344cd1f291359969b38a05b4642/> join approval has membership decision
 - <csr-id-a6fb1fc516a9ef6dae7aa236f3dd440d50697ae2/> impl traceroute feature to trace a message's flow in the network
   - implements traceroute for Client queries and is logged at the client on return

### Bug Fixes

<csr-id-1694c1566ac562216447eb491cc3b2b00b0c5979/>
<csr-id-19bb0b99afee53dd7b6e109919249b25e0a55e48/>
<csr-id-f0d1abf6dd8731310b7749cd6cc7077886215997/>
<csr-id-f6ea1da4a57e40a051c7d1ee3b87fe9b442c537b/>
<csr-id-d2bc1167288724b05d70ae7a305c88c93eac611a/>

 - <csr-id-8bf0aeed0af193322f341bd718f7a5f84fa2d02f/> gossip all votes and start timer after first vote
 - <csr-id-0ed5075304b090597f7760fb51c4a33435a853f1/> fix deadlock introduced after removal of Arc from NetworkPrefixMap
   Removing the checks in compare_and_write_prefix_map and directly
   writing the prefix_map fixed the issue
 - <csr-id-cebf37ea8ef44c51ce84646a83a5d1e6dcab3e7a/> use correct data address type
 - <csr-id-a5028c551d1b3db2c4c912c2897490e9a4b34a0d/> disable rejoins
   It seems that we may have not fully thought through the implications
   of nodes rejoining.
   
   The flow was:
   1. a node leaves the section (reason is not tracked)
2. the node rejoins
3. the section accepts them back and attempts to relocate them

### Other

 - <csr-id-629a5873dd3bdf138649360222c00e3e0a76e097/> remove test files from /resources/test_prefixmap/
   Generate and write prefixmap for the tests instead of storing a copy

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
 - <csr-id-27ba2a63dcfa272cf7ef8c5301987fc6bfe18ed0/> sn_client to only read a default prefix map file, updates to be cached on disk by user
   - CLI to cache the up to date PrefixMap after all commands were executed and right before exiting.
   - Refactoring sn_cli::Config to remove some redundant code.
 - <csr-id-6e65ed8e6c5872bd2c49a1ed2837b1fb16523af1/> remove NetworkKnowledge::chain
 - <csr-id-6b1fee8cf3d0b2995f4b81e59dd684547593b5fa/> reduce AE msgs to one msg with a kind field
 - <csr-id-ed37bb56e5e17d4cba7c1b2165746c193241d618/> move SectionChain into NetworkPrefixMap
 - <csr-id-a0c89ff0e451d2e5dd13fc29635075097f2c7b94/> do not require node write lock on query
 - <csr-id-0f07efd9ef0b75de79f27772566b013bc886bcc8/> remove optional field
 - <csr-id-db4f4d07b155d732ad76d263563d81b5fee535f7/> remove more unused code
 - <csr-id-ff1a10b4aa2b41b7028949101504a29b52927e71/> simplify send msg
 - <csr-id-e0fb940b24e87d86fe920095176362f73503ce79/> use sn_dbc::SpentProof API for verifying SpentProofShares
 - <csr-id-35ebd8e872f9d9db16c42cbe8d61702f9660aece/> expose known keys on network knowledge
 - <csr-id-3f577d2a6fe70792d7d02e231b599ca3d44a5ed2/> rename gen_section_authority_provider to random_sap
 - <csr-id-5c82df633e7c062fdf761a8e6e0a7ae8d26cc73a/> setup step for tests to reissue a set of DBCs from genesis only once
 - <csr-id-24676dadb771bbd966b6a3e1aa53d1c736c90627/> replace sled with filestore for storing registers
 - <csr-id-93614b18b4316af04ab8c74358a5c86510590b85/> make chunk_store accept all datatypes

### Test

 - <csr-id-116b109ecdaaf0f1f10ede96cd68977782ab9ea3/> add proptest to make sure NetworkPrefixMap fields stay in sync
 - <csr-id-f5f73acb035159718780a08f3d10512b11558e59/> generate a random prefix map

### Chore (BREAKING)

 - <csr-id-d3a05a728be8752ea9ebff4e38e7c4c85e5db09b/> having spent proofs and Txs within SpentbookCmd::Send msg to be a set instead of a vec

### Refactor (BREAKING)

 - <csr-id-96da1171d0cac240f772e5d6a15c56f63441b4b3/> nodes to cache their own individual prefix map file on disk
 - <csr-id-dd2eb21352223f6340064e0021f4a7df402cd5c9/> removing Token from sn_interfaces::type as it is now exposed by sn_dbc

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 69 commits contributed to the release over the course of 31 calendar days.
 - 34 days passed between releases.
 - 60 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.9.0/sn_dysfunction-0.8.0/sn_client-0.69.0/sn_node-0.65.0/sn_api-0.67.0/sn_cli-0.60.0 ([`53f60c2`](https://github.com/maidsafe/safe_network/commit/53f60c2327f8a69f0b2ef6d1a4e96644c10aa358))
    - add partial eq for rust 1.63; dep updates ([`66c2678`](https://github.com/maidsafe/safe_network/commit/66c26782759be707edb922daa548e3f0a3f9be8c))
    - reorganise flow control unit tests ([`12360a6`](https://github.com/maidsafe/safe_network/commit/12360a6dcc204153a81adbf842a64dc018c750f9))
    - sn_client to only read a default prefix map file, updates to be cached on disk by user ([`27ba2a6`](https://github.com/maidsafe/safe_network/commit/27ba2a63dcfa272cf7ef8c5301987fc6bfe18ed0))
    - remove wiremsg.priority as uneeded ([`6d60525`](https://github.com/maidsafe/safe_network/commit/6d60525874dc4efeb658433f1f253d54e0cba2d4))
    - gossip all votes and start timer after first vote ([`8bf0aee`](https://github.com/maidsafe/safe_network/commit/8bf0aeed0af193322f341bd718f7a5f84fa2d02f))
    - remove NetworkKnowledge::chain ([`6e65ed8`](https://github.com/maidsafe/safe_network/commit/6e65ed8e6c5872bd2c49a1ed2837b1fb16523af1))
    - serialize NetworkPrefixMap into JSON ([`29de67f`](https://github.com/maidsafe/safe_network/commit/29de67f1e3583eab867d517cb50ed2e404bd63fd))
    - nodes to cache their own individual prefix map file on disk ([`96da117`](https://github.com/maidsafe/safe_network/commit/96da1171d0cac240f772e5d6a15c56f63441b4b3))
    - reduce AE msgs to one msg with a kind field ([`6b1fee8`](https://github.com/maidsafe/safe_network/commit/6b1fee8cf3d0b2995f4b81e59dd684547593b5fa))
    - removing Token from sn_interfaces::type as it is now exposed by sn_dbc ([`dd2eb21`](https://github.com/maidsafe/safe_network/commit/dd2eb21352223f6340064e0021f4a7df402cd5c9))
    - chore(style): organise usings, cleanup - Removes some boilerplate, using fn of `Cmd` to instantiate a send cmd. - Housekeeping, continuing to minimize bloat of usings, by colocating them. - Housekeeping, continuing keeping positions of usings in a file according to a system, from closest (self) on top, down to furthest away (3rd part). ([`8242f2f`](https://github.com/maidsafe/safe_network/commit/8242f2f1035b1c0718e53954951badffa30f3393))
    - use matches! macros, minor refactoring ([`848dba4`](https://github.com/maidsafe/safe_network/commit/848dba48e5959d0b9cfe182fde2f12ede71ba9c2))
    - remove unused async/await ([`35483b3`](https://github.com/maidsafe/safe_network/commit/35483b3f322eeea2c10427e94e4750a8269811c0))
    - remove test files from /resources/test_prefixmap/ ([`629a587`](https://github.com/maidsafe/safe_network/commit/629a5873dd3bdf138649360222c00e3e0a76e097))
    - remove NetworkPrefxiMap::genesis_key, NetworkKnowledge::genesis_key ([`820fcc9`](https://github.com/maidsafe/safe_network/commit/820fcc9a77f756fca308f247c3ea1b82f65d30b9))
    - fix deadlock introduced after removal of Arc from NetworkPrefixMap ([`0ed5075`](https://github.com/maidsafe/safe_network/commit/0ed5075304b090597f7760fb51c4a33435a853f1))
    - remove RwLock from NetworkPrefixMap ([`afcf083`](https://github.com/maidsafe/safe_network/commit/afcf083469c732f10c7c80f4a45e4c33ab111101))
    - add proptest to make sure NetworkPrefixMap fields stay in sync ([`116b109`](https://github.com/maidsafe/safe_network/commit/116b109ecdaaf0f1f10ede96cd68977782ab9ea3))
    - generate a random prefix map ([`f5f73ac`](https://github.com/maidsafe/safe_network/commit/f5f73acb035159718780a08f3d10512b11558e59))
    - make NetowrkPrefixMap::sections_dag private ([`17f0e8a`](https://github.com/maidsafe/safe_network/commit/17f0e8a08c9543d380c16a35d3d7bfe7834a9e5a))
    - move SectionChain into NetworkPrefixMap ([`ed37bb5`](https://github.com/maidsafe/safe_network/commit/ed37bb56e5e17d4cba7c1b2165746c193241d618))
    - rename traceroute fns ([`aafc560`](https://github.com/maidsafe/safe_network/commit/aafc560d3b3b1e375f7be224e0e63a3b567bbd86))
    - traceroute update after cmds flow rebase ([`7394030`](https://github.com/maidsafe/safe_network/commit/7394030fe5aeeb88f4524d2da2a71e36334c831d))
    - make traceroute a default feature ([`73dc9b4`](https://github.com/maidsafe/safe_network/commit/73dc9b4a1757393270e62d265328bab0c0aa3b35))
    - improve Display, Debug impl for Traceroute ([`0a653e4`](https://github.com/maidsafe/safe_network/commit/0a653e4becc4a8e14ffd6d0752cf035430067ce9))
    - improve traceroute readability and other improvements ([`9789797`](https://github.com/maidsafe/safe_network/commit/9789797e3f773285f23bd22957fe45a67aabec24))
    - fix(file_store): use correct data address type The type including `SafeKey` had been incorrectly used (since it is not a network side concept), which caused a lot of `Result` return values bloating the call tree unecessarily. ([`cebf37e`](https://github.com/maidsafe/safe_network/commit/cebf37ea8ef44c51ce84646a83a5d1e6dcab3e7a))
    - refactor: do not require node write lock on query This creates the `AddToPendingQieries` cmd, which adds asyncly to the list. Also cleans up the `read_data_from_adults` fn a bit. ([`a0c89ff`](https://github.com/maidsafe/safe_network/commit/a0c89ff0e451d2e5dd13fc29635075097f2c7b94))
    - remove optional field ([`0f07efd`](https://github.com/maidsafe/safe_network/commit/0f07efd9ef0b75de79f27772566b013bc886bcc8))
    - refactor(messaging): remove more unused code More reuse of methods to replace duplication of code. Deprecates delivery group, since it is no longer used. Also, `DstLocation` and `SrcLocation` are removed. BREAKING CHANGE: WireMsg public type is changed. ([`db4f4d0`](https://github.com/maidsafe/safe_network/commit/db4f4d07b155d732ad76d263563d81b5fee535f7))
    - chore: clarify fn signatures Return single cmd when only one can be returned. Remove some unnecessary Results. Also fixes insufficient adults error being triggered falsely. ([`08af2a6`](https://github.com/maidsafe/safe_network/commit/08af2a6ac3485a696d2a1e799af588943f207e6b))
    - refactor(send): simplify send msg This places signing and wire msg instantiation in one location, and removes lots of old variables that aren't used in the flow anymore. ([`ff1a10b`](https://github.com/maidsafe/safe_network/commit/ff1a10b4aa2b41b7028949101504a29b52927e71))
    - use sn_dbc::SpentProof API for verifying SpentProofShares ([`e0fb940`](https://github.com/maidsafe/safe_network/commit/e0fb940b24e87d86fe920095176362f73503ce79))
    - cleanup cache interface let binding ([`702c33b`](https://github.com/maidsafe/safe_network/commit/702c33b0d78f4a459725ed0c4538819c949978ce))
    - remove RwLock over Cache type ([`2ea0695`](https://github.com/maidsafe/safe_network/commit/2ea069543dbe6ffebac663d4d8d7e0bc33cfc566))
    - remove write lock around non query service msg handling ([`322c698`](https://github.com/maidsafe/safe_network/commit/322c69845e2e14eb029fdbebb24e08063a2323b0))
    - having spent proofs and Txs within SpentbookCmd::Send msg to be a set instead of a vec ([`d3a05a7`](https://github.com/maidsafe/safe_network/commit/d3a05a728be8752ea9ebff4e38e7c4c85e5db09b))
    - chore(naming): reflect the semantics not the type The type is named Kind but the semantics of it is Auth. Often we mindlessly name things after the type names instead of what they represent in the domain. BREAKING CHANGE: fields of public msg renamed ([`ea490dd`](https://github.com/maidsafe/safe_network/commit/ea490ddf749ac9e0c7962c3c21c053663e6b6ee7))
    - Tweak docs of `known_keys` ([`f0b105f`](https://github.com/maidsafe/safe_network/commit/f0b105f48c10a42b3d8434f3900700cd2ec2e484))
    - refactor: expose known keys on network knowledge The method can be called directly instead of passing known keys in the cmds. ([`35ebd8e`](https://github.com/maidsafe/safe_network/commit/35ebd8e872f9d9db16c42cbe8d61702f9660aece))
    - whitespace + typo fix ([`bf2902c`](https://github.com/maidsafe/safe_network/commit/bf2902c18b900b8b4a8abae5f966d1e08d547910))
    - disable rejoins ([`a5028c5`](https://github.com/maidsafe/safe_network/commit/a5028c551d1b3db2c4c912c2897490e9a4b34a0d))
    - prevent rejoins of archived nodes ([`1694c15`](https://github.com/maidsafe/safe_network/commit/1694c1566ac562216447eb491cc3b2b00b0c5979))
    - rename gen_section_authority_provider to random_sap ([`3f577d2`](https://github.com/maidsafe/safe_network/commit/3f577d2a6fe70792d7d02e231b599ca3d44a5ed2))
    - join approval has membership decision ([`df8ea32`](https://github.com/maidsafe/safe_network/commit/df8ea32f9218d344cd1f291359969b38a05b4642))
    - upgrade blsttc to 7.0.0 ([`6f03b93`](https://github.com/maidsafe/safe_network/commit/6f03b93bd2d02f0ffe54b69fbf25070fbe64eab0))
    - adds unique conn info validation to membership ([`19bb0b9`](https://github.com/maidsafe/safe_network/commit/19bb0b99afee53dd7b6e109919249b25e0a55e48))
    - add traceroute feature to testnet bin ([`8b3c4eb`](https://github.com/maidsafe/safe_network/commit/8b3c4eb06fa988dc97b0cb75ed615ec69af29a48))
    - improve traceroute redability and resolve clippy ([`214aded`](https://github.com/maidsafe/safe_network/commit/214adedc31bca576c7f28ff52a1f4ff0a2676757))
    - impl traceroute feature to trace a message's flow in the network ([`a6fb1fc`](https://github.com/maidsafe/safe_network/commit/a6fb1fc516a9ef6dae7aa236f3dd440d50697ae2))
    - broken links ([`e3c9099`](https://github.com/maidsafe/safe_network/commit/e3c90998e1abd10768e861370a65a934f52e2ec3))
    - remove redundant generation field ([`f0d1abf`](https://github.com/maidsafe/safe_network/commit/f0d1abf6dd8731310b7749cd6cc7077886215997))
    - remove unused console-subscriber ([`39c3fdf`](https://github.com/maidsafe/safe_network/commit/39c3fdf4128462e5f7c5fec3c628d394f505e2f2))
    - setup step for tests to reissue a set of DBCs from genesis only once ([`5c82df6`](https://github.com/maidsafe/safe_network/commit/5c82df633e7c062fdf761a8e6e0a7ae8d26cc73a))
    - re-enable registers benchmark and tidy sled residue ([`1e8180c`](https://github.com/maidsafe/safe_network/commit/1e8180c23fab27ac92c93f201efd050cff00db10))
    - replace sled with filestore for storing registers ([`24676da`](https://github.com/maidsafe/safe_network/commit/24676dadb771bbd966b6a3e1aa53d1c736c90627))
    - make chunk_store accept all datatypes ([`93614b1`](https://github.com/maidsafe/safe_network/commit/93614b18b4316af04ab8c74358a5c86510590b85))
    - refactor WireMsg.serialize to use BytesMut and avoid some Vec allocations ([`44cea00`](https://github.com/maidsafe/safe_network/commit/44cea00f54b39eaea0936ec187a8fa9ccdb61661))
    - cleanup unused async ([`f6ea1da`](https://github.com/maidsafe/safe_network/commit/f6ea1da4a57e40a051c7d1ee3b87fe9b442c537b))
    - box LRUCache to avoid stackoverflow ([`d2bc116`](https://github.com/maidsafe/safe_network/commit/d2bc1167288724b05d70ae7a305c88c93eac611a))
    - remove locking from section key cache ([`847db2c`](https://github.com/maidsafe/safe_network/commit/847db2c487cd102af0cf9a477b4c1b65fc2c8aa6))
    - remove refcell on NetworkKnowledge::all_section_chains ([`0a5593b`](https://github.com/maidsafe/safe_network/commit/0a5593b0512d6f059c6a8003634b07e7d2d3e514))
    - remove refcell around NetworkKnowledge::signed_sap ([`707b80c`](https://github.com/maidsafe/safe_network/commit/707b80c3526ae727a7e91330dc386cdb41c51f4c))
    - remove refcell on NetworkKnowledge::chain ([`9bd6ae2`](https://github.com/maidsafe/safe_network/commit/9bd6ae20c1207f99420093fd5c9f4eb53836e3c1))
    - remove awaits from tests as well ([`31d9f9f`](https://github.com/maidsafe/safe_network/commit/31d9f9f99b4e166986b8e51c3d41e0eac55621a4))
    - remove locking around signature aggregation ([`30a7028`](https://github.com/maidsafe/safe_network/commit/30a7028dd702e2f6575e299a609a2416439cbaed))
    - remove unused async ([`dedec48`](https://github.com/maidsafe/safe_network/commit/dedec486f85c1cf6cf2d538238f32e826e08da0a))
    - logging sn_consensus in CI, tweak section min and max elder age ([`879678e`](https://github.com/maidsafe/safe_network/commit/879678e986a722d216ee9a4f37e8ae398221a394))
</details>

## v0.8.2 (2022-07-10)

<csr-id-49e223e2c07695b4c63e253ba19ce43ec24d7112/>
<csr-id-dce3ba214354ad007900efce78273670cfb725d5/>
<csr-id-34bd9bd01a3f042c35e0432df2f0cfcebc32a8a8/>

### Chore

 - <csr-id-49e223e2c07695b4c63e253ba19ce43ec24d7112/> move more deps to clap-v3; rm some deps on rand

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

 - 9 commits contributed to the release over the course of 2 calendar days.
 - 2 days passed between releases.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.8.2/sn_client-0.68.2/sn_node-0.64.2/sn_api-0.66.3/sn_cli-0.59.3 ([`34bd9bd`](https://github.com/maidsafe/safe_network/commit/34bd9bd01a3f042c35e0432df2f0cfcebc32a8a8))
    - move dkg util method definitions onto the DKG structs ([`dce3ba2`](https://github.com/maidsafe/safe_network/commit/dce3ba214354ad007900efce78273670cfb725d5))
    - move more deps to clap-v3; rm some deps on rand ([`49e223e`](https://github.com/maidsafe/safe_network/commit/49e223e2c07695b4c63e253ba19ce43ec24d7112))
    - Merge #1301 ([`9c6914e`](https://github.com/maidsafe/safe_network/commit/9c6914e2688f70a25ad5dfe74307572cb8e8fcc2))
    - Merge branch 'main' into feat-dbc-spent-proof-validations ([`45418f2`](https://github.com/maidsafe/safe_network/commit/45418f2f9b5cc58f2a153bf40966beb2bf36a62a))
    - for QueryResponse, set correlation_id to be the origin msg_id ([`64eb333`](https://github.com/maidsafe/safe_network/commit/64eb333d532694f46f1d0b9dd5109961b3551802))
    - passing the churn test ([`3c383cc`](https://github.com/maidsafe/safe_network/commit/3c383ccf9ad0ed77080fb3e3ec459e5b02158505))
    - Merge branch 'main' into feat-dbc-spent-proof-validations ([`45309c4`](https://github.com/maidsafe/safe_network/commit/45309c4c0463dd9198a49537187417bf4bfdb847))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`6268fe7`](https://github.com/maidsafe/safe_network/commit/6268fe76e9dd81d291492b4611094273f8d1e223))
</details>

## v0.8.1 (2022-07-07)

<csr-id-c79e2aac378b28b373fd7c18c4b9006348960071/>
<csr-id-46262268fc167c05963e5b7bd6261310496e2379/>
<csr-id-8dccb7f1fc81385f9f5f25e6c354ad1d35759528/>
<csr-id-2b00cec961561281f6b927e13e501342843f6a0f/>

### Chore

 - <csr-id-c79e2aac378b28b373fd7c18c4b9006348960071/> bit more low hanging clippy fruit
 - <csr-id-46262268fc167c05963e5b7bd6261310496e2379/> `try!` macro is deprecated
   No need for rustfmt to check/replace this, as the compiler will already
   warn for this. Deprecated since 1.39.
   
   Removing the option seems to trigger a couple of formatting changes that
   rustfmt did not seem to pick on before.
 - <csr-id-8dccb7f1fc81385f9f5f25e6c354ad1d35759528/> clippy runs cargo check already

### New Features

 - <csr-id-8313ed8d5b45b7f4ed3b36ada231e74c49c9f9e6/> perform signature verifications on input DBC SpentProof before signing new spent proof share

### Chore

 - <csr-id-2b00cec961561281f6b927e13e501342843f6a0f/> sn_interface-0.8.1/sn_dysfunction-0.7.1/sn_client-0.68.1/sn_node-0.64.1/sn_api-0.66.1/sn_cli-0.59.1

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 13 commits contributed to the release.
 - 2 days passed between releases.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.8.1/sn_dysfunction-0.7.1/sn_client-0.68.1/sn_node-0.64.1/sn_api-0.66.1/sn_cli-0.59.1 ([`2b00cec`](https://github.com/maidsafe/safe_network/commit/2b00cec961561281f6b927e13e501342843f6a0f))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`7e2a25a`](https://github.com/maidsafe/safe_network/commit/7e2a25ae31ead0fae7824ca794b6c407695080cd))
    - Merge #1315 ([`67686f7`](https://github.com/maidsafe/safe_network/commit/67686f73f9e7b18bb6fbf1eadc3fd3a256285396))
    - Merge branch 'main' into feat-dbc-spent-proof-validations ([`f83724c`](https://github.com/maidsafe/safe_network/commit/f83724cff1e63b35f1612fc82dffdefbeaab6cc1))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`cd2f9aa`](https://github.com/maidsafe/safe_network/commit/cd2f9aa2f7001ae779273745f9ac78fc289525e3))
    - Merge #1308 ([`8421959`](https://github.com/maidsafe/safe_network/commit/8421959b6a80e4386c34fcd6f86a1af5044280ec))
    - Merge branch 'main' into feat-cli-wallet-show-deposited-amount ([`39bd5b4`](https://github.com/maidsafe/safe_network/commit/39bd5b471b6b3acb6ebe90489335c995b0aca82f))
    - Merge branch 'main' into cargo-husky-tweaks ([`6881855`](https://github.com/maidsafe/safe_network/commit/688185573bb71cc44a7103df17f3fbeea6740247))
    - perform signature verifications on input DBC SpentProof before signing new spent proof share ([`8313ed8`](https://github.com/maidsafe/safe_network/commit/8313ed8d5b45b7f4ed3b36ada231e74c49c9f9e6))
    - Merge #1309 ([`f9fa4f7`](https://github.com/maidsafe/safe_network/commit/f9fa4f7857d8161e8c036cca06006bf187a6c6c3))
    - bit more low hanging clippy fruit ([`c79e2aa`](https://github.com/maidsafe/safe_network/commit/c79e2aac378b28b373fd7c18c4b9006348960071))
    - `try!` macro is deprecated ([`4626226`](https://github.com/maidsafe/safe_network/commit/46262268fc167c05963e5b7bd6261310496e2379))
    - clippy runs cargo check already ([`8dccb7f`](https://github.com/maidsafe/safe_network/commit/8dccb7f1fc81385f9f5f25e6c354ad1d35759528))
</details>

## v0.8.0 (2022-07-04)

<csr-id-9314a2db5dc1ae91bc4d80a65c1a8825492fc7c7/>
<csr-id-ddb7798a7b0c5e60960e123414277d58f3da27eb/>
<csr-id-4e04a2b0acc79140bf1d0aefd82c0ad5b046a3cd/>
<csr-id-6a2553a11b1404ad404e67df29bf3ec535d1b954/>
<csr-id-2aae965ca2fdd4ff59034547b5ee8dcef0b7253e/>
<csr-id-068327834c8d07ada6bf42cf78d6f7a117715466/>
<csr-id-976e8c3d8c610d2a34c1bfa6678132a1bad234e8/>
<csr-id-849dfba283362d8fbdddd92be1078c3a963fb564/>
<csr-id-91da4d4ac7aab039853b0651e5aafd9cdd31b9c4/>
<csr-id-e4e2eb56611a328806c59ed8bc80ca2567206bbb/>

### Chore

 - <csr-id-9314a2db5dc1ae91bc4d80a65c1a8825492fc7c7/> Docs - put symbols in backticks
 - <csr-id-ddb7798a7b0c5e60960e123414277d58f3da27eb/> remove let bindings for unit returns
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

### Refactor

 - <csr-id-6a2553a11b1404ad404e67df29bf3ec535d1b954/> remove NetworkInfo::GenesisKey variant
 - <csr-id-2aae965ca2fdd4ff59034547b5ee8dcef0b7253e/> use hardlink instead of symlink
 - <csr-id-068327834c8d07ada6bf42cf78d6f7a117715466/> sn_cli modify tests
 - <csr-id-976e8c3d8c610d2a34c1bfa6678132a1bad234e8/> sn_cli uses NetworkPrefixMap instead of node_conn_info.config
 - <csr-id-849dfba283362d8fbdddd92be1078c3a963fb564/> update PrefixMap symlink if incorrect
 - <csr-id-91da4d4ac7aab039853b0651e5aafd9cdd31b9c4/> remove node_connection_info.config from sn_node, sn_interface, sn_client

### New Features (BREAKING)

 - <csr-id-5dad80d3f239f5844243fedb89f8d4baaee3b640/> have the nodes to attach valid Commitments to signed SpentProofShares

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 11 commits contributed to the release over the course of 6 calendar days.
 - 6 days passed between releases.
 - 11 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.8.0/sn_dysfunction-0.7.0/sn_client-0.68.0/sn_node-0.64.0/sn_api-0.66.0/sn_cli-0.59.0 ([`e4e2eb5`](https://github.com/maidsafe/safe_network/commit/e4e2eb56611a328806c59ed8bc80ca2567206bbb))
    - remove NetworkInfo::GenesisKey variant ([`6a2553a`](https://github.com/maidsafe/safe_network/commit/6a2553a11b1404ad404e67df29bf3ec535d1b954))
    - use hardlink instead of symlink ([`2aae965`](https://github.com/maidsafe/safe_network/commit/2aae965ca2fdd4ff59034547b5ee8dcef0b7253e))
    - sn_cli modify tests ([`0683278`](https://github.com/maidsafe/safe_network/commit/068327834c8d07ada6bf42cf78d6f7a117715466))
    - sn_cli uses NetworkPrefixMap instead of node_conn_info.config ([`976e8c3`](https://github.com/maidsafe/safe_network/commit/976e8c3d8c610d2a34c1bfa6678132a1bad234e8))
    - update PrefixMap symlink if incorrect ([`849dfba`](https://github.com/maidsafe/safe_network/commit/849dfba283362d8fbdddd92be1078c3a963fb564))
    - remove node_connection_info.config from sn_node, sn_interface, sn_client ([`91da4d4`](https://github.com/maidsafe/safe_network/commit/91da4d4ac7aab039853b0651e5aafd9cdd31b9c4))
    - Docs - put symbols in backticks ([`9314a2d`](https://github.com/maidsafe/safe_network/commit/9314a2db5dc1ae91bc4d80a65c1a8825492fc7c7))
    - remove let bindings for unit returns ([`ddb7798`](https://github.com/maidsafe/safe_network/commit/ddb7798a7b0c5e60960e123414277d58f3da27eb))
    - have the nodes to attach valid Commitments to signed SpentProofShares ([`5dad80d`](https://github.com/maidsafe/safe_network/commit/5dad80d3f239f5844243fedb89f8d4baaee3b640))
    - remove unused asyncs (clippy) ([`4e04a2b`](https://github.com/maidsafe/safe_network/commit/4e04a2b0acc79140bf1d0aefd82c0ad5b046a3cd))
</details>

## v0.7.1 (2022-06-28)

<csr-id-8c69306dc86a99a8be443ab8213253983540f1cf/>
<csr-id-58890e5c919ada30f27d4e80c6b5e7291b99ed5c/>

### New Features

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

### Chore

 - <csr-id-58890e5c919ada30f27d4e80c6b5e7291b99ed5c/> sn_interface-0.7.1/sn_dysfunction-0.6.1/sn_client-0.67.1/sn_node-0.63.1

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release.
 - 2 days passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.7.1/sn_dysfunction-0.6.1/sn_client-0.67.1/sn_node-0.63.1 ([`58890e5`](https://github.com/maidsafe/safe_network/commit/58890e5c919ada30f27d4e80c6b5e7291b99ed5c))
    - resend last vote if nothing received after an interval. ([`7528247`](https://github.com/maidsafe/safe_network/commit/752824774884ef77616d26734517c58530cdae1f))
    - Select which adult to query ([`6bfd101`](https://github.com/maidsafe/safe_network/commit/6bfd101ed12a16f3f6a9a0b55252d45d200af7c6))
    - Rename DataQuery with suffix Variant ([`8c69306`](https://github.com/maidsafe/safe_network/commit/8c69306dc86a99a8be443ab8213253983540f1cf))
</details>

## v0.7.0 (2022-06-26)

<csr-id-243cfc48a7f4a9b60b5b7f1fdd609c02197aba5e/>
<csr-id-3f3c39a14987910bb424df51f89d948333ca3e87/>
<csr-id-5ea4c3d60bf84384ed37b5dde25ac4dc26147c24/>

### Chore

 - <csr-id-243cfc48a7f4a9b60b5b7f1fdd609c02197aba5e/> sn_interface-0.7.0/sn_dysfunction-0.6.0/sn_client-0.67.0/sn_node-0.63.0/sn_api-0.65.0/sn_cli-0.58.0
 - <csr-id-3f3c39a14987910bb424df51f89d948333ca3e87/> changes based on review feedback
   * Prefer `map_err` in various places rather than a full `match`.
   * Change key serialization utility functions to static rather than instance.
   * Change `dog` command to print non-support of `SafeKey` data type rather than panic.
   * Remove unnecessary clone on `public_key_hex`.
   * Remove unnecessary match in various tests.
   * Ignore wallet CLI tests that deleted the credentials file. They are problematic when running in
     parallel with other tests. We need better isolated testing mechanisms for these. Will address in a
     separate PR.
   * Use different deposit names in wallet tests where multiple DBCs are deposited.
 - <csr-id-5ea4c3d60bf84384ed37b5dde25ac4dc26147c24/> changes based on review feedback
   * Prefer `map_err` in various places rather than a full `match`.
   * Change key serialization utility functions to static rather than instance.
   * Change `dog` command to print non-support of `SafeKey` data type rather than panic.
   * Remove unnecessary clone on `public_key_hex`.
   * Remove unnecessary match in various tests.
   * Ignore wallet CLI tests that deleted the credentials file. They are problematic when running in
     parallel with other tests. We need better isolated testing mechanisms for these. Will address in a
     separate PR.
   * Use different deposit names in wallet tests where multiple DBCs are deposited.

### New Features (BREAKING)

 - <csr-id-5577695b5d3291c46cd475df8c0933a067b4cfc5/> serialize to bls keys in util functions
   Utility functions were recently added to the API for serializing to the `Keypair` type. This was
   changed to serialize directly to BLS to make it easier for the CLI to deal directly with BLS keys.
   Soon we will be refactoring the `Keypair` type to have a different use case and things like
   `sn_client` would be refactored to directly work with BLS keys. This is a little step in that
   direction.
   
   There was a utility function added to `sn_interface` to create a `Keypair` from a hex-based BLS key
   because we still need to use the `Keypair` at this point in time.
 - <csr-id-67006eb2e84b750a6b9b03d04aafdcfc85b38955/> serialize to bls keys in util functions
   Utility functions were recently added to the API for serializing to the `Keypair` type. This was
   changed to serialize directly to BLS to make it easier for the CLI to deal directly with BLS keys.
   Soon we will be refactoring the `Keypair` type to have a different use case and things like
   `sn_client` would be refactored to directly work with BLS keys. This is a little step in that
   direction.
   
   There was a utility function added to `sn_interface` to create a `Keypair` from a hex-based BLS key
   because we still need to use the `Keypair` at this point in time.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
 - 5 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.7.0/sn_dysfunction-0.6.0/sn_client-0.67.0/sn_node-0.63.0/sn_api-0.65.0/sn_cli-0.58.0 ([`243cfc4`](https://github.com/maidsafe/safe_network/commit/243cfc48a7f4a9b60b5b7f1fdd609c02197aba5e))
    - changes based on review feedback ([`3f3c39a`](https://github.com/maidsafe/safe_network/commit/3f3c39a14987910bb424df51f89d948333ca3e87))
    - serialize to bls keys in util functions ([`5577695`](https://github.com/maidsafe/safe_network/commit/5577695b5d3291c46cd475df8c0933a067b4cfc5))
    - changes based on review feedback ([`5ea4c3d`](https://github.com/maidsafe/safe_network/commit/5ea4c3d60bf84384ed37b5dde25ac4dc26147c24))
    - serialize to bls keys in util functions ([`67006eb`](https://github.com/maidsafe/safe_network/commit/67006eb2e84b750a6b9b03d04aafdcfc85b38955))
</details>

## v0.6.5 (2022-06-24)

<csr-id-d7a831329ad600ad7b5a1b6b68841f96b8ef8cfa/>
<csr-id-1fbc762305a581680b52e2cbdaa7aea2feaf05ab/>
<csr-id-dc69a62eec590b2d621ab2cbc3009cb052955e66/>

### Chore

 - <csr-id-d7a831329ad600ad7b5a1b6b68841f96b8ef8cfa/> misc cleanup and fixes

### Chore

 - <csr-id-dc69a62eec590b2d621ab2cbc3009cb052955e66/> sn_interface-0.6.5/sn_dysfunction-0.5.3/sn_client-0.66.5/sn_node-0.62.8/sn_cli-0.57.6

### New Features

 - <csr-id-71eb46e47032074cdca678783e815b8d55ae39a0/> organize internal work

### Refactor

 - <csr-id-1fbc762305a581680b52e2cbdaa7aea2feaf05ab/> move it to its own file

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 9 commits contributed to the release over the course of 3 calendar days.
 - 3 days passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
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
    - Merge #1257 #1260 ([`19d89df`](https://github.com/maidsafe/safe_network/commit/19d89dfbbf8ac8ab2b08380ce9b4bed58a5dc0d9))
    - refactor(msg_type):  move it to its own file - Moves priority fns to service- and system msg. - Moves deserialise of payload to wire_msg fn when getting priority. ([`1fbc762`](https://github.com/maidsafe/safe_network/commit/1fbc762305a581680b52e2cbdaa7aea2feaf05ab))
    - Merge #1256 ([`cff8b33`](https://github.com/maidsafe/safe_network/commit/cff8b337be20f3e1c0cddc5464c2eee0c8cc9e1c))
    - Merge branch 'main' into refactor-event-channel ([`024883e`](https://github.com/maidsafe/safe_network/commit/024883e9a1b853c02c29daa5c447b03570af2473))
</details>

## v0.6.4 (2022-06-21)

<csr-id-1574b495f17d25af2ed9dd017ccf8dce715a8b28/>
<csr-id-fd7f845f7d1534cf9ff93ee9dc9f3009ab7e5664/>
<csr-id-d204cffdc25a08f604f3a7b97dd74c0f4181b696/>
<csr-id-c038635cf88d32c52da89d11a8532e6c91c8bf38/>
<csr-id-d26d26df6ddd0321555fa3653be966fe91e2dca4/>
<csr-id-d526e0a32d3f09a788899d82db4fe6f13258568c/>

### Chore

 - <csr-id-1574b495f17d25af2ed9dd017ccf8dce715a8b28/> avoid another chain borrow/drop, use cloning api
 - <csr-id-fd7f845f7d1534cf9ff93ee9dc9f3009ab7e5664/> make NetworkKnowledge single threaded
 - <csr-id-d204cffdc25a08f604f3a7b97dd74c0f4181b696/> remove unused deps and enum variants
 - <csr-id-c038635cf88d32c52da89d11a8532e6c91c8bf38/> misc cleanup

### Refactor

 - <csr-id-d26d26df6ddd0321555fa3653be966fe91e2dca4/> cleanup and restructure of enum

### Chore

 - <csr-id-d526e0a32d3f09a788899d82db4fe6f13258568c/> sn_interface-0.6.4/sn_dysfunction-0.5.2/sn_client-0.66.4/sn_node-0.62.7/sn_api-0.64.4

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release.
 - 5 days passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.4/sn_dysfunction-0.5.2/sn_client-0.66.4/sn_node-0.62.7/sn_api-0.64.4 ([`d526e0a`](https://github.com/maidsafe/safe_network/commit/d526e0a32d3f09a788899d82db4fe6f13258568c))
    - refactor(events): cleanup and restructure of enum  - Initiates the use of the node event channel for more structured logging. BREAKING CHANGE: events renamed and restructured ([`d26d26d`](https://github.com/maidsafe/safe_network/commit/d26d26df6ddd0321555fa3653be966fe91e2dca4))
    - Merge #1253 ([`abc95c1`](https://github.com/maidsafe/safe_network/commit/abc95c1093479938a5efbef279190540156ee23a))
    - avoid another chain borrow/drop, use cloning api ([`1574b49`](https://github.com/maidsafe/safe_network/commit/1574b495f17d25af2ed9dd017ccf8dce715a8b28))
    - make NetworkKnowledge single threaded ([`fd7f845`](https://github.com/maidsafe/safe_network/commit/fd7f845f7d1534cf9ff93ee9dc9f3009ab7e5664))
    - chore: remove unused deps and enum variants Was made aware by a comment on the forum that there was a sled dep in `sn_interface`, which seemed wrong, and from there I found more. ([`d204cff`](https://github.com/maidsafe/safe_network/commit/d204cffdc25a08f604f3a7b97dd74c0f4181b696))
    - chore: misc cleanup - Organise usings - Add missing license headers - Update license years As it would take too long to go through all files, a partial cleanup of the code base is made here. It is based on where the using of `sn_interface` has been introduced, as it was a low hanging fruit to cover many occurrences of duplication in many files. ([`c038635`](https://github.com/maidsafe/safe_network/commit/c038635cf88d32c52da89d11a8532e6c91c8bf38))
</details>

## v0.6.3 (2022-06-15)

<csr-id-4eb43fa884d7b047febb18c067ae905969a113bf/>
<csr-id-537b6c08447c15a056d8c79c8592106d9a40b672/>
<csr-id-f599c5973d50324aad1720166156666d5db1ed3d/>

### Chore

 - <csr-id-4eb43fa884d7b047febb18c067ae905969a113bf/> upgrade blsttc to 6.0.0
   There were various other crates that had to be upgraded in this process:
   * secured_linked_list to v0.5.2 because it was also upgraded to reference v6.0.0 of blsttc
   * bls_dkg to v0.10.3 because it was also upgraded to reference v6.0.0 of blsttc
   * sn_consensus to v2.1.1 because it was also upgraded to reference v6.0.0 of blsttc
   * sn_dbc to v4.0.0 because it was also upgraded to reference v6.0.0 of blsttc
 - <csr-id-537b6c08447c15a056d8c79c8592106d9a40b672/> reduce comm error weighting

### Chore

 - <csr-id-f599c5973d50324aad1720166156666d5db1ed3d/> sn_interface-0.6.3/sn_dysfunction-0.5.1/sn_client-0.66.3/sn_api-0.64.3/sn_cli-0.57.4

### New Features

 - <csr-id-7ccb02a7ded7579bb8645c918b9a6108b1b585af/> enable tracking of Dkg issues

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.3/sn_dysfunction-0.5.1/sn_client-0.66.3/sn_api-0.64.3/sn_cli-0.57.4 ([`f599c59`](https://github.com/maidsafe/safe_network/commit/f599c5973d50324aad1720166156666d5db1ed3d))
    - Merge #1241 ([`f9c7544`](https://github.com/maidsafe/safe_network/commit/f9c7544f369e15fb3b6f91158ac3277656737fa4))
    - upgrade blsttc to 6.0.0 ([`4eb43fa`](https://github.com/maidsafe/safe_network/commit/4eb43fa884d7b047febb18c067ae905969a113bf))
    - Merge #1234 ([`05b9b75`](https://github.com/maidsafe/safe_network/commit/05b9b755165304c282cc415419030eee8b6a3636))
    - reduce comm error weighting ([`537b6c0`](https://github.com/maidsafe/safe_network/commit/537b6c08447c15a056d8c79c8592106d9a40b672))
    - enable tracking of Dkg issues ([`7ccb02a`](https://github.com/maidsafe/safe_network/commit/7ccb02a7ded7579bb8645c918b9a6108b1b585af))
</details>

## v0.6.2 (2022-06-15)

<csr-id-b818c3fd10a4e3304b2c5f84dac843397873cba6/>
<csr-id-46246f155ab65f3fcd61381345f1a7f747dfe957/>

### New Features

 - <csr-id-1b1cb77df6c2805ecfa741bb824b359214558929/> remove private registers
 - <csr-id-f1829f99ef1415a83731f855757fbce9970fa4f0/> remove private data addresses
 - <csr-id-8be2f2c9efac1623ea95ff1641c6b9bc22fad455/> remove private safe key addresses

### Bug Fixes

 - <csr-id-616d8cb12bfc257f9b3609239790065ebced8fe3/> replace at_least_one_elders with supermajority for sending cmd
 - <csr-id-60f5a68a1df6114b65d7c57099fea0347ba3d1dd/> some changes I missed in the initial private removal

### Test

 - <csr-id-b818c3fd10a4e3304b2c5f84dac843397873cba6/> cmd sent to all elders

### Chore

 - <csr-id-46246f155ab65f3fcd61381345f1a7f747dfe957/> sn_interface-0.6.2/sn_client-0.66.2/sn_node-0.62.4/sn_api-0.64.2/sn_cli-0.57.3

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 11 commits contributed to the release over the course of 2 calendar days.
 - 8 days passed between releases.
 - 7 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.2/sn_client-0.66.2/sn_node-0.62.4/sn_api-0.64.2/sn_cli-0.57.3 ([`46246f1`](https://github.com/maidsafe/safe_network/commit/46246f155ab65f3fcd61381345f1a7f747dfe957))
    - Merge #1216 ([`9877101`](https://github.com/maidsafe/safe_network/commit/9877101c74dcf75d78520a804cb6f2b7aaddaffb))
    - Merge branch 'main' into simplify_safeurl ([`a0175ab`](https://github.com/maidsafe/safe_network/commit/a0175abfa15e558e54fbb25dc3baf49343f040ac))
    - Merge branch 'main' into drusu/remove-private-data ([`0cd2007`](https://github.com/maidsafe/safe_network/commit/0cd2007e442086d6eb2a39ad1f452e590fad46a9))
    - Merge #1224 ([`2fe452b`](https://github.com/maidsafe/safe_network/commit/2fe452b07d2db0cc622021b76d05605b5d4841c3))
    - replace at_least_one_elders with supermajority for sending cmd ([`616d8cb`](https://github.com/maidsafe/safe_network/commit/616d8cb12bfc257f9b3609239790065ebced8fe3))
    - some changes I missed in the initial private removal ([`60f5a68`](https://github.com/maidsafe/safe_network/commit/60f5a68a1df6114b65d7c57099fea0347ba3d1dd))
    - remove private registers ([`1b1cb77`](https://github.com/maidsafe/safe_network/commit/1b1cb77df6c2805ecfa741bb824b359214558929))
    - remove private data addresses ([`f1829f9`](https://github.com/maidsafe/safe_network/commit/f1829f99ef1415a83731f855757fbce9970fa4f0))
    - remove private safe key addresses ([`8be2f2c`](https://github.com/maidsafe/safe_network/commit/8be2f2c9efac1623ea95ff1641c6b9bc22fad455))
    - cmd sent to all elders ([`b818c3f`](https://github.com/maidsafe/safe_network/commit/b818c3fd10a4e3304b2c5f84dac843397873cba6))
</details>

## v0.6.1 (2022-06-07)

<csr-id-24299786ba730e467c10946c8c152936b96148f8/>
<csr-id-489904e325cfb8efca4289b05125904ad4029f3b/>

### Chore

 - <csr-id-24299786ba730e467c10946c8c152936b96148f8/> address some review comments

### Chore

 - <csr-id-489904e325cfb8efca4289b05125904ad4029f3b/> sn_interface-0.6.1/sn_client-0.66.1/sn_node-0.62.1/sn_api-0.64.1

### New Features

 - <csr-id-dbda86be03f912079776be514828ff5fd034830c/> first version of Spentbook messaging, storage, and client API
   - Storage is implemented using Register as the underlying data type. To be changed when
   actual SpentBook native data type is put in place.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.1/sn_client-0.66.1/sn_node-0.62.1/sn_api-0.64.1 ([`489904e`](https://github.com/maidsafe/safe_network/commit/489904e325cfb8efca4289b05125904ad4029f3b))
    - Merge #1214 ([`992c495`](https://github.com/maidsafe/safe_network/commit/992c4951670afc769feea7e6cd38db021aed88a7))
    - address some review comments ([`2429978`](https://github.com/maidsafe/safe_network/commit/24299786ba730e467c10946c8c152936b96148f8))
    - first version of Spentbook messaging, storage, and client API ([`dbda86b`](https://github.com/maidsafe/safe_network/commit/dbda86be03f912079776be514828ff5fd034830c))
    - Merge #1217 ([`2f26043`](https://github.com/maidsafe/safe_network/commit/2f2604325d533357bad7d917315cf4cba0b2d3c0))
</details>

## v0.6.0 (2022-06-05)

<csr-id-1bf7dfb3ce8b14cbed7a4a8ed98c8310653a2da9/>

### Chore

 - <csr-id-1bf7dfb3ce8b14cbed7a4a8ed98c8310653a2da9/> sn_interface-0.6.0/sn_dysfunction-0.5.0/sn_client-0.66.0/sn_node-0.62.0/sn_api-0.64.0/sn_cli-0.57.0

### New Features

 - <csr-id-95de2ffe6f57ae0e6cebf123da3e9b6c3ad84aaf/> handover sap elder checks with membership knowledge
 - <csr-id-e3169f385c795ada14fde85a88aa04399934b9d7/> add bls type to keypair enum
   Extends the `sn_interface::types::keys::Keypair` enum to support the BLS keypair type. We need this
   because we want to change the CLI to use BLS rather than Ed25519 keys, so we can support signing
   output DBCs with the same keypair we use to sign commands sent from the CLI.
   
   I modified the tests that check each keypair type can be serialized/deserialized. Previously there
   was one test case looping over each type of keypair, but I think it's better style to define each
   test case explicitly: you are calling out what scenarios you want to support and it makes the cases
   easier to understand at a glance, even if there is a small bit of repetition between them.

### New Features (BREAKING)

 - <csr-id-f03fb7e35319dbb9e4745e3cb36c7913c4f220ac/> cli will now use bls keys

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release over the course of 1 calendar day.
 - 8 days passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.6.0/sn_dysfunction-0.5.0/sn_client-0.66.0/sn_node-0.62.0/sn_api-0.64.0/sn_cli-0.57.0 ([`1bf7dfb`](https://github.com/maidsafe/safe_network/commit/1bf7dfb3ce8b14cbed7a4a8ed98c8310653a2da9))
    - handover sap elder checks with membership knowledge ([`95de2ff`](https://github.com/maidsafe/safe_network/commit/95de2ffe6f57ae0e6cebf123da3e9b6c3ad84aaf))
    - cli will now use bls keys ([`f03fb7e`](https://github.com/maidsafe/safe_network/commit/f03fb7e35319dbb9e4745e3cb36c7913c4f220ac))
    - add bls type to keypair enum ([`e3169f3`](https://github.com/maidsafe/safe_network/commit/e3169f385c795ada14fde85a88aa04399934b9d7))
</details>

## v0.5.0 (2022-05-27)

<csr-id-e5fcd032e1dd904e05bc23e119af1d06e3b85a06/>

### Chore

 - <csr-id-e5fcd032e1dd904e05bc23e119af1d06e3b85a06/> sn_interface-0.5.0/sn_dysfunction-0.4.0/sn_client-0.65.0/sn_node-0.61.0/sn_api-0.63.0/sn_cli-0.56.0

### New Features

 - <csr-id-0c449a731b22eb25e616d83182899e12aba95d7f/> handover AE, empty consensus handling, generations

### New Features (BREAKING)

 - <csr-id-294549ebc998d11a2f3621e2a9fd20a0dd9bcce5/> remove sus node flows, replicate data per data

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 7 commits contributed to the release over the course of 2 calendar days.
 - 2 days passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.5.0/sn_dysfunction-0.4.0/sn_client-0.65.0/sn_node-0.61.0/sn_api-0.63.0/sn_cli-0.56.0 ([`e5fcd03`](https://github.com/maidsafe/safe_network/commit/e5fcd032e1dd904e05bc23e119af1d06e3b85a06))
    - Merge #1202 ([`e42a2e3`](https://github.com/maidsafe/safe_network/commit/e42a2e3c212597e68238451a5bb4a8725c4761be))
    - handover AE, empty consensus handling, generations ([`0c449a7`](https://github.com/maidsafe/safe_network/commit/0c449a731b22eb25e616d83182899e12aba95d7f))
    - Merge #1208 ([`6c9b851`](https://github.com/maidsafe/safe_network/commit/6c9b851dd5bab8b2f5d9b3ef1db72d198706ac9d))
    - remove sus node flows, replicate data per data ([`294549e`](https://github.com/maidsafe/safe_network/commit/294549ebc998d11a2f3621e2a9fd20a0dd9bcce5))
    - Merge #1203 ([`cd32ca6`](https://github.com/maidsafe/safe_network/commit/cd32ca6535b17aedacfb4051e97e4b3540bb8a71))
    - Merge branch 'main' into bump-consensus-2.0.0 ([`a1c592a`](https://github.com/maidsafe/safe_network/commit/a1c592a71247660e7372e019e5f9a6ea23299e0f))
</details>

## v0.4.0 (2022-05-25)

<csr-id-ef56cf9cf8de45a9f13c2510c63de245b12aeae8/>
<csr-id-392e522c69803fddbeb3cd9e1cbae8060188578f/>
<csr-id-cf21d66b9b726123e0a4320cd68481b67f7af03d/>
<csr-id-8e2731d8b7923a9050451b31ef3a92f892d2d6d3/>
<csr-id-f2742d92b3c3b56ed80732aa1d6943885fcd4317/>
<csr-id-cb733fd4b1ed642da73f1e9db4fc3d8a1ec49a2b/>
<csr-id-ef798150deb88efac1dcfe9a3cd0f2cebe1e4682/>

### Chore

 - <csr-id-ef56cf9cf8de45a9f13c2510c63de245b12aeae8/> sn_interface-0.4.0/sn_dysfunction-0.3.0/sn_client-0.64.0/sn_node-0.60.0/sn_api-0.62.0/sn_cli-0.55.0
 - <csr-id-392e522c69803fddbeb3cd9e1cbae8060188578f/> bump consensus 1.16.0 -> 2.0.0
 - <csr-id-cf21d66b9b726123e0a4320cd68481b67f7af03d/> sn_dysfunction-0.2.0/sn_client-0.63.0/sn_node-0.59.0/sn_api-0.61.0/sn_cli-0.54.0

### New Features

 - <csr-id-941703e23a53d8d894d5a9a7a253ad1735e900e0/> error triggering on churn join miss
 - <csr-id-fe073bc0674c2099b7cd3f30ac744ea6172e24c2/> section probing for all nodes every 120s

### Refactor

 - <csr-id-8e2731d8b7923a9050451b31ef3a92f892d2d6d3/> de-dupe init_test_logger
 - <csr-id-f2742d92b3c3b56ed80732aa1d6943885fcd4317/> cargo test works without feature flag
 - <csr-id-cb733fd4b1ed642da73f1e9db4fc3d8a1ec49a2b/> move NodeState validations to NodeState struct

### Chore (BREAKING)

 - <csr-id-ef798150deb88efac1dcfe9a3cd0f2cebe1e4682/> add Display for OperationId


### Commit Statistics

<csr-read-only-do-not-edit/>

 - 12 commits contributed to the release over the course of 5 calendar days.
 - 7 days passed between releases.
 - 9 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.4.0/sn_dysfunction-0.3.0/sn_client-0.64.0/sn_node-0.60.0/sn_api-0.62.0/sn_cli-0.55.0 ([`ef56cf9`](https://github.com/maidsafe/safe_network/commit/ef56cf9cf8de45a9f13c2510c63de245b12aeae8))
    - bump consensus 1.16.0 -> 2.0.0 ([`392e522`](https://github.com/maidsafe/safe_network/commit/392e522c69803fddbeb3cd9e1cbae8060188578f))
    - Merge #1195 ([`c6e6e32`](https://github.com/maidsafe/safe_network/commit/c6e6e324164028c6c15a78643783a9f86679f39e))
    - sn_dysfunction-0.2.0/sn_client-0.63.0/sn_node-0.59.0/sn_api-0.61.0/sn_cli-0.54.0 ([`cf21d66`](https://github.com/maidsafe/safe_network/commit/cf21d66b9b726123e0a4320cd68481b67f7af03d))
    - Merge #1196 ([`1d1689f`](https://github.com/maidsafe/safe_network/commit/1d1689f91d0bc450257d1a279561ea7b0c1b71a7))
    - add Display for OperationId ([`ef79815`](https://github.com/maidsafe/safe_network/commit/ef798150deb88efac1dcfe9a3cd0f2cebe1e4682))
    - de-dupe init_test_logger ([`8e2731d`](https://github.com/maidsafe/safe_network/commit/8e2731d8b7923a9050451b31ef3a92f892d2d6d3))
    - cargo test works without feature flag ([`f2742d9`](https://github.com/maidsafe/safe_network/commit/f2742d92b3c3b56ed80732aa1d6943885fcd4317))
    - Merge #1178 ([`d9ba264`](https://github.com/maidsafe/safe_network/commit/d9ba264a6b2b657dce60b5ded78f1cecd840dbb1))
    - move NodeState validations to NodeState struct ([`cb733fd`](https://github.com/maidsafe/safe_network/commit/cb733fd4b1ed642da73f1e9db4fc3d8a1ec49a2b))
    - error triggering on churn join miss ([`941703e`](https://github.com/maidsafe/safe_network/commit/941703e23a53d8d894d5a9a7a253ad1735e900e0))
    - section probing for all nodes every 120s ([`fe073bc`](https://github.com/maidsafe/safe_network/commit/fe073bc0674c2099b7cd3f30ac744ea6172e24c2))
</details>

## v0.2.4 (2022-05-18)

<csr-id-07504faeda6cbfd0b27abea25facde992398ecf9/>
<csr-id-e25fb53a299dd5daa755799c36a316e4b011f4d7/>
<csr-id-9b06304f46e1a1bda90a0fc6ff82edc928c2529d/>

### Chore

 - <csr-id-07504faeda6cbfd0b27abea25facde992398ecf9/> upgrade blsttc to v5.2.0 and rand to v0.8
 - <csr-id-e25fb53a299dd5daa755799c36a316e4b011f4d7/> rename DkgId generation to section chain len

### Chore

 - <csr-id-9b06304f46e1a1bda90a0fc6ff82edc928c2529d/> sn_interface-0.2.4/sn_client-0.62.3/sn_api-0.60.2/sn_cli-0.53.1

### New Features

 - <csr-id-2b18ba8a1b0e8342af176bb78dba08f3e7b65d26/> add membership generation to DKG and SectionInfo agreement
   This prevents bogus DKG failure when two generations (of same prefix)
   may crossover under heavy churn

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release over the course of 1 calendar day.
 - 5 days passed between releases.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.2.4/sn_client-0.62.3/sn_api-0.60.2/sn_cli-0.53.1 ([`9b06304`](https://github.com/maidsafe/safe_network/commit/9b06304f46e1a1bda90a0fc6ff82edc928c2529d))
    - Merge #1190 ([`8833cb8`](https://github.com/maidsafe/safe_network/commit/8833cb8a4ae13f04ea86c67e92fce4d82a107f5a))
    - upgrade blsttc to v5.2.0 and rand to v0.8 ([`07504fa`](https://github.com/maidsafe/safe_network/commit/07504faeda6cbfd0b27abea25facde992398ecf9))
    - Merge #1150 ([`afda86c`](https://github.com/maidsafe/safe_network/commit/afda86c5bd759f6a19cb921c356fad51f76daecd))
    - rename DkgId generation to section chain len ([`e25fb53`](https://github.com/maidsafe/safe_network/commit/e25fb53a299dd5daa755799c36a316e4b011f4d7))
    - add membership generation to DKG and SectionInfo agreement ([`2b18ba8`](https://github.com/maidsafe/safe_network/commit/2b18ba8a1b0e8342af176bb78dba08f3e7b65d26))
</details>

## v0.2.3 (2022-05-12)

<csr-id-00dc9c0ba9afb6de038dda9e20a10e6727a0b0e6/>
<csr-id-a49a007ef8fde53a346403824f09eb0fd25e1109/>

### Chore

 - <csr-id-00dc9c0ba9afb6de038dda9e20a10e6727a0b0e6/> reduce handover logging

### Chore

 - <csr-id-a49a007ef8fde53a346403824f09eb0fd25e1109/> sn_interface-0.2.3/sn_node-0.58.18

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 4 commits contributed to the release over the course of 1 calendar day.
 - 2 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.2.3/sn_node-0.58.18 ([`a49a007`](https://github.com/maidsafe/safe_network/commit/a49a007ef8fde53a346403824f09eb0fd25e1109))
    - Merge #1127 ([`a03107e`](https://github.com/maidsafe/safe_network/commit/a03107ea7ea8a393c818a193eb2489e92cbbda20))
    - reduce handover logging ([`00dc9c0`](https://github.com/maidsafe/safe_network/commit/00dc9c0ba9afb6de038dda9e20a10e6727a0b0e6))
    - Merge branch 'main' into sap_sig_checks ([`f8ec2e5`](https://github.com/maidsafe/safe_network/commit/f8ec2e54943eaa18b50bd9d7562d41f57d5d3248))
</details>

## v0.2.2 (2022-05-10)

<csr-id-61ba367c308a846cb3f1ae065b1fbbdfb85838e4/>
<csr-id-ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9/>

### Chore

 - <csr-id-61ba367c308a846cb3f1ae065b1fbbdfb85838e4/> sn_interface-0.2.2/sn_client-0.62.2/sn_node-0.58.16/sn_api-0.60.1

### Chore

 - <csr-id-ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9/> add ProposalAgreed log marker

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release over the course of 2 calendar days.
 - 3 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.2.2/sn_client-0.62.2/sn_node-0.58.16/sn_api-0.60.1 ([`61ba367`](https://github.com/maidsafe/safe_network/commit/61ba367c308a846cb3f1ae065b1fbbdfb85838e4))
    - Merge branch 'main' into nightly-improvements ([`ee3bbe1`](https://github.com/maidsafe/safe_network/commit/ee3bbe188cea756384dc38d490fe58c59c050292))
    - Merge #1169 ([`e5d0c17`](https://github.com/maidsafe/safe_network/commit/e5d0c17c335a3a25ee0bb4c81906fa176abeb7f5))
    - add ProposalAgreed log marker ([`ae9aeeb`](https://github.com/maidsafe/safe_network/commit/ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9))
    - Merge branch 'main' into main ([`d3f07bb`](https://github.com/maidsafe/safe_network/commit/d3f07bbe5192174082e24869ba86125b6a7b1b20))
    - Merge branch 'main' into retry-count-input ([`925a8a4`](https://github.com/maidsafe/safe_network/commit/925a8a4aaade025433c29028229947de28fcb214))
</details>

## v0.2.1 (2022-05-06)

<csr-id-155d62257546868513627709742215c0c8f9574f/>
<csr-id-e8f4fbca2acb81b3ddc1b275140e5f7b1b56f9a9/>
<csr-id-7766e7d20b392cf5b8563d1dbc9560254b44e756/>
<csr-id-737d906a61f772593ac7df755d995d66059e8b5e/>
<csr-id-ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9/>

### Chore

 - <csr-id-155d62257546868513627709742215c0c8f9574f/> check and log for shrinking SAP on verify_with_chain
 - <csr-id-e8f4fbca2acb81b3ddc1b275140e5f7b1b56f9a9/> make client targets relative to sap size
   The proivided sap could be different from expected, but
   we should be able to trust if if it's valid... As such
   we base target counts off of the provided SAP
 - <csr-id-7766e7d20b392cf5b8563d1dbc9560254b44e756/> rename MsgKind -> AuthKind
   This feels more correct given that the kind is actually about the authority that
   the message carries.

### Chore

 - <csr-id-737d906a61f772593ac7df755d995d66059e8b5e/> sn_interface-0.2.1/sn_client-0.62.1/sn_node-0.58.15/sn_api-0.60.0/sn_cli-0.53.0
 - <csr-id-ae9aeeb94f55f29849c8c5fe1b05419b96fac6e9/> add ProposalAgreed log marker

### New Features

 - <csr-id-0d5cdf940afc390de22d94e91621e76d45a9eaad/> handover integration squashed
 - <csr-id-696414ac858795628872a594268517e99a671b00/> add separate feature flags for register/chunk messages
 - <csr-id-c08f05537b70f2d6e0759a39b3f917c0e305e734/> add service-msg feature flag to messaging
   This allows us to more easily separate out what kind of messaging
   interface we want ndoes to be able to accept.
   
   Eg. Removing all service messages means we can focus on only the
   infrastructure layer..

### Bug Fixes

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

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 15 commits contributed to the release over the course of 10 calendar days.
 - 13 days passed between releases.
 - 10 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.2.1/sn_client-0.62.1/sn_node-0.58.15/sn_api-0.60.0/sn_cli-0.53.0 ([`737d906`](https://github.com/maidsafe/safe_network/commit/737d906a61f772593ac7df755d995d66059e8b5e))
    - Merge branch 'main' into retry-count-input ([`f514f47`](https://github.com/maidsafe/safe_network/commit/f514f471275a54edb26b1b520f76693174796dee))
    - Merge #1162 ([`a4c5ccb`](https://github.com/maidsafe/safe_network/commit/a4c5ccb8bb7fbbf8ab4052d3b1051f8cac100d53))
    - early return when AE required from a vote batch ([`dd353b9`](https://github.com/maidsafe/safe_network/commit/dd353b969ace383c3e89c94f7f242b84b6aee89f))
    - batch MembershipVotes in order to ensure that order is preserved. ([`829eb33`](https://github.com/maidsafe/safe_network/commit/829eb33184c6012faa2020333e72a7c811fdb660))
    - Merge #1160 ([`d46e85b`](https://github.com/maidsafe/safe_network/commit/d46e85bf508be983017b90e6ce18f588039b16ac))
    - client knowledge could not update ([`9f4c3a5`](https://github.com/maidsafe/safe_network/commit/9f4c3a523212c41079afcde8052a0891f3895f3b))
    - check and log for shrinking SAP on verify_with_chain ([`155d622`](https://github.com/maidsafe/safe_network/commit/155d62257546868513627709742215c0c8f9574f))
    - make client targets relative to sap size ([`e8f4fbc`](https://github.com/maidsafe/safe_network/commit/e8f4fbca2acb81b3ddc1b275140e5f7b1b56f9a9))
    - handover integration squashed ([`0d5cdf9`](https://github.com/maidsafe/safe_network/commit/0d5cdf940afc390de22d94e91621e76d45a9eaad))
    - Merge #1138 ([`ee439c1`](https://github.com/maidsafe/safe_network/commit/ee439c13b889a342247bcc5ab9ff62ba2f67a591))
    - add separate feature flags for register/chunk messages ([`696414a`](https://github.com/maidsafe/safe_network/commit/696414ac858795628872a594268517e99a671b00))
    - add service-msg feature flag to messaging ([`c08f055`](https://github.com/maidsafe/safe_network/commit/c08f05537b70f2d6e0759a39b3f917c0e305e734))
    - Merge #1139 ([`22abbc7`](https://github.com/maidsafe/safe_network/commit/22abbc73f909131a0208ddc6e9471d073061134a))
    - rename MsgKind -> AuthKind ([`7766e7d`](https://github.com/maidsafe/safe_network/commit/7766e7d20b392cf5b8563d1dbc9560254b44e756))
</details>

## v0.2.0 (2022-04-23)

<csr-id-318ee1d22970b5f06e93a99b6e8fff6da638c589/>
<csr-id-88421d9cb7872b6397283a0035130bc14de6d4ff/>
<csr-id-9ea06ffe9339d3927897f010314b1be1bf7026bf/>
<csr-id-f37582288da65f27f53eb27453a4693166821064/>
<csr-id-7b8ce1c9d980015768a300ac99d07f69cc1f5ae3/>
<csr-id-8494a01d9be3dddc0d0f4c2c37cdc4d6c3e54521/>
<csr-id-ad7aa2d27c1eeeb11734f5cc2712383a36343d54/>
<csr-id-2f4e7e6305ba387f2e28945aee71df650ac1d3eb/>

### Chore

 - <csr-id-318ee1d22970b5f06e93a99b6e8fff6da638c589/> tidy references in cargo manifests
   All references are organised alphabetically, and random usage of long-form references are removed in
   favour of the short-form version, unless the long-form style is justified, e.g., when lots of
   features are being used.
 - <csr-id-88421d9cb7872b6397283a0035130bc14de6d4ff/> pull sn_client out of the node codebase
 - <csr-id-9ea06ffe9339d3927897f010314b1be1bf7026bf/> sn_dysfunction-0.1.1/safe_network-0.58.13/sn_api-0.58.2/sn_cli-0.51.3
 - <csr-id-f37582288da65f27f53eb27453a4693166821064/> add changelog/readme for sn_interface publishing
 - <csr-id-7b8ce1c9d980015768a300ac99d07f69cc1f5ae3/> remove unused sn_interface deps
 - <csr-id-8494a01d9be3dddc0d0f4c2c37cdc4d6c3e54521/> split put messaging and types into top level crate

### Chore

 - <csr-id-2f4e7e6305ba387f2e28945aee71df650ac1d3eb/> sn_interface-0.2.0/sn_dysfunction-0.1.2/sn_api-0.59.0/sn_cli-0.52.0

### Other

 - <csr-id-ad7aa2d27c1eeeb11734f5cc2712383a36343d54/> create separate sn_interface unit test step

### New Features (BREAKING)

 - <csr-id-c1ee1dbb50fb8128776b4ba0a821e23056801201/> integrate membership into safe-network

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 9 commits contributed to the release over the course of 8 calendar days.
 - 9 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - sn_interface-0.2.0/sn_dysfunction-0.1.2/sn_api-0.59.0/sn_cli-0.52.0 ([`2f4e7e6`](https://github.com/maidsafe/safe_network/commit/2f4e7e6305ba387f2e28945aee71df650ac1d3eb))
    - tidy references in cargo manifests ([`318ee1d`](https://github.com/maidsafe/safe_network/commit/318ee1d22970b5f06e93a99b6e8fff6da638c589))
    - pull sn_client out of the node codebase ([`88421d9`](https://github.com/maidsafe/safe_network/commit/88421d9cb7872b6397283a0035130bc14de6d4ff))
    - integrate membership into safe-network ([`c1ee1db`](https://github.com/maidsafe/safe_network/commit/c1ee1dbb50fb8128776b4ba0a821e23056801201))
    - sn_dysfunction-0.1.1/safe_network-0.58.13/sn_api-0.58.2/sn_cli-0.51.3 ([`9ea06ff`](https://github.com/maidsafe/safe_network/commit/9ea06ffe9339d3927897f010314b1be1bf7026bf))
    - add changelog/readme for sn_interface publishing ([`f375822`](https://github.com/maidsafe/safe_network/commit/f37582288da65f27f53eb27453a4693166821064))
    - remove unused sn_interface deps ([`7b8ce1c`](https://github.com/maidsafe/safe_network/commit/7b8ce1c9d980015768a300ac99d07f69cc1f5ae3))
    - create separate sn_interface unit test step ([`ad7aa2d`](https://github.com/maidsafe/safe_network/commit/ad7aa2d27c1eeeb11734f5cc2712383a36343d54))
    - split put messaging and types into top level crate ([`8494a01`](https://github.com/maidsafe/safe_network/commit/8494a01d9be3dddc0d0f4c2c37cdc4d6c3e54521))
</details>

## v0.1.1 (2022-04-14)

<csr-id-f37582288da65f27f53eb27453a4693166821064/>
<csr-id-7b8ce1c9d980015768a300ac99d07f69cc1f5ae3/>
<csr-id-8494a01d9be3dddc0d0f4c2c37cdc4d6c3e54521/>
<csr-id-ad7aa2d27c1eeeb11734f5cc2712383a36343d54/>

### Chore

 - <csr-id-f37582288da65f27f53eb27453a4693166821064/> add changelog/readme for sn_interface publishing
 - <csr-id-7b8ce1c9d980015768a300ac99d07f69cc1f5ae3/> remove unused sn_interface deps
 - <csr-id-8494a01d9be3dddc0d0f4c2c37cdc4d6c3e54521/> split put messaging and types into top level crate

### Other

 - <csr-id-ad7aa2d27c1eeeb11734f5cc2712383a36343d54/> create separate sn_interface unit test step

## v0.1.0 (2022-04-14)

This first version is being edited manually to trigger a release and publish of the first crate.

Inserting another manual change for testing purposes.

