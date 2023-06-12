## Unreleased (2023-06-12)

### New feature:

- **patch**: patch each property one-by-one([`a66cd53`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/a66cd53a3c1b9e0b0bc0362bb78afb33fd90ae04)) (by ixsluo)
- **patch**: add parallel patch([`d798190`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/d7981909a82e27916fe423dfc7d76632e2a2bc7b)) (by ixsluo)
- **patch**: add patch properties to records([`31ef0f4`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/31ef0f41537b587dbc7664340b728b375370bbd8)) (by ixsluo)
- **readout**: add readout to cdvae DataFrame([`35e63ec`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/35e63ec2deae5cc5fe095c94ff447739c96cc7ef)) (by ixsluo)
- **query**: merge query structure and trajectory together([`bfa6eda`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/bfa6eda2d1054b0035b583e867fe5fa5fa37b193)) (by ixsluo)
- **unique**: add UniqueFinder to add unique structures([`36e23ec`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/36e23ec065a4f7a9d1ba9081396790baca6dc23c)) (by ixsluo)
- **cleanup**: add clean deprecated records in uniqcol([`a473959`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/a473959b456e5faedb17dd3b617c62d4ba1e95bc)) (by ixsluo)
- **query**: add query all projection in query structures([`38a2f88`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/38a2f88ac7e6efc46a2336dbae90c6b79dac894b)) (by ixsluo)
- **itertools**: add standart pairwise, python at least 3.9([`b9918f6`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/b9918f6b7c075397c0d59fe028cd6e08f6d7a06f)) (by ixsluo)
- **cleanup**: deprecate large enthalpy and number of task([`d653bd2`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/d653bd2d8b7d32edd0570ac3a34cdb636864fa82)) (by ixsluo)
- **query**: change to class staticmethod([`590f0da`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/590f0da5efc0da412630efcb50e2460414ad8c22)) (by ixsluo)
- **query**: add query newer 'last_updated_utc'([`0b26f93`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/0b26f9317a1971a38cae8b1998d6b2b469958719)) (by ixsluo)
- **inputdat**: add vscenergy([`9a0ce23`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/9a0ce238c95cb7e4280b0ef52d812cc1aca6a315)) (by ixsluo)
- add extact ini opt and derectly insert to db([`2eb475b`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/2eb475bb5e68c32551afcc274c791b7dc42e3b13)) (by ixsluo)
- **prop**: add calculate properties of one frame([`81ebe00`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/81ebe00f41624a3f8ddc85e447a55890884176e7)) (by ixsluo)
- **query**: add query on calypso max index([`636c28f`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/636c28f97d9c48723f6faa0a98053a0e041e1cc8)) (by ixsluo)
- **login**: split login to db and get collection([`00a569c`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/00a569cc9e7dceb0887b7738175eb69c7be99a7f)) (by ixsluo)
- add extract ini and opt([`b3811b8`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/b3811b8946e75c2bc087922c61a62f0a89ee07dd)) (by ixsluo)
- **cleanup**: add error cleaning queryes([`8eebe90`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/8eebe90a9b387cb61450878c83ccfa2506d321c4)) (by ixsluo)
- **record**: add spacegroup to record([`a1f255a`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/a1f255a39f0eb6dffdbc47cf66731f88883e342f)) (by ixsluo)
- **unique**: add find unique structures([`12f3bc1`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/12f3bc126ea6f106b288bfa3cbf9496a07cb77e4)) (by ixsluo)
- **query**: properties are also returned for query structure and traj([`a7c8d46`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/a7c8d4611a620f5fb2419f10ee6c964b53654c34)) (by ixsluo)
- **utils**: move custom itertools to utils([`b53540d`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/b53540de085d9222a34e38ee36b73e10f70eeb25)) (by ixsluo)
- **query**: add query structure and group piplines([`00a15f6`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/00a15f6ee50f913dff0b74d70ac346a5bc02b240)) (by ixsluo)
- add rawrecord from ase Atoms classmethd(unfinished)([`ce3e8b6`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/ce3e8b6ab63e4d283a5045ddaed55660dd2cee59)) (by ixsluo)
- add maintain index([`92cfdf1`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/92cfdf18d268b2113cda596edad8e88614ae51d6)) (by ixsluo)
- add insert doc from data pkl([`fea1941`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/fea19413eb5bb06c7f5201f680655cac6d5e10e0)) (by ixsluo)
- add script pickle pandas to data([`8282420`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/828242030560f29c78ad4e5ea7b9369ea284ff79)) (by ixsluo)
- **calydb**: add numpy registory and timestamp([`d3a3a2a`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/d3a3a2a6bb4a8ec72561463eca5c606fb8c8aa23)) (by ixsluo)
- ensure material_id unique index([`8f753ea`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/8f753eaac077280268e593174ce5ed2a84e8f0b8)) (by ixsluo)
- add read_inputdat([`70a5730`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/70a57303be57e36df9c92161a0d210d8695703f2)) (by ixsluo)
- **calydb**: update default data dict([`f4d930f`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/f4d930faa2c03df06c469ce369a911467382405d)) (by ixsluo)
- **calydb**: add README of leagcy process for db([`5dd7e29`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/5dd7e29e073c4357d18ac76696c3fc1903db3a2f)) (by ixsluo)
- **calydb**: add binary serialization([`2b14cb4`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/2b14cb4e901765337cb6b9d8ea717c3d425f29b8)) (by ixsluo)
- **calydb**: add db connection and unittest on it([`3fd8c0f`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/3fd8c0f5cd4f893fd76b192a3eebb5210a5cb91b)) (by ixsluo)

### Bugs fixed:

- **property**: return -1 if CrystalNN failed in dim_larsen([`070b191`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/070b19121db2550e8571e899c187165e26d29997)) (by ixsluo)
- **query**: do not pop properties when query structure([`7e4a1b3`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/7e4a1b3a21af761e105afdf078673b0c4c91f3cc)) (by ixsluo)
- **query**: QueryStructure return one dict with key '_structure_'([`7f83185`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/7f8318588fb08158ed21799b191f35f81a72b286)) (by ixsluo)
- **query&cleanup**: clean solitary energy([`7d018f9`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/7d018f9c551b2ebe682fd3bad42cc5aba8b71697)) (by ixsluo)
- groupby_delta can deal with empty list([`cbf570e`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/cbf570ef5bd74d7c015d850d097f9a3d87976f7b)) (by ixsluo)
- **extract**: directly extract ini and opt from results dir([`2f2807c`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/2f2807c82e9d9743f2ab00bc357674fd3c483da1)) (by ixsluo)
- **inputdat**: block elements are pure int or float([`9048c9e`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/9048c9ea5e795878d4db6fc222352cc3527436db)) (by ixsluo)
- **record**: rename to trajectory.source_file([`e685234`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/e6852349d72ec828a68bcc414306e5c2ecf1a2e0)) (by ixsluo)
- **query**: fix pipe of unique records([`acc081d`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/acc081dba741739828c0daa60d33745b292679d2)) (by ixsluo)
- **query**: remove those with ''([`df0dd99`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/df0dd99c57367191370017d1079e81185ea1e9f5)) (by ixsluo)
- **query**: rename to pipe([`1aec818`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/1aec8189f9cad8d7c0aa4ba6c39bd3a79d12c5b3)) (by ixsluo)
- add prefix to trajectory.source_dir([`7b361f4`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/7b361f467dde3aef1c584624f0cb95be9e20d219)) (by ixsluo)
- insert unique record to uniqcol collection([`9bd8841`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/9bd884168d0f1549b61b27069da2b528c1129124)) (by ixsluo)
- **query**: rename to pipe([`59e8367`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/59e83677dcf9766d03cea72483d42277ffb267e1)) (by ixsluo)
- **query**: change to group task and group task&formula([`a6f3dcc`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/a6f3dcc12e46caddd968900749c645e393ef54b4)) (by ixsluo)
- rename env var name of mongodb([`a72dcbc`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/a72dcbcb24274f45e487290199b64c52895fcd37)) (by ixsluo)
- fix error name of trajectory.source_dir([`a5b0a15`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/a5b0a15040386969c173f252ab8bcd66b1e416c3)) (by ixsluo)
- add 'elemcount' and 'species' to records([`c07b05e`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/c07b05e8c400550eee2fbbdaf7b94e231dc0121d)) (by ixsluo)
- **insert**: make source.index same as pkl file if it < 1518541([`3accf73`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/3accf73335355fa3872d45193cae8bc460d93612)) (by ixsluo)
- add source_dir to document([`ada88ce`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/ada88ceaede4f2a688665b688223d803af3038a6)) (by ixsluo)
- login check index material_id unique or not([`6205c5e`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/6205c5e1b19c0e0bdcdaeaf7c0696d427cef978d)) (by ixsluo)
- add 'last_updated_utc' to all doc([`a21c7cd`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/a21c7cdd042a7ce0dd9688ff8aac02592ad08171)) (by ixsluo)
- fix insert data to database([`ba4e373`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/ba4e373c1da3cb51ec53bda6334d6b0c218f0b94)) (by ixsluo)
- fix record check and test on maxidx([`daa3875`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/daa3875c74ea910b749351d1b67f3a31071a4ae4)) (by ixsluo)
- **record**: fix record template([`ebccc64`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/ebccc648c2e4197eda36710e664ee5b7926b3f20)) (by ixsluo)
- add patch on read old-version input.dat([`bc658c3`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/bc658c38d9629449a12278f86df85095875c000c)) (by ixsluo)
- split login and record([`86b8c95`](https://github.com/ICCMS-CALYPSO/CALYPSO-kit/commit/86b8c95a409fb6f5e582d4c8e8cdd57e93f582fd)) (by ixsluo)