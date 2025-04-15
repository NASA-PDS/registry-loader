# Changelog

## [5.0.4](https://github.com/NASA-PDS/registry-mgr/tree/5.0.4) (2025-04-15)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v5.0.5...5.0.4)

**Defects:**

- registry-manager does not process \_all\_ collections in a bundle [\#131](https://github.com/NASA-PDS/registry-mgr/issues/131) [[s.medium](https://github.com/NASA-PDS/registry-mgr/labels/s.medium)] [[s.high](https://github.com/NASA-PDS/registry-mgr/labels/s.high)]

## [v5.0.5](https://github.com/NASA-PDS/registry-mgr/tree/v5.0.5) (2025-04-07)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v5.0.4...v5.0.5)

**Improvements:**

- Quiet SLF4J log message from end user [\#117](https://github.com/NASA-PDS/registry-mgr/issues/117)

**Defects:**

- Getting null message in log with option packageId [\#124](https://github.com/NASA-PDS/registry-mgr/issues/124) [[s.low](https://github.com/NASA-PDS/registry-mgr/labels/s.low)]
- Unable to update archive status of a lidvid in the registry [\#107](https://github.com/NASA-PDS/registry-mgr/issues/107) [[s.critical](https://github.com/NASA-PDS/registry-mgr/labels/s.critical)]

**Other closed issues:**

- Cleanup registry manager command-line options that do not work [\#128](https://github.com/NASA-PDS/registry-mgr/issues/128)
- Update description for `-es` flag to make more sense to a novice user [\#125](https://github.com/NASA-PDS/registry-mgr/issues/125)
- Improve error handling for `document missing` response from OpenSearch [\#133](https://github.com/NASA-PDS/registry-mgr/issues/133)

## [v5.0.4](https://github.com/NASA-PDS/registry-mgr/tree/v5.0.4) (2024-12-19)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/release/5.0.4...v5.0.4)

## [release/5.0.4](https://github.com/NASA-PDS/registry-mgr/tree/release/5.0.4) (2024-12-19)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v5.0.3...release/5.0.4)

**Requirements:**

- As a user, I want to update a bundle and associated collections' and products' archive status given a bundle lidvid [\#112](https://github.com/NASA-PDS/registry-mgr/issues/112)
- As a user, I want to change the archive status for a collection and it's associated products given a collection lidvid [\#113](https://github.com/NASA-PDS/registry-mgr/issues/113)

**Defects:**

- `[ERROR] Need to fill this out when have a return value` when trying to execute `list-dd` command [\#122](https://github.com/NASA-PDS/registry-mgr/issues/122) [[s.medium](https://github.com/NASA-PDS/registry-mgr/labels/s.medium)]
- `Missing required property 'FieldValue.<variant value>'` error when running with `list-dd` command [\#121](https://github.com/NASA-PDS/registry-mgr/issues/121) [[s.high](https://github.com/NASA-PDS/registry-mgr/labels/s.high)]
- set-archive-status does not update the same number of products each time with packageId argument [\#115](https://github.com/NASA-PDS/registry-mgr/issues/115) [[s.high](https://github.com/NASA-PDS/registry-mgr/labels/s.high)]
- registry-mgr return ERROR null [\#111](https://github.com/NASA-PDS/registry-mgr/issues/111) [[s.high](https://github.com/NASA-PDS/registry-mgr/labels/s.high)]
- archive status does not change for all bundle members [\#109](https://github.com/NASA-PDS/registry-mgr/issues/109) [[s.high](https://github.com/NASA-PDS/registry-mgr/labels/s.high)]

## [v5.0.3](https://github.com/NASA-PDS/registry-mgr/tree/v5.0.3) (2024-11-12)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/release/5.0.3...v5.0.3)

## [release/5.0.3](https://github.com/NASA-PDS/registry-mgr/tree/release/5.0.3) (2024-11-12)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v5.0.2...release/5.0.3)

## [v5.0.2](https://github.com/NASA-PDS/registry-mgr/tree/v5.0.2) (2024-10-16)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/release/5.0.2...v5.0.2)

## [release/5.0.2](https://github.com/NASA-PDS/registry-mgr/tree/release/5.0.2) (2024-10-16)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v5.0.1...release/5.0.2)

## [v5.0.1](https://github.com/NASA-PDS/registry-mgr/tree/v5.0.1) (2024-10-16)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/release/5.0.1...v5.0.1)

## [release/5.0.1](https://github.com/NASA-PDS/registry-mgr/tree/release/5.0.1) (2024-10-16)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v5.0.0...release/5.0.1)

**Defects:**

- Update inline help to match latest features [\#104](https://github.com/NASA-PDS/registry-mgr/issues/104) [[s.high](https://github.com/NASA-PDS/registry-mgr/labels/s.high)]

## [v5.0.0](https://github.com/NASA-PDS/registry-mgr/tree/v5.0.0) (2024-08-27)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/release/5.0.0...v5.0.0)

## [release/5.0.0](https://github.com/NASA-PDS/registry-mgr/tree/release/5.0.0) (2024-08-27)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.6.3...release/5.0.0)

**Requirements:**

- As a user, I want to set archive-status using packageId [\#69](https://github.com/NASA-PDS/registry-mgr/issues/69)

**Improvements:**

- As a node person, I want to know all possible values for archive status [\#71](https://github.com/NASA-PDS/registry-mgr/issues/71)

**Defects:**

- set-archive-status and delete-data subcommand do not work on OpenSearch serverless Registry [\#78](https://github.com/NASA-PDS/registry-mgr/issues/78) [[s.critical](https://github.com/NASA-PDS/registry-mgr/labels/s.critical)]

**Other closed issues:**

- Issue with `delete-data` with option `-packageId` when scroll is required [\#93](https://github.com/NASA-PDS/registry-mgr/issues/93)
- Update to utilize new multi-tenancy approach [\#66](https://github.com/NASA-PDS/registry-mgr/issues/66)

## [v4.6.3](https://github.com/NASA-PDS/registry-mgr/tree/v4.6.3) (2023-11-16)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/release/4.6.3...v4.6.3)

## [release/4.6.3](https://github.com/NASA-PDS/registry-mgr/tree/release/4.6.3) (2023-11-16)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.6.1...release/4.6.3)

## [v4.6.1](https://github.com/NASA-PDS/registry-mgr/tree/v4.6.1) (2023-10-02)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.5.5...v4.6.1)

## [v4.5.5](https://github.com/NASA-PDS/registry-mgr/tree/v4.5.5) (2023-03-31)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.5.4...v4.5.5)

## [v4.5.4](https://github.com/NASA-PDS/registry-mgr/tree/v4.5.4) (2022-12-12)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.5.3...v4.5.4)

**Defects:**

- ref\_lid\_collection error when ingesting data sets [\#57](https://github.com/NASA-PDS/registry-mgr/issues/57) [[s.medium](https://github.com/NASA-PDS/registry-mgr/labels/s.medium)]

## [v4.5.3](https://github.com/NASA-PDS/registry-mgr/tree/v4.5.3) (2022-11-09)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.5.2...v4.5.3)

## [v4.5.2](https://github.com/NASA-PDS/registry-mgr/tree/v4.5.2) (2022-10-26)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.5.0...v4.5.2)

## [v4.5.0](https://github.com/NASA-PDS/registry-mgr/tree/v4.5.0) (2022-09-21)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.4.0...v4.5.0)

**Requirements:**

- As a registry user, I want to search on ref\_lid\_\* from the registry metadata [\#50](https://github.com/NASA-PDS/registry-mgr/issues/50)

**Defects:**

- bug with pagination limitations per OpenSearch config [\#53](https://github.com/NASA-PDS/registry-mgr/issues/53) [[s.high](https://github.com/NASA-PDS/registry-mgr/labels/s.high)]

## [v4.4.0](https://github.com/NASA-PDS/registry-mgr/tree/v4.4.0) (2022-04-13)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.3.1...v4.4.0)

**Defects:**

- Improve error message for corrupted registry\_docs.json [\#43](https://github.com/NASA-PDS/registry-mgr/issues/43) [[s.medium](https://github.com/NASA-PDS/registry-mgr/labels/s.medium)]

## [v4.3.1](https://github.com/NASA-PDS/registry-mgr/tree/v4.3.1) (2022-01-11)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.3.0...v4.3.1)

## [v4.3.0](https://github.com/NASA-PDS/registry-mgr/tree/v4.3.0) (2021-12-10)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.2.0...v4.3.0)

**Improvements:**

- Update registy-mgr to use schema from schemaLocation in file [\#31](https://github.com/NASA-PDS/registry-mgr/issues/31)

## [v4.2.0](https://github.com/NASA-PDS/registry-mgr/tree/v4.2.0) (2021-09-30)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.1.0...v4.2.0)

**Improvements:**

- Update registry-mgr-elastic to support new LDD JSON version information [\#27](https://github.com/NASA-PDS/registry-mgr/issues/27)

**Defects:**

- registry-mgr cannot handle updated JSON files output from LDDTool [\#33](https://github.com/NASA-PDS/registry-mgr/issues/33) [[s.medium](https://github.com/NASA-PDS/registry-mgr/labels/s.medium)]
- Load-data command doesn't report Elasticsearch errors [\#25](https://github.com/NASA-PDS/registry-mgr/issues/25)

## [v4.1.0](https://github.com/NASA-PDS/registry-mgr/tree/v4.1.0) (2021-04-17)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/v4.0.2...v4.1.0)

## [v4.0.2](https://github.com/NASA-PDS/registry-mgr/tree/v4.0.2) (2020-12-02)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/4.0.1...v4.0.2)

**Defects:**

- Missing Science\_Facets fields definitions in registry schema [\#14](https://github.com/NASA-PDS/registry-mgr/issues/14)
- registry-mgr  delete-data options are not correct in -help [\#12](https://github.com/NASA-PDS/registry-mgr/issues/12)

## [4.0.1](https://github.com/NASA-PDS/registry-mgr/tree/4.0.1) (2020-10-30)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/4.0.0...4.0.1)

## [4.0.0](https://github.com/NASA-PDS/registry-mgr/tree/4.0.0) (2020-10-26)

[Full Changelog](https://github.com/NASA-PDS/registry-mgr/compare/7fd5a2640c71921f086fa467648c78d108c4bb24...4.0.0)

**Improvements:**

- Implement Authentication [\#3](https://github.com/NASA-PDS/registry-mgr/issues/3)

**Defects:**

- update-schema command doesn't work [\#7](https://github.com/NASA-PDS/registry-mgr/issues/7)

**Other closed issues:**

- Update the field-data type lookup table [\#6](https://github.com/NASA-PDS/registry-mgr/issues/6)
- Create a field-data type lookup table [\#5](https://github.com/NASA-PDS/registry-mgr/issues/5)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
