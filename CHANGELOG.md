# Changelog

## [«unknown»](https://github.com/NASA-PDS/registry-loader/tree/«unknown») (2026-07-07)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.3.0...«unknown»)

**Defects:**

- Detect and remove stale LDD sentinel records across all node registry indexes [\#89](https://github.com/NASA-PDS/registry-loader/issues/89) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]
- LDD fields silently not loaded when PDS4 LDD JSON uses older tooling-generated association format [\#88](https://github.com/NASA-PDS/registry-loader/issues/88) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]
- Incomplete LDD load permanently blocked by stale sentinel record in data dictionary index [\#87](https://github.com/NASA-PDS/registry-loader/issues/87) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]
- Harvest fails when PDS4 label contains empty class definitions [\#86](https://github.com/NASA-PDS/registry-loader/issues/86) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]
- AOSS propagation race: LDD re-downloaded and fields fail to resolve immediately after bulk load into -dd index [\#81](https://github.com/NASA-PDS/registry-loader/issues/81) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]
- LDD JSON parser fails to resolve field data types across IM versions due to format changes [\#80](https://github.com/NASA-PDS/registry-loader/issues/80) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]

## [v1.3.0](https://github.com/NASA-PDS/registry-loader/tree/v1.3.0) (2026-06-23)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.2.4...v1.3.0)

**Requirements:**

- As a node operator, I want to force-load products when namespace schema or attribute type is unresolvable, knowing affected fields will not be fully indexed [\#66](https://github.com/NASA-PDS/registry-loader/issues/66)
- As a node operator, I want harvest to fail product loading when a namespace schema or attribute data type cannot be found [\#65](https://github.com/NASA-PDS/registry-loader/issues/65)

**Improvements:**

- Fallback to pds.nasa.gov mirror when third-party LDD URL is unreachable [\#70](https://github.com/NASA-PDS/registry-loader/issues/70)
- Merge registry-common, registry-mgr, harvest into one uber repo [\#41](https://github.com/NASA-PDS/registry-loader/issues/41)

**Defects:**

- Harvest does not download LDD on Windows: temp file creation fails with 'The system cannot find the path specified' [\#77](https://github.com/NASA-PDS/registry-loader/issues/77) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]
- Force flag discards found field types when any attribute type is unresolvable [\#75](https://github.com/NASA-PDS/registry-loader/issues/75)
- ops: namespace fields missing from data type resolution, causing DataTypeNotFoundException [\#68](https://github.com/NASA-PDS/registry-loader/issues/68) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]
- Harvest does not detect uppercase file extensions for labels, nor report errors for those missing products [\#52](https://github.com/NASA-PDS/registry-loader/issues/52) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]

## [v1.2.4](https://github.com/NASA-PDS/registry-loader/tree/v1.2.4) (2026-01-21)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.2.3...v1.2.4)

## [v1.2.3](https://github.com/NASA-PDS/registry-loader/tree/v1.2.3) (2026-01-14)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.2.2...v1.2.3)

## [v1.2.2](https://github.com/NASA-PDS/registry-loader/tree/v1.2.2) (2026-01-14)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.2.1...v1.2.2)

## [v1.2.1](https://github.com/NASA-PDS/registry-loader/tree/v1.2.1) (2026-01-14)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.2.0...v1.2.1)

## [v1.2.0](https://github.com/NASA-PDS/registry-loader/tree/v1.2.0) (2025-12-15)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.1.1...v1.2.0)

**Defects:**

- Remove `[ERROR] /opt/registry-manager/elastic/data-dic-data.jar`  from log [\#44](https://github.com/NASA-PDS/registry-loader/issues/44) [[s.medium](https://github.com/NASA-PDS/registry-loader/labels/s.medium)]

## [v1.1.1](https://github.com/NASA-PDS/registry-loader/tree/v1.1.1) (2025-05-06)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.1.0...v1.1.1)

## [v1.1.0](https://github.com/NASA-PDS/registry-loader/tree/v1.1.0) (2025-04-09)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v1.0.1...v1.1.0)

**Defects:**

- Continuous integration fails [\#38](https://github.com/NASA-PDS/registry-loader/issues/38) [[s.high](https://github.com/NASA-PDS/registry-loader/labels/s.high)]

## [v1.0.1](https://github.com/NASA-PDS/registry-loader/tree/v1.0.1) (2024-11-22)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.4.1...v1.0.1)

## [v0.4.1](https://github.com/NASA-PDS/registry-loader/tree/v0.4.1) (2023-11-16)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.4.0...v0.4.1)

## [v0.4.0](https://github.com/NASA-PDS/registry-loader/tree/v0.4.0) (2023-10-02)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.3.9...v0.4.0)

**Defects:**

- Stable Roundup can no longer trigger Imaging workflow [\#26](https://github.com/NASA-PDS/registry-loader/issues/26)

## [v0.3.9](https://github.com/NASA-PDS/registry-loader/tree/v0.3.9) (2023-03-31)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.3.8...v0.3.9)

## [v0.3.8](https://github.com/NASA-PDS/registry-loader/tree/v0.3.8) (2023-03-31)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.3.7...v0.3.8)

## [v0.3.7](https://github.com/NASA-PDS/registry-loader/tree/v0.3.7) (2022-12-12)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.3.6...v0.3.7)

## [v0.3.6](https://github.com/NASA-PDS/registry-loader/tree/v0.3.6) (2022-11-09)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.3.5...v0.3.6)

## [v0.3.5](https://github.com/NASA-PDS/registry-loader/tree/v0.3.5) (2022-10-26)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.3.3...v0.3.5)

## [v0.3.3](https://github.com/NASA-PDS/registry-loader/tree/v0.3.3) (2022-10-26)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.2.2...v0.3.3)

## [v0.2.2](https://github.com/NASA-PDS/registry-loader/tree/v0.2.2) (2022-08-02)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.2.1...v0.2.2)

**Other closed issues:**

- Add README / documentation for executing archive status update script and upgrade dd command [\#14](https://github.com/NASA-PDS/registry-loader/issues/14)

## [v0.2.1](https://github.com/NASA-PDS/registry-loader/tree/v0.2.1) (2022-05-03)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.2.0...v0.2.1)

## [v0.2.0](https://github.com/NASA-PDS/registry-loader/tree/v0.2.0) (2022-04-14)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/v0.1.0...v0.2.0)

## [v0.1.0](https://github.com/NASA-PDS/registry-loader/tree/v0.1.0) (2022-01-11)

[Full Changelog](https://github.com/NASA-PDS/registry-loader/compare/1f0366f2e342eeef510c2a20a9d7959880203400...v0.1.0)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
