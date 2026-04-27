# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Maven monorepo containing the PDS (Planetary Data System) Registry Loader tools — utilities for creating and populating the PDS Registry (backed by OpenSearch/Elasticsearch).

**Key characteristics:**
- Maven multi-module Java project (modules: `common`, `harvest`, `manager`)
- Parent version: 1.3.0-SNAPSHOT
- Depends on `registry-common` library for shared OpenSearch/Elasticsearch abstractions

## Modules

- **`common`** — Shared library code used by `harvest` and `manager`
- **`harvest`** — Harvest tool: crawls PDS4 label files and loads product metadata into the registry
- **`manager`** — Registry Manager tool: manages registry lifecycle (create, delete, load data dictionaries, set archive status, etc.)

## Build Commands

```bash
# Build all modules
mvn clean install

# Build without tests
mvn clean install -DskipTests

# Build a single module (e.g., harvest)
mvn clean install -pl harvest -am

# Run tests
mvn test

# Package JARs
mvn package
```

## Code Architecture

### Harvest (`harvest/`)

Entry point: `gov.nasa.pds.harvest.HarvestMain`

Key classes:
- `HarvestCmd` — main harvest command, reads config and drives crawling
- `ProductProcessor` — processes individual PDS4 label files into registry documents
- `LidVidCache` — tracks already-processed LIDVIDs to avoid duplicates
- `WriterManager` / `SupplementalWriter` — manages output writers for different product types

### Registry Manager (`manager/`)

Entry point: `gov.nasa.pds.registry.mgr.RegistryManagerCli`

Command groups:
- `reg/` — registry lifecycle commands (create, delete, fetch, list known registries)
- `dd/` — data dictionary commands (load, export, upgrade, delete LDDs)

### Common (`common/`)

Shared utilities used across harvest and manager: XML handling, connection utilities, etc.

## Development Notes

**Testing:**
- Tests are in `src/test/java/` within each module
- `tt/` packages contain technical/integration tests that require a live OpenSearch/Elasticsearch instance
- Run unit tests with `mvn test`; integration tests require additional setup

**Connection configuration:**
- OpenSearch connection details are configured via config files passed to CLI commands
- Auth config example at `manager/src/main/resources/auth/auth.cfg`

**Pre-commit hooks:**
- Secret detection using `detect-secrets`
- Run `pre-commit install` to enable hooks locally
- Baseline files: `.secrets.baseline` (root), plus one per submodule

**CI/CD:**
- Unstable builds (main branch): `.github/workflows/unstable-cicd.yaml`
- Stable builds (releases): `.github/workflows/stable-cicd.yaml`
- Secrets workflow: `.github/workflows/secrets-detection.yaml`
