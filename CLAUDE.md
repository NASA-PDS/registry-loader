# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Registry Loader is a NASA Planetary Data System (PDS) toolset for loading data into the PDS Registry (Elasticsearch/OpenSearch-based). It combines three Maven modules into a single multi-module project:

- **common** (`registry-common`) - Shared library for Elasticsearch/OpenSearch connectivity, metadata extraction, and data dictionary operations
- **harvest** - CLI tool that crawls file systems to discover PDS4 products and indexes metadata into the Registry
- **manager** (`registry-manager`) - CLI tool for managing the Registry: creating indices, loading data dictionaries, setting archive status, and data operations

## Build Commands

```bash
# Build all modules
mvn package

# Build specific module
mvn -pl common package
mvn -pl harvest package
mvn -pl manager package

# Run tests
mvn test

# Run single test class
mvn -pl common test -Dtest=TestBulkResponseParser

# Skip tests
mvn package -DskipTests

# Clean build
mvn clean package

# Install to local repository
mvn install

# Deploy release (requires GPG setup)
mvn -P release clean deploy
```

## Running the Tools

After building, executable JARs are in `{module}/target/`:

```bash
# Harvest - requires config file
java -jar harvest/target/harvest-*.jar -c <config.xml>

# Registry Manager - various subcommands
java -jar manager/target/registry-manager-*.jar <command> <options>

# Registry Manager commands:
#   create-registry, delete-registry
#   list-dd, load-dd, delete-dd, export-dd, upgrade-dd
#   delete-data, export-file, set-archive-status, update-alt-ids
```

## Architecture

### Module Dependencies
```
harvest ──────┐
              ├──> common ──> Elasticsearch/OpenSearch
manager ──────┘
```

### Key Packages

**common** (`gov.nasa.pds.registry.common`):
- `connection/` - Registry connection handling (AWS OpenSearch Serverless, direct ES/OS)
- `connection/aws/` - AWS-specific implementations using OpenSearch SDK
- `connection/es/` - Standard Elasticsearch REST client implementations
- `es/dao/` - Data Access Objects for registry operations
- `es/service/` - High-level services (schema updates, data loading)
- `meta/` - Metadata extractors for PDS4 labels
- `dd/` - Data dictionary parsing and loading

**harvest** (`gov.nasa.pds.harvest`):
- `HarvestCli` - CLI entry point
- `cmd/` - Command implementations
- `cfg/` - Configuration parsing

**manager** (`gov.nasa.pds.registry.mgr`):
- `RegistryManagerCli` - CLI entry point with command dispatcher
- `cmd/reg/` - Registry management commands
- `cmd/dd/` - Data dictionary commands
- `cmd/data/` - Data manipulation commands

### Connection Configuration

Registry connections use XML configuration files. The `common` module provides:
- Direct connections to Elasticsearch/OpenSearch
- AWS OpenSearch Serverless with Cognito authentication
- Connection factory pattern via `EstablishConnectionFactory`

## Docker

Build Docker image containing both tools:
```bash
docker image build -t nasapds/registry-loader -f docker/Dockerfile \
  --build-arg harvest_package_path=harvest/target/harvest-*-bin.tar.gz \
  --build-arg reg_manager_package_path=manager/target/registry-manager-*-bin.tar.gz .
```

## CI/CD

- Releases triggered by pushing `release/*` tags
- Uses NASA-PDS Roundup Action for automated releases
- Docker images published to Docker Hub on stable releases
