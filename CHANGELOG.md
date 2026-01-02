# Changelog

All notable changes to pgdrift will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-02

### Added

#### Core Commands

- **`pgdrift discover`** - Discover all JSONB columns in a PostgreSQL database
  - Lists schema, table, column, and estimated row counts
  - Supports table, JSON, and markdown output formats

- **`pgdrift analyze <table> <column>`** - Analyze a JSONB column for schema drift
  - Detects type inconsistencies across sampled records
  - Identifies ghost keys (deprecated fields present in <10% of records)
  - Finds sparse fields (optional fields present in 10-80% of records)
  - Detects missing required fields (expected fields present in 80-95% of records)
  - Analyzes schema evolution patterns
  - Configurable sample size (default: 5000 rows)

- **`pgdrift scan-all`** - Scan all JSONB columns in the database
  - Database-wide drift analysis
  - Aggregates issues across all columns
  - Highlights columns with critical issues
  - Continues analysis even if individual columns fail

- **`pgdrift index <table> <column>`** - Generate PostgreSQL index recommendations
  - Recommends B-tree indexes for high-density scalar fields (≥80% density)
  - Recommends GIN indexes for arrays and deep nesting (depth > 2)
  - Recommends partial indexes for sparse fields (5-20% density)
  - Generates ready-to-use SQL CREATE INDEX statements
  - Provides estimated performance benefits

#### Database Layer

- Adaptive sampling strategies based on table size:
  - Random sampling for small tables (<100k rows)
  - Reservoir sampling via primary key index for medium tables (100k-10M rows)
  - PostgreSQL TABLESAMPLE for large tables (>10M rows)
- Connection pooling with health checks
- Row count estimation using PostgreSQL statistics
- SQL injection protection for all user inputs

#### Analysis Engine

- Recursive JSON tree walker supporting nested objects and arrays
- Field path notation: dot notation for nesting (`user.email`), bracket notation for arrays (`addresses[].city`)
- Comprehensive field statistics tracking:
  - Occurrence counts and density calculations
  - Type distribution across samples
  - Null value tracking
  - Nesting depth measurement
  - Example value collection (up to 10 per field)
- Configurable drift detection thresholds
- Severity-based issue categorization (Critical, Warning, Info)

#### Output Formats

- **Table format**: Colored ASCII tables with severity highlighting
- **JSON format**: Machine-readable output for programmatic processing
- **Markdown format**: Copy-paste ready for GitHub issues and documentation

#### Testing & Quality

- **149 total tests** (88 unit tests + 61 integration tests)
- Integration tests using testcontainers for real PostgreSQL environments
- Comprehensive edge case coverage:
  - SQL injection protection
  - Unicode and special character handling
  - Extreme nesting (20+ levels)
  - Mixed type detection
  - Empty and NULL data handling
- Test fixtures representing common drift scenarios

#### CI/CD & Tooling

- GitHub Actions CI pipeline testing on Linux, macOS, and Windows
- GitHub Actions release workflow for multi-platform binary distribution
- Security audit configuration with cargo-audit
- Automated testing on every commit

### Performance

- Analyzes 10,000 samples in under 10 seconds for typical schemas
- Peak memory usage under 500MB
- Minimal production impact with read-only queries

### Documentation

- Comprehensive README with usage examples for all commands
- Example outputs for discover, analyze, scan-all, and index commands
- Drift detection logic explanation with threshold details
- Architecture documentation

### Technical Details

- **Crate structure**: Workspace with three crates (pgdrift-core, pgdrift-db, pgdrift)
- **License**: MIT
- **Rust version**: 1.75+
- **PostgreSQL version**: 12+

---

[0.1.0]: https://github.com/capybarastack/pgdrift/releases/tag/v0.1.0
