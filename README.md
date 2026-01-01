# pgdrift

pgdrift is a command-line tool for detecting schema drift in PostgreSQL JSONB columns. It analyzes semi-structured data, identifies inconsistencies, and helps maintain data quality in production systems.

## What It Does

When you store JSON documents in PostgreSQL JSONB columns, the schema isn't enforced at the database level. Over time, this leads to drift: fields changing types, optional fields appearing inconsistently, deprecated fields lingering in old records. pgdrift scans your JSONB columns and surfaces these issues before they cause runtime errors.

**Key capabilities:**

- Discover all JSONB columns in a database
- Detect type inconsistencies (field appears as both string and number)
- Identify ghost keys (deprecated fields present in <10% of records)
- Find sparse fields (optional fields present in 10-80% of records)
- Detect missing required fields (expected fields present in 80-95% of records)
- Analyze schema evolution patterns
- Generate reports in multiple formats (table, JSON, markdown)

## Getting Started

### Prerequisites

- Rust 1.75 or later
- PostgreSQL 12 or later
- Access to a PostgreSQL database with JSONB columns

### Installation

Currently, pgdrift must be built from source. Having installed Rust, clone this repository:

```bash
git clone https://github.com/capybarastack/pgdrift.git
cd pgdrift
```

### Building

The simplest way to build for development is to use `cargo build`:

```bash
cargo build --release
```

This compiles the binary and places it in `target/release/pgdrift`.

For convenience, you can install it to your cargo bin directory:

```bash
cargo install --path crates/pgdrift
```

## Usage

pgdrift provides two main commands: `discover` and `analyze`.

### Discovering JSONB Columns

List all JSONB columns in your database:

```bash
pgdrift discover --database-url "postgres://user:pass@localhost/mydb"
```

You can also set the database URL via environment variable:

```bash
export DATABASE_URL="postgres://user:pass@localhost/mydb"
pgdrift discover
```

**Example output:**

```
JSONB Columns Found:
┌────────┬───────────┬──────────┬────────────────┐
│ Schema │ Table     │ Column   │ Approx Rows    │
├────────┼───────────┼──────────┼────────────────┤
│ public │ users     │ metadata │ 1,245,892      │
│ public │ events    │ payload  │ 8,932,441      │
│ public │ sessions  │ context  │ 445,201        │
└────────┴───────────┴──────────┴────────────────┘
```

### Analyzing a JSONB Column

Run drift detection on a specific table and column:

```bash
pgdrift analyze users metadata --database-url $DATABASE_URL
```

By default, pgdrift samples 5,000 rows. You can adjust this:

```bash
pgdrift analyze users metadata --sample-size 10000
```

**Example output:**

```
Analyzing public.users.metadata (5000 samples)
[████████████████████] 5000/5000 (00:00:03)

Schema Analysis Summary:
  Total unique paths: 47
  Maximum nesting depth: 5
  Drift issues found: 2 critical, 3 warnings, 8 info

Critical Issues:
┌──────────────────────┬──────────┬─────────────────────────────────────────────────────────┐
│ Path                 │ Severity │ Issue                                                   │
├──────────────────────┼──────────┼─────────────────────────────────────────────────────────┤
│ user.age             │ Critical │ Type inconsistency (minority: 8.0%: string:92.0, num... │
│ user.email           │ Critical │ Missing key: 15.00% missing (750/5000 samples missi...  │
└──────────────────────┴──────────┴─────────────────────────────────────────────────────────┘

Warnings:
┌──────────────────────┬──────────┬─────────────────────────────────────────────────────────┐
│ Path                 │ Severity │ Issue                                                   │
├──────────────────────┼──────────┼─────────────────────────────────────────────────────────┤
│ user.phone           │ Warning  │ Missing key: 8.00% missing (400/5000 samples missing... │
│ prefs.theme          │ Warning  │ Schema evolution: deprecated field 'old_theme' → 't...  │
└──────────────────────┴──────────┴─────────────────────────────────────────────────────────┘

Info:
┌──────────────────────┬──────────┬─────────────────────────────────────────────────────────┐
│ Path                 │ Severity │ Issue                                                   │
├──────────────────────┼──────────┼─────────────────────────────────────────────────────────┤
│ legacy.deprecated_id │ Info     │ Ghost key: 0.80% present (40/5000 samples)              │
│ user.nickname        │ Info     │ Sparse field: 45.00% present (2250/5000 samples)        │
└──────────────────────┴──────────┴─────────────────────────────────────────────────────────┘
```

### Output Formats

pgdrift supports three output formats:

**Table format** (default): Human-readable ASCII tables with color coding

```bash
pgdrift analyze users metadata --format table
```

**JSON format**: Machine-readable output for programmatic processing

```bash
pgdrift analyze users metadata --format json > drift-report.json
```

**Markdown format**: Copy-paste into GitHub issues or documentation

```bash
pgdrift analyze users metadata --format markdown > DRIFT_REPORT.md
```

### Production Safety

When analyzing production databases, use the `--production-mode` flag. This enables additional safety checks and warnings:

```bash
pgdrift analyze users metadata --production-mode
```

pgdrift uses adaptive sampling strategies based on table size:

- **Small tables** (< 100k rows): Random sampling with `ORDER BY random()`
- **Medium tables** (100k-10M rows): Reservoir sampling via primary key index
- **Large tables** (> 10M rows): PostgreSQL native `TABLESAMPLE` (no table locks)

For very large tables, pgdrift automatically selects the safest sampling method to minimize performance impact.

### Row Count Accuracy

pgdrift uses PostgreSQL's internal statistics (`pg_stat_user_tables.n_live_tup`) for estimated row counts. These estimates are fast but can be slightly inaccurate (typically off by 1-2 rows) if the statistics are stale.

If you notice row count discrepancies, update PostgreSQL's statistics by running:

```sql
ANALYZE your_table_name;
```

Or for all tables:

```sql
ANALYZE;
```

This is a lightweight operation that updates metadata without locking tables or affecting performance.

**Future consideration**: If estimate accuracy proves problematic in practice, we may switch to `COUNT(*)` queries for exact counts, trading performance for accuracy.

### Drift Detection Logic

pgdrift categorizes fields based on how frequently they appear across sampled records. The detection logic uses intelligent thresholds to distinguish between different types of schema issues:

#### Field Presence Thresholds

- **Ghost Keys** (<10% present): Deprecated or rarely-used fields that appear in less than 10% of records. These are typically legacy fields that should be cleaned up. Severity: **Info**

- **Sparse Fields** (10-80% present): Optional fields with moderate presence. These represent legitimate optional data that appears in some but not most records. Severity: **Info**

- **Missing Keys** (80-95% present): Fields that appear to be required (high presence) but have unexpected gaps. These likely indicate missing data or incomplete migrations. Severity: **Warning** (90-95%) or **Critical** (<90%)

- **Normal Fields** (≥95% present): Fields consistently present across nearly all records. No issues reported.

#### Type Inconsistency Detection

When a field appears with multiple data types (e.g., sometimes a string, sometimes a number), pgdrift flags it based on the minority type percentage:

- **Critical**: Minority type ≥10% (significant inconsistency)
- **Warning**: Minority type 5-10% (moderate inconsistency)
- **Info**: Minority type <5% (minor inconsistency)

#### Schema Evolution Patterns

pgdrift automatically detects common schema evolution patterns:

- **Version markers**: Fields like `version`, `schema_version`, `api_version`
- **Deprecated naming**: Fields prefixed with `old_`, `legacy_`, `deprecated_`
- **Mutually exclusive fields**: Related fields that never appear together (e.g., `address_v1` and `address_v2`)

All schema evolution detections are reported at **Warning** level.

#### Severity Levels Summary

- **Critical**: Requires immediate attention (major type inconsistencies, missing required fields)
- **Warning**: Should be reviewed (minor type inconsistencies, schema evolution, missing semi-required fields)
- **Info**: Informational (ghost keys, sparse fields, minor issues)

## Testing

pgdrift has comprehensive test coverage across unit and integration tests.

### Running Tests

Run unit tests (no Docker required):

```bash
cargo test --lib
```

Run all tests including integration tests (requires Docker):

```bash
cargo test
```

Run tests for a specific crate:

```bash
cargo test -p pgdrift-core
cargo test -p pgdrift-db
cargo test -p pgdrift
```

### Test Coverage

Current test suite includes:

- **84 total tests** (38 unit tests + 46 integration tests)
- Unit tests for JSON analysis, drift detection, sampling strategies, and field categorization
- Integration tests against real PostgreSQL databases using testcontainers
- Edge case testing for SQL injection, Unicode handling, extreme nesting, and mixed types

Run integration tests separately:

```bash
cargo test --test integration_test
cargo test --test analyze_integration_test
cargo test --test discover_integration_test
```

Integration tests automatically spin up PostgreSQL containers and populate them with fixture data representing common drift scenarios.

## Architecture

pgdrift is structured as a Cargo workspace with three crates:

- **pgdrift-core**: Analysis engine, drift detection algorithms, and core types
- **pgdrift-db**: Database layer, connection pooling, and sampling strategies
- **pgdrift**: Command-line interface and output formatting

This separation allows the analysis engine to be used as a library in other tools.

### How It Works

1. **Discovery**: Query PostgreSQL system catalogs to find all JSONB columns
2. **Sampling**: Select a representative sample using adaptive strategies
3. **Analysis**: Recursively traverse each JSON document, building statistics for every path
4. **Detection**: Apply drift detection algorithms to identify issues
5. **Reporting**: Format and display results with severity levels

The JSON analyzer handles nested objects, arrays, and mixed types. Field paths use dot notation for nesting (`user.profile.email`) and bracket notation for arrays (`addresses[].city`).

## Development Hygiene

Before committing code, ensure you've taken the following steps:

- Run `cargo fmt` to format your code
- Run `cargo clippy` to catch common mistakes
- Run `cargo test` and ensure all tests pass
- Update tests if you've added new functionality

### Code Style

- Follow standard Rust conventions (rustfmt default configuration)
- Write unit tests for new algorithms
- Add integration tests for new commands or database interactions
- Keep functions focused and well-named
- Document public APIs with rustdoc comments

## Configuration

pgdrift reads the database connection string from either:

1. The `--database-url` flag
2. The `DATABASE_URL` environment variable

PostgreSQL connection strings follow the standard format:

```
postgres://username:password@hostname:port/database
```

For local development:

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/dev_db"
```

For production (read-only recommended):

```bash
export DATABASE_URL="postgres://readonly_user:pass@prod.example.com:5432/prod_db"
pgdrift analyze users metadata --production-mode
```

## Performance

pgdrift is designed to handle large-scale databases efficiently:

- **Sampling performance**: 10,000 samples analyzed in under 10 seconds for most schemas
- **Memory usage**: Peak memory typically under 500MB
- **Minimal impact**: Uses read-only queries and adaptive sampling to avoid production load

Benchmark on a table with 5M rows and moderately complex JSONB (20-30 fields, nesting depth 3):

```
Sampling 10,000 rows: ~2 seconds
Analysis: ~3 seconds
Total: ~5 seconds
```

## Roadmap

pgdrift is under active development. Planned features:

- Index recommendation engine for high-density fields
- CI/CD integration mode for drift detection in pipelines
- Migration SQL generation (Pro feature)
- Web dashboard for visual analysis
- Support for additional database types (MySQL JSON, MongoDB)

## Contributing

Contributions are welcome. Please ensure:

- New features include tests
- Code passes `cargo clippy` and `cargo fmt`
- Integration tests pass locally
- Documentation is updated for user-facing changes

When reporting issues, include:

- PostgreSQL version
- Sample database schema (if possible)
- Steps to reproduce
- Expected vs actual behavior

## License

MIT License. See LICENSE file for details.

## Acknowledgments

Built with:

- [sqlx](https://github.com/launchbadge/sqlx) - Async PostgreSQL driver
- [clap](https://github.com/clap-rs/clap) - Command line argument parsing
- [tabled](https://github.com/zhiburt/tabled) - ASCII table formatting
- [serde_json](https://github.com/serde-rs/json) - JSON parsing and manipulation

Inspired by the need for better tooling around semi-structured data in relational databases.
