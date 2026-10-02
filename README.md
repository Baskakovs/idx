[![Release](https://img.shields.io/github/v/release/Baskakovs/idx?sort=semver)](https://github.com/Baskakovs/idx/releases)

[![rhiza v1.8.0](https://img.shields.io/badge/rhiza-v1.8.0-blue)](https://github.com/jebel-quant/rhiza/releases/tag/v1.8.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python versions](https://img.shields.io/badge/Python-3.11%20•%203.12%20•%203.13%20•%203.14-blue?logo=python)](https://www.python.org/)
[![CI](https://github.com/Baskakovs/idx/actions/workflows/rhiza_ci.yml/badge.svg?event=push)](https://github.com/Baskakovs/idx/actions/workflows/rhiza_ci.yml)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg?logo=ruff)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![CodeFactor](https://www.codefactor.io/repository/github/Baskakovs/idx/badge)](https://www.codefactor.io/repository/github/Baskakovs/idx)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Baskakovs/idx/badge)](https://scorecard.dev/viewer/?uri=github.com/Baskakovs/idx)

# idx

STOXX Europe 600 index membership pipeline. Downloads selection lists from stoxx.com, extracts security data, computes index membership using the buffer rule, enriches assets with Yukka entity IDs, and stores everything as Parquet files in Cloudflare R2.

## Architecture

```
stoxx.com (PDF/CSV)
       │
       ▼
   Download ──► Extract ──► Enrich ──► Rank ──► R2 (Parquet)
  (download.py)  (extract.py)  (enrichment.py)  (ranking.py)  (storage.py)
                                                                    │
                                                                    ▼
                                                              Public R2 URL
                                                                    │
                                                                    ▼
                                                           Consuming packages
```

### Pipeline stages

| Stage | Module | Description |
|-------|--------|-------------|
| **Download** | `src/idx/download.py` | Fetches selection list files (PDF before 2023-12, CSV after) from stoxx.com |
| **Extract** | `src/idx/extract.py` | Parses PDF and CSV files into `Asset` and `SelectionListEntry` dataclasses |
| **Membership** | `src/idx/extract.py` | Computes index membership using the STOXX buffer rule (top 550 + buffer 551-750 + fill to 600) |
| **Enrichment** | `src/idx/enrichment.py` | Resolves Yukka entity IDs via ISIN and RIC lookups against the Yukka metadata API |
| **Ranking** | `src/idx/ranking.py` | Builds a wide-format daily ranking table with forward-fill and sentinel-based exit tracking |
| **Storage** | `src/idx/storage.py` | Writes `assets.parquet`, `rankings.parquet`, and per-review Parquet files to Cloudflare R2 |
| **Orchestration** | `src/idx/main.py` | Ties all stages together; orchestrated with Prefect tasks |

### R2 file layout

| File | Description |
|------|-------------|
| `assets.parquet` | Deduplicated security identifiers (latest non-null per column): RIC, ISIN, SEDOL, name, country, currency, yukka_id |
| `membership.parquet` | Long-format daily membership table: one row per member per day with point-in-time RIC and rank |
| `reviews/{date}.parquet` | Per-review snapshot: entries joined with membership (free-float market cap, comments, entry reason) |

## Assumptions

### Data source

- STOXX publishes selection lists at predictable URLs on stoxx.com.
- **PDF format** is used for periods before December 2023. **CSV format** (semicolon-delimited) is used from December 2023 onward.
- CSV files have a variable publication day within the quarter, so the downloader searches across all days in the month (and subsequent months up to the next quarter).
- PDF files use a fixed URL pattern with year and month only.
- The default index symbol is `sxxp` (STOXX Europe 600).
- Available months before 2021 are irregular and hardcoded in `AVAILABLE_MONTHS`. From 2021 onward, quarterly months (March, June, September, December) are assumed.

### Identifier mutability

The only stable identifier across review periods is `internal_key` (STOXX's own
surrogate).  Both RIC and ISIN can change between reviews — exchange migrations,
rebrands, redomiciliations, and share consolidations are common.

**Example — Nestlé (`internal_key` 461669) across three review dates:**

| Review date | ISIN         | RIC | Rank |
|-------------|--------------|-----|-----:|
| 2015-10 | CH0038863350 | NESN.VX | 1 |
| 2017-05 | CH0038863350 | NESN.S | 2 |
| 2026-06 | CH0038863349 | *(empty)* | 4 |

The RIC changed when SIX Swiss Exchange migrated from `.VX` to `.S`.  From
June 2026 STOXX stopped publishing ISINs (and sometimes RICs) in the CSV files
altogether.

**Solution — point-in-time membership table (`membership.parquet`):**

The pipeline stores a long-format daily table with all identifiers that were
valid on each date, rather than collapsing to a single value per company.  On
each review date, members get the identifiers from that period's selection list.
Between reviews, every column except `internal_key` is forward-filled.  When a
review date has an empty or null field (e.g. STOXX stopped publishing ISINs from
June 2026), the last known value for that `internal_key` is carried forward.

```
┌────────────┬──────────────┬─────────┬────────┬─────┬─────┬──────────────┬───────┬──────┐
│    date    │ internal_key │   ric   │  name  │ ... │ ... │     isin     │ sedol │ rank │
├────────────┼──────────────┼─────────┼────────┼─────┼─────┼──────────────┼───────┼──────┤
│ 2015-10-01 │ 461669       │ NESN.VX │ NESTLE │ ... │ ... │ CH0038863350 │ ...   │    1 │
│ 2015-10-01 │ 448816       │ ING.AS  │ ING    │ ... │ ... │ NL0000303600 │ ...   │    3 │
├────────────┼──────────────┼─────────┼────────┼─────┼─────┼──────────────┼───────┼──────┤
│ ...        │ ...          │ ...     │ ...    │     │     │ ...          │       │  ... │
├────────────┼──────────────┼─────────┼────────┼─────┼─────┼──────────────┼───────┼──────┤
│ 2017-05-01 │ 461669       │ NESN.S  │ NESTLE │ ... │ ... │ CH0038863350 │ ...   │    2 │
│ 2017-05-01 │ 448816       │ INGA.AS │ ING    │ ... │ ... │ NL0011821202 │ ...   │    5 │
├────────────┼──────────────┼─────────┼────────┼─────┼─────┼──────────────┼───────┼──────┤
│ ...        │ ...          │ ...     │ ...    │     │     │ ...          │       │  ... │
├────────────┼──────────────┼─────────┼────────┼─────┼─────┼──────────────┼───────┼──────┤
│ 2026-06-01 │ 461669       │ NESN.S  │ NESTLE │ ... │ ... │ CH0038863350 │ ...   │    4 │
│ 2026-06-01 │ 448816       │ INGA.AS │ ING    │ ... │ ... │ NL0011821202 │ ...   │    7 │
└────────────┴──────────────┴─────────┴────────┴─────┴─────┴──────────────┴───────┴──────┘
```

Full columns: `date`, `internal_key`, `ric`, `name`, `country`, `currency`,
`isin`, `sedol`, `rank`.  600 rows per day (one per member), ~3,600 days —
roughly 2.1M rows total.  Consumers filter on `date` to get the current 600
members with correct identifiers — no joins or column-name lookups needed.

The separate `assets.parquet` still exists for enrichment purposes (yukka_id
mapping) and uses a coalesce strategy: for each `internal_key`, it keeps the
**latest non-null** value per column, so that downstream lookups get the current
RIC without losing historical ISINs.

### Extraction

- CSV files contain a `creation_date` column in `YYYYMMDD` format that serves as the review date.
- PDF files contain a "last updated" line in the header with a date in `YYYYMMDD` or `DD.MM.YYYY` format.
- PDF market cap values are in billions EUR (`ff_mcap_beur`) and are converted to millions EUR for consistency with CSV files (`ff_mcap_meur`).
- Each file contains one review date. Assets are deduplicated by `internal_key` within each file.

### Index membership (buffer rule)

- The STOXX Europe 600 uses a buffer rule for membership changes:
  - **Positions 1-550**: automatic members (by free-float market cap rank).
  - **Positions 551-750** (buffer zone): prior members are retained.
  - **Remaining slots**: filled from the next-highest-ranked non-members to reach exactly 600.
- The first review period uses **bootstrap mode** (no prior membership), selecting the top 600 by rank.
- Entries are sorted by `(rank, internal_key)` for deterministic tiebreaking.

### Enrichment

- The Yukka metadata API (`metadata.api.yukkalab.com`) resolves ISINs and RICs to Yukka entity IDs (`alpha_id`).
- ISIN lookup is attempted first; RIC lookup is used as a fallback for unresolved assets.
- Lookups are batched in groups of 100.
- Unresolved assets are reported as a Prefect table artifact.

### Membership table

- The membership table is in long format: one row per member per calendar day, with columns `date`, `internal_key`, `ric`, `name`, `country`, `currency`, `isin`, `sedol`, `rank`.
- On each review date, members are re-ranked 1..N from their original selection list ranks.
- All identifiers are point-in-time: each review date uses the values from that period's selection list. Null or empty fields are forward-filled from the previous review per `internal_key`, so if STOXX stops publishing a field (e.g. ISINs from June 2026), the last known value carries forward.
- Between review dates, all columns are forward-filled daily. When a company exits the index, its rows stop at the next review date.
- Validation checks that ranks 1-600 are present on each review date.

### Storage (Cloudflare R2)

- Data is stored as Parquet files in a Cloudflare R2 bucket, accessed via boto3's S3-compatible API.
- Each pipeline run overwrites the full `assets.parquet` and `membership.parquet` files.
- Review files are written per review date to `reviews/{date}.parquet`.
- Files are publicly readable via the `R2_URL` base URL.

### API

- Consuming packages read Parquet files directly from the public R2 URL, with no API server or dependency on `idx` internals.

## Setup

### Requirements

- Python >= 3.11
- Cloudflare R2 bucket with S3-compatible API access

### Environment variables

Create a `.env` file in the project root:

```env
# Cloudflare R2
R2_ACCESS_KEY_ID=your-access-key-id
R2_SECRET_ACCESS_KEY=your-secret-access-key
R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
R2_BUCKET=your-bucket-name
R2_URL=https://your-public-r2-url.r2.dev
R2_PREFIX=STOXX600_dev  # use STOXX600 for production

# Yukka metadata API
YUKKA_TOKEN=your-bearer-token
```

### Installation

```bash
pip install -e .
```

### Running the pipeline

```bash
python -m idx.main
```

The pipeline is orchestrated with [Prefect](https://www.prefect.io/). Each stage is a Prefect task, so runs are tracked and observable in the Prefect UI.

### Failure notifications

On any unhandled exception, the flow sends a Slack notification via the `yukka-notification` [Prefect SlackWebhook block](https://docs.prefect.io/integrations/prefect-slack) before re-raising, so the run still shows as **Failed** in Prefect. This requires the `prefect-slack` package and a pre-configured `yukka-notification` block in your Prefect workspace.

## Development

Run `make help` to see all available targets:

```
 task                section         needs                 does
 book                Book            test benchmark        build the companion
                                     stress                book
                                     hypothesis-test
                                     paper
 book-nav            Book                                  check that every
                                                           mkdocs nav entry
                                                           resolves in the
                                                           built book
 marimo              Book            install               start the Marimo
                                                           editor
 marimo-validate     Book            install               check that every
                                                           Marimo notebook runs
 serve               Book            book                  build the book and
                                                           serve it on port
                                                           8000
 clean               Dev                                   remove build
                                                           artifacts and stale
                                                           local branches
 doctor              Dev                                   check local
                                                           prerequisites
 setup               Dev                                   run the repository's
                                                           own environment
                                                           setup hook
 docker-build        Docker                                build the Docker
                                                           image
 docker-clean        Docker                                remove the Docker
                                                           image
 docker-run          Docker          docker-build          run the Docker
                                                           container
 lfs-install         Git LFS                               configure git-lfs
                                                           for this repository
 lfs-pull            Git LFS                               download the LFS
                                                           files for the
                                                           current branch
 lfs-status          Git LFS                               show the status of
                                                           LFS files
 lfs-track           Git LFS                               list the patterns
                                                           tracked by git-lfs
 failed-workflows    GitHub Helpers                        list recent failing
                                                           workflow runs
 latest-release      GitHub Helpers                        show information
                                                           about the latest
                                                           GitHub release
 view-issues         GitHub Helpers                        list open issues
 view-prs            GitHub Helpers                        list open pull
                                                           requests
 whoami              GitHub Helpers                        check github auth
                                                           status
 workflow-status     GitHub Helpers                        show recent runs for
                                                           the release workflow
 paper               Paper                                 compile the LaTeX
                                                           paper to PDF
 paper-clean         Paper                                 remove the LaTeX
                                                           build artifacts
 presentation        Presentation                          generate the HTML
                                                           slides with Marp
 presentation-pdf    Presentation                          generate the PDF
                                                           slides with Marp
 presentation-serve  Presentation                          serve the slides
                                                           with Marp's live
                                                           preview
 all                 Python          fmt deps test         run every gate, as
                                     docs-coverage         CI does
                                     security license
                                     typecheck rhiza-test
 coverage            Python          install               measure coverage and
                                                           write
                                                           _tests/coverage.xml
 deps                Python          install               run deptry over the
                                                           contributed folders
 docs-coverage       Python          install               check docstring
                                                           coverage with
                                                           interrogate
 install             Python          setup                 create the venv and
                                                           sync dependencies
 license             Python          install               scan for copyleft
                                                           licences
 security            Python          install               run the bandit
                                                           security scan
 test                Python          install               run all tests
 test-lowest         Python          install               run the tests
                                                           against the oldest
                                                           dependencies the
                                                           manifest allows
 typecheck           Python          install               run ty and/or mypy
                                                           (typechecker = ty |
                                                           mypy | both)
 docs-examples       Quality         install               check the fenced
                                                           examples in the docs
                                                           tree
 fmt                 Quality                               run the pre-commit
                                                           hooks over all files
 complexity          Quality                               fail on a block
                                                           above the
                                                           cyclomatic-complexi…
                                                           ceiling
 test-pyproject      Quality         install               run the
                                                           pyproject.toml
                                                           structure checks,
                                                           verbosely
 rhiza-test          Quality         install               run the rhiza
                                                           repository checks
 semgrep             Quality                               run the semgrep
                                                           static analysis
                                                           rules
 todos               Quality                               list every TODO,
                                                           FIXME and HACK
                                                           comment
 update              Template                              sync the rhiza
                                                           template into this
                                                           repository
 benchmark           Testing extras  install               run the performance
                                                           benchmarks
 hypothesis-test     Testing extras  install               run the
                                                           property-based tests
 stress              Testing extras  install               run the stress and
                                                           load tests
```

### Quick start

```bash
make install   # create venv, sync deps, install pre-commit hooks
make test      # run the full test suite with coverage
make fmt       # run all pre-commit hooks (ruff, bandit, markdownlint, …)
make typecheck # run ty + mypy --strict over src/
```

### CI

CI runs via GitHub Actions using the [Rhiza](https://github.com/jebel-quant/rhiza) reusable workflow (`rhiza_ci.yml`), which runs tests across the Python version matrix defined in `pyproject.toml` classifiers (3.11–3.14), plus linting, type checking, security scanning, and license compliance.

### Branch protection

The `main` branch requires:
- Pull request with review
- Passing CI status checks
- Linear commit history
- No force pushes
