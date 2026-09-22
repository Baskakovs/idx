# CLAUDE.md — guidance for Claude Code sessions

## Commands

Always prefer `make <target>` — never call `.venv/bin/…` directly.

| Target | What it does |
|--------|-------------|
| `make install` | Create venv, sync deps, install pre-commit hooks |
| `make fmt` | Run all pre-commit hooks (ruff, bandit, markdownlint, actionlint, …) |
| `make typecheck` | Run ty + mypy --strict over `src/` |
| `make docs-coverage` | Check docstring coverage with interrogate |
| `make deps` | Run deptry (unused/missing dependency analysis) |
| `make security` | Run bandit security scan |
| `make rhiza-test` | Run the rhiza repository structure checks |
| `make test` | Run the full test suite with coverage (threshold: 90%) |
| `make all` | Run every gate, as CI does |

## Architecture

```
src/idx/
├── __init__.py       # Logger factory
├── download.py       # Fetch STOXX selection list files (PDF/CSV) from stoxx.com
├── enrichment.py     # Resolve Yukka entity IDs via ISIN and RIC lookups
├── extract.py        # Parse PDF/CSV files; compute index membership (buffer rule)
├── main.py           # Prefect flow orchestrating the full pipeline
├── ranking.py        # Build wide-format daily ranking table
└── storage.py        # Write Parquet files to Cloudflare R2

tests/idx/            # Test files mirror src/idx/ (one test file per module)
```

The pipeline is a linear chain: **Download → Extract → Enrich → Rank → Store**, orchestrated by `main.py` as a Prefect flow. Each module only imports `get_logger` from `__init__.py`; `main.py` is the sole orchestrator that imports all others.

## Rhiza template split

This repo is managed by [rhiza](https://github.com/jebel-quant/rhiza) at `v1.8.0`. The `.rhiza/template.lock` `files:` list is the machine-generated record of template-owned paths.

**In scope (locally owned):**
- `src/`, `tests/`, `pyproject.toml`, `README.md`, `CLAUDE.md`, `mkdocs.yml`
- `.rhiza/template.yml` (the pointer)

**Out of scope (template-owned — fix upstream, not here):**
- `Makefile`, `.pre-commit-config.yaml`, `pytest.ini`, `ruff.toml`
- `.github/workflows/*`, `docs/mkdocs-base.yml`, `docs/index.md`
- Everything listed in `.rhiza/template.lock` `files:`

## Conventions

- **Test layout:** Files are mirrored at `tests/idx/test_*.py`. Class-level parity is opted out via `[tool.check_test_layout]` in `pyproject.toml` — tests use `TestFunctionName` classes.
- **Coverage:** 90% minimum enforced by `make test`. Current: ~98%.
- **Docstrings:** 100% interrogate coverage required on `src/` and `tests/`.
- **Type checking:** ty + mypy --strict, zero errors.
- **License ignore:** `text-unidecode` and `Unidecode` are exempted from the GPL license scan via `[tool.rhiza-task]` (transitive deps from prefect, not imported).
