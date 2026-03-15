# Data collection

Fetches option/stock data (Upstox) and writes to TimescaleDB. Intended to run on a schedule (e.g. cron).

## Run from repo root

```bash
# Single date (no-sync version, typical for cron)
python data_collection/opt-stk-data-to-db-upstox-nosync.py 2026-03-15

# With sync
python data_collection/opt-stk-data-to-db-upstox.py 2026-03-15
```

## Cron (from repo root)

```bash
# Example: 7:30 AM on weekdays
30 7 * * 1-5  cd /home/cgraaaj/cgr-trades && ./data_collection/daily_data_collector.sh
```

## Config

- `db_config.py` and `.env` must be at **repo root** (parent of `data_collection/`). Scripts add the parent directory to `sys.path` to load `db_config`.
- Required env: `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME` (see `db_config.py`).

## When splitting to a separate repo

1. Copy this folder and `db_config.py` (or merge into a single config).
2. Copy `.env.example` with `DB_*` (and any Upstox vars).
3. Install deps from `requirements.txt` and run as above (from the new repo root).
