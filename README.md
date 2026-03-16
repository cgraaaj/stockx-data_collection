# StockX Trades – Data Collection

Fetches NSE option/stock data via Upstox API and writes to TimescaleDB. Designed for daily automated collection via cron.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r data_collection/requirements.txt

cp .env.example .env
# Edit .env with your actual DB credentials
```

## Running

```bash
# Single date
python data_collection/opt-stk-data-to-db-upstox-nosync.py 2026-03-15

# Date range
python data_collection/opt-stk-data-to-db-upstox-nosync.py 2026-03-01 2026-03-15

# With sync version
python data_collection/opt-stk-data-to-db-upstox.py 2026-03-15

# Force mode (include weekends/holidays)
python data_collection/opt-stk-data-to-db-upstox-nosync.py 2026-03-15 --force
```

## Cron setup

```bash
# 7:30 AM on weekdays
# 30 7 * * 1-5  cd /home/cgraaaj/cgr-trades && ./data_collection/daily_data_collector.sh
0 7 * * 2-6 (cd /home/cgraaaj/cgr-trades && source venv/bin/activate && python3 data_collection/opt-stk-data-to-db-upstox-nosync.py $(date -d "yesterday" +\%Y-\%m-\%d)) >> /home/cgraaaj/cgr-trades/logs/cron_$(date +\%Y\%m\%d).log 2>&1
```

## Project structure

```
cgr-trades/
├── .env                  # Secrets (gitignored)
├── .env.example          # Template
├── db_config.py          # DB connection config (reads .env)
├── README.md
├── data_collection/
│   ├── opt-stk-data-to-db-upstox.py         # With sync
│   ├── opt-stk-data-to-db-upstox-nosync.py  # Without sync (default)
│   ├── daily_data_collector.sh               # Cron wrapper
│   ├── requirements.txt
│   └── README.md
├── python/
│   └── NSE.json          # Instrument data (gitignored, downloaded at runtime)
└── logs/                 # Runtime logs (gitignored)
```

## Related

Analysis and backtest code lives in a separate repository: **cgr-analysis-backtest**.
