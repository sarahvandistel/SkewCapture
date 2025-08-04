# SkewCapture

Automated forward-tester for the SkewCapture Screener.

## Setup
1. Clone: `git clone git@github.com:your-org/SkewCapture.git`
2. `cd SkewCapture`
3. `python -m venv venv && source venv/bin/activate`
4. `pip install -r requirements.txt`
5. Copy `config/config.yml.example` to `config/config.yml` and fill in credentials.
6. Place Barchart CSV files in `data/barchart/` with naming pattern: `stocks-screener-skewcapture-screener-MM-DD-YYYY.csv`

## Daily Signal Logging
Run:
```
python scripts/fetch_signals.py --date YYYY-MM-DD
```
Or schedule via cron/Cursor: `schedule.every().day.at("03:53").do(...)`

Outputs CSV in `data/raw/signals_YYYYMMDD.csv`. 