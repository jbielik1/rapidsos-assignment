# Emergency Calls Enrichment Script

## Quick Start

```bash
# 1. Setup virtual environment
python -m venv venv
venv\Scripts\activate.bat    # Windows
# source venv/bin/activate   # macOS/Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run data processing
python emergency_calls.py

# 4. Start Jupyter (optional)
jupyter notebook
```

## Prerequisites

- Python 3.7+
- Java 8+ (required by Spark)

## Output

- Processed data: `output/processed_emergency_data.csv`
- Analysis notebook: `data_insights.ipynb`

## Observations and data cleanup

### emergency_calls

1. Timestamps are in double format without significant decimals → cast to integers
2. Unix timestamps are sometimes in milliseconds → unify to seconds
3. Timezones between Unix timestamps (ring_timestamp_unix, pick_up_timestamp_unix) and other datetime fields are shifted by 5 hours → standardize timestamps
4. The last rows are missing pick_up_timestamp, call_end_timestamp, and call_taker_station_id values → remove these rows, as they will not be considered for further analysis

### agent_activity

1. Some logins do not match or are missing logouts → augment data with synthetic logouts to close agent sessions

### Others
- Verify that all stations from **emergency_calls** are present in **agent_activity**
- Identify and analyze calls with no matched agents

## Data insights 
Detailed analysis is available in **data_insights.ipynb** (Jupyter Notebook)

### Busy hours

Metric based on Erlangs → Erlangs = (Call Arrival Rate × Average Handling Time) / Number of Available Agents

### Agent performance

1. Normalized relative productivity index → Normalized ( Total Calls / Total Handle Time (s) ) × ( 1 / (1 + Average Wait Time (s)) )

2. Absolute productivity score → Absolute Productivity Score = ( Total Calls / Total Handle Time (hours) ) x ( 1 / ( 1 + Average Wait Time (hours) ))

3. Composite Score → Composite Score = 𝑤1 x Calls per Hour + 𝑤2 x 1/Handle Time (hours) + 𝑤3 x 1/Wait Time (hours) where *w1*, *w2* and *w3* are adjustable variables according to business priority