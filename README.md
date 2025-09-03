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