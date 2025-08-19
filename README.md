# Amazon Reviews Analysis System

A comprehensive system for analyzing Amazon product reviews with real-time alerting capabilities.

## Overview

This project provides tools for:
- Ingesting and processing Amazon product reviews
- Sentiment analysis of reviews
- Real-time alerting for negative review patterns
- Dashboard visualization of review data
- Database management and data backfilling

## Project Structure

```
amazon_reviews/
├── app.py                 # Main FastAPI application
├── dash_app.py           # Streamlit dashboard application
├── main.py               # Main entry point
├── alert_worker.py       # Real-time alert monitoring
├── ingest_worker.py      # Data ingestion worker
├── init_db.py           # Database initialization
├── backfill_from_csv.py # CSV data backfilling utility
├── inject_negatives.py  # Negative review injection utility
├── sanity_dash.py       # Dashboard sanity check
├── requirements.txt     # Python dependencies
├── reviews.db          # SQLite database (not tracked in git)
└── data/               # Data directory
```

## Features

### Core Components

1. **Alert System** (`alert_worker.py`)
   - Monitors for products with 5+ negative reviews in 10-minute windows
   - Implements cooldown periods to prevent duplicate alerts
   - Real-time monitoring with SQLite database

2. **Data Ingestion** (`ingest_worker.py`)
   - Processes incoming review data
   - Handles sentiment analysis
   - Stores data in SQLite database

3. **Web Dashboard** (`dash_app.py`)
   - Streamlit-based visualization
   - Real-time data display
   - Interactive charts and metrics

4. **API Server** (`app.py`)
   - FastAPI-based REST API
   - Endpoints for data retrieval and management
   - Uvicorn server configuration

### Database Schema

The system uses SQLite with the following main tables:
- `reviews`: Stores individual review data with sentiment analysis
- `alerts`: Tracks alert events for negative review patterns

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd amazon_reviews
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Initialize the database:
```bash
python init_db.py
```

## Usage

### Starting the Alert Worker
```bash
python alert_worker.py
```

### Running the Dashboard
```bash
streamlit run dash_app.py
```

### Starting the API Server
```bash
uvicorn app:app --reload
```

### Data Backfilling
```bash
python backfill_from_csv.py <csv_file>
```

## Configuration

The system uses the following configuration:
- Database: `reviews.db` (SQLite)
- Alert threshold: 5 negative reviews in 10 minutes
- Cooldown period: 10 minutes between alerts for the same product

## Development

### Adding New Features
1. Create feature branch: `git checkout -b feature/new-feature`
2. Implement changes
3. Test thoroughly
4. Commit with descriptive messages
5. Create pull request

### Code Style
- Follow PEP 8 guidelines
- Use type hints where appropriate
- Add docstrings for functions and classes

## Dependencies

- `fastapi`: Web framework for API
- `uvicorn`: ASGI server
- `python-dotenv`: Environment variable management
- `psycopg2-binary`: PostgreSQL adapter (if needed)
- `streamlit`: Dashboard framework
- `sqlite3`: Database (built-in)

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]
