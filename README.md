# Real-Time Product Review Analysis and Alert System Using NLP and Stream Processing

A comprehensive real-time system for analyzing Amazon product reviews using Natural Language Processing (NLP) and stream processing techniques with intelligent alerting capabilities.

## Overview

This project provides tools for:
- Real-time ingestion and processing of Amazon product reviews
- Advanced sentiment analysis using NLP techniques
- Intelligent alerting system for negative review pattern detection
- Real-time dashboard visualization of review data
- Stream processing for continuous data analysis
- Database management and data backfilling capabilities

## Data Source

This project uses the Amazon Product Reviews dataset from [J. McAuley's research group](http://jmcauley.ucsd.edu/data/amazon/). Specifically, we use the **Grocery and Gourmet Food** dataset which contains:

- **5,074,160 reviews** across various grocery and gourmet food products
- **287,209 products** with metadata
- Rich review data including ratings, text, timestamps, and product information

The dataset provides comprehensive coverage of customer feedback in the grocery and gourmet food category, making it ideal for sentiment analysis and review pattern detection.

## Dashboard Preview

![Review Monitor Dashboard](dashboard-screenshot.png)

*Real-time Amazon reviews analysis dashboard showing sentiment trends, keyword analysis, and recent review monitoring*

The dashboard provides:
- **Real-time sentiment trend analysis** with interactive time controls
- **Keyword extraction and frequency analysis** from review content
- **Recent reviews display** with sentiment classification and timestamps
- **Product-specific monitoring** with customizable time windows
- **Interactive charts** for data exploration and pattern detection

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
└── data/               # Data directory (Amazon reviews dataset - not tracked in git)
```

## Features

### Core Components

1. **Real-Time Alert System** (`alert_worker.py`)
   - Stream processing for continuous monitoring of review patterns
   - Intelligent detection of products with 5+ negative reviews in 10-minute windows
   - Implements cooldown periods to prevent duplicate alerts
   - Real-time monitoring with SQLite database

2. **Stream Data Ingestion** (`ingest_worker.py`)
   - Real-time processing of incoming review data streams
   - NLP-based sentiment analysis and keyword extraction
   - Efficient storage in SQLite database with optimized queries

3. **Web Dashboard** (`dash_app.py`)
   - Dash-based visualization (built on Flask)
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

5. Download the dataset (optional - for full functionality):
   - Visit [J. McAuley's Amazon dataset page](http://jmcauley.ucsd.edu/data/amazon/)
   - Download the "Grocery and Gourmet Food" dataset
   - Extract the files to the `data/` directory

## Usage

### Starting the Alert Worker
```bash
python alert_worker.py
```

### Running the Dashboard
```bash
# Navigate to the project directory
cd ~/sushi/wsp/amazon_reviews

# Kill any existing dash processes on port 8052
pkill -f 'dash_app|python .*8052' || true

# Run the Dash app on port 8052
PORT=8052 PYTHONUNBUFFERED=1 python dash_app.py
```

Then open your browser and navigate to: `http://localhost:8052/`

**Note**: If you're running this on a remote server, replace `localhost` with your server's IP address (e.g., `http://192.168.1.64:8052/`)

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
- `dash`: Dashboard framework (built on Flask)
- `sqlite3`: Database (built-in)

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]
