import asyncio
import sys
import json
import requests
import gzip
import re
import uuid
import time
import pandas as pd
import aiohttp
import logging
from datetime import datetime
from urllib.parse import quote
from yarl import URL
from uuid import UUID
from sqlalchemy import create_engine, text
from tqdm.asyncio import tqdm_asyncio
import tqdm
import io
import csv
from aiohttp import ClientError
from sqlalchemy.exc import IntegrityError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
)
import os
# Repo root (parent of data_collection) for db_config
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import db_config as config

# Configure logging
def setup_logging():
    """Set up comprehensive logging configuration"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging with both file and console handlers
    log_filename = os.path.join(log_dir, f'opt_stock_data_{datetime.now().strftime("%Y%m%d")}.log')
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # File handler - detailed logging
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Console handler - important messages only
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Set up logging
logger = setup_logging()

# Global constants
NAMESPACE_STOCK = UUID("233c16a9-0a91-4c9d-adda-8a496c63a1a3")
semaphore = asyncio.Semaphore(1)  # Control concurrency
DB_CONNECTION_STRING = config.DB_CONNECTION_STRING

logger.info("=== Starting NSE Options Data Collection ===")
logger.info(f"Database connection: {DB_CONNECTION_STRING.split('@')[1] if '@' in DB_CONNECTION_STRING else 'configured'}")


# Custom retry condition - only retry on 5xx errors, not 4xx
def should_retry_request(exception):
    """Only retry on server errors (5xx) or network issues, not client errors (4xx)"""
    if isinstance(exception, ClientError):
        # Check if it's a 4xx error (client error - don't retry)
        if hasattr(exception, 'status') and 400 <= exception.status < 500:
            return False
        # Retry on 5xx errors (server errors)
        return True
    return False


# Retry strategy for HTTP requests
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),  # Reduced from 10 to 3 attempts
    retry=should_retry_request,
)
async def fetch_data_with_retries(session, url):
    async with semaphore:
        try:
            logger.debug(f"Fetching data from: {url}")
            # CRITICAL FIX: Prevent URL encoding of pipe character (|) in instrument keys
            # aiohttp automatically encodes | to %7C which causes API to return 400
            # Use yarl.URL with encoded=True to preserve the pipe character
            url_obj = URL(url, encoded=True)
            
            async with session.get(url_obj) as response:
                # Handle 4xx errors gracefully (no data available)
                if response.status == 400:
                    logger.debug(f"No data available (400) for: {url}")
                    return None
                elif response.status == 404:
                    logger.debug(f"Instrument not found (404) for: {url}")
                    return None
                
                response.raise_for_status()  # Raise error for other bad status codes
                data = await response.json()
                logger.debug(f"Successfully fetched data from: {url}")
                return data
        except ClientError as e:
            # Log 4xx errors as debug (expected), 5xx as error (unexpected)
            if hasattr(e, 'status') and 400 <= e.status < 500:
                logger.debug(f"Client error (expected) for {url}: {e.status}")
                return None
            else:
                logger.error(f"Server error fetching {url}: {e}")
                raise
        except RetryError:
            logger.warning(f"Retries exhausted for {url}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return None


def query_to_dataframe(query, connection):
    """Fetch SQL query result into Pandas DataFrame."""
    try:
        logger.debug(f"Executing query: {query[:100]}...")
        result = pd.read_sql(query, connection)
        logger.debug(f"Query returned {len(result)} rows")
        return result
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


async def get_valid_instrument_tickdata(
    session, row, interval="1minute", fromDate="2025-01-01", toDate="2025-01-21"
):
    """Fetch historical candlestick data for a given instrument."""
    url = f"https://api.upstox.com/v2/historical-candle/{row.instrument_key}/{interval}/{toDate}/{fromDate}"
    try:
        logger.debug(f"Fetching tick data for instrument: {row.instrument_key} on {fromDate}")
        res = await fetch_data_with_retries(session, url)
        if not res or "data" not in res or "candles" not in res["data"]:
            logger.warning(f"No data received for instrument: {row.instrument_key}")
            return None

        df = pd.DataFrame(
            res["data"]["candles"],
            columns=[
                "time_stamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "open_interest",
            ],
        )
        if not df.empty:
            df["id"] = df.apply(
                lambda r: uuid.uuid5(NAMESPACE_STOCK, f"{row.id}{r.time_stamp}"), axis=1
            )
            df["instrument_id"] = row.id
            logger.debug(f"Successfully processed {len(df)} candles for {row.instrument_key}")
            return df
        else:
            logger.debug(f"Empty data for instrument: {row.instrument_key}")
            return None
    except Exception as e:
        logger.error(f"Error fetching data for {row.instrument_key}: {e}")
        return None


def generate_dates(start_date, holidays, end_date):
    """Generate valid trading dates, excluding weekends and holidays."""
    try:
        logger.info(f"Generating trading dates from {start_date} to {end_date}")
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        all_dates = pd.date_range(start=start_date, end=end_date)

        trading_dates = [
            d.strftime("%Y-%m-%d")
            for d in all_dates
            if d.weekday() < 5 and d.strftime("%Y-%m-%d") not in holidays
        ]
        
        logger.info(f"Generated {len(trading_dates)} trading dates")
        logger.debug(f"Trading dates: {trading_dates}")
        return trading_dates
    except Exception as e:
        logger.error(f"Error generating dates: {e}")
        raise


def convert_epoch_to_date(epoch_ms):
    """Convert epoch timestamp (ms) to 'YYYY-MM-DD'."""
    try:
        return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning(f"Error converting epoch {epoch_ms}: {e}")
        return None


async def process_instrument(instrument_df, session, date):
    """Fetch tick data for all instruments asynchronously for a given date."""
    logger.info(f"Processing {len(instrument_df)} instruments for date: {date}")
    start_time = time.time()
    
    tasks = [
        asyncio.create_task(
            get_valid_instrument_tickdata(session, row, "1minute", date, date)
        )
        for row in instrument_df.itertuples(index=False)
    ]

    valid_dfs = []
    successful_count = 0
    empty_count = 0
    
    for future in tqdm_asyncio(
        asyncio.as_completed(tasks), total=len(tasks), desc=f"Processing {date}"
    ):
        df = await future
        if df is not None and not df.empty:
            valid_dfs.append(df)
            successful_count += 1
        else:
            empty_count += 1

    processing_time = time.time() - start_time
    result_df = pd.concat(valid_dfs, ignore_index=True) if valid_dfs else pd.DataFrame()
    
    # Detailed summary
    total_instruments = len(tasks)
    success_rate = successful_count/total_instruments*100
    avg_records_per_instrument = len(result_df) / successful_count if successful_count > 0 else 0
    
    logger.info(f"Date {date} completed:")
    logger.info(f"  - Total instruments: {total_instruments}")
    logger.info(f"  - Successful: {successful_count} ({success_rate:.1f}%)")
    logger.info(f"  - Empty/No Data: {empty_count} ({empty_count/total_instruments*100:.1f}%)")
    logger.info(f"  - Total records: {len(result_df):,}")
    logger.info(f"  - Avg records/instrument: {avg_records_per_instrument:.0f}")
    logger.info(f"  - Processing time: {processing_time:.1f} seconds ({processing_time/60:.1f} minutes)")
    logger.info(f"  - Throughput: {total_instruments/processing_time:.1f} instruments/second")
    
    return result_df


# Remove the complex global tracking - keep only the simple approach

def get_id(x, tbl_stock):
    """Fetch stock ID based on trading symbol."""
    try:
        name = x.split()[0]
        if name in tbl_stock["name"].values:
            stock_id = tbl_stock.loc[tbl_stock["name"] == name, "id"].values[0]
            logger.debug(f"Found stock ID for {name}: {stock_id}")
            return stock_id
        else:
            logger.warning(f"No stock ID found for trading symbol: {name}")
            return None
    except Exception as e:
        logger.error(f"Error getting stock ID for {x}: {e}")
        return None


def write_to_sql_with_progress(
    df, table_name, engine, schema="options", chunksize=10000
):
    """Write a DataFrame to SQL database in chunks with progress tracking."""
    if df.empty:
        logger.warning(f"No data to write to {schema}.{table_name}")
        return
    
    logger.info(f"Writing {len(df)} records to {schema}.{table_name}")
    start_time = time.time()
    
    try:
        with engine.begin() as conn:
            for start in tqdm.tqdm(
                range(0, len(df), chunksize),
                total=(len(df) // chunksize) + 1,
                desc=f"Writing {table_name}",
            ):
                chunk = df.iloc[start : start + chunksize]
                chunk.to_sql(
                    table_name,
                    schema=schema,
                    con=conn,
                    if_exists="append",
                    index=True,
                    method="multi",
                )
                logger.debug(f"Written chunk {start//chunksize + 1}: {len(chunk)} records")
        
        write_time = time.time() - start_time
        records_per_second = len(df) / write_time if write_time > 0 else 0
        logger.info(f"Successfully wrote {len(df)} records to {schema}.{table_name}")
        logger.info(f"Write performance: {records_per_second:.0f} records/second")
        
    except Exception as e:
        logger.error(f"Error writing to {schema}.{table_name}: {e}")
        raise


def write_to_sql_postgres(df, table_name, engine, schema="options"):
    """High-speed bulk insert using PostgreSQL COPY command."""
    if df.empty:
        logger.warning(f"No data to write to {schema}.{table_name}")
        return
    
    logger.info(f"Bulk writing {len(df)} records to {schema}.{table_name}")
    start_time = time.time()
    
    try:
        with engine.begin() as conn:
            output = io.StringIO()
            df.to_csv(output, sep=",", index=False, header=False, quoting=csv.QUOTE_MINIMAL)
            output.seek(0)
            columns = ",".join(df.columns)
            sql = f"COPY {schema}.{table_name} ({columns}) FROM STDIN WITH CSV"

            with conn.connection.cursor() as cur:
                cur.copy_expert(sql, output)  # High-speed bulk insert
        
        write_time = time.time() - start_time
        records_per_second = len(df) / write_time if write_time > 0 else 0
        logger.info(f"Successfully bulk-wrote {len(df)} records to {schema}.{table_name}")
        logger.info(f"Bulk write performance: {records_per_second:.0f} records/second")
        
    except Exception as e:
        logger.error(f"Error bulk writing to {schema}.{table_name}: {e}")
        raise


def sync_instrument_to_ticker(engine):
    """Sync instrument_to_ticker table with new ticker data."""
    logger.info("Starting instrument_to_ticker sync...")
    start_time = time.time()

    query = text(
        """
        INSERT INTO options.instrument_to_ticker (instrument_id, ticker_id, stock_id, instrument_type, strike_price, expiry, trade_date)
        SELECT 
            i.id AS instrument_id,
            t.id AS ticker_id,
            i.stock_id,
            i.instrument_type,
            i.strike_price,
            i.expiry,
            DATE(t.time_stamp) AS trade_date
        FROM options.ticker t
        JOIN options.instrument i ON t.instrument_id = i.id
        LEFT JOIN options.instrument_to_ticker it ON t.id = it.ticker_id
        WHERE it.ticker_id IS NULL
    """
    )

    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            connection.commit()
            
            sync_time = time.time() - start_time
            logger.info(f"Inserted {result.rowcount} missing records in {sync_time:.2f} seconds")
            logger.info("Sync completed successfully!")
            
    except Exception as e:
        logger.error(f"Error during sync: {e}")
        raise


async def main(date_strs):
    """Main execution pipeline."""
    script_start_time = time.time()
    
    try:
        if len(date_strs) > 2:
            logger.info(f"Running program for date range: {date_strs[1]} to {date_strs[2]}")
        else:
            logger.info(f"Running program for single date: {date_strs[1]}")

        # Initialize database connection
        logger.info("Initializing database connection...")
        engine = create_engine(DB_CONNECTION_STRING)
        
        # Test database connection
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection verified successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

        # Update stock data and download instruments
        logger.info("Starting stock data update...")
        stock_updater(engine)
        
        logger.info("Starting instrument download...")
        instrument_downloader()

        # Load instrument & stock data
        logger.info("Loading stock and instrument data...")
        with engine.connect() as conn:
            tbl_stock = query_to_dataframe("SELECT * FROM options.stock", conn)
            logger.info(f"Loaded {len(tbl_stock)} stocks from database")

        instrument_file_path = "/home/cgraaaj/cgr-trades/python/NSE.json"
        logger.info(f"Loading instrument data from: {instrument_file_path}")
        
        with open(instrument_file_path, "r") as file:
            data = json.load(file)
            logger.info(f"Loaded {len(data)} instruments from NSE file")

        # Filter out irrelevant instruments
        logger.info("Filtering instruments...")
        pattern = re.compile(r"\d+NSETEST$")
        excluded_names = {"BANKNIFTY", "NIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}
        
        instrument_data = [
            item
            for item in data
            if item["segment"] == "NSE_FO"
            and item["name"] not in excluded_names
            and not pattern.search(item.get("name", ""))
        ]
        
        logger.info(f"Filtered to {len(instrument_data)} relevant instruments")
        logger.info(f"Excluded {len(data) - len(instrument_data)} instruments")

        # Define trading holidays
        nse_holidays_2025 = [
            "2025-01-26", "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-06",
            "2025-04-10", "2025-04-14", "2025-04-18", "2025-05-01", "2025-06-07",
            "2025-07-06", "2025-08-15", "2025-08-27", "2025-10-02", "2025-10-21",
            "2025-10-22", "2025-11-05", "2025-12-25",
        ]
        
        nse_holidays_2026 = [
            "2026-01-26",  # Republic Day
            "2026-03-03",  # Holi
            "2026-03-26",  # Shri Ram Navami
            "2026-03-31",  # Shri Mahavir Jayanti
            "2026-04-03",  # Good Friday
            "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
            "2026-05-01",  # Maharashtra Day
            "2026-05-28",  # Bakri Id
            "2026-06-26",  # Muharram
            "2026-09-14",  # Ganesh Chaturthi
            "2026-10-02",  # Mahatma Gandhi Jayanti
            "2026-10-20",  # Dussehra
            "2026-11-10",  # Diwali-Balipratipada
            "2026-11-24",  # Prakash Gurpurb Sri Guru Nanak Dev
            "2026-12-25",  # Christmas
        ]
        
        # Generate trading dates
        if len(date_strs) > 2:
            dates = generate_dates(date_strs[1], nse_holidays_2026, date_strs[2])
        else:
            dates = generate_dates(date_strs[1], nse_holidays_2026, date_strs[1])

        # Process instrument data
        logger.info("Processing instrument data...")
        instrument_df = pd.DataFrame(instrument_data)
        
        # Add stock IDs
        logger.debug("Mapping trading symbols to stock IDs...")
        instrument_df["stock_id"] = instrument_df["trading_symbol"].apply(
            lambda x: get_id(x, tbl_stock)
        )
        
        # Remove instruments without valid stock IDs
        instruments_before = len(instrument_df)
        instrument_df = instrument_df.dropna(subset=['stock_id'])
        instruments_after = len(instrument_df)
        
        if instruments_before != instruments_after:
            logger.warning(f"Removed {instruments_before - instruments_after} instruments without valid stock IDs")
        
        # Generate instrument IDs and process expiry dates
        instrument_df["id"] = instrument_df.apply(
            lambda r: uuid.uuid5(NAMESPACE_STOCK, f"{r.stock_id}{r.trading_symbol}"), axis=1
        )
        instrument_df["expiry"] = instrument_df["expiry"].apply(convert_epoch_to_date)
        
        logger.info(f"Processed {len(instrument_df)} instruments successfully")

        # Fetch tick data for all dates
        logger.info(f"Starting tick data collection for {len(dates)} trading dates...")
        
        async with aiohttp.ClientSession() as session:
            tasks = [process_instrument(instrument_df, session, date) for date in dates]
            results = await asyncio.gather(*tasks)

        # Combine all results
        ticker_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        logger.info(f"Collected {len(ticker_df)} total ticker records")

        # Store new instruments
        logger.info("Storing instrument data...")
        existing_ids = pd.read_sql("SELECT id FROM options.instrument", engine)["id"]
        instrument_df_filtered = instrument_df[~instrument_df["id"].isin(existing_ids)]
        instrument_df_filtered.set_index("id", inplace=True)

        if not instrument_df_filtered.empty:
            instrument_df_filtered.to_sql(
                "instrument", schema="options", con=engine, if_exists="append", index=True
            )
            logger.info(f"Stored {len(instrument_df_filtered)} new instruments to database")
        else:
            logger.info("No new instruments to store")

        # Store ticker data
        if not ticker_df.empty:
            logger.info("Storing ticker data...")
            ticker_df.set_index("id", inplace=True)
            write_to_sql_with_progress(ticker_df, "ticker", engine)
        else:
            logger.warning("No ticker data to store")

        # Wait before sync (original behavior)
        logger.info("Waiting 10 seconds before sync...")
        time.sleep(10)

        # Sync instrument to ticker relationships
        sync_instrument_to_ticker(engine)
        
        # Calculate and log execution metrics
        total_time = time.time() - script_start_time
        logger.info("=== Execution Summary ===")
        logger.info(f"Total execution time: {total_time / 60:.2f} minutes")
        logger.info(f"Processed {len(dates)} trading dates")
        logger.info(f"Processed {len(instrument_df)} instruments")
        logger.info(f"Collected {len(ticker_df)} ticker records")
        logger.info("=== Script completed successfully ===")
        
    except Exception as e:
        logger.error(f"Critical error in main execution: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


def load_nse_instrument_keys(nse_file_path: str = "/home/cgraaaj/cgr-trades/python/NSE.json") -> dict:
    """Load NSE.json data and create stock name to instrument key mapping."""
    try:
        logger.info(f"Loading NSE instrument keys from: {nse_file_path}")
        
        if not os.path.exists(nse_file_path):
            logger.warning(f"NSE file not found: {nse_file_path}")
            return {}
        
        with open(nse_file_path, 'r') as file:
            nse_data = json.load(file)
        
        logger.info(f"Loaded {len(nse_data)} instruments from NSE.json")
        
        # Create mapping of stock name to instrument key
        stock_to_instrument_key = {}
        matched_count = 0
        
        for instrument in nse_data:
            # Filter criteria: NSE_EQ segment and EQ instrument_type
            if (instrument.get('segment') == 'NSE_EQ' and 
                instrument.get('instrument_type') == 'EQ'):
                
                stock_name = instrument.get('trading_symbol')
                instrument_key = instrument.get('instrument_key')
                
                if stock_name and instrument_key:
                    stock_to_instrument_key[stock_name] = instrument_key
                    matched_count += 1
        
        logger.info(f"Created instrument key mapping for {matched_count} NSE_EQ stocks")
        return stock_to_instrument_key
        
    except Exception as e:
        logger.warning(f"Error loading NSE instrument keys: {e}")
        return {}


def instrument_downloader():
    """Download and save NSE instrument data."""
    logger.info("Starting instrument download from Upstox...")
    start_time = time.time()
    
    # URL of the .gz file
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

    try:
        # Download the .gz file
        logger.info(f"Downloading from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Check if the request was successful
        
        download_size = len(response.content)
        logger.info(f"Downloaded {download_size} bytes successfully")

        # Decompress the .gz file
        logger.debug("Decompressing data...")
        with gzip.open(io.BytesIO(response.content), "rt", encoding="utf-8") as gz_file:
            # Load JSON data
            data = json.load(gz_file)

        logger.info(f"Decompressed {len(data)} instrument records")

        # Save the JSON data to a file
        output_path = "/home/cgraaaj/cgr-trades/python/NSE.json"
        logger.info(f"Saving to: {output_path}")
        
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4)

        download_time = time.time() - start_time
        logger.info(f"Instrument download completed in {download_time:.2f} seconds")
        
    except requests.RequestException as e:
        logger.error(f"Error downloading instrument data: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON data: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during instrument download: {e}")
        raise


@retry(
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
    before_sleep=lambda retry_state: logger.warning(
        f"Attempt {retry_state.attempt_number} failed, retrying in {retry_state.next_action.sleep} seconds..."
    ),
)
def _fetch_nse_data_with_retry(session, url, headers, base_url):
    """Fetch NSE data with retry logic for 401 errors."""
    try:
        # Make the request using the session
        response = session.get(url, headers=headers, timeout=30)
        
        # Check if the request was successful
        response.raise_for_status()
        logger.info(f"Successfully fetched stock data (status: {response.status_code})")
        
        return response
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logger.warning(f"Got 401 Unauthorized, refreshing session and retrying...")
            
            # Clear existing cookies and session
            session.cookies.clear()
            
            # Re-establish session by accessing home page first
            logger.debug("Re-establishing session with NSE home page...")
            home_response = session.get(base_url, headers=headers, timeout=30)
            logger.debug(f"Home page response status: {home_response.status_code}")
            
            # Wait a bit to simulate browser behavior
            time.sleep(2)
            
            # Now activate the session by hitting option-chain URL (this is the key!)
            logger.debug("Activating session with option-chain URL...")
            option_chain_url = f"{base_url}/option-chain"
            option_response = session.get(option_chain_url, headers=headers, timeout=30)
            logger.debug(f"Option-chain response status: {option_response.status_code}")
            
            # Wait a bit more to simulate browser behavior
            time.sleep(2)
            
            # Update headers with new cookies
            cookies_dict = session.cookies.get_dict()
            if cookies_dict:
                cookies_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
                headers["Cookie"] = cookies_str
                logger.debug(f"Updated session with {len(cookies_dict)} fresh cookies")
            
        # Re-raise the exception to trigger retry
        raise


def stock_updater(engine):
    """Update stock data from NSE API."""
    logger.info("Starting stock data update from NSE...")
    start_time = time.time()
    
    # Load NSE instrument keys mapping
    logger.info("Loading NSE instrument keys mapping...")
    stock_to_instrument_key = load_nse_instrument_keys()
    logger.info(f"Loaded instrument keys for {len(stock_to_instrument_key)} stocks")
    
    # Base URL for NSE
    base_url = "https://www.nseindia.com"

    # Create a session to maintain cookies
    session = requests.Session()

    # Comprehensive headers to mimic a browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    try:
        # First, access the home page to get initial cookies
        logger.debug("Accessing NSE home page to establish session...")
        home_response = session.get(base_url, headers=headers, timeout=30)
        logger.debug(f"Home page response status: {home_response.status_code}")

        # Wait a moment to simulate browser behavior
        time.sleep(2)
        
        # Activate the session by hitting option-chain URL (this is crucial for API access)
        logger.debug("Activating session with option-chain URL...")
        option_chain_url = f"{base_url}/option-chain"
        option_response = session.get(option_chain_url, headers=headers, timeout=30)
        logger.debug(f"Option-chain response status: {option_response.status_code}")
        
        # Wait a bit more to simulate browser behavior
        time.sleep(2)

        # Add cookies to request headers
        cookies_dict = session.cookies.get_dict()
        if cookies_dict:
            cookies_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
            headers["Cookie"] = cookies_str
            logger.debug(f"Added {len(cookies_dict)} cookies to session")

        # URL for stock symbols
        url = "https://www.nseindia.com/api/master-quote"
        logger.info(f"Fetching stock symbols from: {url}")

        # Fetch NSE data with retry logic
        response = _fetch_nse_data_with_retry(session, url, headers, base_url)

        # Parse the JSON response
        stock_symbols = json.loads(response.text)
        stock_symbols = sorted(stock_symbols, key=len, reverse=True)
        logger.info(f"Retrieved {len(stock_symbols)} stock symbols from NSE")

        # Create DataFrame with stock data including instrument keys
        df = pd.DataFrame(
            {"name": stock_symbols, "nifty_fifty_index": 0, "is_active": True}
        )
        df["id"] = [uuid.uuid5(NAMESPACE_STOCK, s) for s in stock_symbols]
        
        # Add instrument keys from NSE mapping
        df["instrument_key"] = df["name"].map(stock_to_instrument_key)
        
        # Log instrument key statistics
        stocks_with_keys = df["instrument_key"].notna().sum()
        stocks_without_keys = len(df) - stocks_with_keys
        logger.info(f"Instrument key mapping results:")
        logger.info(f"  Stocks with instrument keys: {stocks_with_keys}")
        logger.info(f"  Stocks without instrument keys: {stocks_without_keys}")
        
        logger.debug("Generated UUIDs and mapped instrument keys for all stocks")

        # Fetch all existing stocks from the DB
        logger.info("Fetching existing stocks from database...")
        allStocks = pd.read_sql_query(
            "SELECT id, name, instrument_key, is_active FROM options.stock", con=engine
        )
        logger.info(f"Found {len(allStocks)} existing stocks in database")

        # Identify different categories of stocks
        newStocks = df[~df["id"].isin(allStocks["id"])]
        oldStocks = allStocks[~allStocks["id"].isin(df["id"]) & allStocks["is_active"]]
        existingStocks = allStocks[
            allStocks["id"].isin(df["id"]) & ~allStocks["is_active"]
        ]
        
        # Identify stocks that need instrument key updates
        activeExistingStocks = allStocks[allStocks["id"].isin(df["id"]) & allStocks["is_active"]]
        stocksNeedingKeyUpdate = []
        
        for _, existing_stock in activeExistingStocks.iterrows():
            stock_name = existing_stock["name"]
            current_key = existing_stock["instrument_key"]
            
            # Get the correct instrument key from NSE data
            if stock_name in stock_to_instrument_key:
                correct_key = stock_to_instrument_key[stock_name]
                
                # Check if update is needed (missing or mismatched key)
                if pd.isna(current_key) or current_key != correct_key:
                    stocksNeedingKeyUpdate.append({
                        "id": existing_stock["id"],
                        "name": stock_name,
                        "current_key": current_key,
                        "correct_key": correct_key,
                        "action": "ADD" if pd.isna(current_key) else "CORRECT"
                    })

        logger.info(f"Stock analysis:")
        logger.info(f"  - New stocks to add: {len(newStocks)}")
        logger.info(f"  - Stocks to reactivate: {len(existingStocks)}")
        logger.info(f"  - Stocks to deactivate: {len(oldStocks)}")
        logger.info(f"  - Existing stocks needing instrument key updates: {len(stocksNeedingKeyUpdate)}")

        # Execute updates in a single transaction
        with engine.connect() as conn:
            changes_made = 0
            
            # Update instrument keys for existing stocks
            if stocksNeedingKeyUpdate:
                logger.info(f"Updating instrument keys for {len(stocksNeedingKeyUpdate)} existing stocks...")
                key_updates_made = 0
                
                for stock_update in stocksNeedingKeyUpdate:
                    try:
                        result = conn.execute(
                            text(
                                "UPDATE options.stock SET instrument_key = :instrument_key, updated_on = now() WHERE id = :stock_id"
                            ),
                            {
                                "instrument_key": stock_update["correct_key"],
                                "stock_id": stock_update["id"]
                            }
                        )
                        
                        if result.rowcount > 0:
                            action_desc = "Added missing" if stock_update["action"] == "ADD" else "Corrected"
                            logger.debug(f"  {action_desc} instrument key for {stock_update['name']}: {stock_update['correct_key']}")
                            key_updates_made += 1
                            
                    except Exception as e:
                        logger.error(f"Error updating instrument key for {stock_update['name']}: {e}")
                
                logger.info(f"Successfully updated instrument keys for {key_updates_made} stocks")
                changes_made += key_updates_made
            
            # Reactivate inactive stocks that reappear
            if not existingStocks.empty:
                conn.execute(
                    text(
                        "UPDATE options.stock SET is_active = True, updated_on = now() WHERE id IN :stock_ids"
                    ),
                    {"stock_ids": tuple(existingStocks["id"])},
                )
                logger.info(f"Reactivated {len(existingStocks)} stocks: {existingStocks['name'].tolist()[:10]}...")
                changes_made += len(existingStocks)

            # Insert new stocks
            if not newStocks.empty:
                newStocks.to_sql(
                    "stock",
                    schema="options",
                    if_exists="append",
                    con=engine,
                    index=False,
                )
                logger.info(f"Added {len(newStocks)} new stocks: {newStocks['name'].tolist()[:10]}...")
                changes_made += len(newStocks)

            # Deactivate stocks that are no longer present
            if not oldStocks.empty:
                conn.execute(
                    text(
                        "UPDATE options.stock SET is_active = False, updated_on = now() WHERE id IN :stock_ids"
                    ),
                    {"stock_ids": tuple(oldStocks["id"])},
                )
                logger.info(f"Deactivated {len(oldStocks)} stocks: {oldStocks['name'].tolist()[:10]}...")
                changes_made += len(oldStocks)

            conn.commit()
            
            update_time = time.time() - start_time
            logger.info(f"Stock update completed in {update_time:.2f} seconds")
            logger.info(f"Total changes made: {changes_made}")

    except requests.RequestException as e:
        logger.error(f"Network error fetching stock data: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response content: {e.response.text[:500]}...")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in stock_updater: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


if __name__ == "__main__":
    start_time = time.time()
    
    try:
        # Parse command line arguments
        if len(sys.argv) > 1:
            date_args = sys.argv
            logger.info(f"Command line arguments: {date_args[1:]}")
        else:
            date_args = ["script_name", datetime.today().strftime("%Y-%m-%d")]
            logger.info(f"No arguments provided, using today's date: {date_args[1]}")
        
        # Run main function
        asyncio.run(main(date_args))
        
        # Log final execution time
        total_execution_time = (time.time() - start_time) / 60
        logger.info(f"=== SCRIPT COMPLETED SUCCESSFULLY ===")
        logger.info(f"Total execution time: {total_execution_time:.2f} minutes")
        
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)
