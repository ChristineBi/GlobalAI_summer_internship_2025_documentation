import os
import requests
import pandas as pd
import time
from tqdm import tqdm

API_TOKEN = '5f3afd582bd7b4.95720069'
BASE_URL = 'https://eodhd.com/api'

FINANCIAL_DATA_FILE = "financial_reports.csv"
FAILED_TICKERS_FILE = "failed_tickers.txt"
PROGRESS_TRACKER_FILE = "progress_tracker.txt"

def load_progress(): 
    #Load progress from previous runs
    processed_tickers = set()
    failed_tickers = set()
    
    if os.path.exists(PROGRESS_TRACKER_FILE):
        with open(PROGRESS_TRACKER_FILE, 'r') as f:
            processed_tickers = set(line.strip() for line in f if line.strip())
    
    if os.path.exists(FAILED_TICKERS_FILE):
        with open(FAILED_TICKERS_FILE, 'r') as f:
            failed_tickers = set(line.strip() for line in f if line.strip())
    
    return processed_tickers, failed_tickers

def update_progress(ticker): 
    #Record a successfully processed ticker
    with open(PROGRESS_TRACKER_FILE, 'a') as f:
        f.write(ticker + "\n")

def update_failed(ticker): 
    #Record a failed ticker
    with open(FAILED_TICKERS_FILE, 'a') as f:
        f.write(ticker + "\n")

def save_data(data, initial_run=False): 
    #Also handles initial vs resumed runs with proper column alignment
    df = pd.DataFrame(data)
    
    if initial_run or not os.path.exists(FINANCIAL_DATA_FILE):
        df.to_csv(FINANCIAL_DATA_FILE, index=False)
    else:
        existing_df = pd.read_csv(FINANCIAL_DATA_FILE)
        
        for col in existing_df.columns:
            if col not in df.columns:
                df[col] = None 
        
        df = df[existing_df.columns]
        df.to_csv(FINANCIAL_DATA_FILE, mode='a', header=False, index=False)

def fetch_all_exchanges():
    url = f"{BASE_URL}/exchanges-list"
    params = {"api_token": API_TOKEN, "fmt": "json"}
    r = requests.get(url, params=params)
    r.raise_for_status()
    return [ex["Code"] for ex in r.json() if "Code" in ex]

def fetch_symbols_from_exchange(exchange_code):
    url = f"{BASE_URL}/exchange-symbol-list/{exchange_code}"
    params = {
        "api_token": API_TOKEN,
        "fmt": "json",
        "delisted": "0"
    }
    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        symbols = r.json()
        return [
            f"{s['Code']}.{exchange_code}"
            for s in symbols
            if s.get("Type") == "Common Stock" and s.get("Code")
        ]
    except Exception as e:
        print(f"Failed to fetch symbols from {exchange_code}: {e}")
        return []

def fetch_annual_reports(ticker):
    url = f"{BASE_URL}/fundamentals/{ticker}"
    params = {
        "filter": "Financials::Balance_Sheet::yearly,Financials::Income_Statement::yearly,Financials::Cash_Flow::yearly",
        "api_token": API_TOKEN,
        "fmt": "json"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    return {}

def process_company_data(ticker, reports, years_to_include=6):
    flat_data = {"Ticker": ticker}
    year_data = []

    for section, yearly_data in reports.items():
        for date_str, metrics in yearly_data.items():
            try:
                report_year = int(date_str[:4])
                year_data.append((section, report_year, metrics))
            except ValueError:
                continue

    unique_years = sorted(set([y for _, y, _ in year_data]), reverse=True)[:years_to_include]

    for section, year, metrics in year_data:
        if year in unique_years:
            for field, value in metrics.items():
                col_name = f"{field}_{year}"
                try:
                    flat_data[col_name] = float(value)
                except:
                    continue

    return flat_data

def main():
    print("Fetching list of global exchanges...")
    exchanges = fetch_all_exchanges()
    print(f"Found {len(exchanges)} exchanges.")

    # Load progress from previous runs
    processed_tickers, failed_tickers = load_progress()
    print(f"Resuming from previous run - {len(processed_tickers)} tickers already processed")
    print(f"{len(failed_tickers)} tickers previously failed")

    all_data = []
    new_failed = []
    initial_run = not os.path.exists(FINANCIAL_DATA_FILE)

    try:
        for exchange in exchanges:
            print(f"\nFetching symbols from {exchange} exchange...")
            tickers = fetch_symbols_from_exchange(exchange)
            print(f"Found {len(tickers)} common stock tickers")

            # Filter out already processed/failed tickers
            tickers_to_process = [
                t for t in tickers 
                if t not in processed_tickers 
                and t not in failed_tickers
            ]
            print(f"{len(tickers_to_process)} new tickers to process")

            if not tickers_to_process:
                continue

            for ticker in tqdm(tickers_to_process, desc=f"Processing {exchange}"):
                try:
                    reports = fetch_annual_reports(ticker)
                    if reports:
                        flat = process_company_data(ticker, reports)
                        if flat:
                            all_data.append(flat)
                            update_progress(ticker)
                            
                            # Save periodically
                            if len(all_data) % 50 == 0:
                                save_data(all_data, initial_run)
                                all_data = []
                                initial_run = False
                    
                    time.sleep(1)  # Rate limiting
                except Exception as e:
                    print(f"\nError with {ticker}: {str(e)[:100]}...")
                    update_failed(ticker)
                    new_failed.append(ticker)

    except KeyboardInterrupt:
        print("\nUser interrupted - saving progress...")

    finally:
        # Save remaining data
        if all_data:
            save_data(all_data, initial_run)
        
        if new_failed:
            print(f"\n{len(new_failed)} new tickers failed during this run")
        
        print("\nScript completed")
        print(f"Data saved to {FINANCIAL_DATA_FILE}")
        print(f"Progress saved for next run")

if __name__ == "__main__":
    main()
