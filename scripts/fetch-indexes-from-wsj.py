#!/usr/bin/env python3
import argparse
import json
import sys
import requests
import os
import logging
from datetime import datetime
from urllib.parse import quote

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)
    ]
)

def parse_args():
    parser = argparse.ArgumentParser(description='Fetch time series data and output as CSV')
    parser.add_argument('token', help='Entitlement token for API access')
    return parser.parse_args()

def read_keys():
    keys = [line.strip() for line in sys.stdin if line.strip()]
    logging.info(f"Read {len(keys)} keys from stdin")
    return keys

def get_timestamp(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').timestamp() * 1000)

def build_url(key, token):
    base_url = 'https://api.wsj.net/api/michelangelo/timeseries/history'
    start_date = get_timestamp('1980-01-01')
    end_date = get_timestamp(datetime.now().strftime('%Y-%m-%d'))
    params = {
        "Step": "P1D",
        "TimeFrame": "all",
        "StartDate": start_date,
        "EndDate": end_date,
        "EntitlementToken": token,
        "IncludeMockTick": True,
        "FilterNullSlots": False,
        "FilterClosedPoints": True,
        "IncludeClosedSlots": False,
        "IncludeOfficialClose": True,
        "InjectOpen": False,
        "ShowPreMarket": False,
        "ShowAfterHours": False,
        "UseExtendedTimeFrame": True,
        "WantPriorClose": False,
        "IncludeCurrentQuotes": False,
        "ResetTodaysAfterHoursPercentChange": False,
        "Series": [{
            "Key": key,
            "Dialect": "Charting",
            "Kind": "Ticker",
            "SeriesId": "s1",
            "DataTypes": ["Last"]
        }]
    }
    encoded_params = quote(json.dumps(params))
    url = f"{base_url}?json={encoded_params}&ckey={quote(token[:10])}"
    logging.info(f"Built URL: {url}")
    return url

def fetch_data(key, token):
    url = build_url(key, token)
    logging.info(f"Fetching data for key: {key} from {url}")
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Dylan2010.EntitlementToken': token
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data for {key} from {url}: {str(e)}")
        raise

def process_data(data):
    ticks = data['TimeInfo']['Ticks']
    values = data['Series'][0]['DataPoints']
    results = []
    for tick, value_list in zip(ticks, values):
        value = value_list[0]
        if value is not None:
            date = datetime.fromtimestamp(tick / 1000).strftime('%Y-%m-%d')
            results.append((date, value))
    logging.info(f"Processed {len(results)} data points")
    return results

def to_kebab_case(text):
    return text.lower().replace(' ', '-').replace('/', '-')

def write_index_csv(full_name, data_points):
    os.makedirs('facts/indexes', exist_ok=True)
    filename = f"facts/indexes/{to_kebab_case(full_name)}.csv"
    logging.info(f"Writing data to {filename}")
    with open(filename, 'w') as f:
        f.write("date,value\n")
        for date, value in data_points:
            f.write(f"{date},{value}\n")

def get_currency(full_name):
    if "USD" in full_name:
        return "USD"
    elif "EUR" in full_name:
        return "EUR"
    elif "eur" in full_name.lower():
        return "EUR"
    elif "S&P" in full_name:
        return "USD"
    else:
        return 'Unknown'

def update_indexes_csv(key, data, data_points):
    os.makedirs('facts/indexes', exist_ok=True)
    filename = 'facts/indexes.csv'
    series = data['Series'][0]
    full_name = series['CommonName']
    name = to_kebab_case(full_name)
    code = series['Ticker']
    currency = get_currency(full_name)
    earliest_date = data_points[0][0] if data_points else 'Unknown'
    url = f"https://www.wsj.com/market-data/quotes/index/{key}"
    
    existing_data = {}
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            lines = f.readlines()
            if lines:
                header = lines[0].strip()
                for line in lines[1:]:
                    parts = line.strip().split(',')
                    existing_data[parts[0]] = line.strip()
    
    logging.info(f"Updating indexes metadata in {filename}")
    with open(filename, 'w') as f:
        f.write("name,full-name,code,currency,earliest_date,url\n")
        for existing_name, line in existing_data.items():
            if existing_name != name:
                f.write(f"{line}\n")
        f.write(f"{name},{full_name},{code},{currency},{earliest_date},{url}\n")

def main():
    args = parse_args()
    keys = read_keys()
    for key in keys:
        try:
            data = fetch_data(key, args.token)
            data_points = process_data(data)
            full_name = data['Series'][0]['CommonName']
            write_index_csv(full_name, data_points)
            update_indexes_csv(key, data, data_points)
        except Exception as e:
            logging.error(f"Error processing key {key}: {str(e)}")
            continue

if __name__ == "__main__":
    main()
