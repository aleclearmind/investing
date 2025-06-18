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
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch time series data and output as CSV"
    )
    return parser.parse_args()


def read_keys():
    keys = [tuple(line.strip().split(",")) for line in sys.stdin if line.strip()]
    logging.info(f"Read {len(keys)} keys from stdin")
    return keys


def get_timestamp(date_str):
    return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000)


def fetch_data(name, key):
    url = f"https://www.zonebourse.com/mods_a/charts/TV/function/history?from=0&to=1750330800&symbol={key}&resolution=D&requestType=GET&src=itfp"
    logging.info(f"Fetching data for {name} (key: {key}) from {url}")

    try:
        response = requests.get(url, headers={"User-Agent": "curl/8.12.1"})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data for {key} from {url}: {str(e)}")
        raise


def process_data(data):
    results = []
    for timestamp, price in zip(data["t"], data["c"]):
        results.append((datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'), price))
    logging.info(f"Processed {len(results)} data points")
    return results


def to_kebab_case(text):
    return text.lower().replace(" ", "-").replace("/", "-")


def write_index_csv(full_name, data_points):
    os.makedirs("facts/indexes", exist_ok=True)
    filename = f"facts/indexes/{to_kebab_case(full_name)}.csv"
    logging.info(f"Writing data to {filename}")
    with open(filename, "w") as f:
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
        return "Unknown"


def update_indexes_csv(full_name, key, data_points):
    os.makedirs("facts/indexes", exist_ok=True)
    filename = "facts/indexes.csv"
    name = to_kebab_case(full_name)
    code = key
    currency = "USD"
    earliest_date = data_points[0][0] if data_points else "Unknown"
    url = f"https://www.zonebourse.com/cours/indice/WHATEVER-{key}/"

    existing_data = {}
    if os.path.exists(filename):
        with open(filename, "r") as f:
            lines = f.readlines()
            if lines:
                header = lines[0].strip()
                for line in lines[1:]:
                    parts = line.strip().split(",")
                    existing_data[parts[0]] = line.strip()

    logging.info(f"Updating indexes metadata in {filename}")
    with open(filename, "w") as f:
        f.write("name,full-name,code,currency,earliest_date,url\n")
        for existing_name, line in existing_data.items():
            if existing_name != name:
                f.write(f"{line}\n")
        f.write(f"{name},{full_name},{code},{currency},{earliest_date},{url}\n")


def main():
    args = parse_args()
    keys = read_keys()
    for full_name, key in keys:
        try:
            data = fetch_data(full_name, key)
            data_points = process_data(data)
            write_index_csv(full_name, data_points)
            update_indexes_csv(full_name, key, data_points)
        except Exception as e:
            logging.error(f"Error processing key {key}: {str(e)}")
            continue


if __name__ == "__main__":
    main()
