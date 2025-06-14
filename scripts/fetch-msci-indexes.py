#!/usr/bin/env python3

import sys
import os
import csv
import re
import glob
import logging
import requests
from datetime import datetime

INDEX_URL = "https://www.msci.com/indexes/index/"
API_URL = "https://www.msci.com/indexes/api/index/performance"
OUTPUT_DIR = "facts/indexes"
SUMMARY_CSV = "facts/indexes.csv"
DATE_FORMAT = "%Y-%m-%d"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def kebab_case(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def fetch_index_data(index_code, end_date):
    logging.info(f"Fetching data for index code {index_code}...")
    params = {
        "currency": "USD",
        "indexCode": index_code,
        "variant": "GRTR",
        "frequency": "daily",
        "baseValue100": "false",
        "startDate": "1957-11-16",
        "endDate": end_date,
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()


def write_summary_row(
    index_summary, normalized_name, index_name, index_code, currency, earliest_date, url
):
    logging.info(f"Adding entry to summary for index {index_name} ({index_code})...")
    with open(SUMMARY_CSV, "a", newline="") as summary_file:
        writer = csv.writer(summary_file)
        writer.writerow(
            [normalized_name, index_name, index_code, currency, earliest_date, url]
        )
        index_summary.add(str(index_code))


def write_performance_csv(index_data, filename):
    filepath = os.path.join(OUTPUT_DIR, filename)
    logging.info(f"Writing performance history to {filepath}...")
    with open(filepath, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["date", "value"])
        for entry in index_data:
            writer.writerow([entry["date"], entry["value"]])


def load_existing_index_codes():
    pattern = os.path.join(OUTPUT_DIR, "*-*.csv")
    existing_files = glob.glob(pattern)
    existing_codes = set()
    for f in existing_files:
        basename = os.path.basename(f)
        match = re.search(r"-([0-9]+)\.csv$", basename)
        if match:
            existing_codes.add(match.group(1))
    return existing_codes


def load_existing_summary_index_codes():
    if not os.path.exists(SUMMARY_CSV):
        with open(SUMMARY_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["name", "full-name", "code", "currency", "earliest_date", "url"]
            )
        return set()
    with open(SUMMARY_CSV, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        return {row[2] for row in reader}


def collect_indexes(index_codes):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    today = datetime.today().strftime(DATE_FORMAT)

    existing_files = load_existing_index_codes()
    existing_summary = load_existing_summary_index_codes()

    for code in index_codes:
        str_code = str(code)
        if str_code in existing_files:
            logging.info(
                f"Skipping index code {code} — performance file already exists."
            )
            continue

        try:
            data = fetch_index_data(code, today)
        except Exception as e:
            logging.error(f"Failed to fetch data for index code {code}: {e}")
            continue

        index_info = data["data"]["indexes"][0]
        index_name = index_info["indexName"]
        currency = data["data"]["currency"]
        performance_history = index_info["performanceHistory"]

        # Extract the earliest date from performance history
        earliest_date = (
            min(entry["date"] for entry in performance_history)
            if performance_history
            else "N/A"
        )

        normalized_name = kebab_case(index_name) + f"-{code}"
        filename = f"{normalized_name}.csv"

        if str_code not in existing_summary:
            write_summary_row(
                existing_summary,
                normalized_name,
                index_name,
                code,
                currency,
                earliest_date,
                f"{INDEX_URL}/{code}",
            )
        else:
            logging.info(
                f"Index code {code} already in summary. Skipping summary update."
            )

        write_performance_csv(performance_history, filename)


def main():
    input_codes = [line.strip() for line in sys.stdin if line.strip()]
    index_codes = [int(code) for code in input_codes]
    collect_indexes(index_codes)


if __name__ == "__main__":
    main()
