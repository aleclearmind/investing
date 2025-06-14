#!/usr/bin/env python3

import csv
import requests
import json
from datetime import datetime
import urllib.parse
import argparse
import sys
import logging
import re

# Dictionaries for mapping numeric values to strings
REPLICATION_METHODS = {1: "Direct", 2: "Indirect", 3: "Other"}

REPLICATION_MODELS = {
    1: "FullReplication",
    2: "OptimizedSampling",
    3: "UnfundedSwap",
    4: "FuturesOrForwards",
    5: "CombinationOfUnfundedAndFundedSwap",
    6: "PhysicallyBacked",
    7: "Unknown",
    8: "ETFBased",
    9: "Other",
}

DIVIDEND_POLICIES = {
    1: "Distribution",
    2: "Capitalization",
    3: "CD",
    4: "NoIncome",
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


def fetch_benchmark_data(full_name, index_name):
    # Remove trailing " (${CURRENCY})" using regex
    cleaned_name = re.sub(r"\s+([A-Z]{3})$", "", full_name.strip()).replace("&", "$26x")
    encoded_name = urllib.parse.quote(cleaned_name)
    url = f"https://www.trackinsight.com/search-api/search_v2/_/benchmark={encoded_name}/USD$3axaum,EUR$3axflow1m,EUR$3axflowYtd,currency,currencyHedged,dividendPolicyId,esg_grade,esg_release,expense_ratio,exposure_description,id,isin,perf1m,perfYtd,rating,shareClasses,shareLabel,ticker,ucits,provider,replication_method,replication_model,creationDate/default/0/100"
    logger.info(f"Requesting benchmark data for index {index_name}: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        docs = data.get("results", {}).get("docs", [])
        logger.info(
            f"Received {len(docs)} entries from benchmark data for index {index_name}"
        )
        logger.debug(
            f"Benchmark response for index {index_name}: {json.dumps(data, indent=2)}"
        )
        return docs
    except requests.RequestException as e:
        logger.error(
            f"Failed to fetch benchmark data for {full_name} (index {index_name}): {e}"
        )
        return []


def fetch_tracking_data(ticker, index_name):
    encoded_ticker = urllib.parse.quote(ticker)
    url = f"https://www.trackinsight.com/data-api/funds/{encoded_ticker}/td.json"
    logger.info(f"Requesting tracking data for {ticker} (index {index_name}): {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data is None:
            logger.info(
                f"Received null tracking data for {ticker} (index {index_name})"
            )
            return {}
        logger.info(f"Received tracking data for {ticker} (index {index_name})")
        logger.debug(
            f"Tracking response for {ticker} (index {index_name}): {json.dumps(data, indent=2)}"
        )
        return data
    except requests.RequestException as e:
        logger.error(
            f"Failed to fetch tracking data for {ticker} (index {index_name}): {e}"
        )
        return {}


def fetch_fund_description(ticker, index_name):
    encoded_ticker = urllib.parse.quote(ticker)
    url = f"https://www.trackinsight.com/data-api/funds/{encoded_ticker}.json"
    logger.info(f"Requesting fund description for {ticker} (index {index_name}): {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Received fund description for {ticker} (index {index_name})")
        logger.debug(
            f"Description response for {ticker} (index {index_name}): {json.dumps(data, indent=2)}"
        )
        return data
    except requests.RequestException as e:
        logger.error(
            f"Failed to fetch description for {ticker} (index {index_name}): {e}"
        )
        return {}


def convert_epoch_to_date(epoch):
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d")


def map_value(value, mapping_dict):
    """Convert numeric value to string if in dictionary, otherwise return original value"""
    return mapping_dict.get(value, value)


def process_isin(isin_str, index_name, ticker):
    if not isin_str:
        return None

    # Split the ISIN string by semicolon
    isins = isin_str.split(";")

    # Filter out ISINs starting with CA, JP, US, or ZA
    filtered_isins = [
        isin
        for isin in isins
        if not isin.startswith(("CA", "JP", "US", "ZA", "HK", "NZ", "KR", "AU", "XS"))
    ]

    if len(filtered_isins) > 1:
        logger.error(
            f"Multiple valid ISINs found for {ticker} (index {index_name}): {filtered_isins}"
        )
        print(
            f"Multiple valid ISINs found for {ticker} (index {index_name}): {filtered_isins}",
            file=sys.stderr,
        )
        sys.exit(1)
    elif len(filtered_isins) == 0:
        logger.info(
            f"No valid ISINs after filtering for {ticker} (index {index_name}), skipping"
        )
        return None

    return filtered_isins[0]


def process_csv(input_file, output_file):
    reader = csv.DictReader(input_file)
    fieldnames = [
        "index-name",
        "isin",
        "share-name",
        "currency-hedged",
        "expense-ratio",
        "provider",
        "replication-method",
        "replication-model",
        "dividend-policy-id",
        "creation-date",
        "currency",
        "size",
        "tracking-error",
        "tracking-difference",
        "description",
        "trackinsight-ticker",
    ]
    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        index_name = row["name"]
        logger.info(f"Processing index: {index_name}")
        # Get benchmark data

        search = row["full-name"]
        benchmark_data = fetch_benchmark_data(search, index_name)

        if not benchmark_data and "STOXX" in search:
            benchmark_data = fetch_benchmark_data(
                search.replace("EURO", "Euro").replace("STOXX", "Stoxx"), index_name
            )

        if not benchmark_data and "EURO" in search:
            benchmark_data = fetch_benchmark_data(
                search.replace("EURO", "Euro"), index_name
            )

        if not benchmark_data:
            logger.warning(
                f"No benchmark data found for {row['full-name']} (index {index_name})"
            )
            continue

        # Process all results from benchmark data
        for fund in benchmark_data:
            ticker = fund.get("ticker")
            logger.info(
                f"Processing fund: {ticker or 'unknown ticker'} (index {index_name})"
            )

            # Process ISIN
            isin = process_isin(fund.get("isin", ""), index_name, ticker)
            if isin is None:
                continue  # Skip this fund if no valid ISIN

            # Get tracking data and description
            tracking_data = fetch_tracking_data(ticker, index_name) if ticker else {}
            description_data = (
                fetch_fund_description(ticker, index_name) if ticker else {}
            )

            # Prepare output row
            output_row = {
                "index-name": index_name,
                "isin": isin,
                "share-name": fund.get("shareLabel", ""),
                "currency-hedged": fund.get("currencyHedged", False),
                "expense-ratio": fund.get("expense_ratio", ""),
                "provider": fund.get("provider", ""),
                "replication-method": map_value(
                    fund.get("replication_method", ""), REPLICATION_METHODS
                ),
                "replication-model": map_value(
                    fund.get("replication_model", ""), REPLICATION_MODELS
                ),
                "dividend-policy-id": map_value(
                    fund.get("dividendPolicyId", ""), DIVIDEND_POLICIES
                ),
                "creation-date": convert_epoch_to_date(fund.get("creationDate", 0)),
                "currency": fund.get("currency", ""),
                "size": fund.get("USD:aum", ""),
                "tracking-error": tracking_data.get("te", ""),
                "tracking-difference": tracking_data.get("td", ""),
                "description": description_data.get("description", ""),
                "trackinsight-ticker": ticker or "",
            }

            writer.writerow(output_row)
        logger.info(
            f"Completed processing {len(benchmark_data)} funds for index {index_name}"
        )


def main():
    parser = argparse.ArgumentParser(description="Process index data from CSV and API")
    parser.add_argument(
        "input",
        nargs="?",
        type=argparse.FileType("r"),
        default=sys.stdin,
        help="Input CSV file (default: stdin)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Output CSV file (default: stdout)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    process_csv(args.input, args.output)


if __name__ == "__main__":
    main()
