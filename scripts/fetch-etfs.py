#!/usr/bin/env python3

# pylint: disable=logging-fstring-interpolation, missing-function-docstring, missing-class-docstring, line-too-long, import-error, missing-module-docstring, invalid-name

import csv
import functools
import os
import json
import urllib.parse
import argparse
import sys
import logging
import re

from datetime import datetime
from typing import List, Tuple

import numpy as np
import requests

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


def fetch_cost_from_borsa_italiana(isin):
    url = f"https://www.borsaitaliana.it/borsa/etf/scheda/{isin}.html"
    logger.info(f"Requesting cost for {isin} to Borsa Italiana from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.text
        logger.info(f"Received fund description for {isin}")
        logger.debug(f"Response: {data}")

        # Remove newlines
        data = data.replace("\n", "")

        match = re.match(r""".*Commissioni totali annue.*?<span.*?>(.*?)</span>.*""", data)
        if not match:
            return None
        cost = float(match.groups()[0].replace(",", ".").replace("%", ""))
        return "{:.4f}".format(cost / 100)
    except requests.RequestException as e:
        logger.error(f"Failed to fetch description for {isin}: {e}")
        return None


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

def zip_scaled_data(json_data):
    stamp_data = json_data["stamp"]["data"]
    stamp_scale = json_data["stamp"]["scale"]
    nav_data = json_data["nav"]["data"]
    nav_scale = json_data["nav"]["scale"]

    return {date_to_string(datetime.fromtimestamp((stamp / stamp_scale) / 1000)): nav / nav_scale for stamp, nav in zip(stamp_data, nav_data)}


def date_to_string(date):
    return date.strftime('%Y-%m-%d')


def get_performance_data(ticker):
    today = date_to_string(datetime.today())
    url = "https://www.trackinsight.com/search-api/snapshot/get_snapshots"
    logger.info(f"Requesting performance_data for {ticker} to TrackInsight from {url}")
    try:
        response = requests.post(url, headers={'Content-Type': 'application/json'}, json={
                                 "enterpriseId": None,
                                 "requests": [
                                     {
                                         "fund": ticker,
                                         "startDate": "1980-01-01",
                                         "endDate": today,
                                         "columns": [
                                             "stamp",
                                             "nav"
                                         ]
                                     }
                                 ]
                                })
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Response: {json.dumps(data, indent=2)}")

        return zip_scaled_data(data[0])

    except requests.RequestException as e:
        logger.error(f"Failed to fetch description for {ticker}: {e}")
        return None


def load_index_performance(index_name):
    result = {}
    with open(f"facts/indexes/{index_name}.csv", mode='r', encoding="utf8") as file:
        csv_dict_reader = csv.DictReader(file)
        for row in csv_dict_reader:
            result[row["date"]] = float(row["value"])
    return result

@functools.cache
def load_exchange_rates(source_currency, target_currency, try_other=True):
    result = {}
    exchange_file = os.path.join(
        "facts",
        "exchange-rates",
        f"{target_currency.lower()}-{source_currency.lower()}.csv",
    )

    if not os.path.exists(exchange_file):
        if try_other:
            return {key: 1 / value for key, value in load_exchange_rates(target_currency, source_currency, False).items()}
        return {}

    with open(exchange_file, mode='r', encoding="utf8") as file:
        csv_dict_reader = csv.DictReader(file)
        for row in csv_dict_reader:
            result[row["date"]] = float(row["rate"])
    return result


def read_config():
    config_path = os.path.join(os.getcwd(), "config.json")
    with open(config_path, "r", encoding="utf8") as f:
        config = json.load(f)
        config["max_date"] = datetime.strptime(config["max_date"], "%Y-%m-%d")
        config["min_date"] = datetime.strptime(config["min_date"], "%Y-%m-%d")
        return config


def convert_currency(data, source_currency, target_currency) -> List[float]:

    if source_currency == target_currency:
        return data

    exchange_rates = load_exchange_rates(source_currency, target_currency)
    if not exchange_rates:
        logger.warning(f"No exchange rates from {source_currency} to {target_currency}")
        return {}

    return {
        date: data[date] / exchange_rates[date]
        for date in list(sorted(set(data) & set(exchange_rates)))
    }


class IndexCorrelation:
    def __init__(self, reference_currency, index_name, index_currency):
        self.reference_currency = reference_currency
        self.index_performance = load_index_performance(index_name)
        self.adjusted_index_performance = convert_currency(
            self.index_performance, index_currency, reference_currency
        )

    def compute(self, fund_ticker, fund_currency) -> Tuple[float, float]:
        fund_performance = get_performance_data(fund_ticker)
        adjusted_index_performance = convert_currency(fund_performance, fund_currency, self.reference_currency)

        def correlation(index_performance, fund_performance):
            common_dates = list(sorted(set(fund_performance) & set(index_performance)))
            def common(performance):
                return [performance[date] for date in common_dates]
            return np.corrcoef(common(index_performance), common(fund_performance))[0, 1]

        return (
            correlation(self.index_performance, adjusted_index_performance),
            correlation(self.adjusted_index_performance, adjusted_index_performance)
        )


def process_csv(input_file, output_file):
    reader = csv.DictReader(input_file)
    fieldnames = [
        "index-name",
        "isin",
        "share-name",
        "currency",
        "currency-hedged",
        "correlation",
        "currency-adjusted-correlation",
        "expense-ratio",
        "provider",
        "replication-method",
        "replication-model",
        "dividend-policy-id",
        "creation-date",
        "size",
        "tracking-error",
        "tracking-difference",
        "description",
        "trackinsight-ticker",
    ]
    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
    writer.writeheader()

    config = read_config()
    reference_currency = config["reference_currency"]

    for index_row in reader:
        index_name = index_row["name"]
        logger.info(f"Processing index: {index_name}")
        # Get benchmark data

        search = index_row["full-name"]
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
                f"No benchmark data found for {index_row['full-name']} (index {index_name})"
            )
            continue


        index_correlator = IndexCorrelation(reference_currency, index_name, index_row["currency"])

        # Process all results from benchmark data
        for fund in benchmark_data:
            fund_ticker = fund.get("ticker")
            logger.info(
                f"Processing fund: {fund_ticker or 'unknown ticker'} (index {index_name})"
            )

            # Process ISIN
            isin = process_isin(fund.get("isin", ""), index_name, fund_ticker)
            if isin is None:
                continue  # Skip this fund if no valid ISIN

            # Get tracking data and description
            tracking_data = fetch_tracking_data(fund_ticker, index_name) if fund_ticker else {}
            description_data = (
                fetch_fund_description(fund_ticker, index_name) if fund_ticker else {}
            )

            fund_currency = fund.get("currency", "")

            index_correlation, adjusted_index_correlation = index_correlator.compute(fund_ticker, fund_currency)

            # Fetch actual cost from Borsa Italiana
            cost_bi = fetch_cost_from_borsa_italiana(isin)

            # Prepare output row
            output_row = {
                "index-name": index_name,
                "isin": isin,
                "share-name": fund.get("shareLabel", ""),
                "currency-hedged": fund.get("currencyHedged", False),
                "expense-ratio": cost_bi or fund.get("expense_ratio", ""),
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
                "trackinsight-ticker": fund_ticker or "",
                "correlation":  index_correlation,
                "currency-adjusted-correlation":  adjusted_index_correlation,
            }

            output_row = {key: value if not isinstance(value, str) else value.replace('\n', '') for key, value in output_row.items()}

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
