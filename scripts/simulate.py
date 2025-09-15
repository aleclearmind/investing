#!/usr/bin/env python

import argparse
import csv
import sys
import os
import json
from datetime import datetime, timedelta
import numpy as np
from scipy.stats import gaussian_kde
import time

verbose = False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Simulate buy/sell outcomes over time using KDE."
    )
    parser.add_argument(
        "index",
        help="Name of the index to load (must match an entry in facts/indexes/indexes.csv)",
    )
    parser.add_argument(
        "--hold", type=float, default=10, help="Holding period in years (default: 10)"
    )
    parser.add_argument(
        "--ignore-inflation", action="store_true", help="Ignore inflation adjustment."
    )
    parser.add_argument(
        "--ignore-currency", action="store_true", help="Ignore currency adjustment."
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output.")
    parser.add_argument(
        "--years",
        type=str,
        default="max",
        help="Years of historical data to consider (default: max, or specify an integer)",
    )
    return parser.parse_args()


def log(message):
    global verbose
    if verbose:
        print(message, file=sys.stderr)


def read_config():
    config_path = os.path.join(os.getcwd(), "config.json")
    with open(config_path, "r") as f:
        config = json.load(f)
        config["max_date"] = datetime.strptime(config["max_date"], "%Y-%m-%d")
        config["min_date"] = datetime.strptime(config["min_date"], "%Y-%m-%d")
        return config


def read_csv_file(file_path, delimiter=","):
    with open(file_path, "r") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return [row for row in reader]


def get_index_metadata(index_name):
    index_file = os.path.join("facts", "indexes.csv")
    indexes = read_csv_file(index_file)
    for index in indexes:
        if index["name"] == index_name:
            return index
    raise ValueError(f"Index '{index_name}' not found in {index_file}")


def read_main_data(file_path, min_date, max_date):
    data = {}
    for row in read_csv_file(file_path):
        date = datetime.strptime(row["date"], "%Y-%m-%d")
        if min_date <= date < max_date:
            value = float(row["value"])
            assert date not in data
            data[date] = value
    return data


def read_inflation_data(file_path):
    inflation_data = {}
    for row in read_csv_file(file_path):
        month = row["month"]
        index = float(row["index"])
        inflation_data[month] = index
    return inflation_data


def read_exchange_rates(file_path):
    rates = {}
    for row in read_csv_file(file_path):
        date = datetime.strptime(row["date"], "%Y-%m-%d")
        rates[date] = float(row["rate"])
    return dict(sorted(rates.items()))


def get_inflation_factor(buy_date, sell_date, inflation_data):
    buy_month = buy_date.strftime("%Y-%m")
    sell_month = sell_date.strftime("%Y-%m")
    assert buy_month in inflation_data, f"Missing inflation data for {buy_month}"
    assert sell_month in inflation_data, f"Missing inflation data for {sell_month}"
    return inflation_data[sell_month] / inflation_data[buy_month]


def get_fx_factor(buy_date, sell_date, exchange_rates):
    def get_nearest_rate(date):
        while date not in exchange_rates:
            date -= timedelta(days=1)
            if date < min(exchange_rates):
                raise ValueError(f"No exchange rate data for or before {date}")
        return exchange_rates[date]

    buy_rate = get_nearest_rate(buy_date)
    sell_rate = get_nearest_rate(sell_date)
    return sell_rate, buy_rate


def simulate_trades(
    data,
    start_date,
    end_date,
    hold_years,
    window_days,
    inflation_data,
    exchange_rates,
    ignore_currency,
    ignore_inflation,
):
    results = []
    hold_days = int(hold_years * 365)

    for i in range((end_date - start_date).days - hold_days - window_days):
        buy_date = start_date + timedelta(days=i)
        if buy_date not in data:
            continue
        buy_value = data[buy_date]
        for j in range(window_days * 2 + 1):
            sell_date = buy_date + timedelta(days=hold_days + j)
            if sell_date not in data:
                continue
            sell_value = data[sell_date]
            total_return = sell_value / buy_value

            log(f"Total return of {buy_date} -> {sell_date}: {total_return}")

            if not ignore_inflation:
                inflation_factor = get_inflation_factor(
                    buy_date, sell_date, inflation_data
                )
                log(f"Using inflation factor {inflation_factor}")
                total_return /= inflation_factor

            if not ignore_currency:
                sell_price, buy_price = get_fx_factor(buy_date, sell_date, exchange_rates)
                log(f"Using currency buying at 1 EUR = {buy_price} USD, selling at 1 EUR = {sell_price}")
                total_return *= buy_price / sell_price

            annualized_return = total_return ** (1 / hold_years)
            percent = (annualized_return - 1) * 100
            results.append(percent)
            log(f"Annualized ROI of {buy_date} -> {sell_date}: {percent}%")
    return results


def write_statistics(
    results, hold_years, index_name, years, ignore_currency, ignore_inflation
):
    sim_file = "simulations/indexes.csv"
    os.makedirs("simulations", exist_ok=True)

    lock_file = sim_file + ".lock"
    max_attempts = 10
    attempt = 0

    while attempt < max_attempts:
        try:
            with open(lock_file, "x"):
                pass
            break
        except FileExistsError:
            time.sleep(1)
            attempt += 1

    if attempt == max_attempts:
        raise RuntimeError("Could not acquire lock on simulations/indexes.csv")

    try:
        file_exists = os.path.exists(sim_file)
        header = [
            "index",
            "hold_years",
            "years",
            "adjust_currency",
            "adjust_inflation",
            "simulations",
            "mean",
            "median",
            "std_dev",
            "std_err",
            "min",
            "max",
        ]
        stats = {
            "index": index_name,
            "hold_years": hold_years,
            "years": years,
            "adjust_currency": "ignore-currency" if ignore_currency else "adjust-currency",
            "adjust_inflation": "ignore-inflation" if ignore_inflation else "adjust-inflation",
            "simulations": len(results),
            "mean": np.mean(results),
            "median": np.median(results),
            "std_dev": np.std(results),
            "std_err": np.std(results) / np.sqrt(len(results)),
            "min": np.min(results),
            "max": np.max(results),
        }

        with open(sim_file, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            if not file_exists:
                writer.writeheader()
            writer.writerow(stats)

    finally:
        os.remove(lock_file)


def save_kde_json(results, json_path, kde_points):
    global_min = min(results)
    global_max = max(results)
    x_range = np.linspace(global_min, global_max, kde_points)
    kde = gaussian_kde(results)
    kde_values = kde(x_range)

    percentiles = np.percentile(results, [5, 25, 50, 75, 95])
    median = np.median(results)
    mean = np.mean(results)

    kde_data = {
        "percentiles": {
            "5th": percentiles[0],
            "25th": percentiles[1],
            "50th": percentiles[2],
            "75th": percentiles[3],
            "95th": percentiles[4],
        },
        "median": median,
        "mean": mean,
        "kde_points": [
            {"x": float(x), "density": float(density)}
            for x, density in zip(x_range, kde_values)
        ],
    }

    log(f"Writing output to {json_path}")

    with open(json_path, "w", encoding="utf8") as f:
        json.dump(kde_data, f, indent=4)


def main():
    config = read_config()
    max_date = config["max_date"]
    min_date = config["min_date"]
    args = parse_args()

    global verbose
    verbose = args.verbose

    country = config.get("country")
    reference_currency = config.get("reference_currency")
    window = config.get("window", 10)
    kde_points = config.get("kde_points", 300)

    assert country, "Country must be specified in config.json"
    assert reference_currency, "Reference currency must be specified in config.json"

    # Compute output path once at the start
    specific_years = args.years
    sim_dir = os.path.join(
        "simulations",
        args.index,
        f"hold-{args.hold}",
        f"years-{specific_years}",
        "ignore-currency" if args.ignore_currency else "adjust-currency",
        "ignore-inflation" if args.ignore_inflation else "adjust-inflation",
    )
    json_path = os.path.join(sim_dir, "kde.json")
    os.makedirs(sim_dir, exist_ok=True)

    index_file = os.path.join("facts", "indexes", f"{args.index}.csv")

    # Calculate effective min_date based on whether --years is not "max"
    if args.years != "max":
        years = int(args.years)
        data = list(read_main_data(index_file, min_date, max_date))
        data_span_years = (data[-1] - data[0]).days / 365.0
        if data_span_years < years:
            log(
                f"Data span ({data_span_years:.2f} years) is less than requested years ({years}). Emitting empty JSON."
            )
            with open(json_path, "w") as f:
                json.dump({}, f)
            return

        earliest_date_from_max = max_date - timedelta(days=years * 365)
        effective_min_date = max(min_date, earliest_date_from_max)
    else:
        effective_min_date = min_date

    log(
        f"Considering data from {effective_min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
    )

    metadata = get_index_metadata(args.index)
    index_currency = metadata["currency"]

    inflation_file = os.path.join("facts", "inflation", f"{country}.csv")
    exchange_file = os.path.join(
        "facts",
        "exchange-rates",
        f"{reference_currency.lower()}-{index_currency.lower()}.csv",
    )

    log(f"Reading index data from: {index_file}")
    data = read_main_data(index_file, effective_min_date, max_date)

    log(f"Reading inflation data from: {inflation_file}")
    inflation_data = read_inflation_data(inflation_file)

    ignore_currency = args.ignore_currency or (
        reference_currency.lower() == index_currency.lower()
    )
    if ignore_currency:
        log("Skipping currency correction due to flag or matching currencies.")
    exchange_rates = {} if ignore_currency else read_exchange_rates(exchange_file)

    log(f"Simulating for hold period: {args.hold} years")
    results = simulate_trades(
        data,
        effective_min_date,
        max_date,
        args.hold,
        window,
        inflation_data,
        exchange_rates,
        ignore_currency=ignore_currency,
        ignore_inflation=args.ignore_inflation,
    )

    if results and len(set(results)) > 1:
        write_statistics(
            results,
            args.hold,
            args.index,
            args.years,
            ignore_currency=ignore_currency,
            ignore_inflation=args.ignore_inflation,
        )
        save_kde_json(results, json_path, kde_points)
    else:
        with open(json_path, "w", encoding="utf8") as f:
            json.dump({}, f, indent=4)
        log("No results!")


if __name__ == "__main__":
    main()
