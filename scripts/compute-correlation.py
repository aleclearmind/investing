#!/usr/bin/env python3

import argparse
import os
import sys

from collections.abc import Sequence
from csv import DictReader, DictWriter
from itertools import combinations
from typing import Dict, List, Tuple

import numpy as np


def read_csv_data(index: str) -> Dict[str, float]:
    """Read date-value pairs from CSV into a dictionary."""
    filename = os.path.join("facts", "indexes", f"{index}.csv")
    data: Dict[str, float] = {}
    with open(filename, "r") as f:
        reader = DictReader(f)
        for row in reader:
            data[row["date"]] = float(row["value"])
    return data


def compute_correlation(
    data1: Dict[str, float],
    data2: Dict[str, float]
) -> (int, float):
    """Compute Pearson correlation for values on common dates."""
    common_dates = list(sorted(set(data1.keys()) & set(data2.keys())))
    if not common_dates:
        return (0, np.nan)  # No common dates, return NaN

    values1 = [data1[date] for date in common_dates]
    values2 = [data2[date] for date in common_dates]

    with np.errstate(invalid='ignore'):
        correlation = np.corrcoef(values1, values2)

    return (len(common_dates), correlation[0, 1])


def main(args: Sequence[str]) -> None:
    """Process CSV files and output correlations."""
    parser = argparse.ArgumentParser(
        description="Compute correlations between date-value CSV files."
    )
    parser.add_argument(
        "indices",
        metavar="INDEX",
        nargs="+",
        help="Input CSV files with \"date\" and \"value\" columns"
    )
    parsed_args = parser.parse_args(args)

    # Read all CSV files
    datasets: Dict[str, Dict[str, float]] = {
        index_name: read_csv_data(index_name)
        for index_name in parsed_args.indices
    }

    # Compute correlations for all pairs
    results: List[Dict[str, str]] = []
    for index1, index2 in combinations(datasets.keys(), 2):
        common_dates, corr = compute_correlation(datasets[index1], datasets[index2])

        if np.isnan(corr):
            continue

        # Add both (a,b) and (b,a) to results
        results.append({
            "a": index1,
            "b": index2,
            "common_dates": str(common_dates),
            "correlation": str(corr)
        })
        results.append({
            "a": index2,
            "b": index1,
            "common_dates": str(common_dates),
            "correlation": str(corr)
        })

    # Write output CSV
    writer = DictWriter(sys.stdout, fieldnames=["a", "b", "common_dates", "correlation"])
    writer.writeheader()
    writer.writerows(results)


if __name__ == "__main__":
    main(sys.argv[1:])
