#!/usr/bin/env python3

import sys
import os
import csv
import re
import glob
import json
import base64
import hashlib
import logging
import itertools
from datetime import datetime

from curl_cffi import requests

INDEX_URL = "https://www.msci.com/indexes/index"
API_URL = "https://www.msci.com/indexes/api/index/performance"
OUTPUT_DIR = "facts/indexes"
SUMMARY_CSV = "facts/indexes.csv"
DATE_FORMAT = "%Y-%m-%d"

# MSCI sits behind Akamai Bot Manager: impersonate Chrome's TLS fingerprint and
# solve the sec_cpt proof-of-work challenge when served.
BROWSER_IMPERSONATE = "chrome"
MAX_CHALLENGE_ATTEMPTS = 4

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def kebab_case(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def new_session():
    return requests.Session(impersonate=BROWSER_IMPERSONATE)


def solve_akamai_challenge(session, challenge_html):
    """Solve an Akamai sec_cpt proof-of-work challenge; False if none present."""
    match = re.search(r'challenge="([^"]+)"', challenge_html)
    if not match:
        return False

    challenge = json.loads(base64.b64decode(match.group(1)))
    nonce = challenge["nonce"]
    threshold = (1 << 256) // challenge["difficulty"]

    # Find integers whose SHA-256 digest falls under the difficulty threshold.
    answers = []
    for _ in range(challenge["count"]):
        for candidate in itertools.count(0):
            digest = hashlib.sha256(f"{nonce}{candidate}".encode()).hexdigest()
            if int(digest, 16) < threshold:
                answers.append(candidate)
                nonce = digest
                break

    logging.info("Solving Akamai proof-of-work challenge...")
    verify = session.post(
        challenge["verify_url"],
        json={"token": challenge["token"], "answer": answers},
        timeout=60,
    )
    try:
        return verify.ok and verify.json().get("success") is True
    except ValueError:
        return False


def fetch_index_data(session, index_code, end_date):
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

    for _ in range(MAX_CHALLENGE_ATTEMPTS):
        response = session.get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        if "application/json" in response.headers.get("content-type", ""):
            return response.json()
        # Non-JSON on a 200 is the Akamai challenge page; solve it and retry.
        if not solve_akamai_challenge(session, response.text):
            break

    content_type = response.headers.get("content-type", "unknown")
    raise RuntimeError(
        f"expected JSON from MSCI but got {content_type} "
        f"({len(response.content)} bytes) — the bot-protection challenge could "
        f"not be cleared"
    )


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

    # Reuse one session so a solved challenge carries over to later codes.
    session = new_session()

    failures = []
    for code in index_codes:
        str_code = str(code)
        if str_code in existing_files:
            logging.info(
                f"Skipping index code {code} — performance file already exists."
            )
            continue

        try:
            data = fetch_index_data(session, code, today)

            index_info = data["data"]["indexes"][0]
            index_name = index_info["indexName"]
            currency = data["data"]["currency"]
            performance_history = index_info["performanceHistory"]["data"]

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
        except Exception as e:
            logging.error(f"Failed to fetch data for index code {code}: {e}")
            failures.append(code)

    return failures


def main():
    input_codes = [line.strip() for line in sys.stdin if line.strip()]
    index_codes = [int(code) for code in input_codes]
    failures = collect_indexes(index_codes)
    if failures:
        logging.error(
            f"{len(failures)} of {len(index_codes)} index(es) could not be "
            f"fetched: {failures}. Failing to avoid publishing incomplete data."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
