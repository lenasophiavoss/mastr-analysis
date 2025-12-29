#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch Anlagenbetreiber data from MaStR API for all operators in Stromerzeuger.csv
and merge the data into a combined CSV file.

Usage:
    python fetch_anlagenbetreiber.py --input Stromerzeuger.csv --output merged_data.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

from zeep.exceptions import Fault

from master_fetch import MastrClient, WSDL_URL_DEFAULT, zeep_to_dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


def fetch_marktakteur_with_retry(
    client: MastrClient,
    target_mastr_nummer: str,
    max_retries: int = 6
) -> tuple[str, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch Marktakteur data with retry logic.
    Returns: (mastr_nummer, data_dict, error_message)
    """
    delay = 0.2
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"Calling GetMarktakteur for {target_mastr_nummer}")
            resp = client.service_akteur.GetMarktakteur(
                apiKey=client.api_key,
                marktakteurMastrNummer=client.marktakteur_mastr_nummer,
                mastrNummer=target_mastr_nummer,
            )
            logger.debug(f"Response received for {target_mastr_nummer}: {type(resp)}")
            # The response itself contains the Marktakteur data, not nested under "Marktakteur"
            # Try both approaches
            obj = getattr(resp, "Marktakteur", None)
            if obj is None:
                # Response is the data itself
                obj = resp
            data = zeep_to_dict(obj) if obj is not None else None
            if data:
                logger.debug(f"Successfully converted to dict with {len(data)} keys")
            else:
                logger.warning(f"No data extracted for {target_mastr_nummer}")
            return target_mastr_nummer, data, None

        except Fault as f:
            msg = str(f)

            # 404-artige Fälle: Marktakteur nicht gefunden/unbekannt -> nicht weiter retryen
            if "MarktakteurNichtGefunden" in msg or "MarktakteurUnbekannt" in msg:
                logger.warning(f"Marktakteur {target_mastr_nummer} not found")
                return target_mastr_nummer, None, "not_found"

            # 429 / TooManyRequests -> Backoff + retry
            if "ToManyRequests" in msg or "429" in msg:
                logger.warning(f"Rate limit hit for {target_mastr_nummer}, waiting {delay}s...")
                time.sleep(delay)
                delay = min(delay * 2, 10.0)
                continue

            # Sonst: echter Fehler -> raus
            logger.error(f"SOAP Fault for {target_mastr_nummer}: {msg}")
            return target_mastr_nummer, None, f"fault:{msg}"

        except Exception as e:
            logger.error(f"Unexpected error for {target_mastr_nummer}: {e}")
            return target_mastr_nummer, None, f"exception:{str(e)}"

    logger.error(f"Retry exhausted for {target_mastr_nummer} after {max_retries} attempts")
    return target_mastr_nummer, None, "retry_exhausted"


def batch_fetch_marktakteure(
    client: MastrClient,
    mastr_nums: List[str],
    max_workers: int = 6
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch multiple Marktakteure concurrently.
    Returns: {mastr_nummer: {"data": data_dict, "error": error_str}}
    """
    results = {}
    total = len(mastr_nums)
    completed = 0
    
    logger.info(f"Starting batch fetch for {total} Marktakteure with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_marktakteur_with_retry, client, mastr_num): mastr_num
            for mastr_num in mastr_nums
        }
        
        for future in as_completed(futures):
            mastr_num, data, err = future.result()
            results[mastr_num] = {"data": data, "error": err}
            completed += 1
            
            if completed % 10 == 0:
                logger.info(f"Progress: {completed}/{total} ({completed*100//total}%)")
            
            if data:
                logger.debug(f"✓ Fetched {mastr_num}")
            else:
                logger.warning(f"✗ Failed {mastr_num}: {err}")
    
    logger.info(f"Batch fetch completed: {len([r for r in results.values() if r['data']])}/{total} successful")
    return results


def read_stromerzeuger_csv(filepath: str) -> tuple[List[Dict[str, str]], Set[str]]:
    """
    Read Stromerzeuger.csv and extract unique Anlagenbetreiber MaStR numbers.
    Returns: (rows, unique_mastr_numbers)
    """
    rows = []
    unique_mastr_nums = set()
    
    logger.info(f"Reading {filepath}...")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        # CSV uses semicolon delimiter
        reader = csv.DictReader(f, delimiter=';')
        
        for row in reader:
            rows.append(row)
            # Column name has a tab character at the start based on the file
            mastr_num = row.get("MaStR-Nr. des Anlagenbetreibers") or row.get("\tMaStR-Nr. des Anlagenbetreibers")
            if mastr_num and mastr_num.strip():
                unique_mastr_nums.add(mastr_num.strip())
    
    logger.info(f"Read {len(rows)} rows, found {len(unique_mastr_nums)} unique Anlagenbetreiber MaStR numbers")
    return rows, unique_mastr_nums


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Flatten nested dictionary for CSV export.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to string representation
            items.append((new_key, str(v)))
        else:
            items.append((new_key, v))
    return dict(items)


def save_anlagenbetreiber_csv(
    results: Dict[str, Dict[str, Any]],
    output_file: str
) -> None:
    """
    Save Anlagenbetreiber data to CSV file.
    """
    logger.info(f"Saving Anlagenbetreiber data to {output_file}...")
    
    if not results:
        logger.warning("No results to save")
        return
    
    # Collect all unique keys from all results
    all_keys = set()
    for result in results.values():
        if result['data']:
            flattened = flatten_dict(result['data'])
            all_keys.update(flattened.keys())
    
    # Add error column
    all_keys.add('error')
    all_keys = sorted(all_keys)
    
    # Ensure MaStR-Nr is first column - use consistent name
    mastr_num_col = 'MaStRNummer'
    if mastr_num_col not in all_keys:
        all_keys.insert(0, mastr_num_col)
    else:
        all_keys.remove(mastr_num_col)
        all_keys.insert(0, mastr_num_col)
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, delimiter=';')
        writer.writeheader()
        
        for mastr_num, result in sorted(results.items()):
            row = {'error': result['error']}
            
            if result['data']:
                flattened = flatten_dict(result['data'])
                row.update(flattened)
            
            # Always set MaStR number (overwrite if it exists in flattened data)
            row[mastr_num_col] = mastr_num
            
            writer.writerow(row)
    
    logger.info(f"✓ Saved {len(results)} Anlagenbetreiber records to {output_file}")


def merge_csvs(
    stromerzeuger_rows: List[Dict[str, str]],
    anlagenbetreiber_results: Dict[str, Dict[str, Any]],
    output_file: str
) -> None:
    """
    Merge Stromerzeuger.csv with Anlagenbetreiber data.
    """
    logger.info(f"Merging data and saving to {output_file}...")
    
    # Get all columns from Stromerzeuger CSV
    if not stromerzeuger_rows:
        logger.error("No Stromerzeuger rows to merge")
        return
    
    stromerzeuger_cols = list(stromerzeuger_rows[0].keys())
    
    # Get all Anlagenbetreiber columns (flattened)
    anlagenbetreiber_cols = set()
    for result in anlagenbetreiber_results.values():
        if result['data']:
            flattened = flatten_dict(result['data'])
            anlagenbetreiber_cols.update(flattened.keys())
    
    # Prefix Anlagenbetreiber columns to avoid conflicts
    prefixed_ab_cols = [f"AB_{col}" for col in sorted(anlagenbetreiber_cols)]
    
    # Combine columns
    all_cols = stromerzeuger_cols + prefixed_ab_cols + ['AB_error']
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=all_cols, delimiter=';')
        writer.writeheader()
        
        for row in stromerzeuger_rows:
            # Get Anlagenbetreiber MaStR number
            mastr_num = row.get("MaStR-Nr. des Anlagenbetreibers") or row.get("\tMaStR-Nr. des Anlagenbetreibers")
            if mastr_num:
                mastr_num = mastr_num.strip()
            
            # Create merged row
            merged_row = row.copy()
            
            if mastr_num and mastr_num in anlagenbetreiber_results:
                result = anlagenbetreiber_results[mastr_num]
                merged_row['AB_error'] = result['error'] or ''
                
                if result['data']:
                    flattened = flatten_dict(result['data'])
                    for key, value in flattened.items():
                        merged_row[f"AB_{key}"] = value
                else:
                    # No data, fill with empty strings
                    for col in prefixed_ab_cols:
                        if col not in merged_row:
                            merged_row[col] = ''
            else:
                # No matching Anlagenbetreiber data
                merged_row['AB_error'] = 'not_found' if mastr_num else 'no_mastr_num'
                for col in prefixed_ab_cols:
                    merged_row[col] = ''
            
            writer.writerow(merged_row)
    
    logger.info(f"✓ Merged {len(stromerzeuger_rows)} rows and saved to {output_file}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Anlagenbetreiber data and merge with Stromerzeuger.csv"
    )
    parser.add_argument(
        "--input",
        "-i",
        default="Stromerzeuger.csv",
        help="Input CSV file (default: Stromerzeuger.csv)"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="merged_stromerzeuger_anlagenbetreiber.csv",
        help="Output merged CSV file"
    )
    parser.add_argument(
        "--anlagenbetreiber-output",
        "-a",
        default="Anlagenbetreiber.csv",
        help="Output file for Anlagenbetreiber data only (default: Anlagenbetreiber.csv)"
    )
    parser.add_argument("--wsdl", default=WSDL_URL_DEFAULT, help="WSDL URL")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=6,
        help="Number of concurrent workers (default: 6)"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--test-mastr",
        action="append",
        help="Test mode: only fetch specific MaStR numbers (can be used multiple times)"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    logger.info("=" * 60)
    logger.info("Anlagenbetreiber Data Fetcher and Merger")
    logger.info("=" * 60)
    
    # Get credentials from environment
    api_key = os.getenv("MASTR_API_KEY", "").strip()
    marktakteur = os.getenv("MASTR_MARKTAKTEUR", "").strip()
    
    if not api_key:
        logger.error("MASTR_API_KEY environment variable not set")
        return 1
    
    if not marktakteur:
        logger.error("MASTR_MARKTAKTEUR environment variable not set")
        return 1
    
    # Read input CSV
    try:
        stromerzeuger_rows, unique_mastr_nums = read_stromerzeuger_csv(args.input)
    except Exception as e:
        logger.error(f"Failed to read input CSV: {e}", exc_info=True)
        return 1
    
    if not unique_mastr_nums:
        logger.error("No Anlagenbetreiber MaStR numbers found in input file")
        return 1
    
    # Filter to test MaStR numbers if specified
    if args.test_mastr:
        test_set = set(args.test_mastr)
        unique_mastr_nums = unique_mastr_nums.intersection(test_set)
        if not unique_mastr_nums:
            logger.error(f"None of the test MaStR numbers {test_set} were found in the input file")
            return 1
        logger.info(f"Test mode: Only processing {len(unique_mastr_nums)} specified MaStR numbers")
    
    # Initialize client
    logger.info(f"Initializing MaStR client...")
    try:
        client = MastrClient(args.wsdl, api_key, marktakteur, timeout_s=args.timeout)
        logger.info("✓ Client initialized")
    except Exception as e:
        logger.error(f"Failed to create client: {e}", exc_info=True)
        return 1
    
    # Fetch Anlagenbetreiber data
    mastr_nums_list = sorted(list(unique_mastr_nums))
    results = batch_fetch_marktakteure(client, mastr_nums_list, max_workers=args.max_workers)
    
    # Save Anlagenbetreiber data
    save_anlagenbetreiber_csv(results, args.anlagenbetreiber_output)
    
    # Merge and save
    merge_csvs(stromerzeuger_rows, results, args.output)
    
    logger.info("=" * 60)
    logger.info("✓ All done!")
    logger.info(f"  - Anlagenbetreiber data: {args.anlagenbetreiber_output}")
    logger.info(f"  - Merged data: {args.output}")
    logger.info("=" * 60)
    
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"ERROR: {e}", exc_info=True)
        sys.exit(1)

