#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch Marktakteur data from MaStR API for all entries in Stromlieferant_Direktvermarkter_Bilanzkreisverantwortlicher.csv
and merge the data into a combined CSV file.

Usage:
    python fetch_marktakteur.py --input Stromlieferant_Direktvermarkter_Bilanzkreisverantwortlicher.csv --output merged_marktakteur.csv
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
    Returns: {mastr_num: {"data": {...}, "error": "..."}}
    """
    results = {}
    total = len(mastr_nums)
    completed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_marktakteur_with_retry, client, n): n for n in mastr_nums}
        
        for fut in as_completed(futures):
            mastr_num, data, err = fut.result()
            results[mastr_num] = {"data": data, "error": err}
            completed += 1
            
            if err:
                logger.warning(f"[{completed}/{total}] {mastr_num}: {err}")
            else:
                logger.info(f"[{completed}/{total}] Successfully fetched {mastr_num}")
    
    return results


def read_marktakteur_csv(csv_filepath: str) -> tuple[List[Dict[str, str]], Set[str]]:
    """
    Read the CSV file and extract unique MaStR-Nr values.
    Returns: (list of all rows, set of unique MaStR-Nr)
    """
    rows = []
    unique_mastr_nums = set()
    
    with open(csv_filepath, 'r', encoding='utf-8') as f:
        # Try to detect delimiter
        sample = f.read(1024)
        f.seek(0)
        delimiter = ';' if ';' in sample else ','
        
        # CSV file uses semicolon delimiter and quotes
        reader = csv.DictReader(f, delimiter=delimiter, quotechar='"')
        
        # Debug: print column names
        if reader.fieldnames:
            logger.debug(f"CSV columns: {list(reader.fieldnames)[:5]}")
        
        for row in reader:
            # Handle different possible column names for MaStR-Nr
            # The CSV has quotes around field names and values, but csv.DictReader should handle this
            # Try all possible variations
            mastr_num = None
            for key in row.keys():
                if 'MaStR' in key or 'MaStr' in key:
                    value = row.get(key, '').strip()
                    if value:
                        mastr_num = value.strip('"').strip("'").strip()
                        break
            
            # Fallback: try common column names
            if not mastr_num:
                mastr_num = (
                    row.get('MaStR-Nr.', '').strip() or
                    row.get('MaStR-Nr', '').strip() or
                    row.get('MaStr-Nr.', '').strip() or
                    row.get('MaStr-Nr', '').strip() or
                    row.get('MaStR-Nr. des Marktakteurs', '').strip()
                )
                if mastr_num:
                    mastr_num = mastr_num.strip('"').strip("'").strip()
            
            if mastr_num:
                # Remove quotes if present
                mastr_num = mastr_num.strip('"').strip("'").strip()
                if mastr_num:
                    unique_mastr_nums.add(mastr_num)
            
            rows.append(row)
    
    logger.info(f"Read {len(rows)} rows from {csv_filepath}")
    logger.info(f"Found {len(unique_mastr_nums)} unique MaStR-Nr values")
    
    return rows, unique_mastr_nums


def flatten_dict(d: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Flatten a nested dictionary for CSV export.
    """
    items = []
    if not isinstance(d, dict):
        return {parent_key: d} if parent_key else {}
    
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to comma-separated strings
            if v and isinstance(v[0], dict):
                # List of dicts: flatten each and join
                flattened_items = [flatten_dict(item, f"{new_key}_{i}", sep=sep) for i, item in enumerate(v)]
                # Merge all flattened items
                for item in flattened_items:
                    items.extend(item.items())
            else:
                items.append((new_key, ', '.join(str(x) for x in v)))
        else:
            items.append((new_key, v))
    
    return dict(items)


def save_marktakteur_csv(
    results: Dict[str, Dict[str, Any]],
    output_filepath: str
) -> None:
    """
    Save fetched Marktakteur data to CSV.
    """
    all_keys = set()
    
    # Collect all keys from all results
    for mastr_num, result in results.items():
        if result.get("data"):
            flattened = flatten_dict(result["data"])
            all_keys.update(flattened.keys())
    
    # Always include MaStRNummer as first column
    fieldnames = ['MaStRNummer'] + sorted([k for k in all_keys if k != 'MaStRNummer'])
    
    with open(output_filepath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        
        for mastr_num in sorted(results.keys()):
            result = results[mastr_num]
            row = {'MaStRNummer': mastr_num}
            
            if result.get("data"):
                flattened = flatten_dict(result["data"])
                row.update(flattened)
            else:
                row['Error'] = result.get("error", "unknown_error")
            
            writer.writerow(row)
    
    logger.info(f"Saved {len(results)} Marktakteur records to {output_filepath}")


def merge_csvs(
    original_rows: List[Dict[str, str]],
    marktakteur_results: Dict[str, Dict[str, Any]],
    output_filepath: str
) -> None:
    """
    Merge original CSV data with fetched Marktakteur data.
    """
    # Get all fieldnames from original CSV
    original_fieldnames = set()
    for row in original_rows:
        original_fieldnames.update(row.keys())
    
    # Get all fieldnames from fetched data
    fetched_fieldnames = set()
    for result in marktakteur_results.values():
        if result.get("data"):
            flattened = flatten_dict(result["data"])
            fetched_fieldnames.update(flattened.keys())
    
    # Create merged fieldnames: original fields first, then fetched fields with prefix
    merged_fieldnames = sorted(list(original_fieldnames))
    
    # Add fetched fields with prefix to avoid conflicts
    prefix = 'MA_'
    for field in sorted(fetched_fieldnames):
        if field not in original_fieldnames:
            merged_fieldnames.append(f"{prefix}{field}")
    
    # Create a mapping from MaStR-Nr to fetched data
    mastr_to_data = {}
    for mastr_num, result in marktakteur_results.items():
        if result.get("data"):
            mastr_to_data[mastr_num] = flatten_dict(result["data"])
    
    # Write merged CSV
    with open(output_filepath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=merged_fieldnames, delimiter=';')
        writer.writeheader()
        
        for original_row in original_rows:
            # Get MaStR-Nr from original row - try all possible column names
            mastr_num = None
            for key in original_row.keys():
                if 'MaStR' in key or 'MaStr' in key:
                    value = original_row.get(key, '').strip()
                    if value:
                        mastr_num = value.strip('"').strip("'").strip()
                        break
            
            # Fallback: try common column names
            if not mastr_num:
                mastr_num = (
                    original_row.get('MaStR-Nr.', '').strip() or
                    original_row.get('MaStR-Nr', '').strip() or
                    original_row.get('MaStr-Nr.', '').strip() or
                    original_row.get('MaStr-Nr', '').strip() or
                    original_row.get('MaStR-Nr. des Marktakteurs', '').strip()
                )
                if mastr_num:
                    mastr_num = mastr_num.strip('"').strip("'").strip()
            
            # Start with original row
            merged_row = original_row.copy()
            
            # Add fetched data with prefix
            if mastr_num and mastr_num in mastr_to_data:
                fetched_data = mastr_to_data[mastr_num]
                for key, value in fetched_data.items():
                    if key not in original_fieldnames:
                        merged_row[f"{prefix}{key}"] = value
                logger.debug(f"Merged data for {mastr_num}: {len(fetched_data)} fields")
            elif mastr_num:
                logger.debug(f"No data found for MaStR-Nr: {mastr_num}")
            
            writer.writerow(merged_row)
    
    logger.info(f"Merged {len(original_rows)} rows to {output_filepath}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Fetch Marktakteur data from MaStR API and merge with CSV'
    )
    parser.add_argument(
        '--input',
        default='Stromlieferant_Direktvermarkter_Bilanzkreisverantwortlicher.csv',
        help='Input CSV file path'
    )
    parser.add_argument(
        '--marktakteur-output',
        default='Marktakteur.csv',
        help='Output CSV file for Marktakteur data only'
    )
    parser.add_argument(
        '--output',
        default='merged_marktakteur.csv',
        help='Output CSV file for merged data'
    )
    parser.add_argument(
        '--wsdl',
        default=WSDL_URL_DEFAULT,
        help='WSDL URL for MaStR API'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=6,
        help='Maximum number of concurrent API calls'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help='Timeout for API calls in seconds'
    )
    parser.add_argument(
        '--test-mastr',
        help='Test mode: only process this specific MaStR-Nr'
    )
    
    args = parser.parse_args()
    
    # Load credentials
    api_key = os.getenv('MASTR_API_KEY')
    marktakteur = os.getenv('MASTR_MARKTAKTEUR')
    
    if not api_key or not marktakteur:
        logger.error("Please set MASTR_API_KEY and MASTR_MARKTAKTEUR environment variables")
        return 1
    
    # Read input CSV
    original_rows, unique_mastr_nums = read_marktakteur_csv(args.input)
    
    # Test mode logic
    if args.test_mastr:
        unique_mastr_nums = {args.test_mastr}
        logger.info(f"Test mode: Only processing {len(unique_mastr_nums)} specified MaStR numbers")
    
    # Initialize client
    client = MastrClient(args.wsdl, api_key, marktakteur, timeout_s=args.timeout)
    
    # Fetch Marktakteur data
    mastr_nums_list = sorted(list(unique_mastr_nums))
    logger.info(f"Fetching data for {len(mastr_nums_list)} Marktakteure...")
    results = batch_fetch_marktakteure(client, mastr_nums_list, max_workers=args.max_workers)
    
    # Save Marktakteur data
    save_marktakteur_csv(results, args.marktakteur_output)
    
    # Merge and save
    merge_csvs(original_rows, results, args.output)
    
    # Summary
    successful = sum(1 for r in results.values() if r.get("data"))
    failed = len(results) - successful
    logger.info(f"Summary: {successful} successful, {failed} failed")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

