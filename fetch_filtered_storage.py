#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch filtered Stromspeichereinheiten (Energy Storage Units) with specific criteria:
- Technologie der Stromspeicherung: Batterie
- Batterietechnologie: Lithium-Ionen
- Bruttoleistung > 150 kW
- Anlagenbetreiber is NOT a natural person (natürliche Person)

Usage:
    python fetch_filtered_storage.py --out filtered_storage.jsonl
    python fetch_filtered_storage.py --limit 2000 --sleep 0.5
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, Optional

from master_fetch import (
    MastrClient,
    WSDL_URL_DEFAULT,
    EINHEITTYP_STROMSPEICHER,
    write_jsonl,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


def get_nested_value(data: Dict[str, Any], *keys: str) -> Any:
    """Helper to get nested dictionary values."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
            if current is None:
                return None
        else:
            return None
    return current


def matches_criteria(
    details: Dict[str, Any], 
    einheit: Dict[str, Any], 
    client: Optional[MastrClient] = None,
    einheit_nummer: Optional[str] = None
) -> bool:
    """
    Check if storage unit matches all criteria:
    1. Technologie der Stromspeicherung: Batterie
    2. Batterietechnologie: Lithium-Ionen
    3. Bruttoleistung > 150 kW
    4. Anlagenbetreiber is NOT a natural person
    """
    unit_id = einheit_nummer or einheit.get("EinheitMastrNummer", "unknown")
    logger.debug(f"Checking criteria for unit {unit_id}")
    
    # 1. Check Technologie der Stromspeicherung = Batterie
    technologie = (
        get_nested_value(details, "TechnologieDerStromspeicherung") or
        get_nested_value(details, "EinheitStromSpeicher", "TechnologieDerStromspeicherung") or
        get_nested_value(details, "Technologie") or
        get_nested_value(details, "Speichertechnologie")
    )
    
    logger.debug(f"  Technologie: {technologie}")
    if not technologie or "Batterie" not in str(technologie):
        logger.debug(f"  ❌ Failed: Technologie is not 'Batterie' (got: {technologie})")
        return False
    logger.debug(f"  ✓ Technologie check passed: {technologie}")
    
    # 2. Check Batterietechnologie = Lithium-Ionen
    batterietechnologie = (
        get_nested_value(details, "Batterietechnologie") or
        get_nested_value(details, "EinheitStromSpeicher", "Batterietechnologie") or
        get_nested_value(details, "BatterieTechnologie") or
        get_nested_value(details, "BatterieTechnologieEnum")
    )
    
    batterie_str = str(batterietechnologie) if batterietechnologie else ""
    logger.debug(f"  Batterietechnologie: {batterietechnologie}")
    if not batterietechnologie or (
        "Lithium-Ionen" not in batterie_str and 
        "LithiumIonen" not in batterie_str and
        "Lithium" not in batterie_str
    ):
        logger.debug(f"  ❌ Failed: Batterietechnologie is not 'Lithium-Ionen' (got: {batterietechnologie})")
        return False
    logger.debug(f"  ✓ Batterietechnologie check passed: {batterietechnologie}")
    
    # 3. Check Bruttoleistung > 150 kW
    bruttoleistung = (
        get_nested_value(details, "Bruttoleistung") or
        get_nested_value(details, "EinheitStromSpeicher", "Bruttoleistung") or
        get_nested_value(details, "BruttoleistungEinheit") or
        get_nested_value(details, "BruttoLeistung") or
        get_nested_value(einheit, "Bruttoleistung") or
        get_nested_value(einheit, "Leistung")
    )
    
    logger.debug(f"  Bruttoleistung: {bruttoleistung}")
    # Convert to float if it's a string or number
    try:
        if bruttoleistung is None:
            logger.debug(f"  ❌ Failed: Bruttoleistung is None")
            return False
        power_kw = float(bruttoleistung) if isinstance(bruttoleistung, (int, float, str)) else None
        if power_kw is None or power_kw <= 150.0:
            logger.debug(f"  ❌ Failed: Bruttoleistung {power_kw} kW <= 150 kW")
            return False
        logger.debug(f"  ✓ Bruttoleistung check passed: {power_kw} kW")
    except (ValueError, TypeError) as e:
        logger.debug(f"  ❌ Failed: Could not parse Bruttoleistung '{bruttoleistung}': {e}")
        return False
    
    # 4. Check Anlagenbetreiber is NOT a natural person
    # First try to get operator MaStR number
    anlagenbetreiber_mastr = (
        get_nested_value(details, "AnlagenbetreiberMastrNummer") or
        get_nested_value(details, "EinheitStromSpeicher", "AnlagenbetreiberMastrNummer") or
        get_nested_value(details, "BetreiberMastrNummer") or
        get_nested_value(einheit, "AnlagenbetreiberMastrNummer") or
        get_nested_value(einheit, "BetreiberMastrNummer")
    )
    
    logger.debug(f"  Anlagenbetreiber MaStR: {anlagenbetreiber_mastr}")
    is_natural_person = False
    
    # First check in details/einheit for direct indicators
    for field in ["AnlagenbetreiberTyp", "BetreiberTyp", "Personenart", "Rechtsform", "PersonenArt"]:
        value = (
            get_nested_value(details, field) or
            get_nested_value(details, "EinheitStromSpeicher", field) or
            get_nested_value(einheit, field)
        )
        if value:
            value_str = str(value).lower()
            logger.debug(f"  Checking {field}: {value}")
            if "natürlich" in value_str or "natural" in value_str:
                is_natural_person = True
                logger.debug(f"  Found natural person indicator in {field}: {value}")
                break
    
    # If we have operator MaStR number and client, fetch operator details
    if not is_natural_person and anlagenbetreiber_mastr and client:
        try:
            logger.debug(f"  Fetching operator details for {anlagenbetreiber_mastr}")
            operator_details = client.get_marktakteur_details(anlagenbetreiber_mastr)
            # Check operator type in the fetched details
            for field in ["Personenart", "PersonenArt", "Rechtsform", "Typ"]:
                value = operator_details.get(field)
                if value:
                    value_str = str(value).lower()
                    logger.debug(f"  Operator {field}: {value}")
                    if "natürlich" in value_str or "natural" in value_str:
                        is_natural_person = True
                        logger.debug(f"  Found natural person indicator in operator {field}: {value}")
                        break
        except Exception as e:
            logger.warning(f"  Could not fetch operator details for {anlagenbetreiber_mastr}: {e}")
            # If we can't fetch operator details, we'll assume it's not a natural person
            # (conservative approach - we want to include units where we can't verify)
    
    if is_natural_person:
        logger.debug(f"  ❌ Failed: Anlagenbetreiber is a natural person")
        return False
    
    logger.debug(f"  ✓ Anlagenbetreiber check passed (not a natural person)")
    logger.info(f"✓ Unit {unit_id} matches all criteria!")
    return True


def fetch_filtered_storage(
    client: MastrClient,
    limit: int = 2000,
    datum_ab_iso: Optional[str] = None,
    sleep_s: float = 0.0,
    verbose: bool = False,
) -> list[Dict[str, Any]]:
    """
    Fetch all storage units and filter by criteria.
    Returns list of matching units with their details.
    """
    if verbose:
        logger.setLevel(logging.DEBUG)
    
    results = []
    total_checked = 0
    total_matched = 0
    total_units_processed = 0
    page_count = 0
    
    logger.info("Starting to fetch units from MaStR API...")
    logger.info(f"Configuration: limit={limit}, datum_ab={datum_ab_iso}, sleep={sleep_s}s")
    
    # Iterate through all units
    try:
        einheiten = client.iter_einheiten(limit=limit, datum_ab_iso=datum_ab_iso, sleep_s=sleep_s)
        
        for einheit in einheiten:
            total_units_processed += 1
            einheittyp = einheit.get("Einheittyp")
            einheit_nummer = einheit.get("EinheitMastrNummer")
            
            # Log page progress
            if total_units_processed % limit == 1:
                page_count += 1
                logger.info(f"Processing page {page_count} (units {total_units_processed} onwards)...")
            
            # Only process storage units
            if einheittyp != EINHEITTYP_STROMSPEICHER:
                continue
            
            if not einheit_nummer:
                logger.warning(f"Skipping storage unit without MaStR number")
                continue
            
            total_checked += 1
            
            if total_checked % 10 == 0:
                logger.info(f"Checked {total_checked} storage units, found {total_matched} matches so far...")
            
            try:
                logger.debug(f"Fetching details for storage unit {einheit_nummer}")
                # Get detailed information
                details = client.get_einheit_stromspeicher_details(einheit_nummer)
                logger.debug(f"Successfully fetched details for {einheit_nummer}")
                
                # Check if it matches all criteria
                if matches_criteria(details, einheit, client, einheit_nummer):
                    total_matched += 1
                    results.append({
                        "EinheitMastrNummer": einheit_nummer,
                        "Einheit": einheit,
                        "Details": details,
                    })
                    logger.info(f"🎯 MATCH #{total_matched}: {einheit_nummer}")
            
            except Exception as e:
                logger.error(f"Error fetching details for {einheit_nummer}: {e}", exc_info=verbose)
                continue
        
        logger.info(f"Finished processing. Total units processed: {total_units_processed}")
        logger.info(f"Total storage units checked: {total_checked}")
        logger.info(f"Total matches found: {total_matched}")
        
    except Exception as e:
        logger.error(f"Error during iteration: {e}", exc_info=True)
        raise
    
    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch filtered Stromspeichereinheiten with specific criteria"
    )
    parser.add_argument("--wsdl", default=WSDL_URL_DEFAULT, help="WSDL URL")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds")
    parser.add_argument("--limit", type=int, default=2000, help="Paging limit (max: 2000)")
    parser.add_argument(
        "--datum-ab",
        default=None,
        help='Delta query from ISO datetime, e.g., "2025-01-01T00:00:00"',
    )
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between pages (seconds)")
    parser.add_argument("--out", default=None, help="Output JSONL file (default: stdout)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("MaStR Filtered Storage Units Fetcher")
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
    
    logger.info(f"Using Marktakteur: {marktakteur[:10]}...")
    logger.info(f"API Key: {'*' * 20}...{api_key[-4:] if len(api_key) > 4 else '****'}")
    
    # Create client
    logger.info(f"Initializing MaStR client (WSDL: {args.wsdl})...")
    try:
        client = MastrClient(args.wsdl, api_key, marktakteur, timeout_s=args.timeout)
        logger.info("✓ Client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to create client: {e}", exc_info=True)
        return 1
    
    # Fetch and filter
    logger.info("Starting data fetch and filtering...")
    try:
        results = fetch_filtered_storage(
            client,
            limit=args.limit,
            datum_ab_iso=args.datum_ab,
            sleep_s=args.sleep,
            verbose=args.verbose,
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        raise
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}", exc_info=True)
        return 1
    
    # Output results
    logger.info("=" * 60)
    logger.info(f"Final Results: {len(results)} matching units found")
    logger.info("=" * 60)
    
    if args.out:
        write_jsonl(args.out, results)
        logger.info(f"✓ Wrote {len(results)} matching units to {args.out}")
    else:
        # Output to stdout as JSONL
        logger.info("Writing results to stdout...")
        for r in results:
            print(json.dumps(r, ensure_ascii=False))
        logger.info("✓ Results written to stdout")
    
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

