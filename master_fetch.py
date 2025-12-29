#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MaStR SOAP Client (Public/1_2) – Abruf:
1) Marktakteure (AkteurImStrommarkt) nach Rollen: BV / LT / MB
2) Stromspeichereinheiten: Liste Einheiten -> filter Einheittyp=Stromspeichereinheit -> Details GetEinheitStromSpeicher

Voraussetzungen:
  pip install zeep requests urllib3

Aufrufbeispiele:
  export MASTR_API_KEY="..."
  export MASTR_MARKTAKTEUR="MASTR1234567890..."   # deine Marktakteur-MaStR-Nummer
  python mastr_fetch.py actors --role BV --limit 2000
  python mastr_fetch.py storage-units --limit 2000 --out storage.jsonl

WSDL:
  https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from zeep import Client, Settings
from zeep.transports import Transport
from zeep.exceptions import Fault


WSDL_URL_DEFAULT = "https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl"

# Rollen (aus MarktrollenEnum in der WSDL/XSD)
ROLE_MAP = {
    # BV = Bilanzkreisverantwortlicher (Strom)
    "BV": "EnergiemarktakteureBilanzkreisverantwortlicherStrom",
    # MB = Messstellenbetreiber (Strom)
    "MB": "EnergiemarktakteureMessstellenbetreiberStrom",
    # LT = Stromlieferant (Strom) – Direktvermarkter/Stromgroßhändler sind NICHT als separate Enum-Rolle vorhanden
    "LT": "EnergiemarktakteureStromlieferant",
}

MARKTFUNKTION_STROMMARKT = "AkteurImStrommarkt"
EINHEITTYP_STROMSPEICHER = "Stromspeichereinheit"


def build_session(retries_total: int = 5, backoff_factor: float = 0.5) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries_total,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


class MastrClient:
    def __init__(self, wsdl_url: str, api_key: str, marktakteur_mastr_nummer: str, timeout_s: int = 60):
        if not api_key:
            raise ValueError("API-Key fehlt. Setze MASTR_API_KEY.")
        if not marktakteur_mastr_nummer:
            raise ValueError("Marktakteur-MaStR-Nummer fehlt. Setze MASTR_MARKTAKTEUR.")

        self.api_key = api_key
        self.marktakteur_mastr_nummer = marktakteur_mastr_nummer

        session = build_session()
        transport = Transport(session=session, timeout=timeout_s)
        settings = Settings(strict=False, xml_huge_tree=True)

        self.client = Client(wsdl=wsdl_url, transport=transport, settings=settings)
        # Access services via specific ports
        # Use the "12" versions which are the Public/1_2 API versions
        self.service_akteur = self.client.bind('Marktstammdatenregister', 'Akteur12')
        self.service_anlage = self.client.bind('Marktstammdatenregister', 'Anlage12')
        # For backward compatibility, also set service to akteur (most common)
        self.service = self.service_akteur

    # ----------------------------
    # 1) Marktakteure (gefilt.)
    # ----------------------------
    def iter_marktakteure_by_role(
        self,
        role_code: str,
        limit: int = 2000,
        datum_ab_iso: Optional[str] = None,
        sleep_s: float = 0.0,
    ) -> Iterable[Dict[str, Any]]:
        """
        Paging über GetGefilterteListeMarktakteure.
        role_code: z.B. EnergiemarktakteureBilanzkreisverantwortlicherStrom
        datum_ab_iso: optional z.B. "2025-01-01T00:00:00"
        """
        start = 0
        while True:
            try:
                resp = self.service_akteur.GetGefilterteListeMarktakteure(
                    apiKey=self.api_key,
                    marktakteurMastrNummer=self.marktakteur_mastr_nummer,
                    startAb=start,
                    limit=limit,
                    datumAb=datum_ab_iso,
                    # Filter:
                    marktfunktion=MARKTFUNKTION_STROMMARKT,
                    Marktrollen=[role_code],
                )
            except Fault as f:
                raise RuntimeError(f"SOAP Fault GetGefilterteListeMarktakteure: {f}") from f

            # resp.Marktakteure ist eine Liste (kann None sein)
            items = getattr(resp, "Marktakteure", None) or []
            for it in items:
                # Zeep-Objekt -> dict-ähnlich serialisieren
                yield zeep_to_dict(it)

            if len(items) < limit:
                break
            start += limit
            if sleep_s:
                time.sleep(sleep_s)

    def get_marktakteur_details(self, marktakteur_mastr_nummer: str) -> Dict[str, Any]:
        try:
            resp = self.service_akteur.GetMarktakteur(
                apiKey=self.api_key,
                marktakteurMastrNummer=self.marktakteur_mastr_nummer,
                marktakteurMastrNummerId=marktakteur_mastr_nummer,
            )
        except Fault as f:
            raise RuntimeError(f"SOAP Fault GetMarktakteur: {f}") from f
        obj = getattr(resp, "Marktakteur", None)
        return zeep_to_dict(obj) if obj is not None else {}

    # ----------------------------
    # 2) Stromspeicher-Einheiten
    # ----------------------------
    def iter_einheiten(
        self,
        limit: int = 2000,
        datum_ab_iso: Optional[str] = None,
        sleep_s: float = 0.0,
    ) -> Iterable[Dict[str, Any]]:
        """
        Liste Einheiten über GetListeAlleEinheiten (paging).
        Danach kann man nach Einheittyp filtern (z.B. Stromspeichereinheit).
        """
        start = 0
        while True:
            try:
                resp = self.service_anlage.GetListeAlleEinheiten(
                    apiKey=self.api_key,
                    marktakteurMastrNummer=self.marktakteur_mastr_nummer,
                    startAb=start,
                    limit=limit,
                    datumAb=datum_ab_iso,
                )
            except Fault as f:
                raise RuntimeError(f"SOAP Fault GetListeAlleEinheiten: {f}") from f

            items = getattr(resp, "Einheiten", None) or []
            for it in items:
                yield zeep_to_dict(it)

            if len(items) < limit:
                break
            start += limit
            if sleep_s:
                time.sleep(sleep_s)

    def get_einheit_stromspeicher_details(self, einheit_mastr_nummer: str) -> Dict[str, Any]:
        try:
            resp = self.service_anlage.GetEinheitStromSpeicher(
                apiKey=self.api_key,
                marktakteurMastrNummer=self.marktakteur_mastr_nummer,
                einheitMastrNummer=einheit_mastr_nummer,
            )
        except Fault as f:
            raise RuntimeError(f"SOAP Fault GetEinheitStromSpeicher: {f}") from f

        # Antwort-Property heißt i.d.R. EinheitStromSpeicher oder ähnlich – wir nehmen "alles" was drin ist:
        return zeep_to_dict(resp)


def zeep_to_dict(obj: Any) -> Any:
    """
    Robust: Zeep-Objekte / Listen / Primitive -> JSON-fähig.
    """
    if obj is None:
        return None
    # Listen/Tuples
    if isinstance(obj, (list, tuple)):
        return [zeep_to_dict(x) for x in obj]
    # Primitive
    if isinstance(obj, (str, int, float, bool)):
        return obj
    # Date/Datetime kommen oft als python datetime/date -> isoformat
    if hasattr(obj, "isoformat") and callable(obj.isoformat):
        try:
            return obj.isoformat()
        except Exception:
            pass
    # Zeep objects: haben __iter__ auf fields
    if hasattr(obj, "__keylist__") or hasattr(obj, "__values__") or hasattr(obj, "__dict__"):
        d: Dict[str, Any] = {}
        # Zeep complex objects: dir() enthält Felder als Attribute
        for k in dir(obj):
            if k.startswith("_"):
                continue
            try:
                v = getattr(obj, k)
            except Exception:
                continue
            # Filter Methoden
            if callable(v):
                continue
            # Heuristik: Zeep legt viele interne Sachen an; wir nehmen nur "wahrscheinlich echte" Felder
            # -> wenn es JSON-serialisierbar wird oder ein Zeep-Objekt/Container ist.
            if k in ("_xsd_type", "_value_1"):
                continue
            # Du kannst hier optional eine Whitelist machen – ich lasse es flexibel.
            d[k] = zeep_to_dict(v)
        # Entferne leere Artefakte
        d = {k: v for k, v in d.items() if v is not None and v != {} and v != []}
        return d
    # Fallback:
    return str(obj)


def write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def cmd_actors(args: argparse.Namespace) -> int:
    api_key = os.getenv("MASTR_API_KEY", "").strip()
    marktakteur = os.getenv("MASTR_MARKTAKTEUR", "").strip()

    role = args.role.upper().strip()
    if role not in ROLE_MAP:
        raise SystemExit(f"Unbekannte role '{role}'. Erlaubt: {', '.join(ROLE_MAP.keys())}")

    c = MastrClient(args.wsdl, api_key, marktakteur, timeout_s=args.timeout)
    role_code = ROLE_MAP[role]

    rows = c.iter_marktakteure_by_role(
        role_code=role_code,
        limit=args.limit,
        datum_ab_iso=args.datum_ab,
        sleep_s=args.sleep,
    )

    if args.out:
        write_jsonl(args.out, rows)
        print(f"Wrote {args.out}")
    else:
        # stdout jsonl
        for r in rows:
            print(json.dumps(r, ensure_ascii=False))
    return 0


def cmd_storage_units(args: argparse.Namespace) -> int:
    api_key = os.getenv("MASTR_API_KEY", "").strip()
    marktakteur = os.getenv("MASTR_MARKTAKTEUR", "").strip()
    c = MastrClient(args.wsdl, api_key, marktakteur, timeout_s=args.timeout)

    # 1) alle Einheiten iterieren, 2) filtern: Einheittyp == Stromspeichereinheit
    einheiten = c.iter_einheiten(limit=args.limit, datum_ab_iso=args.datum_ab, sleep_s=args.sleep)

    def filtered_details():
        for e in einheiten:
            einheittyp = e.get("Einheittyp")
            einheit_nummer = e.get("EinheitMastrNummer")
            if einheittyp == EINHEITTYP_STROMSPEICHER and einheit_nummer:
                details = c.get_einheit_stromspeicher_details(einheit_nummer)
                yield {"Einheit": e, "Details": details}

    rows = filtered_details()

    if args.out:
        write_jsonl(args.out, rows)
        print(f"Wrote {args.out}")
    else:
        for r in rows:
            print(json.dumps(r, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="MaStR SOAP Fetcher (Public/1_2)")
    p.add_argument("--wsdl", default=WSDL_URL_DEFAULT, help="WSDL URL")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds")
    p.add_argument("--limit", type=int, default=2000, help="Paging limit (max sinnvoll: 2000)")
    p.add_argument("--datum-ab", default=None, help='Delta-Abfrage ab ISO datetime, z.B. "2025-01-01T00:00:00"')
    p.add_argument("--sleep", type=float, default=0.0, help="Sleep zwischen Pages (sek)")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("actors", help="Marktakteure (Strommarkt) nach Rolle")
    p1.add_argument("--role", required=True, help="BV | LT | MB")
    p1.add_argument("--out", default=None, help="Output JSONL Datei (sonst stdout)")
    p1.set_defaults(func=cmd_actors)

    p2 = sub.add_parser("storage-units", help="Stromspeicher-Einheiten (Liste->Filter->Details)")
    p2.add_argument("--out", default=None, help="Output JSONL Datei (sonst stdout)")
    p2.set_defaults(func=cmd_storage_units)

    return p


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
