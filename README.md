# MaStR Analysis

MaStR (Marktstammdatenregister) SOAP Client for fetching market participant and energy storage unit data.

## Description

This tool provides a Python client for interacting with the MaStR (Marktstammdatenregister) Public/1_2 SOAP API. It can fetch:

1. **Marktakteure** (Market Participants) filtered by roles: BV (Bilanzkreisverantwortlicher), LT (Stromlieferant), MB (Messstellenbetreiber)
2. **Stromspeichereinheiten** (Energy Storage Units) with detailed information

## Prerequisites

```bash
pip install zeep requests urllib3
```

## Setup

Set the following environment variables:

```bash
export MASTR_API_KEY="your-api-key"
export MASTR_MARKTAKTEUR="your-marktakteur-mastr-number"
```

## Usage

### Fetch Market Participants by Role

```bash
python master_fetch.py actors --role BV --limit 2000
python master_fetch.py actors --role LT --limit 2000 --out actors.jsonl
python master_fetch.py actors --role MB --limit 2000
```

### Fetch Energy Storage Units

```bash
python master_fetch.py storage-units --limit 2000 --out storage.jsonl
```

## Options

- `--wsdl`: WSDL URL (default: MaStR public WSDL)
- `--timeout`: HTTP timeout in seconds (default: 60)
- `--limit`: Paging limit, max recommended: 2000 (default: 2000)
- `--datum-ab`: Delta query from ISO datetime, e.g., "2025-01-01T00:00:00"
- `--sleep`: Sleep between pages in seconds (default: 0.0)
- `--out`: Output JSONL file (default: stdout)

## WSDL

The default WSDL URL is:
https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl

