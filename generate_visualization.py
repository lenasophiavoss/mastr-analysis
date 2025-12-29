#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate HTML visualization of storage systems data.
"""

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
import re

def parse_date(date_str):
    """Parse various date formats."""
    if not date_str or not date_str.strip():
        return None
    
    date_str = date_str.strip()
    
    # Try different formats
    formats = [
        '%d.%m.%Y',  # 10.12.2025
        '%Y-%m-%d',  # 2024-01-31
        '%Y-%m-%dT%H:%M:%S',  # 2024-01-31T07:21:38
        '%Y-%m-%dT%H:%M:%S.%f',  # 2024-01-31T07:21:38.816317
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str.split('T')[0], fmt)
        except:
            continue
    
    return None

def process_data(csv_file):
    """Process CSV and extract relevant data."""
    data = []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            try:
                # Parse power (handle comma as decimal separator)
                bruttoleistung_str = row.get('Bruttoleistung der Einheit', '').strip()
                bruttoleistung = 0
                if bruttoleistung_str:
                    try:
                        bruttoleistung = float(bruttoleistung_str.replace(',', '.'))
                    except ValueError:
                        pass
                
                status = row.get('Betriebs-Status', '').strip()
                plz = row.get('Postleitzahl', '').strip()
                ort = row.get('Ort', '').strip()
                wirtschaftszweig = row.get('AB_HauptwirtdschaftszweigGruppe', '').strip() or 'Unbekannt'
                name = row.get('Anzeige-Name der Einheit', '').strip()
                
                # Parse dates
                inbetriebnahme = parse_date(row.get('Inbetriebnahmedatum der Einheit', ''))
                registrierung = parse_date(row.get('Registrierungsdatum der Einheit', ''))
                
                # Check for existing coordinates
                lat = row.get('Latitude', '').strip()
                lon = row.get('Longitude', '').strip()
                if lat and lon:
                    try:
                        lat = float(lat)
                        lon = float(lon)
                    except:
                        lat = None
                        lon = None
                else:
                    lat = None
                    lon = None
                
                # Anlagenbetreiber information
                anlagenbetreiber_name = row.get('Name des Anlagenbetreibers (nur Org.)', '').strip() or row.get('AB_Firmenname', '').strip()
                anlagenbetreiber_mastr = row.get('MaStR-Nr. des Anlagenbetreibers', '').strip() or row.get('\tMaStR-Nr. des Anlagenbetreibers', '').strip() or row.get('AB_MastrNummer', '').strip()
                anlagenbetreiber_email = row.get('AB_Email', '').strip()
                anlagenbetreiber_telefon = row.get('AB_Telefon', '').strip()
                anlagenbetreiber_strasse = row.get('AB_Strasse', '').strip()
                anlagenbetreiber_hausnummer = row.get('AB_Hausnummer_Wert', '').strip()
                anlagenbetreiber_ort = row.get('AB_Ort', '').strip()
                anlagenbetreiber_plz = row.get('AB_Postleitzahl', '').strip()
                
                data.append({
                    'leistung': bruttoleistung,
                    'status': status,
                    'plz': plz,
                    'ort': ort,
                    'wirtschaftszweig': wirtschaftszweig,
                    'name': name,
                    'inbetriebnahme': inbetriebnahme,
                    'registrierung': registrierung,
                    'lat': lat,
                    'lon': lon,
                    'mastr_num': row.get('MaStR-Nr. der Einheit', '').strip(),
                    'anlagenbetreiber_name': anlagenbetreiber_name,
                    'anlagenbetreiber_mastr': anlagenbetreiber_mastr,
                    'anlagenbetreiber_email': anlagenbetreiber_email,
                    'anlagenbetreiber_telefon': anlagenbetreiber_telefon,
                    'anlagenbetreiber_adresse': f"{anlagenbetreiber_strasse} {anlagenbetreiber_hausnummer}".strip() if anlagenbetreiber_strasse or anlagenbetreiber_hausnummer else '',
                    'anlagenbetreiber_ort': anlagenbetreiber_ort,
                    'anlagenbetreiber_plz': anlagenbetreiber_plz
                })
            except Exception as e:
                continue
    
    return data

def calculate_growth_data(data, min_size=0, wirtschaftszweig=None):
    """Calculate month-over-month growth with optional filters."""
    # Filter data
    filtered_data = data
    if min_size > 0:
        filtered_data = [d for d in filtered_data if d['leistung'] >= min_size]
    if wirtschaftszweig:
        filtered_data = [d for d in filtered_data if d['wirtschaftszweig'] == wirtschaftszweig]
    
    # Group by month
    monthly_data = defaultdict(lambda: {'count': 0, 'capacity': 0})
    
    for d in filtered_data:
        if d['registrierung']:
            month_key = d['registrierung'].strftime('%Y-%m')
            monthly_data[month_key]['count'] += 1
            monthly_data[month_key]['capacity'] += d['leistung']
    
    # Sort by month
    sorted_months = sorted(monthly_data.keys())
    
    # Calculate cumulative and MoM growth
    cumulative_count = 0
    cumulative_capacity = 0
    growth_data = []
    prev_count = 0
    
    for month in sorted_months:
        cumulative_count += monthly_data[month]['count']
        cumulative_capacity += monthly_data[month]['capacity']
        
        mom_growth = monthly_data[month]['count'] - prev_count if prev_count > 0 else monthly_data[month]['count']
        mom_growth_pct = (mom_growth / prev_count * 100) if prev_count > 0 else 0
        
        growth_data.append({
            'month': month,
            'count': monthly_data[month]['count'],
            'capacity': monthly_data[month]['capacity'],
            'cumulative_count': cumulative_count,
            'cumulative_capacity': cumulative_capacity,
            'mom_growth': mom_growth,
            'mom_growth_pct': mom_growth_pct
        })
        
        prev_count = cumulative_count
    
    return growth_data

def calculate_yoy_data(data, min_size=0, wirtschaftszweig=None):
    """Calculate year-over-year growth with optional filters."""
    # Filter data
    filtered_data = data
    if min_size > 0:
        filtered_data = [d for d in filtered_data if d['leistung'] >= min_size]
    if wirtschaftszweig:
        filtered_data = [d for d in filtered_data if d['wirtschaftszweig'] == wirtschaftszweig]
    
    # Group by year
    yearly_data = defaultdict(lambda: {'count': 0, 'capacity': 0})
    
    for d in filtered_data:
        if d['registrierung']:
            year_key = d['registrierung'].strftime('%Y')
            yearly_data[year_key]['count'] += 1
            yearly_data[year_key]['capacity'] += d['leistung']
    
    # Sort by year
    sorted_years = sorted(yearly_data.keys())
    
    # Calculate cumulative and YoY growth
    yoy_data = []
    prev_year_count = 0
    prev_year_capacity = 0
    
    for year in sorted_years:
        current_count = yearly_data[year]['count']
        current_capacity = yearly_data[year]['capacity']
        
        yoy_growth_count = current_count - prev_year_count if prev_year_count > 0 else current_count
        yoy_growth_pct_count = (yoy_growth_count / prev_year_count * 100) if prev_year_count > 0 else 0
        
        yoy_growth_capacity = current_capacity - prev_year_capacity if prev_year_capacity > 0 else current_capacity
        yoy_growth_pct_capacity = (yoy_growth_capacity / prev_year_capacity * 100) if prev_year_capacity > 0 else 0
        
        yoy_data.append({
            'year': year,
            'count': current_count,
            'capacity': current_capacity,
            'yoy_growth_count': yoy_growth_count,
            'yoy_growth_pct_count': yoy_growth_pct_count,
            'yoy_growth_capacity': yoy_growth_capacity,
            'yoy_growth_pct_capacity': yoy_growth_pct_capacity
        })
        
        prev_year_count = current_count
        prev_year_capacity = current_capacity
    
    return yoy_data

def generate_html(data, output_file):
    """Generate HTML visualization."""
    
    # Calculate statistics
    total_capacity = sum(d['leistung'] for d in data)
    status_counts = Counter(d['status'] for d in data)
    
    # Group by Wirtschaftszweig
    wirtschaftszweig_capacity = defaultdict(float)
    wirtschaftszweig_count = Counter()
    for d in data:
        wirtschaftszweig_capacity[d['wirtschaftszweig']] += d['leistung']
        wirtschaftszweig_count[d['wirtschaftszweig']] += 1
    
    # Sort by capacity
    wirtschaftszweig_sorted = sorted(
        wirtschaftszweig_capacity.items(),
        key=lambda x: x[1],
        reverse=True
    )
    
    # Get all unique Wirtschaftszweige
    all_wirtschaftszweige = sorted(set(d['wirtschaftszweig'] for d in data))
    
    # Get largest systems for map (all systems, we'll filter client-side)
    all_systems = sorted(data, key=lambda x: x['leistung'], reverse=True)
    
    # Calculate growth data (base, will be recalculated client-side with filters)
    growth_data = calculate_growth_data(data)
    yoy_data = calculate_yoy_data(data)
    growth_data_all = data  # Store full data for client-side filtering
    
    # Prepare data for JavaScript
    map_data = []
    for d in all_systems:
        map_data.append({
            'name': d['name'],
            'leistung': d['leistung'],
            'status': d['status'],
            'ort': d['ort'],
            'plz': d['plz'],
            'wirtschaftszweig': d['wirtschaftszweig'],
            'lat': d['lat'],
            'lon': d['lon'],
            'mastr_num': d['mastr_num'],
            'anlagenbetreiber_name': d.get('anlagenbetreiber_name', ''),
            'anlagenbetreiber_mastr': d.get('anlagenbetreiber_mastr', ''),
            'anlagenbetreiber_email': d.get('anlagenbetreiber_email', ''),
            'anlagenbetreiber_telefon': d.get('anlagenbetreiber_telefon', ''),
            'anlagenbetreiber_adresse': d.get('anlagenbetreiber_adresse', ''),
            'anlagenbetreiber_ort': d.get('anlagenbetreiber_ort', ''),
            'anlagenbetreiber_plz': d.get('anlagenbetreiber_plz', '')
        })
    
    # Prepare table data
    table_data = []
    for d in sorted(data, key=lambda x: x['leistung'], reverse=True):
        table_data.append({
            'name': d['name'],
            'mastr_num': d['mastr_num'],
            'leistung': d['leistung'],
            'status': d['status'],
            'ort': d['ort'],
            'plz': d['plz'],
            'wirtschaftszweig': d['wirtschaftszweig'],
            'inbetriebnahme': d['inbetriebnahme'].strftime('%Y-%m-%d') if d['inbetriebnahme'] else '',
            'registrierung': d['registrierung'].strftime('%Y-%m-%d') if d['registrierung'] else '',
            'anlagenbetreiber_name': d.get('anlagenbetreiber_name', ''),
            'anlagenbetreiber_mastr': d.get('anlagenbetreiber_mastr', ''),
            'anlagenbetreiber_email': d.get('anlagenbetreiber_email', ''),
            'anlagenbetreiber_telefon': d.get('anlagenbetreiber_telefon', ''),
            'anlagenbetreiber_adresse': d.get('anlagenbetreiber_adresse', ''),
            'anlagenbetreiber_ort': d.get('anlagenbetreiber_ort', ''),
            'anlagenbetreiber_plz': d.get('anlagenbetreiber_plz', '')
        })
    
    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stromspeicher-Analyse</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        h1 {{
            color: #2c3e50;
            margin-bottom: 30px;
            text-align: center;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stat-card h3 {{
            color: #7f8c8d;
            font-size: 14px;
            margin-bottom: 10px;
            text-transform: uppercase;
        }}
        .stat-card .value {{
            color: #2c3e50;
            font-size: 32px;
            font-weight: bold;
        }}
        .chart-container {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        .chart-container h2 {{
            color: #2c3e50;
            margin-bottom: 20px;
        }}
        .chart-small {{
            max-width: 400px;
            margin: 0 auto;
        }}
        #map {{
            height: 600px;
            border-radius: 8px;
            margin-bottom: 30px;
        }}
        .legend {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .info-box {{
            background: #e8f4f8;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            border-left: 4px solid #3498db;
        }}
        .filters {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
        }}
        .filters label {{
            display: flex;
            flex-direction: column;
            gap: 5px;
            font-size: 14px;
            color: #2c3e50;
        }}
        .filters select, .filters input {{
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
        }}
        thead {{
            background: #34495e;
            color: white;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            font-weight: 600;
            cursor: pointer;
            user-select: none;
        }}
        th:hover {{
            background: #2c3e50;
        }}
        tbody tr:hover {{
            background: #f8f9fa;
        }}
        .table-container {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 30px;
            overflow-x: auto;
        }}
        .table-container h2 {{
            color: #2c3e50;
            margin-bottom: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔋 Stromspeicher-Analyse Deutschland</h1>
        
        <div class="info-box">
            <strong>Hinweis:</strong> Die Karte zeigt alle Speichersysteme mit verfügbaren Koordinaten. 
            Systeme ohne Koordinaten werden automatisch geokodiert (max. 50 neue pro Ladevorgang).
            <button onclick="exportCoordinates()" style="margin-left: 10px; padding: 5px 15px; background: #27ae60; color: white; border: none; border-radius: 4px; cursor: pointer;">Koordinaten exportieren</button>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Gesamtkapazität</h3>
                <div class="value">{total_capacity/1000:.1f} MW</div>
            </div>
            <div class="stat-card">
                <h3>Anzahl Systeme</h3>
                <div class="value">{len(data):,}</div>
            </div>
            <div class="stat-card">
                <h3>In Betrieb</h3>
                <div class="value">{status_counts.get('In Betrieb', 0):,}</div>
            </div>
            <div class="stat-card">
                <h3>In Planung</h3>
                <div class="value">{status_counts.get('In Planung', 0):,}</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h2>Status-Verteilung</h2>
            <div class="chart-small">
                <canvas id="statusChart"></canvas>
            </div>
        </div>
        
        <div class="chart-container">
            <h2>Kumulatives Wachstum der Speichersysteme</h2>
            <canvas id="growthChart"></canvas>
        </div>
        
        <div class="chart-container">
            <h2>Monatliches Wachstum (MoM)</h2>
            <canvas id="momGrowthChart"></canvas>
        </div>
        
        <div class="chart-container">
            <h2>Größte Wirtschaftszweige</h2>
            <div id="wirtschaftszweigList" style="max-height: 400px; overflow-y: auto;">
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr style="background-color: #34495e; color: white;">
                            <th style="padding: 10px; text-align: left;">Wirtschaftszweig</th>
                            <th style="padding: 10px; text-align: right;">Gesamtkapazität (MW)</th>
                            <th style="padding: 10px; text-align: right;">Anzahl Systeme</th>
                        </tr>
                    </thead>
                    <tbody id="wirtschaftszweigListBody">
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="chart-container">
            <h2>Wirtschaftszweig Analyse</h2>
            <div class="filters" style="margin-bottom: 20px;">
                <label>
                    <span>Wirtschaftszweig auswählen:</span>
                    <select id="wzFilterSelect" style="min-width: 300px;">
                        <option value="">Bitte wählen...</option>
                        {''.join(f'<option value="{wz}">{wz}</option>' for wz in all_wirtschaftszweige)}
                    </select>
                </label>
                <button onclick="updateWZAnalysis()" style="padding: 8px 16px; background-color: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">Analyse anzeigen</button>
            </div>
            
            <div id="wzAnalysisResults" style="display: none;">
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-bottom: 30px;">
                    <div style="background-color: #ecf0f1; padding: 15px; border-radius: 8px;">
                        <h3 style="margin-top: 0; color: #2c3e50;">Mittelwert Kapazität</h3>
                        <p id="wzMeanCapacity" style="font-size: 24px; font-weight: bold; color: #3498db;">-</p>
                    </div>
                    <div style="background-color: #ecf0f1; padding: 15px; border-radius: 8px;">
                        <h3 style="margin-top: 0; color: #2c3e50;">Median Kapazität</h3>
                        <p id="wzMedianCapacity" style="font-size: 24px; font-weight: bold; color: #27ae60;">-</p>
                    </div>
                </div>
                
                <div style="margin-bottom: 30px;">
                    <h3 style="color: #2c3e50;">Aktivste Anlagenbetreiber</h3>
                    <table id="wzTopOperators" style="width: 100%; border-collapse: collapse; margin-top: 10px;">
                        <thead>
                            <tr style="background-color: #34495e; color: white;">
                                <th style="padding: 10px; text-align: left;">Anlagenbetreiber</th>
                                <th style="padding: 10px; text-align: right;">Gesamtkapazität (MW)</th>
                                <th style="padding: 10px; text-align: right;">Anzahl Projekte</th>
                                <th style="padding: 10px; text-align: left;">Kontakt</th>
                            </tr>
                        </thead>
                        <tbody id="wzTopOperatorsBody">
                        </tbody>
                    </table>
                </div>
                
                <div style="margin-bottom: 30px;">
                    <h3 style="color: #2c3e50;">Top 5 Größte Projekte</h3>
                    <table id="wzTopProjects" style="width: 100%; border-collapse: collapse; margin-top: 10px;">
                        <thead>
                            <tr style="background-color: #34495e; color: white;">
                                <th style="padding: 10px; text-align: left;">Projektname</th>
                                <th style="padding: 10px; text-align: right;">Kapazität (MW)</th>
                                <th style="padding: 10px; text-align: left;">Status</th>
                                <th style="padding: 10px; text-align: left;">Ort</th>
                                <th style="padding: 10px; text-align: left;">Anlagenbetreiber</th>
                                <th style="padding: 10px; text-align: left;">Kontakt</th>
                            </tr>
                        </thead>
                        <tbody id="wzTopProjectsBody">
                        </tbody>
                    </table>
                </div>
                
                <div>
                    <h3 style="color: #2c3e50;">Kumulatives Wachstum</h3>
                    <canvas id="wzGrowthChart"></canvas>
                </div>
            </div>
        </div>
        
        <div class="table-container">
            <h2>Jährliches Wachstum (YoY)</h2>
            <div class="filters">
                <label>
                    <span>Min. Projektgröße (kW):</span>
                    <input type="number" id="yoyMinSize" value="0" min="0" step="100">
                </label>
                <label>
                    <span>Wirtschaftszweig:</span>
                    <select id="yoyWirtschaftszweig">
                        <option value="">Alle</option>
                        {''.join(f'<option value="{wz}">{wz}</option>' for wz in all_wirtschaftszweige)}
                    </select>
                </label>
            </div>
            <table id="yoyTable">
                <thead>
                    <tr>
                        <th>Jahr</th>
                        <th>Neue Systeme</th>
                        <th>YoY Wachstum (Anzahl)</th>
                        <th>YoY % (Anzahl)</th>
                        <th>Neue Kapazität (MW)</th>
                        <th>YoY Wachstum (Kapazität)</th>
                        <th>YoY % (Kapazität)</th>
                    </tr>
                </thead>
                <tbody id="yoyTableBody">
                </tbody>
            </table>
        </div>
        
        <div class="chart-container">
            <h2>Größte Speichersysteme - Karte</h2>
            <div class="filters">
                <label>
                    <span>Min. Kapazität (kW):</span>
                    <input type="number" id="mapMinSize" value="0" min="0" step="100">
                </label>
                <label>
                    <span>Max. Kapazität (kW):</span>
                    <input type="number" id="mapMaxSize" value="" placeholder="Kein Maximum" min="0" step="100">
                </label>
                <label>
                    <span>Wirtschaftszweig:</span>
                    <select id="mapWirtschaftszweig">
                        <option value="">Alle</option>
                        {''.join(f'<option value="{wz}">{wz}</option>' for wz in all_wirtschaftszweige)}
                    </select>
                </label>
                <label>
                    <span>Status:</span>
                    <select id="mapStatus">
                        <option value="">Alle</option>
                        <option value="In Betrieb">In Betrieb</option>
                        <option value="In Planung">In Planung</option>
                    </select>
                </label>
                <button onclick="updateMap()" style="padding: 8px 20px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">Karte aktualisieren</button>
            </div>
            <div id="map"></div>
            <div class="legend" style="margin-top: 15px;">
                <h3 style="margin-bottom: 10px; font-size: 14px;">Legende:</h3>
                <div style="display: flex; gap: 20px; flex-wrap: wrap;">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <div style="width: 15px; height: 15px; background: #27ae60; border-radius: 50%; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>
                        <span>In Betrieb</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <div style="width: 15px; height: 15px; background: #e74c3c; border-radius: 50%; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>
                        <span>In Planung</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <div style="width: 15px; height: 15px; background: #95a5a6; border-radius: 50%; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>
                        <span>Sonstige</span>
                    </div>
                    <div style="margin-left: auto; font-size: 12px; color: #7f8c8d;">
                        Marker-Größe = Kapazität
                    </div>
                </div>
            </div>
        </div>
        
        <div class="table-container">
            <h2>Speichersysteme Tabelle</h2>
            <div class="filters">
                <label>
                    <span>Min. Kapazität (kW):</span>
                    <input type="number" id="tableMinSize" value="0" min="0" step="100">
                </label>
                <label>
                    <span>Max. Kapazität (kW):</span>
                    <input type="number" id="tableMaxSize" value="" placeholder="Kein Maximum" min="0" step="100">
                </label>
                <label>
                    <span>Wirtschaftszweig:</span>
                    <select id="tableWirtschaftszweig">
                        <option value="">Alle</option>
                        {''.join(f'<option value="{wz}">{wz}</option>' for wz in all_wirtschaftszweige)}
                    </select>
                </label>
                <label>
                    <span>Status:</span>
                    <select id="tableStatus">
                        <option value="">Alle</option>
                        <option value="In Betrieb">In Betrieb</option>
                        <option value="In Planung">In Planung</option>
                        <option value="Endgültig stillgelegt">Endgültig stillgelegt</option>
                        <option value="Vorübergehend stillgelegt">Vorübergehend stillgelegt</option>
                    </select>
                </label>
                <label>
                    <span>Status suchen:</span>
                    <input type="text" id="tableStatusSearch" placeholder="Status eingeben..." style="width: 200px;">
                </label>
                <button onclick="updateTable()" style="padding: 8px 20px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">Tabelle aktualisieren</button>
            </div>
            <table id="dataTable">
                <thead>
                    <tr>
                        <th onclick="sortTable(0)">Name ↕</th>
                        <th onclick="sortTable(1)">MaStR-Nr. ↕</th>
                        <th onclick="sortTable(2)">Kapazität (kW) ↕</th>
                        <th onclick="sortTable(3)">Status ↕</th>
                        <th onclick="sortTable(4)">Ort ↕</th>
                        <th onclick="sortTable(5)">PLZ ↕</th>
                        <th onclick="sortTable(6)">Wirtschaftszweig ↕</th>
                        <th onclick="sortTable(7)">Inbetriebnahme ↕</th>
                        <th>Anlagenbetreiber</th>
                        <th>Kontaktdaten</th>
                    </tr>
                </thead>
                <tbody id="dataTableBody">
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        // Data from Python
        const mapData = {json.dumps(map_data, ensure_ascii=False)};
        const statusData = {json.dumps(dict(status_counts), ensure_ascii=False)};
        const wirtschaftszweigData = {json.dumps(dict(wirtschaftszweig_sorted[:20]), ensure_ascii=False)};
        const growthData = {json.dumps(growth_data, ensure_ascii=False, default=str)};
        const yoyData = {json.dumps(yoy_data, ensure_ascii=False, default=str)};
        const tableData = {json.dumps(table_data, ensure_ascii=False, default=str)};
        const allData = tableData; // Use tableData as allData for analysis
        const allDataForGrowth = {json.dumps([{'leistung': d['leistung'], 'wirtschaftszweig': d['wirtschaftszweig'], 'registrierung': d['registrierung'].strftime('%Y-%m') if d['registrierung'] else ''} for d in data], ensure_ascii=False, default=str)};
        const allDataForYoY = {json.dumps([{'leistung': d['leistung'], 'wirtschaftszweig': d['wirtschaftszweig'], 'registrierung': d['registrierung'].strftime('%Y') if d['registrierung'] else ''} for d in data], ensure_ascii=False, default=str)};
        
        let map;
        let markers = [];
        let currentSortColumn = -1;
        let sortDirection = 1;
        let wzGrowthChart = null; // Global variable for WZ growth chart
        
        // Initialize map
        function initMap() {{
            map = L.map('map').setView([51.1657, 10.4515], 6);
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
                attribution: '© OpenStreetMap contributors'
            }}).addTo(map);
            
            updateMap();
        }}
        
        // Update map with filters
        function updateMap() {{
            // Clear existing markers
            markers.forEach(m => map.removeLayer(m));
            markers = [];
            
            const minSize = parseFloat(document.getElementById('mapMinSize').value) || 0;
            const maxSize = parseFloat(document.getElementById('mapMaxSize').value) || Infinity;
            const wirtschaftszweig = document.getElementById('mapWirtschaftszweig').value;
            const status = document.getElementById('mapStatus').value;
            
            const filtered = mapData.filter(item => {{
                return item.leistung >= minSize && 
                       item.leistung <= maxSize &&
                       (wirtschaftszweig === '' || item.wirtschaftszweig === wirtschaftszweig) &&
                       (status === '' || item.status === status);
            }});
            
            let geocodeCount = 0;
            const maxNewGeocodes = 50;
            
            filtered.forEach(item => {{
                if (item.lat && item.lon) {{
                    // Use existing coordinates
                    addMarker(item, item.lat, item.lon);
                }} else if (geocodeCount < maxNewGeocodes) {{
                    // Geocode new ones
                    geocodeAndAddMarker(item, geocodeCount);
                    geocodeCount++;
                }}
            }});
        }}
        
        function addMarker(item, lat, lon) {{
            const statusColor = item.status === 'In Betrieb' ? '#27ae60' : 
                               item.status === 'In Planung' ? '#e74c3c' : '#95a5a6';
            const markerSize = Math.min(20, Math.max(8, Math.sqrt(item.leistung / 100)));
            const statusIcon = L.divIcon({{
                className: 'custom-marker',
                html: `<div style="background-color: ${{statusColor}}; width: ${{markerSize}}px; height: ${{markerSize}}px; border-radius: 50%; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>`,
                iconSize: [markerSize, markerSize],
                iconAnchor: [markerSize/2, markerSize/2]
            }});
            
            const marker = L.marker([lat, lon], {{icon: statusIcon}}).addTo(map);
            
            const capacityMW = (item.leistung / 1000).toFixed(2);
            const abAdresse = item.anlagenbetreiber_adresse ? `${{item.anlagenbetreiber_adresse}}<br>` : '';
            const abOrt = item.anlagenbetreiber_ort ? `${{item.anlagenbetreiber_ort}}` : '';
            const abPlz = item.anlagenbetreiber_plz ? ` (${{item.anlagenbetreiber_plz}})` : '';
            const abKontakt = (item.anlagenbetreiber_email || item.anlagenbetreiber_telefon) ? 
                `<br><strong>Kontakt:</strong><br>` + 
                (item.anlagenbetreiber_email ? `Email: ${{item.anlagenbetreiber_email}}<br>` : '') +
                (item.anlagenbetreiber_telefon ? `Tel: ${{item.anlagenbetreiber_telefon}}<br>` : '') : '';
            
            const popupContent = `
                <div style="min-width: 250px;">
                    <strong>${{item.name || 'Unbekannt'}}</strong><br>
                    <span style="font-size: 11px; color: #7f8c8d;">MaStR-Nr.: ${{item.mastr_num || 'N/A'}}</span>
                    <hr style="margin: 5px 0;">
                    <strong>Kapazität:</strong> ${{capacityMW}} MW (${{item.leistung.toFixed(2)}} kW)<br>
                    <strong>Status:</strong> ${{item.status}}<br>
                    <strong>Ort:</strong> ${{item.ort}} (${{item.plz}})<br>
                    <strong>Wirtschaftszweig:</strong> ${{item.wirtschaftszweig}}<br>
                    <hr style="margin: 8px 0;">
                    <strong>Anlagenbetreiber:</strong><br>
                    ${{item.anlagenbetreiber_name || 'Unbekannt'}}<br>
                    <span style="font-size: 11px; color: #7f8c8d;">MaStR-Nr.: ${{item.anlagenbetreiber_mastr || 'N/A'}}</span><br>
                    ${{abAdresse}}${{abOrt}}${{abPlz}}${{abKontakt}}
                </div>
            `;
            marker.bindPopup(popupContent);
            markers.push(marker);
        }}
        
        async function geocodeAndAddMarker(item, index) {{
            const query = item.plz && item.ort ? `${{item.plz}} ${{item.ort}}, Deutschland` : 
                         item.ort ? `${{item.ort}}, Deutschland` : 
                         item.plz ? `${{item.plz}}, Deutschland` : null;
            
            if (!query) return;
            
            try {{
                const delay = Math.floor(index / 5) * 1000;
                await new Promise(resolve => setTimeout(resolve, delay));
                
                const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${{encodeURIComponent(query)}}&limit=1&countrycodes=de`, {{
                    headers: {{'User-Agent': 'Stromspeicher-Analyse'}}
                }});
                const data = await response.json();
                
                if (data && data.length > 0) {{
                    const lat = parseFloat(data[0].lat);
                    const lon = parseFloat(data[0].lon);
                    
                    addMarker(item, lat, lon);
                    
                    // Store coordinates for export
                    if (item.mastr_num) {{
                        if (!window.geocodedCoords) {{
                            window.geocodedCoords = {{}};
                        }}
                        window.geocodedCoords[item.mastr_num] = {{lat, lon}};
                        localStorage.setItem(`coords_${{item.mastr_num}}`, JSON.stringify({{lat, lon}}));
                    }}
                }}
            }} catch (error) {{
                console.error('Geocoding error:', error);
            }}
        }}
        
        // Calculate YoY data with filters
        function calculateFilteredYoY(minSize, wirtschaftszweig) {{
            const filtered = allDataForYoY.filter(item => {{
                return item.leistung >= minSize &&
                       (wirtschaftszweig === '' || item.wirtschaftszweig === wirtschaftszweig) &&
                       item.registrierung;
            }});
            
            // Group by year
            const yearly = {{}};
            filtered.forEach(item => {{
                const year = item.registrierung;
                if (!yearly[year]) {{
                    yearly[year] = {{count: 0, capacity: 0}};
                }}
                yearly[year].count++;
                yearly[year].capacity += item.leistung;
            }});
            
            // Sort and calculate YoY
            const sortedYears = Object.keys(yearly).sort();
            const result = [];
            let prevYearCount = 0;
            let prevYearCapacity = 0;
            
            sortedYears.forEach(year => {{
                const currentCount = yearly[year].count;
                const currentCapacity = yearly[year].capacity;
                
                const yoyGrowthCount = prevYearCount > 0 ? currentCount - prevYearCount : currentCount;
                const yoyGrowthPctCount = prevYearCount > 0 ? (yoyGrowthCount / prevYearCount * 100) : 0;
                
                const yoyGrowthCapacity = prevYearCapacity > 0 ? currentCapacity - prevYearCapacity : currentCapacity;
                const yoyGrowthPctCapacity = prevYearCapacity > 0 ? (yoyGrowthCapacity / prevYearCapacity * 100) : 0;
                
                result.push({{
                    year,
                    count: currentCount,
                    capacity: currentCapacity,
                    yoyGrowthCount,
                    yoyGrowthPctCount,
                    yoyGrowthCapacity,
                    yoyGrowthPctCapacity
                }});
                
                prevYearCount = currentCount;
                prevYearCapacity = currentCapacity;
            }});
            
            return result;
        }}
        
        // Calculate MoM growth data for chart
        function calculateMoMGrowthForChart() {{
            const filtered = allDataForGrowth.filter(item => item.registrierung);
            
            // Group by month
            const monthly = {{}};
            filtered.forEach(item => {{
                const month = item.registrierung;
                if (!monthly[month]) {{
                    monthly[month] = {{count: 0, capacity: 0}};
                }}
                monthly[month].count++;
                monthly[month].capacity += item.leistung;
            }});
            
            // Sort and calculate MoM growth
            const sortedMonths = Object.keys(monthly).sort();
            const result = [];
            let prevCount = 0;
            let prevCapacity = 0;
            
            sortedMonths.forEach(month => {{
                const currentCount = monthly[month].count;
                const currentCapacity = monthly[month].capacity;
                
                const momGrowthCount = prevCount > 0 ? currentCount - prevCount : currentCount;
                const momGrowthPctCount = prevCount > 0 ? (momGrowthCount / prevCount * 100) : 0;
                
                const momGrowthCapacity = prevCapacity > 0 ? currentCapacity - prevCapacity : currentCapacity;
                const momGrowthPctCapacity = prevCapacity > 0 ? (momGrowthCapacity / prevCapacity * 100) : 0;
                
                result.push({{
                    month,
                    count: currentCount,
                    capacity: currentCapacity,
                    momGrowthCount,
                    momGrowthPctCount,
                    momGrowthCapacity,
                    momGrowthPctCapacity
                }});
                
                prevCount = currentCount;
                prevCapacity = currentCapacity;
            }});
            
            return result;
        }}
        
        // Update YoY table
        function updateYoYTable() {{
            const minSize = parseFloat(document.getElementById('yoyMinSize').value) || 0;
            const wirtschaftszweig = document.getElementById('yoyWirtschaftszweig').value;
            
            const filteredYoY = calculateFilteredYoY(minSize, wirtschaftszweig);
            
            const tbody = document.getElementById('yoyTableBody');
            tbody.innerHTML = '';
            
            if (filteredYoY.length === 0) {{
                const row = document.createElement('tr');
                row.innerHTML = '<td colspan="7" style="text-align: center; color: #7f8c8d;">Keine Daten für die gewählten Filter</td>';
                tbody.appendChild(row);
                return;
            }}
            
            filteredYoY.forEach(y => {{
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${{y.year}}</td>
                    <td>${{y.count}}</td>
                    <td>${{y.yoyGrowthCount > 0 ? '+' : ''}}${{y.yoyGrowthCount}}</td>
                    <td>${{y.yoyGrowthPctCount > 0 ? '+' : ''}}${{y.yoyGrowthPctCount.toFixed(1)}}%</td>
                    <td>${{(y.capacity / 1000).toFixed(2)}}</td>
                    <td>${{y.yoyGrowthCapacity > 0 ? '+' : ''}}${{(y.yoyGrowthCapacity / 1000).toFixed(2)}}</td>
                    <td>${{y.yoyGrowthPctCapacity > 0 ? '+' : ''}}${{y.yoyGrowthPctCapacity.toFixed(1)}}%</td>
                `;
                tbody.appendChild(row);
            }});
        }}
        
        
        // Update data table
        function updateTable() {{
            const minSize = parseFloat(document.getElementById('tableMinSize').value) || 0;
            const maxSize = parseFloat(document.getElementById('tableMaxSize').value) || Infinity;
            const wirtschaftszweig = document.getElementById('tableWirtschaftszweig').value;
            const status = document.getElementById('tableStatus').value;
            const statusSearch = document.getElementById('tableStatusSearch').value.toLowerCase();
            
            const filtered = tableData.filter(item => {{
                const statusMatch = status === '' || item.status === status;
                const statusSearchMatch = statusSearch === '' || item.status.toLowerCase().includes(statusSearch);
                
                return item.leistung >= minSize && 
                       item.leistung <= maxSize &&
                       (wirtschaftszweig === '' || item.wirtschaftszweig === wirtschaftszweig) &&
                       statusMatch && statusSearchMatch;
            }});
            
            const tbody = document.getElementById('dataTableBody');
            tbody.innerHTML = '';
            
            filtered.slice(0, 1000).forEach(item => {{
                const row = document.createElement('tr');
                const abAdresse = item.anlagenbetreiber_adresse ? `${{item.anlagenbetreiber_adresse}}<br>` : '';
                const abOrt = item.anlagenbetreiber_ort ? `${{item.anlagenbetreiber_ort}}` : '';
                const abPlz = item.anlagenbetreiber_plz ? ` ${{item.anlagenbetreiber_plz}}` : '';
                const abInfo = `
                    <strong>${{item.anlagenbetreiber_name || 'Unbekannt'}}</strong><br>
                    <span style="font-size: 11px; color: #7f8c8d;">MaStR-Nr.: ${{item.anlagenbetreiber_mastr || 'N/A'}}</span><br>
                    ${{abAdresse}}${{abOrt}}${{abPlz}}
                `;
                const kontaktInfo = `
                    ${{item.anlagenbetreiber_email ? 'Email: ' + item.anlagenbetreiber_email + '<br>' : ''}}
                    ${{item.anlagenbetreiber_telefon ? 'Tel: ' + item.anlagenbetreiber_telefon : ''}}
                    ${{!item.anlagenbetreiber_email && !item.anlagenbetreiber_telefon ? '-' : ''}}
                `;
                
                row.innerHTML = `
                    <td>${{item.name || 'Unbekannt'}}</td>
                    <td style="font-size: 11px; color: #7f8c8d;">${{item.mastr_num || 'N/A'}}</td>
                    <td>${{item.leistung.toFixed(2)}}</td>
                    <td>${{item.status}}</td>
                    <td>${{item.ort}}</td>
                    <td>${{item.plz}}</td>
                    <td>${{item.wirtschaftszweig}}</td>
                    <td>${{item.inbetriebnahme || '-'}}</td>
                    <td style="font-size: 12px;">${{abInfo}}</td>
                    <td style="font-size: 12px;">${{kontaktInfo}}</td>
                `;
                tbody.appendChild(row);
            }});
            
            if (filtered.length > 1000) {{
                const row = document.createElement('tr');
                row.innerHTML = `<td colspan="10" style="text-align: center; color: #7f8c8d;">... und ${{filtered.length - 1000}} weitere Einträge (nur erste 1000 angezeigt)</td>`;
                tbody.appendChild(row);
            }}
        }}
        
        function sortTable(column) {{
            if (currentSortColumn === column) {{
                sortDirection *= -1;
            }} else {{
                currentSortColumn = column;
                sortDirection = 1;
            }}
            
            const tbody = document.getElementById('dataTableBody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            
            rows.sort((a, b) => {{
                const aVal = a.cells[column].textContent.trim();
                const bVal = b.cells[column].textContent.trim();
                
                // Try numeric comparison
                const aNum = parseFloat(aVal);
                const bNum = parseFloat(bVal);
                
                if (!isNaN(aNum) && !isNaN(bNum)) {{
                    return (aNum - bNum) * sortDirection;
                }}
                
                return aVal.localeCompare(bVal) * sortDirection;
            }});
            
            rows.forEach(row => tbody.appendChild(row));
        }}
        
        // Export coordinates for saving to CSV
        function exportCoordinates() {{
            if (!window.geocodedCoords || Object.keys(window.geocodedCoords).length === 0) {{
                alert('Keine neuen Koordinaten zum Exportieren vorhanden.');
                return;
            }}
            
            const dataStr = JSON.stringify(window.geocodedCoords, null, 2);
            const dataBlob = new Blob([dataStr], {{type: 'application/json'}});
            const url = URL.createObjectURL(dataBlob);
            const link = document.createElement('a');
            link.href = url;
            link.download = 'geocoded_coordinates.json';
            link.click();
            URL.revokeObjectURL(url);
            
            alert(`${{Object.keys(window.geocodedCoords).length}} Koordinaten exportiert. Speichern Sie die Datei und führen Sie aus:\\n\\npython update_coordinates.py merged_stromerzeuger_anlagenbetreiber.csv geocoded_coordinates.json`);
        }}
        
        // Load existing coordinates from localStorage
        function loadStoredCoordinates() {{
            mapData.forEach(item => {{
                if (item.mastr_num && !item.lat && !item.lon) {{
                    const stored = localStorage.getItem(`coords_${{item.mastr_num}}`);
                    if (stored) {{
                        try {{
                            const coords = JSON.parse(stored);
                            item.lat = coords.lat;
                            item.lon = coords.lon;
                        }} catch (e) {{
                            console.error('Error loading stored coordinates:', e);
                        }}
                    }}
                }}
            }});
        }}
        
        // Initialize charts
        window.onload = function() {{
            loadStoredCoordinates();
            initMap();
            updateYoYTable();
            updateTable();
            
            // Add event listeners for YoY filters
            document.getElementById('yoyMinSize').addEventListener('change', updateYoYTable);
            document.getElementById('yoyWirtschaftszweig').addEventListener('change', updateYoYTable);
            
            // Add event listener for status search
            document.getElementById('tableStatusSearch').addEventListener('input', updateTable);
            
            // Status Chart (smaller)
            const statusCtx = document.getElementById('statusChart').getContext('2d');
            new Chart(statusCtx, {{
                type: 'doughnut',
                data: {{
                    labels: Object.keys(statusData),
                    datasets: [{{
                        data: Object.values(statusData),
                        backgroundColor: ['#27ae60', '#e74c3c', '#f39c12', '#3498db', '#95a5a6']
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {{
                        legend: {{position: 'right'}}
                    }}
                }}
            }});
            
            // Growth Chart
            const growthCtx = document.getElementById('growthChart').getContext('2d');
            new Chart(growthCtx, {{
                type: 'line',
                data: {{
                    labels: growthData.map(g => g.month),
                    datasets: [{{
                        label: 'Kumulative Anzahl',
                        data: growthData.map(g => g.cumulative_count),
                        borderColor: '#3498db',
                        backgroundColor: 'rgba(52, 152, 219, 0.1)',
                        fill: true,
                        yAxisID: 'y'
                    }}, {{
                        label: 'Kumulative Kapazität (MW)',
                        data: growthData.map(g => g.cumulative_capacity / 1000),
                        borderColor: '#27ae60',
                        backgroundColor: 'rgba(39, 174, 96, 0.1)',
                        fill: true,
                        yAxisID: 'y1'
                    }}]
                }},
                options: {{
                    responsive: true,
                    scales: {{
                        y: {{
                            type: 'linear',
                            position: 'left',
                            title: {{display: true, text: 'Anzahl Systeme'}}
                        }},
                        y1: {{
                            type: 'linear',
                            position: 'right',
                            title: {{display: true, text: 'Kapazität (MW)'}},
                            grid: {{drawOnChartArea: false}}
                        }}
                    }}
                }}
            }});
            
            // MoM Growth Chart
            const momGrowthData = calculateMoMGrowthForChart();
            const momGrowthCtx = document.getElementById('momGrowthChart').getContext('2d');
            new Chart(momGrowthCtx, {{
                type: 'line',
                data: {{
                    labels: momGrowthData.map(g => g.month),
                    datasets: [{{
                        label: 'MoM Wachstum (Anzahl)',
                        data: momGrowthData.map(g => g.momGrowthCount),
                        borderColor: '#e74c3c',
                        backgroundColor: 'rgba(231, 76, 60, 0.1)',
                        fill: true,
                        yAxisID: 'y'
                    }}, {{
                        label: 'MoM Wachstum % (Anzahl)',
                        data: momGrowthData.map(g => g.momGrowthPctCount),
                        borderColor: '#f39c12',
                        backgroundColor: 'rgba(243, 156, 18, 0.1)',
                        fill: true,
                        yAxisID: 'y1',
                        borderDash: [5, 5]
                    }}]
                }},
                options: {{
                    responsive: true,
                    interaction: {{
                        mode: 'index',
                        intersect: false
                    }},
                    scales: {{
                        x: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'Monat'
                            }}
                        }},
                        y: {{
                            type: 'linear',
                            display: true,
                            position: 'left',
                            title: {{
                                display: true,
                                text: 'MoM Wachstum (Anzahl)'
                            }}
                        }},
                        y1: {{
                            type: 'linear',
                            display: true,
                            position: 'right',
                            title: {{
                                display: true,
                                text: 'MoM Wachstum %'
                            }},
                            grid: {{
                                drawOnChartArea: false
                            }}
                        }}
                    }}
                }}
            }});
            
            // Populate Wirtschaftszweig List
            function populateWZList() {{
                const tbody = document.getElementById('wirtschaftszweigListBody');
                tbody.innerHTML = '';
                
                const wzArray = Object.entries(wirtschaftszweigData)
                    .map(([name, capacity]) => ({{
                        name,
                        capacity,
                        count: allData.filter(d => d.wirtschaftszweig === name).length
                    }}))
                    .sort((a, b) => b.capacity - a.capacity);
                
                wzArray.forEach((wz, index) => {{
                    const row = document.createElement('tr');
                    row.style.backgroundColor = index % 2 === 0 ? '#ffffff' : '#f8f9fa';
                    row.style.cursor = 'pointer';
                    row.onclick = () => {{
                        document.getElementById('wzFilterSelect').value = wz.name;
                        updateWZAnalysis();
                    }};
                    row.innerHTML = `
                        <td style="padding: 10px;">${{wz.name}}</td>
                        <td style="padding: 10px; text-align: right; font-weight: bold;">${{(wz.capacity / 1000).toFixed(2)}}</td>
                        <td style="padding: 10px; text-align: right;">${{wz.count}}</td>
                    `;
                    tbody.appendChild(row);
                }});
            }}
            
            populateWZList();
        }};
        
        // Make updateWZAnalysis globally available
        function updateWZAnalysis() {{
            try {{
                const selectedWZ = document.getElementById('wzFilterSelect').value;
                if (!selectedWZ) {{
                    document.getElementById('wzAnalysisResults').style.display = 'none';
                    return;
                }}
                
                if (!allData || !Array.isArray(allData)) {{
                    console.error('allData is not available or not an array');
                    alert('Daten nicht verfügbar. Bitte Seite neu laden.');
                    return;
                }}
                
                const filtered = allData.filter(d => d.wirtschaftszweig === selectedWZ);
            if (filtered.length === 0) {{
                alert('Keine Daten für diesen Wirtschaftszweig gefunden.');
                return;
            }}
            
            document.getElementById('wzAnalysisResults').style.display = 'block';
            
            // Calculate mean and median
            const capacities = filtered.map(d => d.leistung).sort((a, b) => a - b);
            const mean = capacities.reduce((a, b) => a + b, 0) / capacities.length;
            const median = capacities.length % 2 === 0
                ? (capacities[capacities.length / 2 - 1] + capacities[capacities.length / 2]) / 2
                : capacities[Math.floor(capacities.length / 2)];
            
            document.getElementById('wzMeanCapacity').textContent = `${{(mean / 1000).toFixed(2)}} MW`;
            document.getElementById('wzMedianCapacity').textContent = `${{(median / 1000).toFixed(2)}} MW`;
            
            // Top Anlagenbetreiber
            const operatorStats = {{}};
            filtered.forEach(d => {{
                const key = d.anlagenbetreiber_mastr || d.anlagenbetreiber_name || 'Unbekannt';
                if (!operatorStats[key]) {{
                    operatorStats[key] = {{
                        name: d.anlagenbetreiber_name || 'Unbekannt',
                        mastr: d.anlagenbetreiber_mastr || '',
                        email: d.anlagenbetreiber_email || '',
                        telefon: d.anlagenbetreiber_telefon || '',
                        capacity: 0,
                        count: 0
                    }};
                }}
                operatorStats[key].capacity += d.leistung;
                operatorStats[key].count += 1;
            }});
            
            const topOperators = Object.values(operatorStats)
                .sort((a, b) => b.capacity - a.capacity)
                .slice(0, 10);
            
            const operatorsTbody = document.getElementById('wzTopOperatorsBody');
            operatorsTbody.innerHTML = '';
            topOperators.forEach(op => {{
                const row = document.createElement('tr');
                const kontakt = `${{op.email ? 'Email: ' + op.email + '<br>' : ''}}${{op.telefon ? 'Tel: ' + op.telefon : ''}}${{!op.email && !op.telefon ? '-' : ''}}`;
                row.innerHTML = `
                    <td style="padding: 10px;">${{op.name}}<br><span style="font-size: 11px; color: #7f8c8d;">${{op.mastr || 'N/A'}}</span></td>
                    <td style="padding: 10px; text-align: right; font-weight: bold;">${{(op.capacity / 1000).toFixed(2)}}</td>
                    <td style="padding: 10px; text-align: right;">${{op.count}}</td>
                    <td style="padding: 10px; font-size: 12px;">${{kontakt}}</td>
                `;
                operatorsTbody.appendChild(row);
            }});
            
            // Top 5 Projects
            const topProjects = filtered
                .sort((a, b) => b.leistung - a.leistung)
                .slice(0, 5);
            
            const projectsTbody = document.getElementById('wzTopProjectsBody');
            projectsTbody.innerHTML = '';
            topProjects.forEach(proj => {{
                const row = document.createElement('tr');
                const kontakt = `${{proj.anlagenbetreiber_email ? 'Email: ' + proj.anlagenbetreiber_email + '<br>' : ''}}${{proj.anlagenbetreiber_telefon ? 'Tel: ' + proj.anlagenbetreiber_telefon : ''}}${{!proj.anlagenbetreiber_email && !proj.anlagenbetreiber_telefon ? '-' : ''}}`;
                row.innerHTML = `
                    <td style="padding: 10px;">${{proj.name || 'Unbekannt'}}<br><span style="font-size: 11px; color: #7f8c8d;">${{proj.mastr_num || 'N/A'}}</span></td>
                    <td style="padding: 10px; text-align: right; font-weight: bold;">${{(proj.leistung / 1000).toFixed(2)}}</td>
                    <td style="padding: 10px;">${{proj.status}}</td>
                    <td style="padding: 10px;">${{proj.ort}} (${{proj.plz}})</td>
                    <td style="padding: 10px;">${{proj.anlagenbetreiber_name || 'Unbekannt'}}</td>
                    <td style="padding: 10px; font-size: 12px;">${{kontakt}}</td>
                `;
                projectsTbody.appendChild(row);
            }});
            
            // Cumulative Growth Chart
            const monthlyData = {{}};
            filtered.forEach(d => {{
                if (d.registrierung) {{
                    // Extract YYYY-MM from date string (format: YYYY-MM-DD)
                    const month = d.registrierung.substring(0, 7);
                    if (!monthlyData[month]) {{
                        monthlyData[month] = {{count: 0, capacity: 0}};
                    }}
                    monthlyData[month].count += 1;
                    monthlyData[month].capacity += d.leistung;
                }}
            }});
            
            const sortedMonths = Object.keys(monthlyData).sort();
            let cumulativeCount = 0;
            let cumulativeCapacity = 0;
            const chartData = sortedMonths.map(month => {{
                cumulativeCount += monthlyData[month].count;
                cumulativeCapacity += monthlyData[month].capacity;
                return {{
                    month,
                    cumulativeCount,
                    cumulativeCapacity
                }};
            }});
            
            const wzChartCtx = document.getElementById('wzGrowthChart').getContext('2d');
            if (wzGrowthChart) {{
                wzGrowthChart.destroy();
            }}
            wzGrowthChart = new Chart(wzChartCtx, {{
                type: 'line',
                data: {{
                    labels: chartData.map(d => d.month),
                    datasets: [{{
                        label: 'Kumulative Anzahl',
                        data: chartData.map(d => d.cumulativeCount),
                        borderColor: '#3498db',
                        backgroundColor: 'rgba(52, 152, 219, 0.1)',
                        fill: true,
                        yAxisID: 'y'
                    }}, {{
                        label: 'Kumulative Kapazität (MW)',
                        data: chartData.map(d => d.cumulativeCapacity / 1000),
                        borderColor: '#27ae60',
                        backgroundColor: 'rgba(39, 174, 96, 0.1)',
                        fill: true,
                        yAxisID: 'y1'
                    }}]
                }},
                options: {{
                    responsive: true,
                    interaction: {{
                        mode: 'index',
                        intersect: false
                    }},
                    scales: {{
                        x: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'Monat'
                            }}
                        }},
                        y: {{
                            type: 'linear',
                            display: true,
                            position: 'left',
                            title: {{
                                display: true,
                                text: 'Anzahl Systeme'
                            }}
                        }},
                        y1: {{
                            type: 'linear',
                            display: true,
                            position: 'right',
                            title: {{
                                display: true,
                                text: 'Kapazität (MW)'
                            }},
                            grid: {{
                                drawOnChartArea: false
                            }}
                        }}
                    }}
                }}
            }});
            }} catch (error) {{
                console.error('Error in updateWZAnalysis:', error);
                alert('Fehler beim Laden der Analyse: ' + error.message);
            }}
        }}
    </script>
</body>
</html>"""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✓ Generated {output_file}")

if __name__ == "__main__":
    data = process_data('merged_stromerzeuger_anlagenbetreiber.csv')
    generate_html(data, 'stromspeicher_visualization.html')
    print(f"✓ Processed {len(data)} storage systems")
