#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Update merged CSV with geocoded coordinates.
Can be called from the HTML visualization to save coordinates.
"""

import csv
import json
import sys

def update_coordinates(csv_file, coordinates_json):
    """
    Update CSV file with coordinates.
    coordinates_json should be a JSON string with format:
    {"MaStR-Nr": {"lat": float, "lon": float}, ...}
    """
    # Read coordinates
    try:
        coords = json.loads(coordinates_json)
    except:
        coords = {}
    
    # Read CSV
    rows = []
    headers = None
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        headers = reader.fieldnames
        
        # Add Latitude and Longitude columns if they don't exist
        if 'Latitude' not in headers:
            headers = list(headers) + ['Latitude', 'Longitude']
        
        for row in reader:
            mastr_num = row.get('MaStR-Nr. der Einheit', '').strip()
            
            # Update coordinates if available
            if mastr_num in coords:
                row['Latitude'] = str(coords[mastr_num]['lat'])
                row['Longitude'] = str(coords[mastr_num]['lon'])
            elif 'Latitude' not in row:
                row['Latitude'] = ''
                row['Longitude'] = ''
            
            rows.append(row)
    
    # Write back
    with open(csv_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=';')
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✓ Updated {len(coords)} coordinates in {csv_file}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python update_coordinates.py <csv_file> <coordinates_json_file>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    coords_file = sys.argv[2]
    
    with open(coords_file, 'r', encoding='utf-8') as f:
        coordinates_json = f.read()
    
    update_coordinates(csv_file, coordinates_json)

