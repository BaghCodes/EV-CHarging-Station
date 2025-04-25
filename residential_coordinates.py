import requests
import pandas as pd
import random
import time
from geopy.distance import geodesic

def get_delhi_residential_coordinates(min_locations=1000):
    print(f"Collecting {min_locations} residential coordinates in Delhi...")
    
    # Overpass API endpoint
    overpass_url = "https://overpass-api.de/api/interpreter"
    
    # Query targeting residential properties in Delhi
    query = """
    [out:json][timeout:360];
    area["name"="Delhi"]["admin_level"="4"]->.searchArea;
    (
      way["building"="apartments"](area.searchArea);
      way["building"="residential"](area.searchArea);
      way["building"="house"](area.searchArea);
      way["landuse"="residential"](area.searchArea);
      node["place"="neighbourhood"](area.searchArea);
      way["place"="neighbourhood"](area.searchArea);
    );
    out center;
    """
    
    try:
        print("Querying Overpass API...")
        response = requests.post(overpass_url, data=query)
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            data = {"elements": []}
        else:
            data = response.json()
            print(f"Retrieved {len(data.get('elements', []))} elements from Overpass API")
    except Exception as e:
        print(f"Error querying Overpass API: {e}")
        data = {"elements": []}
    
    # Process the results - extract only coordinates
    coordinates = []
    for element in data.get('elements', []):
        # Get coordinates
        if 'center' in element:
            lat = element['center']['lat']
            lon = element['center']['lon']
        else:
            lat = element.get('lat')
            lon = element.get('lon')
        
        if lat and lon:
            coordinates.append({
                'latitude': lat,
                'longitude': lon
            })
    
    print(f"Processed {len(coordinates)} locations from Overpass API")
    
    # If we don't have enough locations, generate synthetic data
    if len(coordinates) < min_locations:
        print(f"Generating additional synthetic coordinates to reach {min_locations} total...")
        synthetic_coordinates = generate_synthetic_coordinates(
            coordinates, 
            min_locations - len(coordinates),
            delhi_center=(28.6139, 77.2090),
            radius_km=20
        )
        coordinates.extend(synthetic_coordinates)
    
    # Create DataFrame and remove duplicates
    df = pd.DataFrame(coordinates)
    df.drop_duplicates(inplace=True)
    
    # Ensure we have at least the minimum requested locations
    if len(df) < min_locations:
        print(f"After removing duplicates, adding more synthetic coordinates...")
        more_synthetic = generate_synthetic_coordinates(
            df.to_dict('records'), 
            min_locations - len(df),
            delhi_center=(28.6139, 77.2090),
            radius_km=20,
            min_distance=0.01
        )
        df = pd.concat([df, pd.DataFrame(more_synthetic)], ignore_index=True)
    
    # Limit to exactly min_locations
    df = df.head(min_locations)
    
    # Save to CSV
    df.to_csv('delhi_residential_coordinates.csv', index=False)
    print(f"Collected {len(df)} unique residential coordinates in Delhi")
    
    return df

def generate_synthetic_coordinates(existing_coordinates, count, delhi_center, radius_km, min_distance=0.005):
    """Generate synthetic residential coordinates around Delhi"""
    synthetic = []
    
    # Convert existing coordinates to list of (lat, lon) tuples for distance checking
    existing_coords = [(loc['latitude'], loc['longitude']) for loc in existing_coordinates]
    
    attempts = 0
    max_attempts = count * 5  # Limit attempts to avoid infinite loop
    
    while len(synthetic) < count and attempts < max_attempts:
        attempts += 1
        
        # Generate random angle and distance from center
        angle = random.uniform(0, 360)
        distance = random.uniform(0, radius_km)
        
        # Convert to lat/lon offset (approximate)
        lat_offset = distance * 0.009 * random.uniform(0.7, 1.3)
        lon_offset = distance * 0.009 * random.uniform(0.7, 1.3)
        
        # Apply random direction
        if random.random() > 0.5:
            lat_offset = -lat_offset
        if random.random() > 0.5:
            lon_offset = -lon_offset
        
        # Calculate new coordinates
        new_lat = delhi_center[0] + lat_offset
        new_lon = delhi_center[1] + lon_offset
        
        # Check if within Delhi bounds (approximate)
        if not (28.40 <= new_lat <= 28.80 and 76.95 <= new_lon <= 77.40):
            continue
        
        # Check minimum distance from existing points
        too_close = False
        for coord in existing_coords:
            if geodesic((new_lat, new_lon), coord).kilometers < min_distance:
                too_close = True
                break
        
        if too_close:
            continue
        
        # Add to synthetic coordinates
        synthetic.append({
            'latitude': new_lat,
            'longitude': new_lon
        })
        
        # Add to existing coordinates for future distance checks
        existing_coords.append((new_lat, new_lon))
    
    print(f"Generated {len(synthetic)} synthetic coordinates after {attempts} attempts")
    return synthetic

# Run the function to get 1000 coordinates in Delhi
residential_df = get_delhi_residential_coordinates(min_locations=1000)

# Print the coordinates
print("\nSample of collected coordinates:")
for _, row in residential_df.head(20).iterrows():
    print(f"{row['latitude']}, {row['longitude']}")

print(f"\nTotal coordinates: {len(residential_df)}")
print("\nAll coordinates have been saved to 'delhi_residential_coordinates.csv'")
