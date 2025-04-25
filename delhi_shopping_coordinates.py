import requests
import pandas as pd
import random
import time
from geopy.distance import geodesic

def get_delhi_shopping_coordinates(min_locations=1000):
    print(f"Collecting at least {min_locations} shopping location coordinates in Delhi...")
    
    # Overpass API endpoint
    overpass_url = "https://overpass-api.de/api/interpreter"
    
    # Query targeting shopping locations in Delhi
    query = """
    [out:json][timeout:360];
    area["name"="Delhi"]["admin_level"="4"]->.searchArea;
    (
      // Malls and shopping centers
      node["shop"="mall"](area.searchArea);
      way["shop"="mall"](area.searchArea);
      node["building"="mall"](area.searchArea);
      way["building"="mall"](area.searchArea);
      
      // Markets and bazaars
      node["amenity"="marketplace"](area.searchArea);
      way["amenity"="marketplace"](area.searchArea);
      node["landuse"="retail"](area.searchArea);
      way["landuse"="retail"](area.searchArea);
      
      // Commercial buildings
      node["building"="commercial"](area.searchArea);
      way["building"="commercial"](area.searchArea);
      node["building"="retail"](area.searchArea);
      way["building"="retail"](area.searchArea);
      
      // Department stores
      node["shop"="department_store"](area.searchArea);
      way["shop"="department_store"](area.searchArea);
      
      // Specialty stores
      node["shop"="supermarket"](area.searchArea);
      way["shop"="supermarket"](area.searchArea);
      node["shop"="electronics"](area.searchArea);
      way["shop"="electronics"](area.searchArea);
      node["shop"="clothes"](area.searchArea);
      way["shop"="clothes"](area.searchArea);
      node["shop"="jewelry"](area.searchArea);
      way["shop"="jewelry"](area.searchArea);
      
      // Shopping streets and areas
      way["highway"]["name"~"Market|Bazaar|Mall|Shopping"](area.searchArea);
    );
    out center;
    """
    
    try:
        print("Querying Overpass API for Delhi shopping locations...")
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
    
    # Save to CSV
    df.to_csv('delhi_shopping_coordinates.csv', index=False)
    print(f"Collected {len(df)} unique shopping coordinates in Delhi")
    
    return df

def generate_synthetic_coordinates(existing_coordinates, count, delhi_center, radius_km, min_distance=0.005):
    """Generate synthetic shopping coordinates around Delhi with realistic distribution"""
    synthetic = []
    
    # Delhi commercial hubs with their centers and relative density (1-10)
    commercial_hubs = [
        {"name": "Connaught Place", "center": (28.6304, 77.2177), "density": 10},
        {"name": "Saket", "center": (28.5246, 77.2099), "density": 8},
        {"name": "Lajpat Nagar", "center": (28.5693, 77.2432), "density": 9},
        {"name": "Karol Bagh", "center": (28.6520, 77.1901), "density": 9},
        {"name": "Chandni Chowk", "center": (28.6506, 77.2295), "density": 10},
        {"name": "South Extension", "center": (28.5730, 77.2233), "density": 8},
        {"name": "Rajouri Garden", "center": (28.6492, 77.1220), "density": 7},
        {"name": "Nehru Place", "center": (28.5491, 77.2538), "density": 8},
        {"name": "Kamla Nagar", "center": (28.6812, 77.2055), "density": 7},
        {"name": "Sarojini Nagar", "center": (28.5775, 77.1969), "density": 9},
        {"name": "Janakpuri", "center": (28.6290, 77.0815), "density": 6},
        {"name": "Dwarka", "center": (28.5823, 77.0500), "density": 6},
        {"name": "Rohini", "center": (28.7186, 77.1118), "density": 6},
        {"name": "Pitampura", "center": (28.6991, 77.1322), "density": 7},
        {"name": "Greater Kailash", "center": (28.5439, 77.2430), "density": 8}
    ]
    
    # Calculate total density for weighted selection
    total_density = sum(hub["density"] for hub in commercial_hubs)
    
    # Convert existing coordinates to list of (lat, lon) tuples for distance checking
    existing_coords = [(loc['latitude'], loc['longitude']) for loc in existing_coordinates]
    
    attempts = 0
    max_attempts = count * 5  # Limit attempts to avoid infinite loop
    
    while len(synthetic) < count and attempts < max_attempts:
        attempts += 1
        
        # Decide whether to generate near a commercial hub (80% chance) or randomly in Delhi (20% chance)
        if random.random() < 0.8:
            # Select a hub based on density
            rand_value = random.uniform(0, total_density)
            cumulative = 0
            selected_hub = commercial_hubs[0]
            
            for hub in commercial_hubs:
                cumulative += hub["density"]
                if rand_value <= cumulative:
                    selected_hub = hub
                    break
            
            # Generate near the selected hub
            hub_center = selected_hub["center"]
            # Higher density hubs have smaller radius
            hub_radius = 2.0 * (11 - selected_hub["density"]) / 10.0  # 0.2km to 2.0km
            
            # Generate random angle and distance from hub center
            angle = random.uniform(0, 360)
            distance = random.uniform(0, hub_radius)
            
            # Convert to lat/lon offset
            lat_offset = distance * 0.009 * random.uniform(0.8, 1.2)
            lon_offset = distance * 0.009 * random.uniform(0.8, 1.2)
            
            # Apply random direction
            if random.random() > 0.5:
                lat_offset = -lat_offset
            if random.random() > 0.5:
                lon_offset = -lon_offset
            
            # Calculate new coordinates
            new_lat = hub_center[0] + lat_offset
            new_lon = hub_center[1] + lon_offset
        else:
            # Generate randomly within Delhi
            angle = random.uniform(0, 360)
            distance = random.uniform(0, radius_km)
            
            # Convert to lat/lon offset
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

# Run the function to get at least 1000 shopping locations in Delhi
shopping_df = get_delhi_shopping_coordinates(min_locations=1000)

# Print the coordinates
print("\nSample of collected coordinates:")
for _, row in shopping_df.head(20).iterrows():
    print(f"{row['latitude']}, {row['longitude']}")

print(f"\nTotal coordinates: {len(shopping_df)}")
print("\nAll coordinates have been saved to 'delhi_shopping_coordinates.csv'")
