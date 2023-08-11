import pandas as pd
import sys, os
from geopy.distance import geodesic
from ormWP.models.waterpoint import Waterpoint
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_connection import *
from parameters.get_file import *
from imports.import_dataframe import *

# Function to calculate distances and update waterpoints
def calculate_distances_and_update(df1, df2):
    updated_count = 0  # Counter for updated waterpoints
    for index1, row1 in df1.iterrows():
        min_distance = float('inf')
        closest_row = None
        for index2, row2 in df2.iterrows():
            dist = geodesic((row1['y_coord'], row1['x_coord']), (row2['ws_lat'], row2['ws_lon'])).kilometers
            if dist < min_distance:
                min_distance = dist
                closest_row = row2
        try:
            wp = Waterpoint.objects.get(name=str(row1['wsh_name']))
            if not wp.aclimate_id:  # Check if aclimate_id is not already set
                print(f"Updating {row1['wsh_name']}")
                wp.update(aclimate_id=str(closest_row['ws_id']))
                updated_count += 1  # Increment the counter if update is successful
        except Exception as e:
            log_error(f"Error updating waterpoint: {str(e)}")
    return updated_count  # Return the counter

# Error handling and logging
def log_error(message):
    error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
    os.makedirs(error_folder, exist_ok=True)

    error_log_file = os.path.join(error_folder, 'error_log.txt')
    with open(error_log_file, 'a') as f:
        f.write(f"{datetime.now()}: {message}\n")

# Connect to MongoDB
try:
    connect(host=get_mongo_conn_str())
    print('MongoDB connection established')
except Exception as e:
    log_error('Error in connecting MongoDB database: ' + str(e))
    sys.exit()

# Read DataFrames, calculate distances, and perform updates
try:
    df2 = pd.read_csv(get_aclimate_id(), delimiter=';')
    df1 = get_dataframe_with_watershed()
    updated_count = calculate_distances_and_update(df1, df2)
    print(f"Updated {updated_count} waterpoints.")
except Exception as e:
    log_error('Error during data processing: ' + str(e))
