#script checks the shapefile 

import os
data_path='D:\ETL\data'
#data path to the shape file
def get_shp_file_import():
    try:
        shape_dir = 'admin_levels_ws_id'
        data = os.path.join(data_path, shape_dir)
        file_name = 'admin_levels_ws_id'
        file_shp = os.path.join(data, file_name + '.shp')
        if os.path.exists(file_shp):
            return file_shp
        else:
            raise FileNotFoundError(f"Shapefile {file_name}.shp not found in directory: {data}")
    except Exception as e:
        # Create an "error" folder if it doesn't exist
        error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
        os.makedirs(error_folder, exist_ok=True)

        # Log the error message to a file
        error_log_file = os.path.join(error_folder, 'error_geting_shpfile_log.txt')
        with open(error_log_file, 'a') as f:
            f.write(str(e) + '\n')
        return None



def get_watershed_file():
    try:
        file_name = 'w_shade'
        file_csv = os.path.join(data_path, file_name + '.csv')
        if os.path.exists(file_csv):
            return file_csv
        else:
            raise FileNotFoundError(f"CSV file {file_name}.csv not found in directory: {data_path}")
    except Exception as e:
        # Create an "error" folder if it doesn't exist
        error_folder = os.path.join(os.path.dirname(__file__), '..', 'error')
        os.makedirs(error_folder, exist_ok=True)

        # Log the error message to a file
        error_log_file = os.path.join(error_folder, 'error_geting_csv_file_log.txt')
        with open(error_log_file, 'a') as f:
            f.write(str(e) + '\n')
        return None

