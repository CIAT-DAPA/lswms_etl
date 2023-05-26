from mongoengine import *
from database import *
import glob

connect(host="mongodb://root:s3cr3t@localhost:27017/waterpointa?authSource=admin")

# Obtener la lista de archivos de texto en la carpeta especificada
folder_path = "C:/Users/cnasayo/Downloads/wp_vip/txt/"
txt_files = glob.glob(folder_path + "*.txt")


for txt_file in txt_files:
    print(f"Importinf folder: {txt_file}")
    with open(txt_file, "r") as file:
        lines = file.readlines()[1:]

    
    for line in lines:
        
        data = line.split()
        date = data[0]
        rain = float(data[1])
        evaporation = float(data[2])
        depth = float(data[3])
        scaled_depth = float(data[4])

        document = {
            "date": date,
            "rain": rain,
            "evaporation": evaporation,
            "depth": depth,
            "scaled_depth": scaled_depth
        }

        HistoricalData(**document).save()
