from mongoengine import *
from orm.database import *
import glob
import openpyxl

connect(host="mongodb://root:s3cr3t@localhost:27017/waterpointa?authSource=admin")

# get the files from a path
folder_path = "C:/Users/cnasayo/Downloads/wp_vip/txt/"
txt_files = glob.glob(folder_path + "*.txt")

# Create excel file for erros
error_file = "error_log.xlsx"
workbook = openpyxl.Workbook()
worksheet = workbook.active
worksheet.append(["File", "Line", "Error"])

for txt_file in txt_files:
    print(f"Importing File: {txt_file}")
    with open(txt_file, "r") as file:
        lines = file.readlines()[1:]

    for index, line in enumerate(lines, start=2):
        try:
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
        except Exception as e:
            worksheet.append([txt_file, index, str(e)])

# save the errors
workbook.save(error_file)
print(f"erros has been saved in : {error_file}")
