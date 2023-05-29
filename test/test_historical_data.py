import unittest
from mongomock import MongoClient
from database import *
import glob

# Simular una conexión a MongoDB con mongomock
client = MongoClient()
connect(host=client)

# Obtener la lista de archivos de texto en la carpeta especificada
folder_path = "C:/Users/cnasayo/Downloads/wp_vip/txt/"
txt_files = glob.glob(folder_path + "*.txt")


class TestImportData(unittest.TestCase):
    def setUp(self):
        # Eliminar los documentos de prueba después de cada prueba
        HistoricalData.objects().delete()

    def test_import_data(self):
        for txt_file in txt_files:
            print(f"Importing file: {txt_file}")
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

            # Verificar que los documentos se hayan guardado correctamente
            count = HistoricalData.objects().count()
            self.assertEqual(len(lines), count, "Number of saved documents does not match")

            # Verificar que los datos guardados coincidan con los datos originales
            for i, line in enumerate(lines):
                data = line.split()
                date = data[0]
                rain = float(data[1])
                evaporation = float(data[2])
                depth = float(data[3])
                scaled_depth = float(data[4])

                document = HistoricalData.objects(date=date).first()
                self.assertEqual(document.rain, rain, f"Rain data mismatch at line {i+2}")
                self.assertEqual(document.evaporation, evaporation, f"Evaporation data mismatch at line {i+2}")
                self.assertEqual(document.depth, depth, f"Depth data mismatch at line {i+2}")
                self.assertEqual(document.scaled_depth, scaled_depth, f"Scaled depth data mismatch at line {i+2}")


if __name__ == "__main__":
    unittest.main()
