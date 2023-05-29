import unittest
from mongomock import MongoClient
from orm.database import *
import glob

# Simulate a conection with mongomuck
client = MongoClient()
connect(host=client)

# # get the files from a path
folder_path = "C:/Users/cnasayo/Downloads/wp_vip/txt/"
txt_files = glob.glob(folder_path + "*.txt")


class TestImportData(unittest.TestCase):
    def setUp(self):
        # Delete the test documents after each test.
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

            # Verify that the documents have been saved correctly now
            count = HistoricalData.objects().count()
            self.assertEqual(len(lines), count, "Number of saved documents does not match")

            # Verify that the saved data matches the original data now.
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
