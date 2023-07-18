# Install the mongomock library
# pip install mongomock

import pandas as pd
from pymongo import MongoClient
import unittest
from mongomock import MongoClient as MockMongoClient

def save_watershed_to_mongo(dataframe, db_name, collection_name):
    """
    Save the watershed names ('name' column) and their corresponding area ('area' column) from a DataFrame to a MongoDB collection.

    Args:
        dataframe (pd.DataFrame): The pandas DataFrame containing the 'ext_id' and 'name' columns.
        db_name (str): The name of the MongoDB database.
        collection_name (str): The name of the collection where the data will be stored.

    Returns:
        int: The number of documents inserted into the MongoDB collection.
    """
    # Check if the 'area' and 'name' columns exist in the DataFrame
    if 'area' not in dataframe.columns or 'name' not in dataframe.columns:
        raise ValueError("The DataFrame must contain 'ext_id' and 'name' columns.")

    # Connect to MongoDB (in this case, we use mongomock for in-memory connection)
    client = MockMongoClient()
    db = client[db_name]
    collection = db[collection_name]

    # Convert the DataFrame to a list of documents (dictionaries)
    data_to_insert = dataframe[['area', 'name']].to_dict(orient='records')

    # Insert the documents into the MongoDB collection
    result = collection.insert_many(data_to_insert)

    # Return the number of inserted documents
    return len(result.inserted_ids)

# Unit Test using unittest
class TestSavewatershedsToMongo(unittest.TestCase):

    def setUp(self):
        # Create a sample DataFrame with watershed names and areas
        data = {
            'area': [0.1697,94.4816,0.105415],
            'name': ['Jilo Dokicha', 'Burra', 'Dire'],
            'adm3': ['64b53d9adc061db2fa37fda5', '64b53d9adc061db2fa37fda6', '64b53d9adc061db2fa37fda7'],
        }
        self.df = pd.DataFrame(data)

    def test_save_watersheds_to_mongo(self):
        # Define the names of the test database and collection
        db_name = 'test_db'
        collection_name = 'test_collection'

        # Call the function to save watershed names to MongoDB
        result = save_watershed_to_mongo(self.df, db_name, collection_name)
        # Verify that the watershed names were inserted correctly
        self.assertEqual(result, 3)  # We expect 3 watershed names to be inserted

if __name__ == '__main__':
    unittest.main()
# Install the mongomock library
# pip install mongomock


