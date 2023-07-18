# Install the mongomock library
# pip install mongomock

import pandas as pd
from pymongo import MongoClient
import unittest
from mongomock import MongoClient as MockMongoClient

def save_kebele_to_mongo(dataframe, db_name, collection_name):
    """
    Save the kebele names ('name' column) and their corresponding IDs ('ext_id' column) from a DataFrame to a MongoDB collection.

    Args:
        dataframe (pd.DataFrame): The pandas DataFrame containing the 'ext_id' and 'name' columns.
        db_name (str): The name of the MongoDB database.
        collection_name (str): The name of the collection where the data will be stored.

    Returns:
        int: The number of documents inserted into the MongoDB collection.
    """
    # Check if the 'ext_id' and 'name' columns exist in the DataFrame
    if 'ext_id' not in dataframe.columns or 'name' not in dataframe.columns:
        raise ValueError("The DataFrame must contain 'ext_id' and 'name' columns.")

    # Connect to MongoDB (in this case, we use mongomock for in-memory connection)
    client = MockMongoClient()
    db = client[db_name]
    collection = db[collection_name]

    # Convert the DataFrame to a list of documents (dictionaries)
    data_to_insert = dataframe[['ext_id', 'name']].to_dict(orient='records')

    # Insert the documents into the MongoDB collection
    result = collection.insert_many(data_to_insert)

    # Return the number of inserted documents
    return len(result.inserted_ids)

# Unit Test using unittest
class TestSavekebelesToMongo(unittest.TestCase):

    def setUp(self):
        # Create a sample DataFrame with kebele names and IDs
        data = {
            'ext_id': [40204015, 60201015, 40307016],
            'name': ['Senibo Gadisa', 'Chidaniguya', 'Mreto'],
            'adm2': ['64b53c99dc061db2fa37fd97', '64b53c99dc061db2fa37fd97', '64b53c99dc061db2fa37fd97'],
        }
        self.df = pd.DataFrame(data)

    def test_save_kebeles_to_mongo(self):
        # Define the names of the test database and collection
        db_name = 'test_db'
        collection_name = 'test_collection'

        # Call the function to save kebele names to MongoDB
        result = save_kebele_to_mongo(self.df, db_name, collection_name)
        # Verify that the kebele names were inserted correctly
        self.assertEqual(result, 3)  # We expect 3 kebele names to be inserted

if __name__ == '__main__':
    unittest.main()
# Install the mongomock library
# pip install mongomock


