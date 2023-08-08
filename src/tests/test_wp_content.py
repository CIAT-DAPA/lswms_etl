# Install the mongomock library
# pip install mongomock

import pandas as pd
from pymongo import MongoClient
import unittest
from mongomock import MongoClient as MockMongoClient

def save_wp_content_to_mongo(dataframe, db_name, collection_name):
    """
    Save the wp_content names ('agricuture' column) and their corresponding livestock content ('sheeps,cattles and cows' column) from a DataFrame to a MongoDB collection.

    Args:
        dataframe (pd.DataFrame): The pandas DataFrame containing the 'ext_id' and 'name' columns.
        db_name (str): The name of the MongoDB database.
        collection_name (str): The name of the collection where the data will be stored.

    Returns:
        int: The number of documents inserted into the MongoDB collection.
    """
    # Check if the 'area' and 'name' columns exist in the DataFrame
    if 'agriculture' not in dataframe.columns or 'number' not in dataframe.columns:
        raise ValueError("The DataFrame must contain 'agriculture' and 'number' columns.")

    # Connect to MongoDB (in this case, we use mongomock for in-memory connection)
    client = MockMongoClient()
    db = client[db_name]
    collection = db[collection_name]

    # Convert the DataFrame to a list of documents (dictionaries)
    data_to_insert = dataframe[['agriculture', 'number']].to_dict(orient='records')

    # Insert the documents into the MongoDB collection
    result = collection.insert_many(data_to_insert)

    # Return the number of inserted documents
    return len(result.inserted_ids)

# Unit Test using unittest
class TestSavewp_contentsToMongo(unittest.TestCase):

    def setUp(self):
        # Create a sample DataFrame with wp_content names and areas
        data = {
            'agriculture': ["cows","sheeps","cattle"],
            'number': [3, 5, 6],
            'waterpoint': ['64b53d9adc061db2fa37fda5', '64b53d9adc061db2fa37fda6', '64b53d9adc061db2fa37fda7'],
        }
        self.df = pd.DataFrame(data)

    def test_save_wp_contents_to_mongo(self):
        # Define the names of the test database and collection
        db_name = 'test_db'
        collection_name = 'test_collection'

        # Call the function to save wp_content names to MongoDB
        result = save_wp_content_to_mongo(self.df, db_name, collection_name)
        # Verify that the wp_content names were inserted correctly
        self.assertEqual(result, 3)  # We expect 3 wp_content names to be inserted

if __name__ == '__main__':
    unittest.main()
# Install the mongomock library
# pip install mongomock


