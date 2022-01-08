import unittest
import time
from src.Database import Database


class MyTestCase(unittest.TestCase):
    def test_create_database(self):
        database = Database()
        time.sleep(2)
        database.create_collection("test_collection")
        time.sleep(2)
        database.index_manager.create_db_index("user_id", "test_collection", "integer")
        database.index_manager.create_db_index("user_name", "test_collection", "string")
        time.sleep(2)
        database.create_new_item("test_collection", {"user_id": 1, "user_name": "Alex"})
        database.create_new_item("test_collection", {"user_id": 2, "user_name": "Andrew"})
        database.create_new_item("test_collection", {"user_id": 3, "user_name": "Julia"})
        database.create_new_item("test_collection", {"user_id": 4, "user_name": "Philippe"})
        database.create_new_item("test_collection", {"user_id": 5, "user_name": "Keegan"})
        database.create_new_item("test_collection", {"user_id": 1, "user_name": "Colin"})
        time.sleep(2)
        database.shutdown()
        print('done')

    def test_query(self):
        database = Database()
        database.load_params_from_file()
        collection = "test_collection"
        query = {"$or": [{"$lt": {"user_id": 3}}, {"user_name": "k"}]}
        time.sleep(2)
        test = database.command_parser(query, collection)
        self.assertEqual(test, {0: {'user_id': 1, 'user_name': 'Alex'}, 1: {'user_id': 2, 'user_name': 'Andrew'},
                                4: {'user_id': 5, 'user_name': 'Keegan'}, 5: {'user_id': 1, 'user_name': 'Colin'}})
        print(test)
        database.shutdown()

    def test_db_load_and_insert(self):
        database = Database()
        database.load_params_from_file()
        time.sleep(2)
        database.create_new_item("test_collection", {"user_id": 6, "user_name": "Another One"})
        time.sleep(2)
        database.shutdown()

if __name__ == '__main__':
    unittest.main()
