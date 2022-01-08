import time
import os
from src.DataItem import DataItem
from src.IndexManager import IndexManager
from src.Writer import Writer
from src.DiskManager import DiskManager
from src.Reader import Reader
from src.DataProcessor import DataProcessor
from src.CacheManager import CacheManager
from src.QueryManager import QueryManager


class Database:
    """
    Main database class.
    """
    def __init__(self):
        self.largest_id = 0
        self.all_files = self.get_all_files()
        self.documents_per_file = 10000
        self.shutdown_flag = False
        self.processing_threads_running = True
        self.saving_threads_running = True
        self.find_ready_data_thread_running = True

        self.writer = Writer()
        self.reader = Reader()
        self.index_manager = IndexManager(self, self.reader)
        self.disk_manager = DiskManager(self, self.reader)
        self.cache_manager = CacheManager(self, self.reader)
        self.data_processor = DataProcessor(self, self.disk_manager)
        self.query_manager = QueryManager(self, self.cache_manager, self.index_manager)

    def create_new_item(self, collection, data={}):
        """
        Creates a new data item with the object provided.\n
        :param data: Data to be input to the database in the form of a dictionary
        :param collection: the collection that the data will belong to
        :return: Integer. Primary key of the new data item.
        """
        item_primary_key = self.largest_id
        data_row = DataItem(item_primary_key, data, collection)
        self.data_processor.add(data_row)
        self.index_manager.add(data_row)
        self.largest_id = self.largest_id + 1
        return item_primary_key

    def create_collection(self, collection_name):
        """
        Creates a new collection folder with the name provided.
        :param collection_name: String. Name of the collection to be created.
        :return: None
        """
        parent_dir = "../data/"
        new_dir = parent_dir + collection_name
        parent_dir_index = "../indices/"
        new_dir_index = parent_dir_index + collection_name
        try:
            os.mkdir(new_dir)
            os.mkdir(new_dir_index)
        except FileExistsError:
            print("collection already exists")

    def command_parser(self, query, collection):
        """
        Takes a query passed from the server and parses it out.  If the key is "pk", finds the data by
        primary key.  Otherwise, it finds the data by a field name.\n
        :param query: an object with a single key-value pair.
        :param collection: String. The collection the data belongs to.
        :return: an object with database primary keys as the keys and the data as the values.
        """
        for key in query:
            if key == "pk":
                return self.query_manager.find_by_primary_key_wrapper(query[key], collection)
            elif key == "shutdown":
                self.shutdown()
                return {}
            else:
                return self.query_manager.process_query(query, collection)

    def get_all_files(self):
        """
        gets the file_names of all files in the data directory and returns them as a set
        :return: a set of filenames
        """
        folders_array = os.listdir('../data')
        return_set = set()
        for folder in folders_array:
            files_array = os.listdir('../data/' + folder)
            for file_name in files_array:
                return_set.add("../data/" + folder + "/" + file_name)
        return return_set

    def get_file_name(self, database_id, collection):
        """
        Uses a simple hashing function on the database_id to obtain the filename containing
        the given id.\n
        :param database_id: integer representing database primary key
        :param collection: string representing the database collection
        :return: file name string
        """
        file_id = int(database_id / self.documents_per_file)
        file_name = "../data/" + collection + "/file" + str(file_id) + ".json"
        return file_name

    def update_item(self,item_primary_key, collection, data=dict()):
        """
        Updates an existing item with the data provided.\n
        :param item_primary_key: database primary key
        :param data: Updated data corresponding to the database primary key.
        :param collection: String. Collection the data belongs to.
        :return: Integer. Primary key of the updated item.
        """
        data_row = DataItem(item_primary_key, data, collection)
        self.data_processor.add(data_row)
        return item_primary_key

    def write_to_file(self, file_name, data, write_to_cache=True, remove_from_cache=False):
        """
        Writes a data object to a file.\n
        :param file_name: file name string
        :param data: object with format {"data": (dict)}
        :param write_to_cache: flag to indicate if data should be saved in the cache
        :param remove_from_cache: flag to indicate if cache data should be deleted
        :return: None
        """
        self.writer.write_to_file(file_name, data)
        if write_to_cache:
            self.cache_manager.update_cache_data(file_name, data)
        if remove_from_cache:
            self.cache_manager.remove_from_cache(file_name)

    def shutdown(self):
        """
        Initializes the shutdown routine to save indices and data to files. \n
        :return: None
        """
        print("Beginning shutdown routine.")
        self.shutdown_flag = True
        self.data_processor.stop()
        self.processing_threads_running = False
        self.disk_manager.stop()
        self.saving_threads_running = False
        self.index_manager.save_indices_to_files()
        self.cache_manager.clear_cache()
        self.save_params_to_file()
        print("Shutdown complete.")

    def save_params_to_file(self):
        """
        Saves the database parameters to a file.
        :return: None
        """
        save_object = {'largest_id': self.largest_id, 'documents_per_file': self.documents_per_file}
        self.write_to_file('../config/db_params.json', save_object, write_to_cache=False, remove_from_cache=False)

    def load_params_from_file(self):
        """
        Loads the database parameters from a file.
        :return: None
        """
        data = self.reader.read_from_file('../config/db_params.json')
        self.largest_id = data['largest_id']
        self.documents_per_file = data['documents_per_file']
