from src.BlockingQueue import BlockingQueue
from src.PrefixTree import PrefixTree
from src.BST import BST
import threading
import os


class IndexManager:
    """
    Class for managing the database indices
    """
    def __init__(self, database, reader):
        self.database = database
        self.reader = reader
        self.index_queue = BlockingQueue()
        self.indexing_threads_running = True
        self.indexing_thread = threading.Thread(target=self.get_data_to_index, name="indexing_thread_1", args=())
        self.indexing_thread.start()
        self.indices = dict()

        self.load_existing_indices()

    def load_existing_indices(self):
        """
        Searches the indices folder to see if any indices exist.  Loads the indices in if yes. \n
        :return: None
        """
        files_array = os.listdir('../indices')
        for file in files_array:
            print(file)
            data = self.reader.read_from_file("../indices/" + file)
            self.init_index(data)

    def init_index(self, data):
        """
        Adds an index to the index manager. \n
        :param data: Contents of an index file
        :return: None
        """
        name = data['name']
        index_type = data['index_type']
        index = data['index']
        self.create_db_index(name, index_type)
        self.indices[name].from_dict(index)

    def add(self, data_row):
        self.index_queue.add(data_row)

    def contains(self, key):
        """
        Determines if an index exists for the provided key
        :param key: String - field name
        :return: Boolean
        """
        if key in self.indices:
            return True
        else:
            return False

    def create_db_index(self, key, data_type="unknown"):
        """
        Creates an index for the database \n
        :param key: The database object key that will be indexed
        :param data_type: reflects the type of index to be used
        :return: None
        """
        if data_type == "integer":
            self.indices[key] = BST(key)
        elif data_type == "string":
            self.indices[key] = PrefixTree(key)

    def get_data_to_index(self):
        """
        If the index queue is not empty, calls the index_value method to add item to the index.\n
        :return: None
        """
        while self.database.processing_threads_running or self.database.saving_threads_running:
            if not self.index_queue.isEmpty():
                self.index_value()

        print("Indexing threads stopped.")
        self.indexing_threads_running = False

    def index_value(self):
        """
        Adds data item to the index.\n
        :return: None
        """
        data_row = self.index_queue.pop()

        if not data_row:
            return

        for index_field in self.indices:
            if index_field not in data_row.data:
                break
            else:
                self.indices[index_field].add(data_row.data[index_field], data_row.getId())
        return

    def save_indices_to_files(self):
        """
        Saves each of the indices in the self.indices dict to files. \n
        :return: None
        """
        for index in self.indices:
            data = self.indices[index].to_dict()
            file_name = "../indices/" + index + ".json"
            self.database.write_to_file(file_name, data, write_to_cache=False)

    def find(self, key, query):
        """
        Finds the primary keys associated with a specific field value \n
        :param key: a string representing a key in the indices dictionary
        :param query: an object with a single key value pair
        :return: an array of primary keys
        """
        primary_keys = self.indices[key].find(query)
        return primary_keys
