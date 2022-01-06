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
        folders_array = os.listdir('../indices')
        for folder in folders_array:
            files_array = os.listdir('../indices/' + folder)
            for file in files_array:
                print(folder + "/" + file)
                data = self.reader.read_from_file("../indices/" + folder + "/" + file)
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
        collection = data['collection']
        self.create_db_index(name, collection, index_type)
        if collection not in self.indices:
            self.indices[collection] = dict()
        self.indices[collection][name].from_dict(index)

    def add(self, data_row):
        self.index_queue.add(data_row)

    def contains(self, key, collection):
        """
        Determines if an index exists for the provided key
        :param key: String - field name
        :param collection: String. Collection name.
        :return: Boolean
        """
        if collection not in self.indices:
            return False

        if key in self.indices[collection]:
            return True
        else:
            return False

    def create_db_index(self, key, collection, data_type="unknown"):
        """
        Creates an index for the database \n
        :param key: The database object key that will be indexed
        :param collection: String. The collection the index belongs to.
        :param data_type: reflects the type of index to be used
        :return: None
        """
        if collection not in self.indices:
            self.indices[collection] = dict()
        if data_type == "integer":
            self.indices[collection][key] = BST(key, collection)
        elif data_type == "string":
            self.indices[collection][key] = PrefixTree(key, collection)

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

        collection = data_row.get_collection()

        for index_field in self.indices[collection]:
            if index_field not in data_row.data:
                break
            else:
                self.indices[collection][index_field].add(data_row.data[index_field], data_row.getId())
        return

    def save_indices_to_files(self):
        """
        Saves each of the indices in the self.indices dict to files. \n
        :return: None
        """
        for collection in list(self.indices.keys()):
            for index in list(self.indices[collection].keys()):
                data = self.indices[collection][index].to_dict()
                file_name = "../indices/" + "/" + collection + "/" + index + ".json"
                self.database.write_to_file(file_name, data, write_to_cache=False)

    def find(self, key, query, collection):
        """
        Finds the primary keys associated with a specific field value \n
        :param key: a string representing a key in the indices dictionary
        :param query: an object with a single key value pair
        :param collection: String. The collection the index belongs to.
        :return: an array of primary keys
        """
        primary_keys = self.indices[collection][key].find(query)
        return primary_keys
