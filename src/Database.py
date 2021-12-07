import sys
from DataItem import DataItem
import ujson as json
import threading
from DataQueue import DataQueue
import time
import os
from BST import BST
from PrefixTree import PrefixTree

class Database:
    """
    Main database class.
    """
    def __init__(self):
        self.largest_id = 0
        self.indices = dict()
        self.data_queue = DataQueue()
        self.saving_queue = DataQueue()
        self.index_queue = DataQueue()
        self.processing_data = dict()
        self.all_files = self.get_all_files()
        self.processing_thread_object = dict()
        self.saving_threads = []
        self.processing_threads = []
        self.documents_per_file = 10000
        self.cache_data = dict()
        """{'file_name':  {last_accessed: (int), data: (dict)}}"""

        self.cache_queue = DataQueue()

        self.find_ready_data_thread = threading.Thread(target=self.find_ready_data, name="find_ready_data_thread_1", args=())
        self.indexing_thread_1 = threading.Thread(target=self.get_data_to_index, name="indexing_thread_1", args=())
        self.find_ready_data_thread.start()
        self.indexing_thread_1.start()

        self.init_processing_threads()
        self.init_saving_threads()
        self.create_db_index("user_id", "integer")
        self.create_db_index("user_name", "string")

    def create_db_index(self, key, data_type="unknown"):
        """
        Creates an index for the database
        :param key: The database object key that will be indexed
        :param data_type: reflects the type of index to be used
        :return: None
        """
        if data_type == "integer":
            self.indices[key] = BST(key)
        elif data_type == "string":
            self.indices[key] = PrefixTree(key)

    def create_new_item(self, data={}):
        """
        Creates a new data item with the object provided.\n
        :param data: Data to be input to the database in the form of a dictionary
        :return: Integer. Primary key of the new data item.
        """
        item_primary_key = self.largest_id
        data_row = DataItem(item_primary_key, data)
        self.data_queue.add(data_row)
        self.largest_id = self.largest_id + 1
        return item_primary_key

    def find_by_field(self, query):
        """
        Function for retrieving a record by the a field value.
        :param query: object with structure {fieldName(String): value}
        :return: object with database primary keys as the key, and data as the values
        """
        t1 = time.time()
        primary_keys = []
        for key in query:
            if key in self.indices:
                primary_keys = self.__find_by_field_index(query)
            else:
                primary_keys = self.__find_by_field_scan(query)
        return_data = {}
        for key in primary_keys:
            return_data[key] = self.find_by_primary_key(key)
        t2 = time.time()
        print(t2-t1)
        return return_data

    def __find_by_field_index(self, query):
        """
        Finds the primary keys associated with a specific field value
        :param query: an object with a single key value pair
        :return: an array of primary keys
        """
        for key in query:
            primary_keys = self.indices[key].find(query[key])
        return primary_keys

    # TODO finish this
    def __find_by_field_scan(self, query):
        """
        Performs a full collection scan of the documents using the query provided
        :param query: an object containing a single key to search on, and the corresponding value
        :return: a set of primary keys
        """
        return

    def find_by_primary_key(self, database_id):
        """
        Function for retrieving a single record by the primary key.\n
        :param database_id: database primary key
        :return: Object associated with the primary key in the form {'data': dict()}
        """
        file_name = self.get_file_name(database_id)
        database_id_string = str(database_id)
        if file_name in self.cache_data:
            return_data = self.cache_data[file_name]['data'][database_id_string]
            self.cache_data[file_name]['last_accessed'] = time.time()
        else:
            data = self.read_from_file(file_name)
            return_data = data['data'][database_id_string]
            self.update_cache_data(file_name, data)
        return return_data

    def find_by_primary_key_wrapper(self, database_id):
        """
        takes a database id, passes it to the find_by_primary_key function, puts results into an object
        and returns the object.\n
        :param database_id: an integer
        :return: an object containing the integer as the key and the data as the value
        """
        return_data = {}
        return_data[database_id] = self.find_by_primary_key(database_id)
        return return_data

    def find_parser(self, query):
        """
        Takes a query passed from the server and parses it out.  If the key is "pk", finds the data by
        primary key.  Otherwise, it finds the data by a field name.\n
        :param query: an object with a single key-value pair.
        :return: an object with database primary keys as the keys and the data as the values
        """
        for key in query:
            if key == "pk":
                return self.find_by_primary_key_wrapper(query[key])
            else:
                return self.find_by_field(query)

    def find_ready_data(self):
        """
        Scans the processing_data dictionary and checks the reference count on each object.  If
        the reference count drops below 3, the object is removed from teh processing_data dictionary
        and added to the saving_queue.\n
        :return: None
        """
        found_data = False
        while True:
            keys = self.processing_data.keys()
            for key in list(keys):
                ref_count = sys.getrefcount(self.processing_data[key])
                if (ref_count) <= 2:
                    data = dict()
                    data[key] = self.processing_data[key]
                    found_data = True
                    break
            if found_data:
                del self.processing_data[key]
                self.saving_queue.add(data)
                found_data = False

    def get_all_files(self):
        """
        gets the file_names of all files in the data directory and returns them as a set
        :return: a set of filenames
        """
        filesArray = os.listdir('/Users/alexanderclements/PycharmProjects/ADB/data')
        return_set = set()
        for file_name in filesArray:
            return_set.add("./data/" + file_name)
        return return_set

    def get_data_to_index(self):
        """
        If the index queue is not empty, calls the index_value method to add item to the index.\n
        :return: None
        """
        while True:
            if not self.index_queue.isEmpty():
                self.index_value()

    def get_file_name(self, database_id):
        """
        Uses a simple hashing function on the database_id to obtain the filename containing
        the given id.\n
        :param database_id: integer representing database primary key
        :return: file name string
        """
        file_id = int(database_id / self.documents_per_file)
        file_name = "./data/file" + str(file_id) + ".json"
        return file_name

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

    def init_processing_threads(self):
        """
        Initalizes the processing threads for the database
        :return: None
        """
        for i in range(0, 2):
            name = "processing_thread_" + str(i)
            self.processing_threads.append(threading.Thread(target=self.process_thread_init, name=name, args=()))
            self.processing_threads[i].start()

    def init_saving_threads(self):
        """
        Initializes the saving threads for the database.\n
        :return: None
        """
        for i in range(0, 10):
            name = "saving_thread_" + str(i)
            self.saving_threads.append(threading.Thread(target=self.save_thread_main, name=name, args=()))
            self.saving_threads[i].start()

    def process_data(self):
        """
        Grabs a data row from the data queue, adds it to the corresponding file name in the
        corresponding processing_data dictionary entry.\n
        :return: None
        """
        data_row = self.data_queue.pop()

        if not data_row:
            return

        self.index_queue.add(data_row)

        id = data_row.getId()
        file_name = self.get_file_name(id)

        if not file_name in self.processing_data:
            myDict = dict()
            self.processing_data[file_name] = myDict
        else:
            myDict = self.processing_data[file_name]

        myDict[str(id)] = data_row.data

        thread_name = str(threading.current_thread().name)
        self.processing_thread_object[thread_name] = myDict

    def process_thread_init(self):
        """
        Initializes the process thread, adding the thread name to the processing_thread_object
        to hold the references to working data objects.\n
        :return: None
        """
        thread_name = str(threading.current_thread().name)
        self.processing_thread_object[thread_name] = None
        self.process_thread_main()

    def process_thread_main(self):
        """
        Runs to find new items in the data_queue that are ready to be processed.  Holds onto the
        reference for the data item while its in use to prevent it from being saved.\n
        :return: None
        """
        t1 = time.time()
        while True:
            while not self.data_queue.isEmpty():
                t1 = time.time()
                self.process_data()
            t2 = time.time()
            if (t2 - t1) > 1:
                thread_name = str(threading.current_thread().name)
                self.processing_thread_object[thread_name] = None

    def read_from_file(self, file_name):
        """
        Reads in data from a file and returns the object. Creates the file if it doesn't already
        exist.\n
        :param file_name:
        :return: Object with the structure {'data': (dict)}
        """
        try:
            with open(file_name, 'r+') as f:
                try:
                    data = json.load(f)
                except:
                    data = {"data": {}}
        except:
            with open(file_name, 'a+') as f:
                try:
                    data = json.load(f)
                except:
                    data = {"data": {}}
        return data

    def save_data(self):
        """
        Pulls an item off the saving_queue, reads in any existing data from the corresponding file,
        merges any existing data, and then saves the combined data to a file.\n
        :return: None
        """
        data_item = self.saving_queue.pop()
        if not data_item:
            return
        file_name = list(data_item.keys())[0]
        existing_data = self.read_from_file(file_name)
        new_data = data_item[file_name]
        existing_data['data'].update(new_data)
        self.write_to_file(file_name, existing_data)
        self.all_files.add(file_name)

    def save_thread_main(self):
        """
        Runs to find new items ready for saving on the saving queue.  If an item appears on the
        saving_queue then it the saveData method is called to save it to a file.
        :return: None
        """
        while True:
            if not self.saving_queue.isEmpty():
                self.save_data()
            time.sleep(1)

    def update_cache_data(self, file_name, data):
        """
        Updates data cache at the file_name location with the data provided.\n
        :param file_name: file name string
        :param data: object with structure {'data': (dict)}
        :return: None
        """
        if file_name not in self.cache_data:
            new_cache_dict = dict()
            new_cache_dict['data'] = data['data']
            new_cache_dict['last_accessed'] = time.time()
            self.cache_data[file_name] = new_cache_dict
        else:
            self.cache_data[file_name]['data'] = data['data']
            self.cache_data[file_name]['last_accessed'] = time.time()

        print(file_name)
        return

    def update_item(self,item_primary_key, data={}):
        """
        Updates an existing item with the data provided.\n
        :param item_primary_key: database primary key
        :param data: Updated data corresponding to the database primary key.
        :return: Integer. Primary key of the updated item.
        """
        data_row = DataItem(item_primary_key, data)
        self.data_queue.add(data_row)
        return item_primary_key

    def write_to_file(self, file_name, data):
        """
        Writes a data object to a file.\n
        :param file_name: file name string
        :param data: object with format {"data": (dict)}
        :return: None
        """
        with open(file_name, 'w+') as outfile:
            outfile.seek(0)
            json.dump(data, outfile, indent=4)

        self.update_cache_data(file_name, data)

















