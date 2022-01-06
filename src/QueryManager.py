import time


class QueryManager:
    def __init__(self, database, cache_manager, disk_manager, index_manager):
        self.database = database
        self.cache_manager = cache_manager
        self.disk_manager = disk_manager
        self.index_manager = index_manager

    def find_by_field(self, query, collection):
        """
        Function for retrieving a record by the a field value. \n
        :param query: object with structure {fieldName(String): value}
        :param collection: collection that the data belongs to.
        :return: object with database primary keys as the key, and data as the values.
        """
        t1 = time.time()
        primary_keys = []
        for key in query:
            if self.index_manager.contains(key, collection):
                primary_keys = self.index_manager.find(key, query[key], collection)
            else:
                primary_keys = self.__find_by_field_scan(query, collection)
        return_data = {}
        if not primary_keys:
            t2 = time.time()
            print(t2-t1)
            return return_data
        for key in primary_keys:
            return_data[key] = self.find_by_primary_key(key, collection)
        t2 = time.time()
        print(t2-t1)
        return return_data

    # TODO finish this
    def __find_by_field_scan(self, query, collection):
        """
        Performs a full collection scan of the documents using the query provided \n
        :param query: an object containing a single key to search on, and the corresponding value.
        :param collection: collection that the data belongs to.
        :return: a set of primary keys.
        """
        return

    def find_by_primary_key(self, database_id, collection):
        """
        Function for retrieving a single record by the primary key.\n
        :param database_id: database primary key.
        :param collection: collection data belongs to.
        :return: Document associated with the primary key in the form {'data': dict()}.
        """
        file_name = self.database.get_file_name(database_id, collection)
        database_id_string = str(database_id)
        return_data = self.cache_manager.get_data(file_name, database_id_string)
        return return_data

    def find_by_primary_key_wrapper(self, database_id, collection):
        """
        takes a database id, passes it to the find_by_primary_key function, puts results into an object
        and returns the object.\n
        :param database_id: an integer.
        :param collection: collection data belongs to.
        :return: an object containing the integer as the key and the data as the value.
        """
        return_data = dict()
        return_data[database_id] = self.find_by_primary_key(database_id, collection)
        return return_data
