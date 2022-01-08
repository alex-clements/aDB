import time


class QueryManager:
    def __init__(self, database, cache_manager, index_manager):
        self.database = database
        self.cache_manager = cache_manager
        self.index_manager = index_manager
        self.operator_tokens = {"$and", "$or"}
        self.comparison_tokens = {"$gt", "$lt"}

    def find_primary_keys_by_field(self, query, collection, comparison_operator=None):
        """
        Function for retrieving a record by the a field value. \n
        :param query: object with structure {fieldName(String): value}
        :param collection: collection that the data belongs to.
        :param comparison_operator: String. Indicates the comparison operator to note when retrieving primary keys.
        :return: Set of database primary keys for the given collection.
        """
        t1 = time.time()
        primary_keys = []
        for key in query:
            if self.index_manager.contains(key, collection):
                primary_keys = self.index_manager.find(key, query[key], collection, comparison_operator)
            else:
                primary_keys = self.__find_by_field_scan(query, collection, comparison_operator)
        return_data = {}
        if not primary_keys:
            return return_data
        else:
            return primary_keys

    def find_primary_keys(self, query, collection, comparison_operator=None):
        """
        Function for retrieving primary keys corresponding to a field value. \n
        :param query: object with structure {fieldName(String): value}
        :param collection: collection that the data belongs to.
        :param comparison_operator: String. Comparison operator to note when retrieving primary keys.
        :return: object with database primary keys as the key, and data as the values.
        """
        for key in query:
            if self.index_manager.contains(key, collection):
                primary_keys = self.index_manager.find(key, query[key], collection, comparison_operator)
            else:
                primary_keys = self.__find_by_field_scan(query, collection, comparison_operator)
        return primary_keys

    # TODO finish this
    def __find_by_field_scan(self, query, collection, comparison_operator):
        """
        Performs a full collection scan of the documents using the query provided \n
        :param query: an object containing a single key to search on, and the corresponding value.
        :param collection: collection that the data belongs to.
        :return: a set of primary keys.
        """
        return set()

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

    def find_by_primary_key_wrapper(self, primary_key, collection):
        """
        takes a database id, passes it to the find_by_primary_key function, puts results into an object
        and returns the object.\n
        :param primary_key: an integer.
        :param collection: collection data belongs to.
        :return: an object containing the integer as the key and the data as the value.
        """
        return_data = dict()
        return_data[primary_key] = self.find_by_primary_key(primary_key, collection)
        return return_data

    def process_query(self, query, collection):
        """
        Processes the provided query for the collection in question. \n
        :param query: Query dictionary.
        :param collection: String. Name of the collection to search.
        :return: Dictionary of data. {primary_key: data, primary_key: data}.
        """
        primary_key_set = self.process(query, collection)
        return_data = dict()
        if not primary_key_set:
            return return_data
        for key in primary_key_set:
            return_data[key] = self.find_by_primary_key(key, collection)
        return return_data

    def process(self, query, collection):
        """
        Processes a provided query.
        :param query: The query to be executed.
        :param collection: String. Collection to search.
        :return: Set of primary keys satisfying all query requirements.
        """
        return_set = set()
        for key in list(query.keys()):
            if key in self.operator_tokens:
                return_set = self.process_operator(key, query[key], collection)
            elif key in self.comparison_tokens:
                return_set = self.process_comparison(key, query[key], collection)
            else:
                return_set = self.process_field_query(key, query[key], collection)
        return return_set

    def process_operator(self, operator_token, subqueries, collection):
        """
        Processes a set of sub-queries and combines results using operator specified.
        :param operator_token: Operator token
        :param subqueries: set containing sub-queries
        :param collection: String. Name of collection to search.
        :return: Set of primary keys
        """
        if operator_token not in self.operator_tokens:
            raise InvalidOperatorException
        elif not subqueries:
            raise InvalidQueryException
        return_set = set()
        subquery_result_array = []
        for query in subqueries:
            subquery_result_array.append(self.process(query, collection))
        print(subquery_result_array)
        if operator_token == "$and":
            return_set = subquery_result_array[0]
            for i in range(1, len(subquery_result_array)):
                return_set = return_set.intersection(subquery_result_array[i])
        if operator_token == "$or":
            return_set = subquery_result_array[0]
            for i in range(1, len(subquery_result_array)):
                return_set = return_set.union(subquery_result_array[i])
        return return_set

    def process_field_query(self, key, query, collection, comparison_operator=None):
        return self.find_primary_keys_by_field({key: query}, collection, comparison_operator)

    def process_comparison(self, key, query, collection):
        return self.find_primary_keys_by_field(query, collection, key)


class InvalidOperatorException(Exception):
    pass


class InvalidQueryException(Exception):
    pass
