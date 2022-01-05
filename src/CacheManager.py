import time


class CacheManager:
    def __init__(self, database, reader):
        self.database = database
        self.reader = reader
        self.cache_data = dict()
        """{'file_name':  {last_accessed: (int), data: (dict)}}"""

    def get_data(self, file_name, database_id_string):
        """
        Gets data from the cache.  Updates cache with data from the disk if data is not in the cache. \n
        :param file_name: file name.
        :param database_id_string: database id to find data for.
        :return: document corresponding to database_id.
        """
        if not self.contains(file_name):
            data = self.reader.read_from_file(file_name)
            self.update_cache_data(file_name, data)
        try:
            return_data = self.cache_data[file_name]['data'][database_id_string]
            self.cache_data[file_name]['last_accessed'] = time.time()
        except KeyError:
            print('key not found:')
            print('file_name: ' + file_name)
            print('database_id_string: ' + database_id_string)
            print(self.cache_data[file_name]['data'].keys())
            return_data = {}
        return return_data

    def contains(self, file_name):
        """
        Determines if the data for a given file is in the cache. \n
        :param file_name: file name to search for.
        :return: Boolean.
        """
        if file_name in self.cache_data:
            return True
        else:
            return False

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

    def remove_from_cache(self, file_name):
        """
        Removes an item from the cache. \n
        :param file_name: cache item to be removed
        :return: None
        """
        if file_name in self.cache_data:
            del self.cache_data[file_name]

    def clear_cache(self):
        """
        Empties out the cache to save all data to files. \n
        :return: None
        """
        for key in list(self.cache_data.keys()):
            self.database.write_to_file(key, self.cache_data[key], write_to_cache=False, remove_from_cache=True)
        print("Cache cleared.")