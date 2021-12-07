import ujson as json


class Reader:
    """
    Class for reading JSON from files
    """
    def read_file(self, file_name):
        """
        Loads a JSON file into a python object and returns it.\n
        :param file_name: String representing file name to load.
        :return: Python object in form {"data": {}} or None
        """
        try:
            data = json.loads(file_name)
            return data
        except IOError:
            print("IO Error Occurred!")
            return None
