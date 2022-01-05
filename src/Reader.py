import ujson as json


class Reader:
    """
    Class for reading JSON from files
    """
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
