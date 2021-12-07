import ujson as json


class Writer:
    """
    Class for writing data to JSON files
    """
    def write_to_json(self, data, file_name):
        """
        Writes a python object to a given file as a JSON object.\n
        :param file_name: name of file to be written
        :param data: Python object.
        :return: None
        """
        json.dumps(data, file_name)
        return
