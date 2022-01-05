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
