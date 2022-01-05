import ujson as json


class DataItem:
    def __init__(self, id, data, collection):
        self.data = data
        self.id = id
        self.collection = collection

    def set(self, key, val):
        self.data[key] = val

    def getId(self):
        return self.id

    def remove(self, key):
        del self.data[key]

    def toJson(self):
        json_data = self.data
        return json.dumps(json_data)
