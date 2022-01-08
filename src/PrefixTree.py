class Node:
    def __init__(self, val):
        self.val = val
        self.children = dict()
        self.data = set()

    def to_dict(self):
        """
        Turns the node into a dictionary object.
        :return: a Python dictionary object with the node data and the children data
        """
        return_object = {'val': self.val, 'data': self.__data_to_array(), 'children': {}}
        children = {}
        for child in self.children:
            children[child] = self.children[child].to_dict()
        return_object['children'] = children
        return return_object

    def __data_to_array(self):
        """
        Turns the data from the node into an array
        :return: Array with the node data
        """
        return_array = []
        for item in self.data:
            return_array.append(item)
        return return_array


class PrefixTree:
    """Prefix Tree created using Trie data structure"""
    def __init__(self, name, collection):
        self.root = Node("")
        self.name = name
        self.collection = collection

    def get_collection(self):
        """
        Returns the collection the index belongs to.
        :return: The collection the index belongs to.
        """
        return self.collection

    def add(self, my_string, node_data):
        """
        Begins the adding process. Inserts a string to the prefix tree as well as the accompanying data.
        :param my_string: Contains a string with any number of words.
        :param node_data: Data to be stored with Prefix Tree node.
        :return: None
        """
        my_string_list = my_string.split(" ")
        for test_word in my_string_list:
            word = str.lower(test_word)
            curr = self.root
            for character in word:
                if character in curr.children:
                    curr = curr.children[character]
                    curr.data.add(node_data)
                else:
                    curr.children[character] = Node(character)
                    curr = curr.children[character]
                    curr.data.add(node_data)
        return None

    def __find_helper(self, word):
        """
        Finds a word in the prefix tree and returns the data associated with the word.
        :param word: String
        :return: Set of data associated with the Prefix Tree node.
        """
        word = str.lower(word)
        curr = self.root
        for character in word:
            if character in curr.children:
                curr = curr.children[character]
        return curr.data

    def find(self, my_string, comparison_operator=None):
        """
        Finds a string of multiple words in the prefix tree.
        :param my_string: String of several words.
        :param comparison_operator: String. Operator to note when retrieving primary keys.
        :return: Set of data associated with Prefix tree nodes.
        """
        data = set()
        words = my_string.split(" ")

        for word in words:
            data = set.union(data, self.__find_helper(word))
        return data

    def to_dict(self):
        """
        Converts the prefix tree to a json representation.
        :return: JSON serialized string representing the prefix tree.
        """
        index = self.root.to_dict()
        name = self.name
        collection = self.collection
        index_type = 'string'
        return_dict = {'index': index, 'name': name, 'index_type': index_type, 'collection': collection}
        return return_dict

    def from_dict(self, my_dict):
        """
        Populates the prefix tree using the data in the provided file_name. \n
        :param my_dict: String representing location of the JSON prefix tree.
        :return: the populated prefix tree
        """
        return_data = self.__from_dict_helper(my_dict, self.root)
        return return_data

    def __from_dict_helper(self, my_dict, current_node):
        """
        Populates the prefix tree using the data in the provided file_name. \n
        :param my_dict: String representing location of the JSON prefix tree.
        :param current_node: Node to which dictionary data will be added
        :return: the populated prefix tree
        """
        for key in my_dict['children']:
            node = Node(key)
            for data_element in my_dict['children'][key]['data']:
                node.data.add(data_element)
            current_node.children[key] = node
            if my_dict['children']:
                self.__from_dict_helper(my_dict['children'][key], node)
        return self
