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
    def __init__(self, name):
        self.root = Node("")
        self.name = name

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

    def find(self, my_string):
        """
        Finds a string of multiple words in the prefix tree.
        :param my_string: String of several words.
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
        return self.root.to_dict()

    # TODO: Complete implementation of PrefixTree from_dict method.
    def from_dict(self, file_name):
        """
        Populates the prefix tree using the data in the provided file_name.
        :param file_name: String representing location of the JSON prefix tree.
        :return: None
        """
        pass
