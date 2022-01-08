import ujson as json


class Node:
    """
    Class representing a BST node.\n
    """
    def __init__(self, key, val):
        """
        :param key: the key of the BST
        :param val: INT or LIST. The data value of the BST.
        """
        self.key = key
        self.data = set()

        self.left = None
        self.right = None
        self.parent = None

        if isinstance(val, list):
            for item in val:
                self.data.add(item)
        else:
            self.data.add(val)


class BST:
    """
    Class representing a binary search tree.\n
    """
    def __init__(self, name, collection):
        self.name = name
        self.root = None
        self.collection = collection

    def __repr__(self):
        return json.dumps(self.to_dict())

    def get_collection(self):
        """
        Returns the collection the index belongs to.
        :return: The collection the index belongs to.
        """
        return self.collection

    def to_dict(self):
        """
        Method to write the BST to a Python dictionary
        :return: Object with structure {"keys": [], "data": []}.  Represents BST keys and associated data.\n
        """
        result_key = []
        result_data = []
        self.__to_dict_rec(self.root, result_key, result_data)

        storage = {'keys': result_key, 'data': result_data}
        index_type = 'integer'
        name = self.name
        collection = self.collection

        return_dict = {'index': storage, 'index_type': index_type, 'name': name, 'collection': collection}
        return return_dict

    def __to_dict_rec(self, root, result_key, result_data):
        """
        Recursive method to traverse the BST and add keys and data to the result_key and result_data arrays.\n
        :param root: root BST node.
        :param result_key: array used to store the keys of the BST.
        :param result_data: array used to store the data in the BST nodes.
        :return: None
        """
        if not root:
            result_key.append(None)
            result_data.append(None)
        else:
            result_key.append(root.key)
            result_data.append(list(root.data))

            self.__to_dict_rec(root.left, result_key, result_data)
            self.__to_dict_rec(root.right, result_key, result_data)
        return

    def from_dict(self, data):
        """
        Constructs the BST from JSON data
        :param data:
        :return:
        """
        keys = data['keys']
        vals = data['data']

        self.root = Node(0, [])
        self.__from_dict_dfs(self.root, keys, vals, 0)

    def __from_dict_dfs(self, root, keys, vals, counter):
        """
        Helper function for constructing BST from inorder traversal
        :param root: node being examined
        :param keys: array of keys being examined
        :param vals: array of node vals being examined
        :param counter: pointer to specific node data
        :return: Integer of next counter
        """
        root.key = keys[counter]
        root.data = set(vals[counter])

        if keys[counter+1] is not None:
            root.left = Node(0, [])
            counter_2 = self.__from_dict_dfs(root.left, keys, vals, counter + 1)
        else:
            counter_2 = counter + 2

        if keys[counter_2] is not None:
            root.right = Node(0, [])
            counter_3 = self.__from_dict_dfs(root.right, keys, vals, counter_2)
        else:
            counter_3 = counter_2 + 1
        return counter_3

    def add(self, key, data):
        """
        Begins the adding operation.  Creates a new node with the data provided and inserts the new node.
        :param key: BST node key.
        :param data: BST node data.
        :return: None
        """
        new_node = Node(key, data)
        self.__insert_new_node(new_node)
        return

    def __insert_new_node(self, new_node):
        """
        Begins the insert operation.  If the root doesn't currently exist, then it adds the new_node as the
        root of the BST.  Otherwise it calls the dfs function to complete the insert operation.
        :param new_node: new BST node to be inserted
        :return: None
        """
        if not self.root:
            self.root = new_node
            return

        self.__dfs_insert(self.root, new_node)
        return

    def __dfs_insert(self, root, new_node):
        """
        Traverses the tree and inserts the node where applicable.\n
        :param root: root bst node to examine.
        :param new_node: the new node to insert into the BST.
        :return: None
        """
        if new_node.key > root.key:
            if not root.right:
                root.right = new_node
                root.right.parent = root
                return
            else:
                self.__dfs_insert(root.right, new_node)
        elif new_node.key < root.key:
            if not root.left:
                root.left = new_node
                root.left.parent = root
                return
            else:
                self.__dfs_insert(root.left, new_node)
        elif new_node.key == root.key:
            root.data = root.data.union(new_node.data)

        return

    def find(self, key, comparison_operator=None):
        """
        Initializes the find function searching for a key in the BST.  If the root doesn't exist,
        this will return an empty set.  Otherwise, it begins the dfs_find method.\n
        :param key: the BST key to search for.
        :param comparison_operator: operator used to find values above or below those specified.  Supports $gt and $lt.
        :return: Set populated with database primary keys.
        """
        if not self.root:
            return set()

        if not comparison_operator:
            return self.__dfs_find(self.root, key)
        elif comparison_operator == "$gt":
            print("key = ", key)
            return self.__dfs_find_greater(self.root, key)
        elif comparison_operator == "$lt":
            return self.__dfs_find_less(self.root, key)

    def __dfs_find(self, root, key):
        """
        Finds a node with a specified val in the BST.  Returns an empty set if the key cannot be found. \n
        :param root: the BST node to examine.
        :param key: the BST key to search for.
        :return: Set populated with database primary keys.
        """
        if not root:
            return set()
        if key == root.key:
            return root.data
        elif key > root.key:
            return self.__dfs_find(root.right, key)
        elif key < root.key:
            return self.__dfs_find(root.left, key)

    def __dfs_find_greater(self, root, key):
        """
        Finds all nodes with specified vals greater than the one specified. \n
        :param root: BST root node.
        :param key: BST key to compare against.
        :return: Set populated with database primary keys.
        """

        if not root:
            return set()
        print("key = ", key)
        print("root.key = ", root.key)
        if key == root.key:
            return self.__dfs_find_greater(root.right, key)
        if root.key > key:
            print(root.key)
            return_set = root.data.union(self.__dfs_find_greater(root.right, key), self.__dfs_find_greater(root.left, key))
            return return_set
        if root.key < key:
            return_set = set().union(self.__dfs_find_greater(root.right, key))
            return return_set

    def __dfs_find_less(self, root, key):
        """
        Finds all nodes with specified keys less than the one specified. \n
        :param root: BST root node.
        :param key: BST key to compare against.
        :return: Set populated with database primary keys.
        """
        if not root:
            return set()
        if key == root.key:
            return self.__dfs_find_less(root.left, key)
        if root.key < key:
            return_set = root.data.union(self.__dfs_find_less(root.right, key), self.__dfs_find_less(root.left, key))
            return return_set
        if root.key > key:
            return_set = set().union(self.__dfs_find_less(root.left, key))
            return return_set
