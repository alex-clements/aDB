import unittest
from src.BST import BST
import ujson as json

class MyTestCase(unittest.TestCase):
    def test_add(self):
        bst = BST("TEST")
        bst.add(20, 20)
        bst.add(20, 10)
        bst.add(30, 20)
        expectedVal = {"keys": [20, None, 30, None, None], "data": [[10, 20], None, [20], None, None]}
        self.assertEqual(expectedVal, bst.to_dict())
    def test_to_json(self):
        bst = BST("TEST")
        bst.add(10, 1)
        jsonval = bst.to_dict()
        self.assertEqual(jsonval, {"keys": [10, None, None], "data": [[1], None, None]})

    def test_from_json_1(self):
        bst = BST("TEST")
        bst.add(10, 1)
        bst.add(20, 2)
        bst.add(1, 3)
        bst.add(4, 2)
        bst.add(3, 4)
        bst.add(920, 43)
        bst.add(1, 42)
        bst1_json = bst.to_dict()
        bst2 = BST("TEST2")
        bst2.from_dict(bst1_json)
        self.assertEqual(bst.to_dict(), bst2.to_dict())

    def test_bst_from_json_2(self):
        bst_1 = BST("1")
        bst_1.add(1, 1)
        bst_1.add(2, 2)
        bst_1.add(3, 3)
        bst_1.add(0, 1)
        bst_1.add(-3, 2)
        bst_1_json = bst_1.to_dict()
        bst_2 = BST("2")
        bst_2.from_dict(bst_1_json)
        self.assertEqual(bst_1.to_dict(), bst_2.to_dict())

if __name__ == '__main__':
    unittest.main()
