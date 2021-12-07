import unittest
from src.PrefixTree import PrefixTree
import ujson as json

class MyTestCase(unittest.TestCase):
    def test_insert_find(self):
        pt = PrefixTree("1")
        pt.add("I'll have you know i graduated at the top of my class in the navy seals.", 1)
        pt.add(";alksj;lakjf grad alskjjalkjdsh dddd.", 2)

        result1 = pt.find("graduated")
        self.assertEqual(result1, {1})

        result2 = pt.find("grad")
        self.assertEqual(result2, {1, 2})

    def test_insert_find_2(self):
        pt = PrefixTree("1")
        pt.add("Alex", 1)
        pt.add("Andrew", 2)
        pt.add("Julia", 3)
        pt.add("Test Name", 4)

    def test_to_dict(self):
        pt = PrefixTree("1")
        pt.add("Alex", 1)
        pt.add("Andrew", 2)
        pt.add("Julia", 3)
        pt.add("Test Name", 4)
        dictionary = pt.to_dict()
        print(dictionary)

if __name__ == '__main__':
    unittest.main()