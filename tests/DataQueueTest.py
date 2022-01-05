import unittest
from src.BlockingQueue import BlockingQueue


class MyTestCase(unittest.TestCase):
    def testDataQueue(self):
        data_queue = BlockingQueue()
        for i in range(1,11):
            data_queue.add(i)

        for i in range(1, 11):
            val = data_queue.pop()
            self.assertEqual(val, i)


if __name__ == '__main__':
    unittest.main()
