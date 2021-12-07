import unittest
from src.DataQueue import DataQueue

class MyTestCase(unittest.TestCase):
    def testDataQueue(self):
        data_queue = DataQueue()
        for i in range(1,11):
            data_queue.add(i)

        self.assertEqual(data_queue.tail.val, 1)
        test_val = data_queue.pop()
        self.assertEqual(test_val, 1)
        self.assertEqual(data_queue.tail.val, 2)
        data_queue.pop()
        data_queue.pop()
        data_queue.pop()
        data_queue.pop()
        data_queue.pop()
        data_queue.pop()
        data_queue.pop()
        self.assertEqual(data_queue.tail.val, 9)
        data_queue.pop()
        self.assertEqual(data_queue.tail.val, 10)
        data_queue.pop()
        self.assertEqual(data_queue.tail, None)

if __name__ == '__main__':
    unittest.main()
