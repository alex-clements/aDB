from queue import Queue


class BlockingQueue(Queue):
    """Class extending Queue"""

    def __init__(self):
        super().__init__()

    def isEmpty(self):
        """
        Used to indicate if the queue is empty.
        :return: boolean. indicates whether or not queue is empty.
        """
        return super().empty()

    def add(self, data):
        """
        Adds an item to the queue.
        :param data: item to be added to the queue.
        :return: none
        """
        super().put(data)

    def pop(self):
        """
        Removes the first item from the queue and returns it.
        :return: first item from the queue.
        """
        return super().get()
