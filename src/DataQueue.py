from queue import Queue

class Node:
    def __init__(self, val):
        self.next = None
        self.prev = None
        self.val = val

class DataQueue:
    """A custom Queue class to handle incoming and outgoing data simultaneously.  It has been
    implemented as a LinkedList."""
    def __init__(self):
        self.head = None
        self.tail = None

    def add(self, data):
        """Creates a new node with the data provided and adds it to the end of the DataQueue."""
        node = Node(data)
        if self.tail is None or self.head is None:
            self.head = node
            self.tail = node
        else:
            temp = self.head
            self.head = node
            self.head.next = temp
            self.head.next.prev = self.head

        return

    def pop(self):
        """Removes the last node from the DataQueue and returns it."""
        return_node = self.tail

        if not return_node:
            return None

        try:
            self.tail = self.tail.prev
        except AttributeError:
            print("self.tail.prev failed")
            return None

        try:
            if self.tail:
                self.tail.next = None
        except AttributeError:
            print("self.tail.next failed")
            return None

        return return_node.val

    def isEmpty(self):
        """Returns true is the DataQueue is empty."""
        return self.tail is None
