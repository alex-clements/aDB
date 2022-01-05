import threading
from BlockingQueue import BlockingQueue


class DiskManager:
    def __init__(self, database, reader):
        self.database = database
        self.reader = reader
        self.saving_threads = []
        self.saving_queue = BlockingQueue()

        self.init_saving_threads()

    def add(self, data):
        """
        Adds items to the saving queue. \n
        :param data: dictionary
        :return: None
        """
        self.saving_queue.add(data)

    def stop(self):
        """
        Stops the disk manager. \n
        :return: None
        """
        self.saving_queue.join()
        print("Saving threads stopped.")

    def init_saving_threads(self):
        """
        Initializes the saving threads for the database.\n
        :return: None
        """
        for i in range(0, 10):
            name = "saving_thread_" + str(i)
            self.saving_threads.append(threading.Thread(target=self.save_thread_main, daemon=True, name=name, args=()))
            self.saving_threads[i].start()

    def save_thread_main(self):
        """
        Runs to find new items ready for saving on the saving queue.  If an item appears on the
        saving_queue then it the save_data method is called to save it to a file.
        :return: None
        """
        while self.database.processing_threads_running or not self.saving_queue.isEmpty():
            self.save_data()

    def save_data(self):
        """
        Pulls an item off the saving_queue, reads in any existing data from the corresponding file,
        merges any existing data, and then saves the combined data to a file.\n
        :return: None
        """
        data_item = self.saving_queue.pop()
        if not data_item:
            return
        file_name = list(data_item.keys())[0]
        existing_data = self.reader.read_from_file(file_name)
        new_data = data_item[file_name]['data']
        existing_data['data'].update(new_data)
        self.database.write_to_file(file_name, existing_data)
        self.database.all_files.add(file_name)
        self.saving_queue.task_done()
