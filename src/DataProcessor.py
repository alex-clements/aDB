from BlockingQueue import BlockingQueue
import threading
import time


class DataProcessor:
    def __init__(self, database, disk_manager):
        self.database = database
        self.disk_manager = disk_manager
        self.processing_threads = []
        self.data_queue = BlockingQueue()
        self.ready_data_queue = BlockingQueue()
        self.processing_data = dict()

        self.find_ready_data_thread = threading.Thread(target=self.find_ready_data, name="find_ready_data_thread_1",
                                                       args=())
        self.find_ready_data_thread.start()
        self.init_processing_threads()

    def add(self, data):
        """
        Adds an item to the processing queue. \n
        :param data: Item to be added to the processing queue.
        :return: None
        """
        self.data_queue.add(data)

    def stop(self):
        """
        Stops the data processor.
        :return: None
        """
        self.data_queue.join()
        self.ready_data_queue.join()
        print("Processing threads stopped.")

    def init_processing_threads(self):
        """
        Initalizes the processing threads for the database
        :return: None
        """
        for i in range(0, 2):
            name = "processing_thread_" + str(i)
            self.processing_threads.append(threading.Thread(target=self.process_thread_init, daemon=True, name=name,
                                                            args=()))
            self.processing_threads[i].start()

    def process_thread_init(self):
        """
        Initializes the process thread, adding the thread name to the processing_thread_object
        to hold the references to working data objects.\n
        :return: None
        """
        self.process_thread_main()

    def process_thread_main(self):
        """
        Runs to find new items in the data_queue that are ready to be processed.\n
        :return: None
        """
        while not self.database.shutdown_flag:
            self.process_data()

    def process_data(self):
        """
        Grabs a data row from the data queue, adds it to the corresponding file name in the
        corresponding processing_data dictionary entry.\n
        :return: None
        """
        data_row = self.data_queue.pop()

        data_row_id = data_row.getId()
        file_name = self.database.get_file_name(data_row_id)

        if file_name not in self.processing_data:
            my_dict = dict()
            my_dict['last_accessed'] = None
            my_dict['data'] = dict()
            self.processing_data[file_name] = my_dict
            self.ready_data_queue.add(True)
        else:
            my_dict = self.processing_data[file_name]

        my_dict['data'][str(data_row_id)] = data_row.data
        my_dict['last_accessed'] = time.time()

        self.data_queue.task_done()

    def find_ready_data(self):
        """
        Scans the processing_data dictionary and checks the last_accessed time in each object. If
        the last_accessed time was more than 1 second ago, the object is removed from the processing_data
        dictionary and added to the saving_queue.\n
        :return: None
        """
        test_val = False
        while self.database.saving_threads_running or self.database.processing_threads_running:
            if not test_val:
                test_val = self.ready_data_queue.pop()
            keys = self.processing_data.keys()
            found_data = False
            for key in list(keys):
                t2 = time.time()
                t1 = self.processing_data[key]['last_accessed']
                if (t2 - t1) > 1:
                    found_data = True
                    data = dict()
                    data[key] = self.processing_data[key]
                    break
            if found_data:
                del self.processing_data[key]
                self.disk_manager.add(data)
                self.ready_data_queue.task_done()
                test_val = False

        print("Shutting down find_ready_data thread")
        self.database.find_ready_data_thread_running = False
