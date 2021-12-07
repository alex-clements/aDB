from src.Database import Database
import random
import time

if __name__ == '__main__':
    database = Database()

    for i in range(0,1000000):
        my_str = str(i)
        random_number = random.randint(0,1000)
        data = {'task_number': my_str,
                "description": "task description",
                "user_id": random_number,
                "details": "kjsd;lksjdf",
                "due_date": "10/12/2021"}
        database.create_new_item(data)

    # database.find_by_primary_key(12432)
    # database.find_by_primary_key(1)
    # database.find_by_primary_key(1001)
    # database.find_by_primary_key(493829)
    # database.find_by_primary_key(463)
    #
    # database.find("938274")
    # database.find("483")

    time.sleep(30)

    print("user_id = 2")
    print(database.find_by_field(2))
    print("")
    print("user_id = 18")
    print(database.find_by_field(18))
    print("")
    print("user_id = 4932")
    print(database.find_by_field(4932))
