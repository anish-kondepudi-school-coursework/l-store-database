from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange
import numpy as np

if __name__ == "__main__":
    print("64K")
    # Student Id and 4 grades
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    keys = []
    secondary_keys = []
    selection_times_si = [[], [], [], [], []]

    insert_time_0 = process_time()
    for i in range(0, 64000):
        # random number between 1 and 4
            replacement = randrange(1, 5)
            record = [906659671 + i, 0, 0, 0, 0]
            record[replacement] = randrange(0, 100)
            query.insert(*record)
            secondary_keys.append((906659671 + i, replacement, record[replacement]))
            keys.append(906659671 + i)
    insert_time_1 = process_time()
    print("Inserting 64K records took:  \t\t\t", insert_time_1 - insert_time_0)

    # Measuring Secondary Index Search Performance
    secondary_time_0 = process_time()
    for primary_key, secondary_key, value in secondary_keys[:5000]:
        current = process_time()
        query.select(value, secondary_key, [1, 1 == secondary_key, 2 == secondary_key, 3 == secondary_key, 4 == secondary_key])
        selection_times_si[secondary_key].append(process_time() - current)
    secondary_time_1 = process_time()
    print("Secondary Index Search 5000 records took:  \t", secondary_time_1 - secondary_time_0)

    # for secondary_time in selection_times_si[1:]:
    #     secondary_time.sort(reverse=True)
    #     print(secondary_time[:10])
    #     secondary_time.sort()
    #     print(secondary_time[:10])
    #     print(np.mean(secondary_time))

    # Measuring update Performance
    update_cols = [
        [None, None, None, None, None],
        [None, randrange(0, 100), None, None, None],
        [None, None, randrange(0, 100), None, None],
        [None, None, None, randrange(0, 100), None],
        [None, None, None, None, randrange(0, 100)],
    ]

    update_time_0 = process_time()
    for i in range(0, 64000):
        query.update(choice(keys), *(choice(update_cols)))
    update_time_1 = process_time()
    print("Updating 64K records took:  \t\t\t", update_time_1 - update_time_0)

    # Measuring Select Performance
    select_time_0 = process_time()
    for i in range(0, 100000):
        query.select(choice(keys),0 , [1, 1, 1, 1, 1])
    select_time_1 = process_time()
    print("Selecting 100K records took:  \t\t\t", select_time_1 - select_time_0)

    # Measuring Aggregate Performance
    agg_time_0 = process_time()
    for i in range(0, 100000, 100):
        start_value = 906659671 + i
        end_value = start_value + 100
        result = query.sum(start_value, end_value - 1, randrange(0, 5))
    agg_time_1 = process_time()
    print("Aggregate 100K of 100 record batch took:\t", agg_time_1 - agg_time_0)

    # Measuring Delete Performance
    delete_time_0 = process_time()
    for i in range(0, 36000):
        query.delete(906659671 + i)
    delete_time_1 = process_time()
    print("Deleting 36K records took:  \t\t\t", delete_time_1 - delete_time_0)