import unittest
import random
from lstore import (
    PageDirectory,
    PageRange,
    RID_Generator,
    MAX_BASE_PAGES_IN_PAGE_RANGE,
    PHYSICAL_PAGE_SIZE,
    ATTRIBUTE_SIZE,
    INVALID_RID
)

class TestNon(unittest.TestCase):
    pass

def getLatestColVals(page_range):
    cols = []
    for i in range(6):
        cols.append(page_range.get_latest_column_value(1, i))
    return cols

if __name__ == '__main__':
    
    page_range = PageRange(6, PageDirectory(), RID_Generator(), False)
    record = [1, 2, 3, 4, 5, 6]
    new_record = [1, 2, 3, 4, 5, 6]
    page_range.insert_record(record)

    import random
    for i in range(10000):
        for j in range(6):
            if random.randint(1, 2) == 1:
                new_record[j] = random.randint(1, 1213)
                record[j] = new_record[j]
            else:
                new_record[j] = None 
        #print("New record: ", new_record)
        #print("Record: ", record)
        page_range.update_record(1, new_record)
        given_cols = getLatestColVals(page_range)
        #print("Given cols: ", given_cols)
        for j in range(6):
            assert record[j] == given_cols[j]

    
