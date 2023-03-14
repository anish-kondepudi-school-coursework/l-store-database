from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from lstore.planner import Planner

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 10
number_of_transactions = 5
num_threads = 4
number_of_operations_per_record = 5

planner = Planner(grades_table, num_threads)

# create index on the non primary columns
try:
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

# array of insert transactions
insert_transactions = []

for i in range(number_of_transactions):
    insert_transactions.append(Transaction())

for i in range(0, number_of_records):
    key = 92106429 + 2*i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    t = insert_transactions[i % number_of_transactions]
    t.add_query(query.insert, grades_table, *records[key])

# combine these
planner.plan(insert_transactions)
# planner.find_primary_key_count(insert_transactions)
# planner.create_queues()
# queue_list = planner.plan(insert_transactions)
# planner.print_queues()
# assert(0)
# transaction_workers = []
# for i in range(num_threads):
#     transaction_workers.append(TransactionWorker())
    
# for i in range(number_of_transactions):
#     transaction_workers[i % num_threads].add_transaction(insert_transactions[i])

# run insertion
# replace this with executor!!
for i in range(planner.planner_threads):
    for j in range(planner.primary_key_count):
        query_obj = planner.queue_list[i].inner_queue_list[j]
        if not query_obj.empty():
            for item in query_obj.queue:
                _query, _args = item[0], item[1]
                _query(*_args)
print("Insert finished")
# # run transaction workers
# for i in range(num_threads):
#     transaction_workers[i].run()

# # wait for workers to finish
# for i in range(num_threads):
#     transaction_workers[i].join()


# Check inserted records using select query in the main thread outside workers
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

update_transactions = []

for i in range(number_of_transactions):
    update_transactions.append(Transaction())
# x update on every column
for j in range(number_of_operations_per_record):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(2, grades_table.num_columns):
            # updated value
            value = randint(0, 20)
            updated_columns[i] = value
            # copy record to check
            original = records[key].copy()
            # update our test directory
            records[key][i] = value
            update_transactions[key % number_of_transactions].add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
            update_transactions[key % number_of_transactions].add_query(query.update, grades_table, key, *updated_columns)

planner.plan(update_transactions)

for i in range(planner.planner_threads):
    for j in range(planner.primary_key_count):
        query_obj = planner.queue_list[i].inner_queue_list[j]
        if not query_obj.empty():
            for item in query_obj.queue:
                _query, _args = item[0], item[1]
                _query(*_args)
print("Update finished")


db.close()
