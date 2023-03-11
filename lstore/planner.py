from lstore.table import Table, Record
from lstore.query import Query
import queue
import threading
import time

class QueueCC:
    def __init__(self, queue_list, record_index, max_priority):
        self.queue_list = queue_list
        self.record_index = record_index
        self.priority_index = 0
        self.max_priority = max_priority

    def execute_next_queries(self) -> None:
        assert self.priority_index < self.max_priority
        queries = self.queue_list[self.priority_index]
        for query, args in queries:
            try:
                result = query(*args)
                if not result:
                    raise Exception("Error while running query")
            except:
                #print(f"Query {query.__func__} failed on {args}")
                continue
        self.priority_index += 1
    
    def done(self) -> bool:
        return self.priority_index >= self.max_priority

class Executor:
    def __init__(self, table, num_threads):
        self.table = table
        self.num_threads = num_threads

    def execute(self, groups): 
        self.groups = groups
        self.num_active_groups = len(groups)
        executor_threads = []
        self.num_active_lock = threading.Lock()
        self.group_lock = threading.Lock()
        for _ in range(self.num_threads):
            new_thread = threading.Thread(target = self.run_transactions)
            new_thread.start()
            executor_threads.append(new_thread)
        for executor_thread in executor_threads:
            executor_thread.join()
    
    def run_transactions(self):
        while self.num_active_groups > 0:
            self.group_lock.acquire()
            if len(self.groups) == 0:
                self.group_lock.release()
                # this is how to yield, according to: https://stackoverflow.com/questions/787803/how-does-a-threading-thread-yield-the-rest-of-its-quantum-in-python
                time.sleep(0.0001)
                continue
            else:
                quecc = self.groups.pop()
                self.group_lock.release()
                quecc.execute_next_queries()
                if quecc.done():
                    self.num_active_lock.acquire()
                    self.num_active_groups -= 1
                    self.num_active_lock.release()
                else:
                    self.group_lock.acquire()
                    self.groups.append(quecc)
                    self.group_lock.release()
        print("Done")

class Planner:

    """
    # Creates a planner object.
    """

    def __init__(self, table, num_planner_threads):
        self.table: Table=table
        self.primary_key_count = 0
        self.queue_list = []
        self.transaction_count: int = 0
        self.primary_key_to_index = dict()
        #Get planner thread count from config
        self.planner_threads = num_planner_threads
        #self.planner_threads = PLANNER_THREAD_COUNT
        pass

    def reset(self):
        self.primary_key_count = 0
        self.primary_key_to_index.clear
        self.queue_list = []

    def find_primary_key_count(self, transaction_list: list):
        queryObj = Query(self.table)
        for transaction in transaction_list:
            for query, args in transaction.queries:
                if(query.__func__==queryObj.insert.__func__):
                    primary_key=args[self.table.primary_key_col]
                elif(query.__func__==queryObj.select.__func__):
                    if args[1]==self.table.primary_key_col:
                        primary_key=args[0]
                elif(query.__func__==queryObj.update.__func__):
                    primary_key=args[0]
                elif(query.__func__==queryObj.delete.__func__):
                    primary_key=args[0]
                if primary_key not in self.primary_key_to_index:
                    self.primary_key_to_index[primary_key] = self.primary_key_count
                    self.primary_key_count += 1
        return self.primary_key_count

    def print_queues(self):
        #Checks to make sure all transactions are queued
        count:int = 0
        #newList: list = []
        test_dict = dict()
        for i in range(1000):
            test_dict[92106429+i]=1
        for key in self.queue_list:
            #print("Primary key: ", key)
            while not self.queue_list[key].empty():
                #print(self.queue_list[key].get())
                value = self.queue_list[key].get()
                count+=1
                if key in test_dict:
                    del test_dict[key]
                elif key-1000 in test_dict:
                    del test_dict[key-1000]
                elif key-2000 in test_dict:
                    del test_dict[key-2000]
                elif key-3000 in test_dict:
                    del test_dict[key-3000]
        print(count)
        #for key in newList:
            #del self.queue_list[key]
        print("Remaining: ",test_dict)

    def create_queues(self):
        for _ in range(self.primary_key_count):
            self.queue_list.append([[] for _ in range(self.planner_threads)])

    def get_split_transactions(self, transaction_list):
        split_transactions:list =[]
        transactions_per_priority = len(transaction_list)//self.planner_threads
        for i in range(self.planner_threads-1): 
            sublist = transaction_list[(i*transactions_per_priority):(((i+1)*transactions_per_priority))]
            split_transactions.append(sublist)
        final_sublist = transaction_list[(self.planner_threads-1)*transactions_per_priority:]
        split_transactions.append(final_sublist)
        return split_transactions
 
    def get_thread_list(self, transaction_list):
        split_transactions = self.get_split_transactions(transaction_list)
        thread_list:list = []
        for i in range(self.planner_threads):
            new_thread = threading.Thread(target = self.separate, args=(split_transactions[i], i))
            thread_list.append(new_thread)
        return thread_list

    def plan(self, transaction_list: list):
        self.find_primary_key_count(transaction_list)
        self.create_queues()
        thread_list = self.get_thread_list(transaction_list)
        for i in range(self.planner_threads):
            thread_list[i].start()
        for i in range(self.planner_threads):
            thread_list[i].join()
        groups = [QueueCC(self.queue_list[i], i, self.planner_threads) for i in range(self.primary_key_count)]
        return groups
        
    def separate(self, transaction_list, priority):
        queryObj = Query(self.table)
        for transaction in transaction_list:
            for query, args in transaction.queries:
                primary_key: int
                if(query.__func__==queryObj.insert.__func__):
                    primary_key=args[self.table.primary_key_col]
                elif(query.__func__==queryObj.select.__func__):
                    if args[1]==self.table.primary_key_col:
                        primary_key=args[0]
                elif(query.__func__==queryObj.update.__func__):
                    primary_key=args[0]
                elif(query.__func__==queryObj.delete.__func__):
                    primary_key=args[0] 
                assert primary_key in self.primary_key_to_index
                record_ind = self.primary_key_to_index[primary_key]
                # could this cause slow down due to cache line problem? -> http://www.nic.uoregon.edu/~khuck/ts/acumem-report/manual_html/multithreading_problems.html
                self.queue_list[record_ind][priority].append((query, args))