from lstore.table import Table, Record
from lstore.query import Query
import queue
import threading

class QueueCC:
    def __init__(self, num_unique_records):
        self.inner_queue_list = [queue.Queue() for _ in range(num_unique_records)]
    
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
        for _ in range(self.planner_threads):
            self.queue_list.append(QueueCC(self.primary_key_count))

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
            print(f"In get thread list, split transactions size at {i}: ", len(split_transactions[i]))
            new_thread = threading.Thread(target = self.separate, args=(split_transactions[i], i))
            thread_list.append(new_thread)
        return thread_list

    def plan(self, transaction_list: list):
        self.find_primary_key_count(transaction_list)
        print("Num unique primary keys: ", self.primary_key_count)
        print(self.primary_key_to_index)
        self.create_queues()
        thread_list = self.get_thread_list(transaction_list)
        for i in range(self.planner_threads):
            thread_list[i].start()
        for i in range(self.planner_threads):
            thread_list[i].join()
        print("done")
        for i in range(self.planner_threads):
            for j in range(self.primary_key_count):
                print(f"For {i}, {j}: {self.queue_list[i].inner_queue_list[j].qsize()}")
    
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
                self.queue_list[priority].inner_queue_list[self.primary_key_to_index[primary_key]].put((query, args))
