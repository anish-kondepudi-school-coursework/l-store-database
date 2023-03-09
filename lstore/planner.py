from lstore.table import Table, Record
from lstore.query import Query
import queue
import threading

class Planner:

    """
    # Creates a planner object.
    """

    def __init__(self, table):
        self.table: Table=table
        self.primary_key_count = 0
        self.queue_list= dict()
        self.transaction_count: int = 0
        self.primary_key_list= dict()
        #Get planner thread count from config
        self.planner_threads = 4
        #self.planner_threads = PLANNER_THREAD_COUNT
        pass

    def reset(self):
        self.primary_key_count = 0
        self.primary_key_list.clear
        self.queue_list.clear

    def find_primary_key_count(self, transaction_list: list):
        queryObj = Query(self.table)
        for transaction in transaction_list:
            for query, args in transaction.queries:
                if(query.__func__==queryObj.insert.__func__):
                    if args[self.table.primary_key_col] not in self.primary_key_list:
                        self.primary_key_list[args[self.table.primary_key_col]]=1
                        self.primary_key_count+=1
                elif(query.__func__==queryObj.select.__func__):
                    if args[1]==self.table.primary_key_col and args[0] not in self.primary_key_list:
                        self.primary_key_list[args[0]]=1
                        self.primary_key_count+=1
                elif(query.__func__==queryObj.update.__func__):
                    if args[0] not in self.primary_key_list:
                        self.primary_key_list[args[0]]=1
                        self.primary_key_count+=1
                elif(query.__func__==queryObj.delete.__func__):
                    if args[0] not in self.primary_key_list:
                        self.primary_key_list[args[0]]=1
                        self.primary_key_count+=1
        return self.primary_key_count

    def create_queues(self):
        for offset_mult in range(self.planner_threads):
            offset = offset_mult*self.primary_key_count
            for primary_key in self.primary_key_list:
                self.queue_list[primary_key + offset] = queue.Queue()
    
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

    def plan(self, transaction_list: list):
        #Divide transaction_list into N equal sized lists of transactions, where N = number of transactions
        split_transactions:list =[]
        transactions_per_priority = len(transaction_list)//self.planner_threads
        for i in range(self.planner_threads-1): 
            split_transactions.append(0)
            #print((i*transactions_per_priority), (((i+1)*transactions_per_priority)-1))
            split_transactions[i]=transaction_list[(i*transactions_per_priority):(((i+1)*transactions_per_priority))]
        split_transactions.append(0)
        split_transactions[self.planner_threads-1]=transaction_list[(self.planner_threads-1)*transactions_per_priority:]

        #Create as many threads as planner threads states
        thread_list:list = []
        for i in range(self.planner_threads):
            thread_list.append(0)
            thread_list[i]= threading.Thread(target = self.separate, args=(split_transactions[i], i))
            thread_list[i].start()
        #Run each planner thread on its sectioned data, create the transaction queues
        for i in range(self.planner_threads):
            thread_list[i].join()
        #Queues with transactions separated by which record they act on will be in self.queue_list
        #In executor, before taking a queue, run a while loop to check primary_key + x, which is the primary key at lower priority levels

    def separate(self, transaction_list, priority):
        queryObj = Query(self.table)
        primary_key: int
        offset=priority*self.primary_key_count
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
                self.queue_list[primary_key + offset].put((query, args))
