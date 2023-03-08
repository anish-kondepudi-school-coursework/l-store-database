from lstore.table import Table, Record
from lstore.query import Query
import queue

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
        return self.primary_key_count

    def create_queues(self):
        for offset_mult in range(self.planner_threads):
            offset = offset_mult*self.primary_key_count
            for primary_key in self.primary_key_list:
                self.queue_list[primary_key + offset] = queue.Queue()

    def separate(self, transaction_list: list):
        #Create as many threads as planner threads states
        #Divide transaction_list into N equal sized lists of transactions, where N = number of transactions
        #Run each planner thread on its sectioned data, create the transaction queues
        #Queues with transactions separated by which record they act on will be in self.queue_list
        #In executor, before taking a queue, run a while loop to check primary_key + x, which is the primary key at lower priority levels
        self.reset()