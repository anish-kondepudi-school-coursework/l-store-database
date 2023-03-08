from lstore.table import Table, Record
from lstore.query import Query

class Planner:

    """
    # Creates a planner object.
    """

    def __init__(self, table):
        self.table: Table=table
        self.primary_key_count = 0
        self.queue_list: list = []
        self.transaction_count: int = 0
        self.primary_key_list= dict()
        pass

    def reset(self):
        self.primary_key_count = 0
        self.primary_key_list.clear

    def find_primary_key_count(self, transaction_list: list):
        queryObj = Query(self.table)
        for transaction in transaction_list:
            for query, args in transaction.queries:
                if(query.__func__==queryObj.insert.__func__):
                    print(args)
                    if args[self.table.primary_key_col] not in self.primary_key_list:
                        self.primary_key_list[args[self.table.primary_key_col]]=1
                        self.primary_key_count+=1
                elif(query==queryObj.select):
                    if args[1]==self.table.primary_key_col and args[0] not in self.primary_key_list:
                        self.primary_key_list[args[0]]=1
                        self.primary_key_count+=1
                elif(query==queryObj.update):
                    if args[0] not in self.primary_key_list:
                        self.primary_key_list[args[0]]=1
                        self.primary_key_count+=1
        return self.primary_key_count


    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()