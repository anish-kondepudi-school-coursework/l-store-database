class RID_Generator():
    def __init__(self):
        self.curr_rid = 1
    
    def new_rid(self):
        self.curr_rid += 1
        return self.curr_rid - 1