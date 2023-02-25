from .config import START_BASE_RID, START_TAIL_RID
class RID_Generator:
    def __init__(self):
        self.curr_base_rid = START_BASE_RID
        self.curr_tail_rid = START_TAIL_RID

    def new_base_rid(self):
        self.curr_base_rid += 1
        return self.curr_base_rid - 1
    
    def new_tail_rid(self):
        self.curr_tail_rid -= 1
        return self.curr_tail_rid + 1
    
