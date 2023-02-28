from .config import START_BASE_RID, START_TAIL_RID
from .phys_page import PhysicalPage
class RID_Generator:
    def __init__(self):
        self.curr_base_rid = START_BASE_RID
        self.curr_tail_rid = START_TAIL_RID

    def base_rid_to_starting_rid(self, base_rid) -> int:
        max_num_records = PhysicalPage.max_number_of_records
        return START_BASE_RID + ((base_rid-START_BASE_RID) // max_num_records) * max_num_records

    def tail_rid_to_starting_rid(self, tail_rid) -> int:
        associated_base_rid = -tail_rid
        return -self.base_rid_to_starting_rid(associated_base_rid)

    def get_base_rids(self) -> list[int]:
        rid_high = self.curr_base_rid + PhysicalPage.max_number_of_records
        page_rid_space = [rid for rid in range(self.curr_base_rid, rid_high)]
        self.curr_base_rid = rid_high
        return page_rid_space[::-1]
    
    def get_tail_rids(self) -> list[int]:
        rid_low = self.curr_tail_rid - PhysicalPage.max_number_of_records
        page_rid_space = [rid for rid in range(rid_low+1, self.curr_tail_rid+1)]
        self.curr_tail_rid = rid_low
        return page_rid_space

    def get_slot_num(self, rid: int) -> int:
        assert rid != 0
        return ((abs(rid) - 1) % PhysicalPage.max_number_of_records)