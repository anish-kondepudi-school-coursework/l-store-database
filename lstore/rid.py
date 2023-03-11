from .config import START_BASE_RID, START_TAIL_RID
from .phys_page import PhysicalPage
from threading import Lock


class RID_Generator:
    def __init__(self):
        self.curr_base_rid: int= START_BASE_RID
        self.curr_tail_rid: int = START_TAIL_RID
        self.get_base_rids_lock: Lock = Lock()
        self.get_tail_rids_lock: Lock = Lock()

    def base_rid_to_starting_rid(self, base_rid) -> int:
        max_num_records = PhysicalPage.max_number_of_records
        return START_BASE_RID + ((base_rid - START_BASE_RID) // max_num_records) * max_num_records

    def tail_rid_to_starting_rid(self, tail_rid) -> int:
        associated_base_rid = -int(tail_rid)
        return -int(self.base_rid_to_starting_rid(associated_base_rid))

    def get_base_rids(self) -> list[int]:
        self.get_base_rids_lock.acquire()
        rid_high = self.curr_base_rid + PhysicalPage.max_number_of_records
        page_rid_space = [rid for rid in range(self.curr_base_rid, rid_high)]
        self.curr_base_rid = rid_high
        self.get_base_rids_lock.release
        return page_rid_space[::-1]

    def get_tail_rids(self) -> list[int]:
        self.get_tail_rids_lock.acquire()
        rid_low = self.curr_tail_rid - PhysicalPage.max_number_of_records
        page_rid_space = [rid for rid in range(rid_low + 1, self.curr_tail_rid + 1)]
        self.curr_tail_rid = rid_low
        self.get_tail_rids_lock.release()
        return page_rid_space

    def get_slot_num(self, rid: int) -> int:
        assert rid != 0
        return (abs(rid) - 1) % PhysicalPage.max_number_of_records