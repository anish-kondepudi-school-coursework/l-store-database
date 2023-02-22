from .page import PhysicalPage

class DiskInterface:
    def __init__(self, path: str) -> None:
        if len(path) > 0 and path[-1] == "/":
            self.path = path[:-1]
        else:
            self.path = path

    def get_page(self, page_id : str) -> PhysicalPage:
        pass

    def write_page(self, page_id : str, page_to_write : PhysicalPage) -> None:
        pass