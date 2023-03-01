import unittest
from unittest import mock
from lstore import (
    PageRange,
    Table,
    Query,
)


class TestQuery(unittest.TestCase):
    def test_insert_record_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record)
        query.table.index.get_rid(1)

    def test_insert_record_dup_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record)
        result: bool = query.insert(*record)
        assert not result

    def test_select_record_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record)
        recordList = query.select(1, query.table.primary_key_col, [1, 1, 1, 1, 1])
        assert recordList[0].columns == record

    def test_select_secondary_records(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        # defining records for secondary query
        records: list[list[int]] = [
            [1, 2, 3, 4, 5],
            [2, 2, 3, 4, 5],
            [3, 2, 3, 4, 5],
        ]
        # inserting records
        for record in records:
            query.insert(*record)
        # selecting records
        recordList = query.select(2, 1, [1, 1, 1, 1, 1])
        # asserting records
        for i in range(len(records)):
            assert recordList[i].columns == records[i]

    def test_update_record_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record)
        updateList: list[int] = [None, 3, 4, 5, 6]
        query.update(1, *updateList)
        recordList = query.select(1, query.table.primary_key_col, [1, 1, 1, 1, 1])
        assert recordList[0].columns == [1, 3, 4, 5, 6]

    def test_update_record_does_not_exist_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        updateList: list[int] = [None, 3, 4, 5, 6]
        result: bool = query.update(1, *updateList)
        assert not result

    def test_aggregate_record_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record1: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record1)
        record2: list[int] = [2, 2, 3, 4, 5]
        query.insert(*record2)
        record3: list[int] = [3, 2, 3, 4, 5]
        query.insert(*record3)
        aggregateSum1 = query.sum(1, 3, 1)
        assert aggregateSum1 == 6
        aggregateSum2 = query.sum(1, 3, 2)
        assert aggregateSum2 == 9

    def test_aggregate_record_delete_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record1: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record1)
        record2: list[int] = [2, 2, 3, 4, 5]
        query.insert(*record2)
        record3: list[int] = [3, 2, 3, 4, 5]
        query.insert(*record3)
        aggregateSum1 = query.sum(1, 3, 1)
        assert aggregateSum1 == 6
        query.delete(2)
        aggregateSum2 = query.sum(1, 3, 1)
        assert aggregateSum2 == 4

    def test_aggregate_record_none_in_range_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record1: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record1)
        record2: list[int] = [2, 2, 3, 4, 5]
        query.insert(*record2)
        record3: list[int] = [3, 2, 3, 4, 5]
        query.insert(*record3)
        aggregateSum1 = query.sum(4, 5, 1)
        assert not aggregateSum1

    def test_delete_fail_select_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record1: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record1)
        query.delete(1)
        try:
            query.select(1, query.table.primary_key_col, [1, 1, 1, 1, 1])
            assert 0
        except:
            assert 1

    def test_delete_reinsert_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record1: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record1)
        query.delete(1)
        query.insert(*record1)

    def test_increment_record_query(self) -> None:
        table: Table = Table("table1", 5, 0)
        query: Query = Query(table)
        record1: list[int] = [1, 2, 3, 4, 5]
        query.insert(*record1)
        record2: list[int] = [2, 2, 3, 4, 5]
        query.insert(*record2)
        record3: list[int] = [3, 2, 3, 4, 5]
        query.insert(*record3)
        result: bool = query.increment(1, 2)
        assert result
        recordList = query.select(1, query.table.primary_key_col, [1, 1, 1, 1, 1])
        assert recordList[0].columns == [1, 2, 4, 4, 5]


if __name__ == "__main__":
    unittest.main()
