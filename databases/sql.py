import time
from typing import Optional


class SqlDatabase:

    def __init__(self, db_name: str, username: str, password: str):
        self.db_name = db_name
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None

    def time_query(self, query: str, read_only: bool = False) -> float:
        t1 = time.time()
        self.cursor.execute(query)
        if read_only:
            self.cursor.fetchall()
        else:
            self.connection.commit()
        t2 = time.time()
        return t2 - t1

    def insert_dummy_data(self,
                          table: str,
                          n: int,
                          col1: str,
                          type1: str,
                          col2: str,
                          type2: str,
                          col3: str = None,
                          type3: str = None) -> None:
        raise NotImplementedError()

    def clear_table(self, table: str, index: Optional[str] = None) -> None:
        self.cursor.execute(f'delete from {table};')
        self.connection.commit()
        if index is not None:
            self.drop_index(index, table)

    def drop_index(self, index: str, table: Optional[str] = None) -> None:
        raise NotImplementedError()

    def create_index(self, table: str, index: str, column: str, geospatial: bool = False) -> None:
        raise NotImplementedError()
