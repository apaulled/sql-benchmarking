import random
from typing import Optional

import psycopg2
import progressbar
import time

import utils
from databases.sql import SqlDatabase


class PostgresDatabase(SqlDatabase):

    def __init__(self, db_name: str, username: str, password: str, schema: str):
        super().__init__(db_name, username, password)
        self.connection = psycopg2.connect(
            database=db_name,
            user=username,
            password=password,
            host="localhost",
            port="5432",
            options=f"-c search_path={schema}"
        )
        self.cursor = self.connection.cursor()

    def insert_dummy_data(self, table, n, col1, type1, col2, type2, col3=None, type3=None):
        print(f'Inserting {n} rows into {table}')

        t1 = time.time()

        columns = {col1: type1, col2: type2}
        column_data = {col1: [], col2: []}
        if col3 is not None:
            columns[col3] = type3
            column_data[col3] = []

        for col_name, type_name in columns.items():
            print(f'Creating Data for {col_name} of type {type_name}')
            for i in range(1, n+1):
                if type_name == 'int':
                    column_data[col_name].append(i)
                elif type_name == 'text':
                    column_data[col_name].append(f"'{utils.random_string(36)}'")
                elif type_name == 'geometry(point, 4326)':
                    column_data[col_name].append(
                        f'st_setsrid(st_makepoint({random.uniform(-90.0, 90.0)}, {random.uniform(-180.0, 180.0)}), 4326)'
                    )
                elif type_name == 'geometry(polygon, 4326)':
                    rp = [(random.uniform(-90.0, 90.0), random.uniform(-180.0, 180.0)) for i in range(4)]
                    column_data[col_name].append(
                        f"'POLYGON(({rp[0][0]} {rp[0][1]}, {rp[1][0]} {rp[1][1]}, {rp[2][0]} {rp[2][1]}, {rp[3][0]} {rp[3][1]}, {rp[0][0]} {rp[0][1]}))'::geometry"
                    )
                elif type_name == 'bytea':
                    column_data[col_name].append('gen_random_bytes(32)')
                elif type_name == 'uuid':
                    column_data[col_name].append('gen_random_uuid()')
                elif type_name == 'char(16)':
                    column_data[col_name].append(f"'{utils.random_string(16)}'")

                if i % 100000 == 0 and i > 0:
                    print(f'Created: {i}/{n}')

        if type3 == type2:
            column_data[col3] = column_data.get(col2)

        if col3 is not None:
            print('Inserting Data')
            query_head = f'insert into {table} ({col1}, {col2}, {col3}) values '
            mut_query = query_head
            for i in range(n):
                mut_query += f'\n({column_data[col1][i]}, {column_data[col2][i]}, {column_data[col3][i]}),'
                if (i % 500 == 0 and i > 0) or i == n-1:
                    mut_query = mut_query[:-1] + ';'
                    self.cursor.execute(mut_query)
                    self.connection.commit()
                    mut_query = query_head
                if i % 100000 == 0 and i > 0:
                    print(f'Inserted: {i}/{n}')
        else:
            print('Inserting Data')
            query_head = f'insert into {table} ({col1}, {col2}) values '
            mut_query = query_head
            for i in range(n):
                mut_query += f'\n({column_data[col1][i]}, {column_data[col2][i]}),'
                if (i % 500 == 0 and i > 0) or i == n-1:
                    mut_query = mut_query[:-1] + ';'
                    self.cursor.execute(mut_query)
                    self.connection.commit()
                    mut_query = query_head
                if i % 100000 == 0 and i > 0:
                    print(f'Inserted: {i}/{n}')

        t2 = time.time()
        print(f'Inserted {n} rows into {table} in {t2 - t1} seconds')

    def drop_index(self, index: str, table: Optional[str] = None) -> None:
        self.cursor.execute(f'drop index {index};')
        self.connection.commit()

    def create_index(self, table: str, index: str, column: str, geospatial: bool = False):
        if geospatial:
            self.cursor.execute(f'create index {index} on {table} using GIST ({column});')
        else:
            self.cursor.execute(f'create unique index {index} on {table} ({column});')
