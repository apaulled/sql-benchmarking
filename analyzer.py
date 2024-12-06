import json
import time
from dataclasses import dataclass
from typing import Optional

from databases.mysql import MySqlDatabase
from databases.postgresql import PostgresDatabase
from databases.sql import SqlDatabase
from utils import generate_queries, generate_id_query


@dataclass
class SelectSection:
    str_plain: list[float]
    str_ndx: list[float]
    int_plain: list[float]
    int_ndx: list[float]
    point_plain: Optional[list[float]]
    point_ndx: Optional[list[float]]
    poly_plain: Optional[list[float]]
    poly_ndx: Optional[list[float]]


@dataclass
class JoinSection:
    str_plain: list[float]
    str_ndx: list[float]
    int_plain: list[float]
    int_ndx: list[float]
    point_plain: Optional[list[float]]
    point_ndx: Optional[list[float]]
    poly_plain: Optional[list[float]]
    poly_ndx: Optional[list[float]]
    point_poly_plain: Optional[list[float]]
    point_poly_ndx: Optional[list[float]]


@dataclass
class IdSection:
    integer: list[float]
    uuid: list[float]
    binary: Optional[list[float]]
    char: list[float]


@dataclass
class Report:
    selects: SelectSection
    joins: JoinSection
    ids: Optional[IdSection]


class Analyzer:

    def __init__(self, database: SqlDatabase):
        self.database: SqlDatabase = database
        self.report: Optional[Report] = None

    def run_analysis(self, out_file: str, start: int = 100, stop: int = 1000000, stop_small: int = 10000, geospatial: bool = False, ids: bool = False, num_points: int = 10):
        text_type = 'text' if type(self.database) is PostgresDatabase else 'char(36)'
        tables = {
            'int_test': {'id': 'int', 'int_plain': 'int', 'int_ndx': 'int'},
            'str_test': {'id': 'int', 'str_plain': text_type, 'str_ndx': text_type}
        }

        id_tables = {}

        if ids:
            id_tables['id_test_char'] = {'id': 'char(16)', 'num': 'int'}
            id_tables['id_test_int'] = {'id': 'int', 'num': 'int'}
            if type(self.database) is PostgresDatabase:
                id_tables['id_test_uuid'] = {'id': 'uuid', 'num': 'int'}
                id_tables['id_test_binary'] = {'id': 'bytea', 'num': 'int'}
            elif type(self.database) is MySqlDatabase:
                id_tables['id_test_uuid'] = {'id': 'binary(16)', 'num': 'int'}

        if geospatial:
            if type(self.database) is PostgresDatabase:
                tables['point_test'] = {'id': 'int',
                                        'pt_plain': 'geometry(point, 4326)',
                                        'pt_ndx': 'geometry(point, 4326)'}
                tables['poly_test'] = {'id': 'int',
                                       'poly_plain': 'geometry(polygon, 4326)',
                                       'poly_ndx': 'geometry(polygon, 4326)'}
            elif type(self.database) is MySqlDatabase:
                tables['point_test'] = {'id': 'int',
                                        'pt_plain': 'point not null srid 4326',
                                        'pt_ndx': 'point not null srid 4326'}
                tables['poly_test'] = {'id': 'int',
                                       'poly_plain': 'polygon not null srid 4326',
                                       'poly_ndx': 'polygon not null srid 4326'}

        step = (stop - start) // num_points
        step_small = (stop_small - start) // num_points

        results = {}
        for name, data in tables.items():
            if len(data) == 3:  # This is a probably-unnecessary safety precaution
                points_plain = []
                points_ndx = []
                points_join_plain = []
                points_join_ndx = []

                table_geospatial = 'point' in name or 'poly' in name

                real_stop = stop_small if table_geospatial else stop
                real_step = step_small if table_geospatial else step
                for i in range(2):
                    keys = list(data.keys())
                    ndx = [x for x in keys if 'ndx' in x][0]
                    plain = [x for x in keys if 'plain' in x][0]
                    queries = generate_queries(name, plain, data[plain], ndx, data[ndx])

                    self.database.insert_dummy_data(
                        name, i, 'id', data['id'], plain, data[plain], ndx, data[ndx])
                    self.database.create_index(name, ndx, ndx, table_geospatial)

                    trials_plain = []
                    trials_ndx = []
                    trials_join_plain = []
                    trials_join_ndx = []

                    print('Timing Queries')
                    t1 = time.time()
                    num_trials = 10 if table_geospatial else 100
                    for j in range(num_trials):
                        trials_plain.append(self.database.time_query(queries[0]))
                        trials_ndx.append(self.database.time_query(queries[1]))
                        if (j + 1) % 5 == 0:
                            print(f'Timed: {j + 1}/{num_trials}')
                    t2 = time.time()
                    print(f'Timed {num_trials} * 2 queries in {t2 - t1} seconds')

                    print('Timing Join Queries')
                    t1 = time.time()
                    num_join_trials = 2 if table_geospatial else 10
                    for j in range(num_join_trials):
                        trials_join_plain.append(self.database.time_query(queries[2]))
                        trials_join_ndx.append(self.database.time_query(queries[3]))
                        print(f'Timed: {j + 1}/{num_join_trials}')
                    t2 = time.time()
                    print(f'Timed {num_join_trials} * 2 join queries in {t2 - t1} seconds')

                    points_plain.append(sum(trials_plain) / num_trials)
                    points_ndx.append(sum(trials_ndx) / num_trials)
                    points_join_plain.append(sum(trials_join_plain) / num_join_trials)
                    points_join_ndx.append(sum(trials_join_ndx) / num_join_trials)

                    self.database.clear_table(name, ndx)
                results[name] = {
                    'plain': points_plain,
                    'ndx': points_ndx,
                    'join_plain': points_join_plain,
                    'join_ndx': points_join_ndx
                }

                if 'point' in name:
                    points_pp_plain = []
                    points_pp_ndx = []
                    for i in range(start, 20000, (20000 - start) // 10):
                        data_point = tables['point_test']
                        data_poly = tables['poly_test']
                        queries = generate_queries(
                            'point_test', 'pt_plain', data_point['pt_plain'], 'pt_ndx', data_point['pt_ndx'],
                            'poly_test', 'poly_plain', data_poly['poly_plain'], 'poly_ndx', data_poly['poly_ndx']
                        )

                        self.database.insert_dummy_data(
                            'point_test', i, 'id', 'int', 'pt_plain', data_point['pt_plain'], 'pt_ndx', data_point['pt_ndx'])
                        self.database.create_index('point_test', 'pt_ndx', 'pt_ndx', True)

                        self.database.insert_dummy_data(
                            'poly_test', i, 'id', 'int', 'poly_plain', data_poly['poly_plain'], 'poly_ndx', data_poly['poly_ndx'])
                        self.database.create_index('poly_test', 'poly_ndx', 'poly_ndx', True)

                        trials_pp_plain = []
                        trials_pp_ndx = []

                        print('Timing Point/Poly Queries')
                        t1 = time.time()
                        num_trials = 2
                        for j in range(num_trials):
                            trials_pp_plain.append(self.database.time_query(queries[0]))
                            trials_pp_ndx.append(self.database.time_query(queries[1]))
                            print(f'Timed: {j + 1}/{num_trials}')
                        t2 = time.time()
                        print(f'Timed {num_trials} * 2 Point/Poly queries in {t2 - t1} seconds')
                        points_pp_plain.append(sum(trials_pp_plain) / num_trials)
                        points_pp_ndx.append(sum(trials_pp_ndx) / num_trials)

                        self.database.clear_table('point_test', 'pt_ndx')
                        self.database.clear_table('poly_test', 'poly_ndx')

                    results['point_poly_test'] = {'join_plain': points_pp_plain, 'join_ndx': points_pp_ndx}

        selects = SelectSection(
            str_plain=results['str_test']['plain'],
            str_ndx=results['str_test']['ndx'],
            int_plain=results['int_test']['plain'],
            int_ndx=results['int_test']['ndx'],
            point_plain=results['point_test']['plain'] if 'point_test' in results else None,
            point_ndx=results['point_test']['ndx'] if 'point_test' in results else None,
            poly_plain=results['poly_test']['plain'] if 'poly_test' in results else None,
            poly_ndx=results['poly_test']['ndx'] if 'poly_test' in results else None
        )

        joins = JoinSection(
            str_plain=results['str_test']['join_plain'],
            str_ndx=results['str_test']['join_ndx'],
            int_plain=results['int_test']['join_plain'],
            int_ndx=results['int_test']['join_ndx'],
            point_plain=results['point_test']['join_plain'] if 'point_test' in results else None,
            point_ndx=results['point_test']['join_ndx'] if 'point_test' in results else None,
            poly_plain=results['poly_test']['join_plain'] if 'poly_test' in results else None,
            poly_ndx=results['poly_test']['join_ndx'] if 'poly_test' in results else None,
            point_poly_plain=results['point_poly_test']['join_plain'] if 'point_poly_test' in results else None,
            point_poly_ndx=results['point_poly_test']['join_ndx'] if 'point_poly_test' in results else None
        )

        id_results = {}
        for name, data in id_tables.items():
            points = []
            for i in range(2):
                self.database.insert_dummy_data(
                    name, i, 'id', data['id'], 'num', data['num'])

                query = generate_id_query(name, data['id'])
                trials = []

                print('Timing Queries')
                t1 = time.time()
                num_trials = 100
                for j in range(num_trials):
                    trials.append(self.database.time_query(query))
                    if (j + 1) % 5 == 0:
                        print(f'Timed: {j + 1}/{num_trials}')
                t2 = time.time()
                print(f'Timed {num_trials} * 4 queries in {t2 - t1} seconds')

                points.append(sum(trials) / num_trials)
                self.database.clear_table(name)
            id_results[name] = points

        ids_section = None
        if ids:
            ids_section = IdSection(
                integer=id_results.get('id_test_int'),
                uuid=id_results.get('id_test_uuid'),
                binary=id_results.get('id_test_binary'),
                char=id_results.get('id_test_char')
            )

        self.report = Report(selects, joins, ids_section)

        with open(out_file, 'w') as file:
            file.write(json.dumps({k: v.__dict__ for k, v in self.report.__dict__.items()}, indent=4))

        return self.report
