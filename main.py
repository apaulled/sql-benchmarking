from analyzer import Analyzer
from databases.mysql import MySqlDatabase
from databases.postgresql import PostgresDatabase
from databases.sqlite import SqliteDatabase


# pg.insert_dummy_data('poly_test', 1000000, 'id', 'int', 'poly_plain', 'geometry(polygon, 4326)', 'poly_ndx', 'geometry(polygon, 4326)')
def run_analysis(postgres=False, mysql=False, sqlite=False):
    if postgres:
        pg = PostgresDatabase(
            'dbfinal_postgres',
            'dbfinal',
            'password',
            'testing'
        )
        pg_analyzer = Analyzer(pg)
        pg_analyzer.run_analysis('analyses/analysis_pg2.json', geospatial=True, ids=True)
        print(pg_analyzer.report)

    if mysql:
        ms = MySqlDatabase(
            'dbfinal_mysql',
            'dbfinal',
            'password'
        )
        ms_analyzer = Analyzer(ms)
        ms_analyzer.run_analysis('analyses/analysis_ms2.json', geospatial=True, ids=True)
        print(ms_analyzer.report)

    if sqlite:
        sl = SqliteDatabase('/Users/paulgagliano/Downloads/dbfinal_sqlite.db')
        sl_analyzer = Analyzer(sl)
        sl_analyzer.run_analysis('analyses/analysis_sl.json', geospatial=False, ids=True)
        print(sl_analyzer.report)


def main():
    run_analysis(postgres=True, mysql=True)


if __name__ == "__main__":
    main()
