import string
import random


def random_string(length: int) -> str:
    characters = string.ascii_uppercase + string.ascii_lowercase + string.digits
    return ''.join(random.choices(characters, k=length))


def generate_queries(table: str, col1: str, type1: str, col2: str, type2: str,
                     table2: str = None, col3: str = None, type3: str = None, col4: str = None, type4: str = None):
    where = None
    joins = None
    if table2 is not None:
        if type1 == 'geometry(point, 4326)' and type3 == 'geometry(polygon, 4326)':
            joins = [f"st_contains(b.{col3}, a.{col1});", f"st_contains(b.{col4}, a.{col2});"]
            return [f"select * from {table} a join {table2} b on " + x for x in joins]
    if type1 == type2 == 'int':
        where = [f"{col} = -1;" for col in [col1, col2]]
        joins = [f"a.{col} = b.{col};" for col in [col1, col2]]
    if type1 == type2 == 'text':
        where = [f"{col} = 'a';" for col in [col1, col2]]
        joins = [f"a.{col} = b.{col};" for col in [col1, col2]]
    if type1 == type2 == 'char(36)':
        where = [f"{col} = '{'a' * 36}';" for col in [col1, col2]]
        joins = [f"a.{col} = b.{col};" for col in [col1, col2]]
    if type1 == type2 == 'geometry(point, 4326)':  # These two are postgres datatypes
        where = [f"st_dwithin('0101000020E6100000C8A6504D69534940ACCE10014CF562C0'::geometry, {col}, 10);" for col in [col1, col2]]
        joins = [f"st_dwithin(a.{col}, b.{col}, 10);" for col in [col1, col2]]
    if type1 == type2 == 'geometry(polygon, 4326)':
        where = [f"st_overlaps(st_setsrid('POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'::geometry, 4326), {col});" for col in [col1, col2]]
        joins = [f"st_overlaps(a.{col}, b.{col});" for col in [col1, col2]]
    if type1 == type2 == 'point not null srid 4326':  # These two are mysql datatypes
        where = [f"st_distance_sphere(st_geomfromtext('POINT(0 0)', 4326), {col}) < 10;" for col in [col1, col2]]
        joins = [f"st_distance_sphere(a.{col}, b.{col}) < 10;" for col in [col1, col2]]
    if type1 == type2 == 'polygon not null srid 4326':
        where = [f"st_overlaps({col}, st_geomfromtext('POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))', 4326));" for col in [col1, col2]]
        joins = [f"st_overlaps(a.{col}, b.{col});" for col in [col1, col2]]
    return [f"select * from {table} where " + x for x in where] + [f"select * from {table} a join {table} b on " + x for x in joins]


def generate_id_query(table: str, _type: str):
    where = None
    if _type == 'int':
        where = "id = -1;"
    if _type == 'char(16)':
        where = "id = 'aaaaaaaaaaaaaaaa';"
    if _type == 'uuid':
        where = "id = gen_random_uuid();"
    if _type == 'bytea':
        where = "id = gen_random_bytes(32);"
    if _type == 'binary(16)':
        where = "id = unhex(replace(uuid(),'-',''));"
    return f"select * from {table} where " + where
