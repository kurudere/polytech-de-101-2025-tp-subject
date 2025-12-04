import duckdb


def create_agregate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


def agregate_dim_city():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)

def agregate_dim_station():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        CITY_NAME,
        CITY_CODE,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql_statement)

