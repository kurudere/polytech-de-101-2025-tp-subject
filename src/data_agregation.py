"""
Module: data_agregation.py
Responsabilité :
    - Construction du modèle en étoile (DIM + FACT)
"""

import duckdb


def create_agregate_tables():
    """Crée les tables DIM_* et FACT_*."""
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        sql = fd.read()

    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            print(f"[SQL AGREGATION]\n{stmt}")
            con.execute(stmt)


def agregate_dim_city():
    """
    Remplit la dimension ville :
        - ID : code INSEE
        - NAME
        - NB_INHABITANTS
    """
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)

    sql = """
    INSERT OR REPLACE INTO DIM_CITY (ID, NAME, NB_INHABITANTS)
    SELECT ID, NAME, NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql)


def agregate_dim_station():
    """
    Remplit la dimension station pour TOUTES les villes.
    """
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)

    sql = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT
        ID, CODE, NAME, ADDRESS,
        LONGITUDE, LATITUDE, STATUS, CAPACITY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql)


def aggregate_fact_station_statement():
    """
    Construit la table de faits :
        - station_id
        - city_id
        - quantité de vélos
        - disponibilité
        - timestamp
    """

    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)

    sql = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
    (
        STATION_ID,
        CITY_ID,
        BICYCLE_DOCKS_AVAILABLE,
        BICYCLE_AVAILABLE,
        LAST_STATEMENT_DATE,
        CREATED_DATE
    )
    SELECT
        st.station_id,
        cs.city_code AS city_id,
        st.bicycle_docks_available,
        st.bicycle_available,
        st.last_statement_date,
        CAST(st.created_date AS DATE)
    FROM CONSOLIDATE_STATION_STATEMENT st
    JOIN CONSOLIDATE_STATION cs ON st.station_id = cs.id
    JOIN DIM_CITY city ON cs.city_code = city.id;
    """

    con.execute(sql)
    print("[FACT] FACT_STATION_STATEMENT mise à jour.")
