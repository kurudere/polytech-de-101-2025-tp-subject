"""
Module: data_consolidation.py
Responsabilité :
    - Standardisation des données ingérées dans des tables consolidées
    - Tables créées :
        * CONSOLIDATE_CITY
        * CONSOLIDATE_STATION
        * CONSOLIDATE_STATION_STATEMENT
"""

import duckdb
import pandas as pd
import json
from datetime import date


# ---------------------------------------------------------
# Création des tables consolidées (schéma global du TP)
# ---------------------------------------------------------

def create_consolidate_tables():
    """
    Crée les tables CONSOLIDATE_* si elles n'existent pas encore.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        sql = fd.read()

    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            print(f"[SQL CONSOLIDATION]\n{stmt}")
            con.execute(stmt)


# ---------------------------------------------------------
# CONSOLIDATION DES VILLES (INSEE)
# ---------------------------------------------------------

def consolidate_city_data():
    """
    Consolidation de TOUTES les villes de France via INSEE.
    Remplit CONSOLIDATE_CITY avec :
        ID (code INSEE), NAME, NB_INHABITANTS, CREATED_DATE
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    today = date.today()
    with open(f"data/raw_data/{today}/french_cities_data.json") as fd:
        raw = json.load(fd)

    df = pd.json_normalize(raw)

    df = df.rename(columns={
        "code": "ID",
        "nom": "NAME",
        "population": "NB_INHABITANTS"
    })

    df["CREATED_DATE"] = today

    con.register("city_df", df[["ID", "NAME", "NB_INHABITANTS", "CREATED_DATE"]])
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_df;")

    print(f"[CONSOLIDATION] {len(df)} villes consolidées.")


# ---------------------------------------------------------
# CONSOLIDATION PARIS
# ---------------------------------------------------------

def consolidate_station_paris_data():
    """
    Consolidation des stations Paris.
    Source : open data Paris → tableau JSON simple.
    """
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)

    today = date.today()
    with open(f"data/raw_data/{today}/paris_realtime_bicycle_data.json") as fd:
        raw = json.load(fd)

    df = pd.DataFrame(raw)

    station_df = pd.DataFrame({
        "id": df["stationcode"].astype(str),
        "code": df["stationcode"].astype(str),
        "name": df["name"],
        "city_name": df["nom_arrondissement_communes"],
        "city_code": df["code_insee_commune"],
        "address": None,
        "longitude": df["coordonnees_geo"].apply(lambda x: x["lon"]),
        "latitude": df["coordonnees_geo"].apply(lambda x: x["lat"]),
        "status": df["is_renting"].apply(lambda x: "OPEN" if x == "OUI" else "CLOSED"),
        "created_date": today,
        "capacity": df["capacity"]
    })

    con.register("paris_station_df", station_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_df;")


def consolidate_station_statement_paris_data():
    """
    Consolidation des disponibilités Paris.
    """
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)

    today = date.today()
    with open(f"data/raw_data/{today}/paris_realtime_bicycle_data.json") as fd:
        raw = json.load(fd)

    df = pd.DataFrame(raw)

    statement_df = pd.DataFrame({
        "station_id": df["stationcode"].astype(str),
        "bicycle_docks_available": df["numdocksavailable"],
        "bicycle_available": df["numbikesavailable"],
        "last_statement_date": today,
        "created_date": today
    })

    con.register("paris_statement_df", statement_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_statement_df;")


# ---------------------------------------------------------
# CONSOLIDATION NANTES
# ---------------------------------------------------------

def consolidate_station_nantes_data():
    """
    Consolidation des stations Nantes (format JCDecaux).
    """
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)
    today = date.today()

    with open(f"data/raw_data/{today}/nantes_realtime_bicycle_data.json") as fd:
        raw = json.load(fd)

    df = pd.json_normalize(raw["results"])

    station_df = pd.DataFrame({
        "id": df["number"].astype(str),
        "code": df["number"].astype(str),
        "name": df["name"],
        "city_name": "Nantes",
        "city_code": "44109",
        "address": df["address"],
        "longitude": df["position.lon"],
        "latitude": df["position.lat"],
        "status": "OPEN",
        "created_date": today,
        "capacity": df["bike_stands"]
    })

    con.register("nantes_station_df", station_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_df;")


def consolidate_station_statement_nantes_data():
    """
    Consolidation des disponibilités Nantes.
    """
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb", read_only=False)
    today = date.today()

    with open(f"data/raw_data/{today}/nantes_realtime_bicycle_data.json") as fd:
        raw = json.load(fd)

    df = pd.json_normalize(raw["results"])

    statement_df = pd.DataFrame({
        "station_id": df["number"].astype(str),
        "bicycle_docks_available": df["available_bike_stands"],
        "bicycle_available": df["available_bikes"],
        "last_statement_date": pd.to_datetime(df["last_update"]),
        "created_date": today
    })

    con.register("nantes_statement_df", statement_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM nantes_statement_df;")
