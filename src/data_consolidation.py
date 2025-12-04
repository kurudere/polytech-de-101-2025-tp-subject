import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_city_paris_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Chargement des données brutes
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    # Création de la colonne nb_inhabitants vide
    raw_data_df["nb_inhabitants"] = None

    # Sélection et renommage des colonnes de base
    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]].copy()

    city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)

    city_data_df.drop_duplicates(inplace=True)
    city_data_df["created_date"] = date.today()

    # Chargement des données INSEE
    with open(f"data/raw_data/{today_date}/french_cities_data.json") as fd:
        cities_raw = json.load(fd)

    if cities_raw:
        insee_df = pd.json_normalize(cities_raw)

        # Normalisation INSEE
        insee_df = insee_df[["code", "nom", "population"]].copy()
        insee_df.rename(columns={
            "code": "id",
            "nom": "official_name",
            "population": "nb_inhabitants"
        }, inplace=True)

        # Supprime la colonne nb_inhabitants temporaire pour éviter conflit
        city_data_df = city_data_df.drop(columns=["nb_inhabitants"], errors="ignore")

        # Merge avec les données INSEE
        city_data_df = city_data_df.merge(
            insee_df[["id", "nb_inhabitants"]],
            on="id",
            how="left"
        )

    # Supprime colonne official_name si elle existe
    city_data_df = city_data_df.drop(columns=["official_name"], errors="ignore")

    # Sélection finale du schéma
    city_data_df = city_data_df[["id", "name", "nb_inhabitants", "created_date"]]

    # Enregistrement dans DuckDB
    con.register("city_data_df", city_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def consolidate_station_paris_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    station_data_df = pd.DataFrame({
        "id": raw_data_df["stationcode"],
        "code": raw_data_df["stationcode"],
        "name": raw_data_df["name"],
        "city_name": raw_data_df["nom_arrondissement_communes"],
        "city_code": raw_data_df["code_insee_commune"],
        "address": None,
        "longitude": raw_data_df["coordonnees_geo.lon"],
        "latitude": raw_data_df["coordonnees_geo.lat"],
        "status": raw_data_df["is_renting"],
        "created_date": date.today(),
        "capacity": raw_data_df["capacity"]
    })

    con.register("station_data_df", station_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")
    con.close()

def consolidate_station_statement_paris_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    statement_data_df = pd.DataFrame({
        "station_id": raw_data_df["stationcode"],
        "bicycle_docks_available": raw_data_df["numdocksavailable"],
        "bicycle_available": raw_data_df["numbikesavailable"],
        "last_statement_date": pd.to_datetime(raw_data_df["duedate"]),
        "created_date": date.today()
    })

    con.register("statement_data_df", statement_data_df)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM statement_data_df;")
    con.close()
