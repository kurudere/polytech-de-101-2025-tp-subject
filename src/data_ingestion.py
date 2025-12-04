"""
Module: data_ingestion.py
Responsabilité:
    - Récupération des données brutes (Paris, Nantes, INSEE)
    - Sérialisation des JSON dans data/raw_data/YYYY-MM-DD/
"""

import os
from datetime import datetime
import requests


def serialize_data(raw_json: str, file_name: str):
    """
    Enregistre un JSON brut dans un dossier daté.

    Args:
        raw_json: contenu JSON sous forme de chaîne.
        file_name: nom du fichier cible.
    """
    today_date = datetime.now().strftime("%Y-%m-%d")
    out_dir = f"data/raw_data/{today_date}"

    os.makedirs(out_dir, exist_ok=True)

    with open(f"{out_dir}/{file_name}", "w") as fd:
        fd.write(raw_json)

    print(f"[INGESTION] Fichier enregistré : {out_dir}/{file_name}")


# --------------------------
# INGESTION PARIS
# --------------------------

def get_paris_realtime_bicycle_data():
    """
    Télécharge les données temps réel Vélib Paris.
    Source officielle : opendata.paris.fr
    """
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        serialize_data(response.text, "paris_realtime_bicycle_data.json")
    except Exception as e:
        print(f"[ERREUR INGESTION PARIS] {e}")


# --------------------------
# INGESTION NANTES
# --------------------------

def get_nantes_realtime_bicycle_data():
    """
    Télécharge les données temps réel vélos Nantes Métropole.
    Format JCDecaux → results[]
    """
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/records?limit=20"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        serialize_data(response.text, "nantes_realtime_bicycle_data.json")
    except Exception as e:
        print(f"[ERREUR INGESTION NANTES] {e}")


# --------------------------
# INGESTION INSEE (toutes les villes de France)
# --------------------------

def get_french_cities_data():
    """
    Télécharge la liste des communes françaises depuis l'API INSEE.
    Indispensable pour remplir DIM_CITY et relier Paris/Nantes via codes INSEE.
    """
    url = "https://geo.api.gouv.fr/communes"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        serialize_data(response.text, "french_cities_data.json")
    except Exception as e:
        print(f"[ERREUR INGESTION INSEE] {e}")
