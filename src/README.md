# 🚲 Mobility Analysis – Paris & Nantes (ETL + Data Warehouse DuckDB)

Ce projet développe une **pipeline de traitement de données (ETL)** pour analyser
les stations de vélos en libre-service des villes de **Paris** et **Nantes**.

Le système collecte les données en temps réel, les consolide, et construit un
**modèle en étoile (Data Warehouse DuckDB)** permettant d'exécuter des analyses
sur l'état du réseau vélo (stations, disponibilités, villes…).

---

# Fonctionnalités principales

- **Ingestion automatique** des données :
  - Vélib Paris (temps réel)
  - Nantes Métropole (temps réel)
  - Communes françaises (API INSEE)

- **Consolidation** des données hétérogènes :
  - unification des formats Paris/Nantes  
  - normalisation des géométries, capacités, disponibilités  
  - intégration du référentiel INSEE pour relier stations ↔ villes  

- **Construction d’un modèle en étoile** :
  - `DIM_CITY`
  - `DIM_STATION`
  - `FACT_STATION_STATEMENT`

- **Analyses SQL prêtes à l'emploi** :
  - vélos disponibles par ville  
  - stations les plus / moins disponibles  
  - évolution de la disponibilité  

---

# 🏗 Architecture du projet

```text

project/
│
├── data/
│ ├── raw_data/ # Données brutes organisées par date
│ ├── duckdb/ # Base DuckDB
│ └── sql_statements/ # Scripts SQL pour créer les tables
│
├── src/
│ ├── data_ingestion.py # Récupération et sauvegarde des données brutes
│ ├── data_consolidation.py # Normalisation et insertion en tables CONSOLIDATE
│ ├── data_agregation.py # Construction du data warehouse (DIM + FACT)
│ └── main.py # Pipeline ETL complet
│
└── README.md
```

---

# 🔄 Pipeline ETL

```text
        INGESTION
    ┌─────────────────┐
    │ Paris API       │
    │ Nantes API      │
    │ INSEE API       │
    └─────────┬───────┘
              │ raw json
              ▼
          CONSOLIDATION
    ┌──────────────────────────────────────┐
    │ CONSOLIDATE_CITY                     │  (INSEE)
    │ CONSOLIDATE_STATION                  │  (Paris + Nantes)
    │ CONSOLIDATE_STATION_STATEMENT        │  (Paris + Nantes)
    └─────────┬────────────────────────────┘
              │ tables propres
              ▼
            AGRÉGATION (DW)
    ┌──────────────────────────────────────┐
    │ DIM_CITY                             │
    │ DIM_STATION                          │
    │ FACT_STATION_STATEMENT               │
    └──────────────────────────────────────┘

🗄 Modèle en étoile (Data Warehouse)

    🟦 Table : DIM_CITY
        Colonne	Description
        ID	Code INSEE (clé primaire)
        NAME	Nom de la commune
        NB_INHABITANTS	Population INSEE

    🟨 Table : DIM_STATION
        Colonne	Description
        ID	Identifiant unique de la station
        CODE	Code original de la source
        NAME	Nom
        ADDRESS	Adresse (si disponible)
        LATITUDE / LONGITUDE	Coordonnées
        STATUS	OPEN / CLOSED
        CAPACITY	Capacité totale

    🟥 Table : FACT_STATION_STATEMENT
        Colonne	Description
        STATION_ID	Station de rattachement
        CITY_ID	Ville (code INSEE)
        BICYCLE_AVAILABLE	Nombre de vélos disponibles
        BICYCLE_DOCKS_AVAILABLE	Bornettes libres
        LAST_STATEMENT_DATE	Timestamp
        CREATED_DATE	Date d'observation

▶️ Exécuter le projet

Depuis la racine du projet :

    > python3 src/main.py


Le pipeline :

- Télécharge les données

- Construit les tables CONSOLIDATE_*

- Construit les tables DIM_* et FACT_*

- Charge tout dans DuckDB

- Pour ouvrir la base DuckDB :

    > duckdb data/duckdb/mobility_analysis.duckdb
