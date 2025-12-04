"""
Module: main.py
Pipeline ETL complet :
    1. Ingestion
    2. Consolidation
    3. Agrégation (modèle en étoile)
"""

from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
    get_french_cities_data
)

from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_paris_data,
    consolidate_station_statement_paris_data,
    consolidate_station_nantes_data,
    consolidate_station_statement_nantes_data
)

from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    aggregate_fact_station_statement
)


def main():

    print("\n===== ETL START =====")

    # -------------------------- INGESTION --------------------------
    print("\n[ETAPE 1] INGESTION...")
    get_french_cities_data()
    get_paris_realtime_bicycle_data()
    get_nantes_realtime_bicycle_data()

    # -------------------------- CONSOLIDATION -----------------------
    print("\n[ETAPE 2] CONSOLIDATION...")
    create_consolidate_tables()
    consolidate_city_data()
    consolidate_station_paris_data()
    consolidate_station_statement_paris_data()
    consolidate_station_nantes_data()
    consolidate_station_statement_nantes_data()

    # -------------------------- AGREGATION -------------------------
    print("\n[ETAPE 3] AGREGATION...")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    aggregate_fact_station_statement()

    print("\n===== ETL FINI =====")


if __name__ == "__main__":
    main()
