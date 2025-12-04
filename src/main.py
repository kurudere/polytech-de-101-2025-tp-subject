from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station
)
from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_paris_data,
    consolidate_station_paris_data,
    consolidate_station_statement_paris_data
)
from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
    get_french_cities_data
)

def main():
    print("Process start.")
    # data ingestion

    print("Data ingestion started.")
    get_nantes_realtime_bicycle_data()
    get_paris_realtime_bicycle_data()
    get_french_cities_data()
    print("Data ingestion ended.")

    # data consolidation
    print("Consolidation data started.")
    create_consolidate_tables()
    consolidate_city_paris_data()
    consolidate_station_paris_data()
    consolidate_station_statement_paris_data()
    # Other consolidation here
    print("Consolidation data ended.")

    # data agregation
    print("Agregate data started.")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    # Other agregations here
    print("Agregate data ended.")

if __name__ == "__main__":
    main()