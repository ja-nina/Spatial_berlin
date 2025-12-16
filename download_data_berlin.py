import geopandas as gpd
from sqlalchemy import create_engine
import dotenv
from dotenv import load_dotenv
load_dotenv()

# Load database credentials from .env (you already did this above)
user     = dotenv.get_key('.env', 'DB_USER') or 'postgres'
password = dotenv.get_key('.env', 'DB_PASSWORD')
host     = dotenv.get_key('.env', 'DB_HOST')     or "localhost"
dbname   = dotenv.get_key('.env', 'DB_NAME')     or "berlin_spatial"
port     = dotenv.get_key('.env', 'DB_PORT')     or "5432"

DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

engine = create_engine(DB_URL)
datasets = [
    ("https://tsb-opendata.s3.eu-central-1.amazonaws.com/bezirksgrenzen/bezirksgrenzen.geojson", "berlin_bezirke"),
    ("https://tsb-opendata.s3.eu-central-1.amazonaws.com/plz/plz.geojson", "berlin_plz"),
    ("https://tsb-opendata.s3.eu-central-1.amazonaws.com/verkehrszellen/verkehrszellen.geojson", "berlin_traffic_cells"),
    ("WFS:https://gdi.berlin.de/services/wfs/ua_einwohnerdichte_2023", "berlin_population_density"),
    ("WFS:https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_brw_2024", "berlin_land_values"),

]

for url, table_name in datasets:
    print(f"Importing {table_name}...")
    try:
        gdf = gpd.read_file(url)
        gdf.to_postgis(table_name, engine, if_exists='replace')
        print(f"  ✓ {len(gdf)} features")
    except Exception as e:
        print(f"  ✗ Error: {e}")

print("Done!")