import os
import warnings
import geopandas as gpd
from sqlalchemy import create_engine
import dotenv
from dotenv import load_dotenv
load_dotenv()
# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# Default path for your PLZ GeoJSON file
DEFAULT_PATH = r"PLZ_Gebiete_2313071530551189147.geojson"

# Load database credentials from .env (you already did this above)
user     = dotenv.get_key('.env', 'DB_USER') or 'postgres'
password = dotenv.get_key('.env', 'DB_PASSWORD') 
host     = dotenv.get_key('.env', 'DB_HOST')     or "localhost"
dbname   = dotenv.get_key('.env', 'DB_NAME')     or "berlin_spatial"
port     = dotenv.get_key('.env', 'DB_PORT')     or "5432"

DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

engine = create_engine(DB_URL)

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# LOAD GEOJSON
# ─────────────────────────────────────────────────────────────────────────────

geojson_file = DEFAULT_PATH
if not os.path.exists(geojson_file):
    raise FileNotFoundError(f"GeoJSON file not found: {geojson_file}")

print(f"Loading GeoJSON from: {geojson_file} ...")
plz_gdf = gpd.read_file(geojson_file)

print(f"Loaded {len(plz_gdf):,} features")
print(plz_gdf.columns)

# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZE DATA
# ─────────────────────────────────────────────────────────────────────────────

# Try to find sensible column names for PLZ and population
possible_plz_cols = ['plz', 'PLZ', 'postalcode', 'postal_code']
possible_pop_cols = ['einwohner', 'population', 'Einwohner', 'POPULATION']

plz_col = next((c for c in possible_plz_cols if c in plz_gdf.columns), None)
pop_col = next((c for c in possible_pop_cols if c in plz_gdf.columns), None)

if plz_col is None:
    raise ValueError("Could not find a PLZ column in the GeoJSON")

print(f"Using PLZ column: {plz_col}")
print(f"Using population column: {pop_col}")

# Normalize PLZ and population
plz_gdf['plz'] = plz_gdf[plz_col].astype(str).str.zfill(5)
if pop_col:
    plz_gdf['einwohner'] = plz_gdf[pop_col].fillna(0).astype(int)
else:
    plz_gdf['einwohner'] = None

# Ensure correct CRS (GeoJSON likely in EPSG:4326)
plz_gdf = plz_gdf.to_crs(epsg=4326)

# ─────────────────────────────────────────────────────────────────────────────
# WRITE TO POSTGIS
# ─────────────────────────────────────────────────────────────────────────────

table_name = "germany_plz_with_pop"
print(f"Writing to PostGIS table: {table_name} ...")

plz_gdf.to_postgis(
    table_name,
    con=engine,
    if_exists="replace",
    index=False
)

print("Done! Table has been created in the database.")
print("You can now run SQL queries such as:")
print(f"SELECT plz, einwohner FROM {table_name} LIMIT 10;")

