# 🗺 Berlin Spatial Database Setup

This documentation guides you through the creation and population of the `berlin_spatial` PostgreSQL/PostGIS database using OSM and other geospatial datasets.

---

## 1️⃣ Create an Empty Spatial Database

Create a new PostgreSQL database and enable PostGIS extensions:

【sql】
-- Create database
CREATE DATABASE berlin_spatial;

-- Connect to the database
\c berlin_spatial

-- Enable PostGIS and related extensions
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
CREATE EXTENSION hstore;
【/sql】

---

## 2️⃣ Install OSM Import Tools

1. Download osm2pgsql for Windows:  
[https://osm2pgsql.org/download/windows/](https://osm2pgsql.org/download/windows/)

2. Extract the zip:

【powershell】
Expand-Archive -Path "C:\Users\ismyn\Downloads\osm2pgsql-2.2.0-x64.zip" -DestinationPath "C:\tools" -Force
$env:PATH += ";C:\tools\osm2pgsql-bin"
【/powershell】

3. Verify installation:

【powershell】
osm2pgsql --version
【/powershell】

---

## 3️⃣ Import Brandenburg OSM Data

1. Download the PBF file:  
[https://download.geofabrik.de/europe/germany/brandenburg-251214.osm.pbf](https://download.geofabrik.de/europe/germany/brandenburg-251214.osm.pbf)

2. Import it into the database:

【powershell】
osm2pgsql -d berlin_spatial -H localhost -U postgres --create --slim -G --hstore -S "C:\tools\osm2pgsql-bin\default.style" "C:\Users\ismyn\Downloads\brandenburg-251214.osm.pbf"
【/powershell】

---

## 4️⃣ Install GDAL

1. Download GDAL MSI installer:  
[https://www.gisinternals.com/query.html?content=filelist&file=release-1930-x64-gdal-3-11-3-mapserver-8-4-0.zip](https://www.gisinternals.com/query.html?content=filelist&file=release-1930-x64-gdal-3-11-3-mapserver-8-4-0.zip)

2. Add PostgreSQL binaries to PATH:

【powershell】
$env:PATH += ";C:\Program Files\PostgreSQL\18\bin"
【/powershell】

3. Test GDAL:

【powershell】
ogr2ogr --version
【/powershell】

---

## 5️⃣ Download Additional Datasets

- **German PLZ polygons and population**:  
[https://hub.arcgis.com/datasets/esri-de-content%3A%3Apostleitzahlengebiete-in-deutschland/about](https://hub.arcgis.com/datasets/esri-de-content%3A%3Apostleitzahlengebiete-in-deutschland/about)

- **Berlin-specific datasets**:  
  - Bezirke  
  - Verkehrszellen  
  - Einwohnerdichte  
  - Land values

- **Brandenburg land prices**: BORIS WFS (official portal)

---

## 6️⃣ Install Python Dependencies

【bash】
pip install geopandas sqlalchemy psycopg2-binary geoalchemy2
【/bash】

---

## 7️⃣ Configure `.env` File

Create a `.env` in your project directory with your database credentials:

【text】
DB_USER=postgres
DB_PASSWORD=YourPasswordHere
DB_HOST=localhost
DB_NAME=berlin_spatial
DB_PORT=5432
【/text】

---

## 8️⃣ Run Python Scripts

Run the scripts in the following order to populate the database:

【powershell】
python download_geodata_berlin.py
python download_plz_population.py
【/powershell】

- Scripts read `.env` credentials.  
- Outputs are saved as GeoJSON / GeoPackage if database writes fail.

---

## ✅ Notes

- Ensure PostGIS is correctly installed.  
- For large OSM imports, sufficient RAM and disk space are required.  
- Use `psql` or `pgAdmin` to inspect the tables.

---

📝 **Author**: Your Name  
📅 **Date**: 2025-12-16

* the zipped spatial db file can be found at: https://drive.google.com/file/d/1OGGfaZlL91rd5co9gMHhC2yBpsTwOpe9/view?usp=drive_link