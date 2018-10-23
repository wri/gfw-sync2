# Notes on WDPA data updates

Everyone's favorite! WDPA data management!

### Data Prep
At one point this was all automated in ArcGIS, but given the frequency of the data update process (monthly) and how often our ArcGIS server is updated (yearly), I prefer to handle this manually on a large (m4.10xlarge) spot machine.

1. Download the data locally and convert to postgis:  `ogr2ogr -f Postgresql PG:"dbname=ubuntu" -t_srs PROJCS["World_Eckert_VI",GEOGCS["GCS_WGS_1984",DATUM["WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Eckert_VI"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["Central_Meridian",0],UNIT["Meter",1],AUTHORITY["EPSG","54010"]] WDPA_Oct2018_Public/WDPA_Oct2018_Public.gdb WDPA_poly_Oct2018 -nln wdpa -lco GEOMETRY_NAME=geom  `
2. Simplify geometry: `UPDATE wdpa SET geom = ST_Multi(ST_SimplifyPreserveTopology(geom, 10));`
3. Make valid: `UPDATE wdpa SET geom = ST_CollectionExtract(ST_MakeValid(geom), 3) WHERE ST_IsValid(geom) <> '1';`
4.  Export: `ogr2ogr wdpa_simplified.shp PG:"dbname=ubuntu" -sql "SELECT * FROM wdpa"`
5. Zip and upload to s3, then download to the DM server
6. Unzip and set this as self.data_source in `wdpa_datasource.py`

### Fun temporary modifications to existing code

1. Comment out the repair_geometry step in `vector_layer.py` - not necessary as we've already handled this in PostGIS . . . plus we'll likely get a memory error if we run it here.
2. Delete two fields from our the SDE database copy of this feature class on the DM server localhost connection:
	- wdpa_pid
	- iso3
3. Add those fields back in, with wdpa_pid as type string, length 30, and iso3 as string 100. This handles a schema change in the WDPA data.
4. The alternative to this monthly workaround is recreating the GDB replica- something that's definitely possible but not my area of expertise

### Run raster-to-vector process locally

Given that much of this data points to live services, it can be difficult to automate these updates. We'll copy some stuff locally first, then stop services before we update the data.

1. copy this (and all accessory files) to the desktop
	- "P:\data\rasters\image_services\analysis\wdpa_world_eck.tif"
2. Set the `vector_to_raster_output` field to point to that desktop location in the gfw-sync2 google config sheet

### Run it!

`python gfw-sync.py -e prod -l wdpa`

### Update tile cache

Part of this process generates cached tiles for zooms 0 to 6. We do this locally on the DM server so as not to generate load on the production machine. gfw-sync will print the source and destination location of the tiles as part of this process- check the command window to find these and copy them over.

### Update analysis services

When gfw-sync finishes, stop the service here:
GIS Servers\arcgis on gis-gfw.wri.org (admin)\image_services\analysis.ImageServer

Then copy the wdpa_world_eck.tif on the desktop (and all accessory files) here:
`P:\data\rasters\image_services\analysis`
`P:\data\rasters\image_services\analysis\analysis_layers`

Then restart the service and check that it's responding properly.

### Further adjustments before syncing

Before we sync the data to the esri production server, we need to set those fields back.

1. Delete the wdpa_pid field, then re add it as type double. Fine to leave this field as blank- we don't actually use this column
2. Find the iso3 record that is too long for our schema
	- Add the WDPA data on the localhost SDE to ArcMap
	- Start editing the feature class
	- run a select by attribute `WHERE CHAR_LENGTH("iso3") > 20`
	- Edit that one record to that iso3 is < 20 characters
	- Save edits
3. Sync the database! Right click on the database itself in ArcCatalog, then choose `Distributed Geodatabase` --> `Synchronize Changes`

### Update gfw-api metadata

1. update this doc: [https://docs.google.com/spreadsheets/d/1hJ48cMrADMEJ67L5hTQbT5hhV20YCJHpN1NwjXiC3pI/edit#gid=1027496669](https://docs.google.com/spreadsheets/d/1hJ48cMrADMEJ67L5hTQbT5hhV20YCJHpN1NwjXiC3pI/edit#gid=1027496669) in both `citation` and `frequency_of_update` fields

2. rebuild the metadata cache: [http://gis-gfw.wri.org/metadata/rebuild_cache](http://gis-gfw.wri.org/metadata/rebuild_cache)

3. then make sure those updates worked: [http://gis-gfw.wri.org/metadata/wdpa_protected_areas](http://gis-gfw.wri.org/metadata/wdpa_protected_areas)


