[![Code Issues](https://www.quantifiedcode.com/api/v1/project/b5cf4925d48f4bfc8a7b24ed9dbc87c1/badge.svg)](https://www.quantifiedcode.com/app/project/b5cf4925d48f4bfc8a7b24ed9dbc87c1)

# gfw-sync2

This is a suite of tools used to synchronize data for all websites in the GFW platform, including but not limited to the [GFW Flagship](globalforestwatch.org/map), [GFW Commodities](http://commodities.globalforestwatch.org), [the Open Data Portal](http://data.globalforestwatch.org) and various [CartoDB](https://wri-01.cartodb.com/me) and [ArcGIS Server](http://gis-gfw.wri.org/arcgis/rest/) endpoints.

## Updating a Layer
The data update process is driven by `layers`. Each `layer` has configuration options defined in the [gfw-sync2 config table](https://docs.google.com/spreadsheets/d/1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI/edit#gid=0). When we have new source data for a layer (i.e. tiger conservation landscapes), we can update it across the platform by running: \

`python gfw-sync2 -e PROD -l tiger_conservation_landscapes`

This will take the options defined on the `PROD` tab of the `config table` and process the layer specified. The script will use the `config table` to do things like copy the data locally, apply a fieldmap, add a country code and then append it to various esri and CartoDB tables.

## Global Datasets
In addition to processing input country datasets, this process will also update associated global datasets. Whenever a dataset of type `country_vector` is updated, the layer specified in the `global_layer` field will also be updated-- deleting the previous records for that country dataset, and appending the new data.

## Automatic Updates
Other info: layers can be set to update automatically based on the `update_days` field. A nightly cronjob on the data management server (running `utilities\cronjob.cmd`) will compare today's date to the value in `update_days` to determine if the layer should be updated. Logs for these processes (and all updates) are written to the `\logs` dir (not included in this repo). 

## Config Table Fields
Attribute | Description
--- | ---
tech_title | Layer title
type | Must match the options defined in `layer_decision_tree.py`
add_country_value | ISO country code, required for `country_vector` layers
source | Path to the source dataset
transformation | Any transformations that need to be applied to the source
delete_features_input_where_clause | A where clause filter features from the source
esri_merge_where_field | Will generate a list of values for a field (i.e. field: country, value: PER) in the source table and delete all records in `esri_service_output` with that value, then append the source. If nothing specified, will truncate `esri_service_output` and then append
esri_service_output | esri output to append the source to
cartodb_merge_where_field | same as `esri_merge_where_field` above, but for cartoDB
cartodb_service_output | cartoDB output to append to
archive_output | path to the output archive ZIP created
download_output | path to the download ZIP created
field_map | A .ini file used to map fields from source to outputs
tile_cache_output | location for storage of tile cache generated
update_days | numeric days of the month to check for updates. Can be `[1-10]` (run on all days 1-10) or `[1,5,10]`, (run on the 1st, 5th, and 10th of each month).
global_layer | If this dataset is part of global layer, specify it's `tech_title` here
last_updated | Automatically updated by the script when a layer is updated
