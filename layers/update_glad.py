import urllib
import os
import arcpy

# Check out ArcGIS Extensions
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True

# specify S3 urls to the umd alert data
urls = [
    r"http://umd-landsat-alerts.s3.amazonaws.com/roc_day2016.tif",
    r"http://umd-landsat-alerts.s3.amazonaws.com/peru_day2016.tif",
    r"http://umd-landsat-alerts.s3.amazonaws.com/borneo_day2016.tif",
]

print "downloading files"
for url in urls:
    file_name = url.split("/")[-1]
    path_var = os.path.join(r"R:\glad_alerts", file_name)
    urllib.urlretrieve(url, path_var)
    print "downloaded"

files = [
    r"R:\glad_alerts\roc_day2016.tif",
    r"R:\glad_alerts\peru_day2016.tif",
    r"R:\glad_alerts\peru_day2016.tif",
]

print "calculating stats on rasters"
for file in files:
    arcpy.CalculateStatistics_management(file)
    print "statistics calculated on rasters"

print "calculating stats on 2016 mosaic"
mosaic = r"R:\glad_alerts\glad_alerts.gdb/glad2016"
arcpy.CalculateStatistics_management(mosaic)

print "generating overviews"
arcpy.env.workspace = r"R:\glad_alerts"
arcpy.RemoveRastersFromMosaicDataset_management("glad_alerts.gdb/glad2016", "OBJECTID>3", "UPDATE_BOUNDARY",
                                                "MARK_OVERVIEW_ITEMS", "#", "#", "#", "#")
print "previous overviews removed"
arcpy.BuildOverviews_management("glad_alerts.gdb/glad2016", "", "NO_DEFINE_MISSING_TILES", "GENERATE_OVERVIEWS", "#",
                                "#")
print "new overviews generated"
