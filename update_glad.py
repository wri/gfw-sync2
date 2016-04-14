import urllib
import os
import arcpy
from arcpy import env
from arcpy.sa import *

#Check out ArcGIS Extensions
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True

#specify S3 urls to the umd alert data
urls = [
r"http://umd-landsat-alerts.s3.amazonaws.com/roc_day2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/peru_day2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/borneo_day2016.tif",
]

print "downloading files"
for url in urls:
	file_name = url.split("/")[-1]
	path_var = os.path.join (r"R:\glad_alerts", file_name)
	urllib.urlretrieve(url,path_var)
	print "downloaded"

files = [
r"R:\glad_alerts\roc_day2016.tif",
r"R:\glad_alerts\peru_day2016.tif",
r"R:\glad_alerts\peru_day2016.tif",
]

print "calculating stats"
for file in files:
    arcpy.CalculateStatistics_management(file)
    print "statistics calculated"

mosaics = [
r"R:\glad_alerts\glad_alerts.gdb\glad2015",
r"R:\glad_alerts\glad_alerts.gdb\glad2016",
]

print "building overviews"
for mosaic in mosaics:
	arcpy.BuildOverviews_management(mosaic, "", "NO_DEFINE_MISSING_TILES", "", "", "")
	print "overviews generated"
