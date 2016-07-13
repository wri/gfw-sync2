### Prepare GLAD Data for Analysis API
### Should download latest data from S3 and copy to R Drive
### Should recalculate statistics on mosaic rasters in glad_alerts_analysis.gdb
### Test API Calls: https://basecamp.com/3063126/projects/10726211/todos/250035816
### TODO: Need to restart the service

import urllib
import os
import arcpy
import datetime

#specify urls to GLAD Alerts in S3
urls = [
r"http://umd-landsat-alerts.s3.amazonaws.com/roc_day2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/peru_day2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/borneo_day2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/brazil_day2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/roc_conf2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/peru_conf2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/borneo_conf2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/brazil_conf2016.tif",
]

#download files and save to R:/glad_alerts
for url in urls:
	file_name = url.split("/")[-1]
	path_var = os.path.join (r"R:\glad_alerts", file_name)
	urllib.urlretrieve(url,path_var)
	print "downloaded"

#list path to the new raster data
files = [
r"R:\glad_alerts\roc_day2016.tif",
r"R:\glad_alerts\peru_day2016.tif",
r"R:\glad_alerts\borneo_day2016.tif",
r"R:\glad_alerts\brazil_day2016.tif",
r"R:\glad_alerts\roc_conf2016.tif",
r"R:\glad_alerts\peru_conf2016.tif",
r"R:\glad_alerts\borneo_conf2016.tif",
r"R:\glad_alerts\brazil_conf2016.tif",
]

for file in files:
    arcpy.CalculateStatistics_management(file, "1", "1", "", "OVERWRITE", "")
    print "stats calculated on file"

#Calculate statistics on mosaics
mosaics = [
r"R:\glad_alerts\glad_alerts_analysis.gdb\glad2016",
r"R:\glad_alerts\glad_alerts_analysis.gdb\glad_con",
]

for mosaic in mosaics:
    arcpy.CalculateStatistics_management(mosaic, "1", "1", "", "OVERWRITE", "")
    print "stats calculated on mosaic"

# Restart image Services
# http://gis-gfw.wri.org/arcgis/rest/services/image_services/glad_alerts_analysis/ImageServer
# http://gis-gfw.wri.org/arcgis/rest/services/image_services/glad_alerts_con_analysis/ImageServer

cwd = r"C:\Program Files\ArcGIS\Server\tools\admin"
cmd = ['python', "manageservice.py"]
username = 'astrong'
auth_key = util.get_token('arcgis_server_pass')
cmd += ['-u', username, '-p', auth_key, '-s', 'http://gis-gfw.wri.org/arcgis/admin', '-n', 'image_services/glad_alerts_analysis', '-o', 'start']
subprocess.call(cmd, cwd=cwd)
print "service restarted"
