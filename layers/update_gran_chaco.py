### Update Gran Chaco Deforestation for map services and ODP

import urllib
import os
import arcpy
import datetime
import zipfile

def download_chaco():
    url = r"http://gfw2-data.s3.amazonaws.com/forest_change/guyra_deforestation/zip/gran_chaco_deforestation.zip"
    file_name = url.split("/")[-1]
    path_var = os.path.join (r"D:\GIS Data\GFW\temp", file_name)
    urllib.urlretrieve(url,path_var)
    print "downloaded"
    zip = zipfile.ZipFile(r"D:\GIS Data\GFW\temp\gran_chaco_deforestation.zip")
    zip.extractall(r"D:\GIS Data\GFW\temp\gran_chaco_deforestation")
    print "unzipped"
    delete_append()

def delete_append():
    print "deleting old data"
    arcpy.env.workspace = r"C:\Users\astrong\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\gfw (gfw@localhost).sde"
    fc ='gfw.gfw.gran_chaco_deforestation'
    arcpy.DeleteRows_management(fc)
    print "appending new data"
    file = r"D:\GIS Data\GFW\temp\gran_chaco_deforestation\gran_chaco_deforestation.shp"
    arcpy.Append_management (file, fc, "TEST", "", "")
    print "new data appended to FC"

download_chaco()
