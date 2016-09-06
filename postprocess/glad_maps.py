import arcpy
import numpy
from arcpy import env
from arcpy.sa import *
import urllib
import os
import time
import datetime
from os import walk

#Check out ArcGIS Extensions
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = r"D:\temp\environment"

rasters = [
r"http://umd-landsat-alerts.s3.amazonaws.com/borneo_day2016.tif",
r"http://umd-landsat-alerts.s3.amazonaws.com/peru_day2016.tif",
# r"http://umd-landsat-alerts.s3.amazonaws.com/roc_day2016.tif",
# r"http://umd-landsat-alerts.s3.amazonaws.com/brazil_day2016.tif"
]

new_rasters = [
r"D:\GIS Data\GFW\glad\raster\borneo_day2016.tif",
r"D:\GIS Data\GFW\glad\raster\peru_day2016.tif",
# r"D:\GIS Data\GFW\glad\raster\roc_day2016.tif",
# r"D:\GIS Data\GFW\glad\raster\brazil_day2016.tif"
]

#paths to past points for analysis
borneo_past = r"D:\GIS Data\GFW\glad\past_points\borneo_past.shp"
peru_past = r"D:\GIS Data\GFW\glad\past_points\peru_past.shp"
roc_past = r"D:\GIS Data\GFW\glad\past_points\roc_past.shp"
brazil_past = r"D:\GIS Data\GFW\glad\past_points\brazil_past.shp"

#Paths to latest points
borneo_latest = r"D:\GIS Data\GFW\glad\latest_points\borneo_latest.shp"
peru_latest = r"D:\GIS Data\GFW\glad\latest_points\peru_latest.shp"
roc_latest = r"D:\GIS Data\GFW\glad\latest_points\roc_latest.shp"
brazil_latest = r"D:\GIS Data\GFW\glad\latest_points\brazil_latest.shp"

#Grab max values from past rasters
borneo_max = arcpy.SearchCursor(borneo_past, "", "", "","GRID_CODE D").next().getValue("GRID_CODE")
peru_max = arcpy.SearchCursor(peru_past, "", "", "","GRID_CODE D").next().getValue("GRID_CODE")
roc_max = arcpy.SearchCursor(roc_past, "", "", "","GRID_CODE D").next().getValue("GRID_CODE")
brazil_max = arcpy.SearchCursor(brazil_past, "", "", "","GRID_CODE D").next().getValue("GRID_CODE")

#set global variables for maps
borneo_mxd = arcpy.mapping.MapDocument(r"D:\GIS Data\GFW\glad\maps\mxds\borneo.mxd")
peru_mxd = arcpy.mapping.MapDocument(r"D:\GIS Data\GFW\glad\maps\mxds\peru.mxd")
roc_mxd = arcpy.mapping.MapDocument(r"D:\GIS Data\GFW\glad\maps\mxds\roc.mxd")
brazil_mxd = arcpy.mapping.MapDocument(r"D:\GIS Data\GFW\glad\maps\mxds\brazil.mxd")
density = r"D:\GIS Data\GFW\glad\maps\density_rasters\borneo_density.tif"

def convert_rasters():

    for raster in rasters:
        file_name = raster.split("/")[-1]
        print "downloading %s and calculating stats" %(file_name)
        path_var = os.path.join (r"D:\GIS Data\GFW\glad\raster", file_name)
        urllib.urlretrieve(url,path_var)
        arcpy.CalculateStatistics_management(raster)


    for new_raster in new_rasters:
        file_name = new_raster.split("/")[-1]
        prefix = file_name.split("_")[0] + "_max"
        latest = file_name.split("_")[0] + "_latest.tif"
        print "extracting values for %s" %(file_name)
        where_clause = "Value > " + str(prefix)
        raster_extract = ExtractByAttributes(new_raster, where_clause)
        raster_extract.save(os.path.join (r"D:\GIS Data\GFW\glad\latest_points", latest))

    files = []
    my_path = r"D:\GIS Data\GFW\glad\latest_points"
    for (dirpath, dirnames, filenames) in walk(my_path):
        files.extend(filenames)
        break

    file_paths = []
    for file in files:
        f = os.path.join(my_path, file)
        file_paths.append(f)

    for path in file_paths:
        n = path.split("\\")[-1]
        name = n.split("_")[0]
        output = os.path.join(my_path, name + ".shp")
        if "latest" in path:
            arcpy.RasterToPoint_conversion(file, output, "Value")


#Archive
def archive(point):
    print "archiving %s" %(point)
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d')
    file_name = point.split("//")[-1].replace(".shp","")
    path_var = os.path.join (r"D:\GIS Data\GFW\glad\archive", file_name + "_" + timestamp + ".shp")
    arcpy.Copy_management(point, path_var)


#Replace old points
def replace(past_point, latest_point):
    print "replacing past points"
    arcpy.Copy_management(latest_point, past_point)

#Run kernel density analysis
def kernel(latest_point):
    print "running density analysis for %s" %(latest_point)
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d')
    outKDens = KernelDensity(latest_point, "NONE", "", "", "HECTARES")
    path = r"D:\GIS Data\GFW\glad\maps\density_rasters"
    n = latest_point.split("\\")[-1]
    name = n.split("_")[0]
    output = os.path.join(path, name + "_density.tif")
    outKDens.save(output)

#find extent of GADM boundary, clip data, and add to mxd file
def select(mxd):

    #set variables
    gadm = r"D:\GIS Data\Country Boundaries\country boundaries.gdb\countries"

    if mxd == borneo_mxd:
        ISO = "'BRN'"
        name = "borneo"
    if mxd == peru_mxd:
        ISO = "PER"
        name = "peru"
    if mxd == brazil_mxd:
        ISO = "BRA"
        name = "brazil"
    if mxd == roc_mxd:
        ISO = "COG"
        name = "roc"


    arcpy.MakeFeatureLayer_management(gadm,"lyr")
    SQL_query = " code_iso3 = "+ISO
    gadm_path = r"D:\GIS Data\GFW\glad\maps\mxds"
    gadm_output = os.path.join(gadm_path, name + "_clip.shp")
    selection = arcpy.Select_analysis("lyr",gadm_output, SQL_query)
    rows = arcpy.SearchCursor(selection)
    extent = 0
    shapeName = arcpy.Describe(gadm).shapeFieldName
    for row in rows:
        feat = row.getValue(shapeName)
        extent = feat.extent
    Xmin= str(extent.XMin)
    Ymin= str(extent.YMin)
    Xmax= str(extent.XMax)
    Ymax= str(extent.YMax)
    rectangle = Xmin + " " + Ymin + " " + Xmax + " " + Ymax
    print "clipping density layer to GADM boundary"
    clip_path = r"D:\GIS Data\GFW\glad\maps\mxds"
    clip_output = os.path.join(clip_path, name + "_clip.tif")
    density_clip = arcpy.Clip_management(density, rectangle, clip_output, selection, "#", "ClippingGeometry")
    # clip_result = density_clip.getOutput(0)
    print "adding data to mxd file and setting extent"
    df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
    add_results = arcpy.mapping.Layer(clip_output)
    add_gadm = arcpy.mapping.Layer("lyr")
    arcpy.mapping.AddLayer(df, add_gadm, "TOP")
    arcpy.mapping.AddLayer(df, add_results, "TOP")
    gadm_lyr = arcpy.mapping.ListLayers(mxd, "lyr", df)[0]
    arcpy.SelectLayerByAttribute_management(gadm_lyr,"NEW_SELECTION", SQL_query)
    df.zoomToSelectedFeatures()
    arcpy.RefreshActiveView()
    arcpy.mapping.RemoveLayer(df,gadm_lyr)

#apply symbology and export
def export():

    #set variables
    if mxd == borneo_mxd:
        ISO = "'BRN'"
        name = "IDN13"
    if mxd == peru_mxd:
        ISO = "PER"
        name = "PER"
    if mxd == brazil_mxd:
        ISO = "BRA"
        name = "BRA"
    if mxd == roc_mxd:
        ISO = "COG"
        name = "COG"

    print "adding symbology and exporting"
    ts = time.time()
    time_year = datetime.datetime.fromtimestamp(ts).strftime("%Y")
    time_week = datetime.datetime.fromtimestamp(ts).strftime("%W")
    density_lyr = arcpy.mapping.ListLayers(mxd)[0]
    Symbology = r"D:\GIS Data\GFW\glad\maps\mxds\color.lyr"
    arcpy.ApplySymbologyFromLayer_management(density_lyr, Symbology)
    arcpy.RefreshActiveView()
    output = os.path.join(r"F:\climate\glad_maps", name + "_" + time_year + "_" + time_week + ".png")
    arcpy.mapping.ExportToPNG(mxd, output)
    print "map created"

convert_rasters()
archive_point(borneo_past)
# archive_point(roc_past)
archive_point(peru_past)
# archive_point(brazil_past)
replace(borneo_past, borneo_latest)
# replace(roc_past, roc_latest)
replace(peru_past, peru_latest)
# replace(brazil_past, brazil_latest)
kernel(borneo_latest)
# kernel(roc_latest)
kernel(peru_latest)
# kernel(brazil_latest)
select(borneo_mxd)
# select(roc_mxd)
select(peru_mxd)
# select(brazil_mxd)
export()
