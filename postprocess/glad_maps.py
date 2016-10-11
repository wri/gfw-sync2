import arcpy
import numpy
from arcpy import env
from arcpy.sa import *
import urllib
import os
import time
import datetime
from os import walk
import sys
import logging
import subprocess

#Check out ArcGIS Extensions
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True

borneo_mxd = arcpy.mapping.MapDocument(r"D:\GIS Data\GFW\glad\maps\mxds\borneo.mxd")
peru_mxd = arcpy.mapping.MapDocument(r"D:\GIS Data\GFW\glad\maps\mxds\peru.mxd")
roc_mxd = arcpy.mapping.MapDocument(r"D:\GIS Data\GFW\glad\maps\mxds\roc.mxd")
brazil_mxd = arcpy.mapping.MapDocument(r"D:\GIS Data\GFW\glad\maps\mxds\brazil.mxd")


def post_process(layerdef):
    """
    Create density maps for GFW Climate Visualization
    :param layerdef: the layerdef
    """
    logging.debug('starting postprocess glad maps')
    print layerdef.source

    olddata_hash = {}
    #replace path with D:\GIS Data\GFW\glad\past_points
    past_points = [
    r"D:\GIS Data\GFW\glad\past_points\borneo_day2016.shp",
    r"D:\GIS Data\GFW\glad\past_points\peru_day2016.shp",
    r"D:\GIS Data\GFW\glad\past_points\roc_day2016.shp",
    r"D:\GIS Data\GFW\glad\past_points\brazil_day2016.shp"
    ]

    latest_rasters = [
    r"D:\GIS Data\GFW\glad\latest_points\borneo_day2016.tif",
    r"D:\GIS Data\GFW\glad\latest_points\peru_day2016.tif",
    r"D:\GIS Data\GFW\glad\latest_points\brazil_day2016.tif",
    r"D:\GIS Data\GFW\glad\latest_points\roc_day2016.tif"
    ]
    new_points = []

    for point in past_points:
        point_name = os.path.basename(point)
        olddata_hash[point_name] = arcpy.SearchCursor(point, "", "", "","GRID_CODE D").next().getValue("GRID_CODE")

    for ras in layerdef.source:
        if "day" in ras:
            ras_name = os.path.basename(ras)
            shp_name = ras_name.replace(".tif", ".shp")
            where_clause = "Value > " + str(olddata_hash[shp_name])
            raster_extract = ExtractByAttributes(ras, where_clause)
            latest_raster = raster_extract.save(os.path.join (r"D:\GIS Data\GFW\glad\latest_points", ras_name))
            # latest_rasters.append(latest_raster)
            print "new values for %s extracted" %(ras_name)
        else:
            pass

    #failed here
    for ras in latest_rasters:
        ras_name = os.path.basename(ras).replace(".tif", ".shp")
        output = os.path.join(os.path.dirname(ras), ras_name)
        new_point = arcpy.RasterToPoint_conversion(ras, output, "Value")
        new_points.append(output)
        print "converted %s to points" %(ras)

    for newp in new_points:
        for pastp in past_points:
            if os.path.basename(newp) == os.path.basename(pastp):
                arcpy.Copy_management(newp, pastp)
                print "copied %s to %s" %(newp, pastp)

    for newp in new_points:
        outKDens = KernelDensity(newp, "NONE", "", "", "HECTARES")
        path = r"D:\GIS Data\GFW\glad\maps\density_rasters"
        name = os.path.basename(newp).replace(".shp", "")
        output = os.path.join(path, name + "_density.tif")
        outKDens.save(output)

    for layer in layerdef.source:
        if "peru" in layerdef.source:
            make_maps(peru_mxd)
        if "roc" in layerdef.source:
            make_maps(roc_mxd)
        if "brazil" in layerdef.source:
            make_maps(brazil_mxd)
        if "borneo" in layerdef.source:
            make_maps(borneo_mxd)
        else:
            pass

    #start country page analysis stuff (not map related)
    cmd = ['python', 'update_country_stats.py', '-d', 'umd_landsat_alerts', '-a', 'gadm1_boundary', '--emissions']
    cwd = r'D:\scripts\gfw-country-pages-analysis'

    if layerdef.gfw_env == 'DEV':
        cmd.append('--test')

    subprocess.check_call(cmd, cwd=cwd)

def make_maps(mxd):

    logging.debug('starting postprocess make maps')

    gadm = r"D:\GIS Data\Country Boundaries\country boundaries.gdb\countries"
    global ISO
    global name
    global density

    if mxd == borneo_mxd:
        ISO = "'BRN'"
        name = "IDN13"
        density = r"D:\GIS Data\GFW\glad\maps\density_rasters\borneo_day2016_density.tif"
    if mxd == peru_mxd:
        ISO = "'PER'"
        name = "PER"
        density = r"D:\GIS Data\GFW\glad\maps\density_rasters\peru_day2016_density.tif"
    if mxd == brazil_mxd:
        ISO = "'BRA'"
        name = "BRA"
        density = r"D:\GIS Data\GFW\glad\maps\density_rasters\brazil_day2016_density.tif"
    if mxd == roc_mxd:
        ISO = "'COG'"
        name = "COG"
        density = r"D:\GIS Data\GFW\glad\maps\density_rasters\roc_day2016_density.tif"

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

    #export
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
