''' author: Sam Gibbes and Asa Strong
date: 2/2016
'''
import arcpy
import datetime, time
import urllib
import os
from datetime import date
import glob
from settings import *
from s3_vector_layer import S3VectorLayer

#enable extentions and overwrite permissions
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True

#Download the data from stable url and add to R Drive
def download_data():
    print "downloading data"
    url = r"http://www.terra-i.org/data/current/raster/latin_decrease_current.tif"
    file_name = url.split("/")[-1]
    path = "R:\\"
    path_var = os.path.join(path, file_name)
    urllib.urlretrieve(url,path_var)
    print "Terra_I Downloaded to R drive"
    build_table()

#Build attribute table/add field/calculate field
def build_table():
    print "building attribute table"
    raster = "R:\\latin_decrease_current.tif"
    arcpy.BuildRasterAttributeTable_management(raster, "Overwrite")
    arcpy.AddField_management(raster, "date", "DATE")
    print "attribute table built"
    calculate_date()

def calculate_date():
    print "calculating dates"
    raster = "R://latin_decrease_current.tif"
    fields = ['Value','date']
    with arcpy.da.UpdateCursor(raster,fields) as cursor:
        for row in cursor:
            gridcode = row[0]
            year = 2004+int((gridcode)/23)
            year_format = datetime.datetime.strptime(str(year) +"/01/01",'%Y/%m/%d')
            days = datetime.timedelta(days=(gridcode%23)*16)
            date_formatted= (year_format+days).strftime('%m/%d/%Y')
            row[1]=date_formatted
            cursor.updateRow(row)
            print "dates calculated"
            export_shp()

#export data to shp in S3
def export_shp():
    print "sending points to S3"
    input = "R://latin_decrease_current.tif"
    output = "F://forest_change//terra_i_alerts//terra_i.shp"
    arcpy.RasterToPoint_conversion(input, output, "date")
    print "points uploaded to S3"

#Use class method to zip file in s3
def zip_file
zip = S3VectorLayer()
zip.src_path("D:\\forest_change\\terra_i_alerts")

#call function 1 through 3
download_data()

################################################################################

#optional function to Reproject the data to equal area in the R:/analysis folder
'''def reproject_data():
    print "reprojecting data"
    input = "R:\\latin_decrease_current.tif"
    output = "R:\\analysis\\terra_i.tif"
    prj = "R:\\analysis\\terra_i\\prj.adf"
    arcpy.ProjectRaster_management(input, output, prj)
    print "Terra_I Reproject to R:/analysis folder"

#Create zipped, unzipped, and archived raster in s3 for visualization.

def zip_raster(rst, dst):
    basepath, fname, base_fname = gen_paths(rst)
    # zip_name = timestamp + "_"+ base_fname + ".zip"
    zip_name = base_fname + ".zip"
    zip_path = os.path.join(dst, zip_name)
    zf = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True)

    bname = os.path.basename(rst)
    if (base_fname in bname) and (bname != zip_name):
        add_to_zip(rst, zf)
    zf.close()

destination_latest_raster = r'F:\forest_change\terra_i_alerts\blank'
zip_folder = r'F:\forest_change\terra_i_alerts\zip'
archive_directory = r'F:\forest_change\terra_i_alerts\archive'

timestamp =  date.fromtimestamp(time.time()).strftime("%m%d%y")
basename = "latest_raster.tif"


latest_raster = os.path.join(destination_latest_raster,basename)
existing = glob.glob(os.path.join(destination_latest_raster,basename + "*"))
print existing
if len(existing) == 1:
    for i in existing:
##        print "archiving old raster"
        shutil.copy(i,os.path.join(archive_directory,timestamp+"_"+basename))
##        print "deleting old raster"
        os.remove(i)
if len(existing) > 1:
    print "more than 1 raster exists, not sure which to archive, fail"
    quit()
##print "delete old zip"
zips = glob.glob(zip_folder+"\\"+"*")
if len(zips)>0:
    for z in zips:
        os.remove(z)
##print "downloading latest raster"
terrai_file = "R://latin_decrease_current.tif"
##print "zip latest raster"
zip_raster(latest_raster,zip_folder)'''
