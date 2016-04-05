__author__ = 'Thomas.Maschler'

import zipfile
import os
import sys
import glob
import arcpy
import shutil
import time
import datetime
import util


def unzip(source_filename, dest_dir):
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)


def add_to_zip(fname, zf):

    bname = os.path.basename(fname)
    ending = os.path.splitext(bname)[1]
    if not ending ==  ".lock" and not ending == ".zip":
        #print 'Writing %s to archive' % ending
        # flatten zipfile
        zf.write(fname, bname)

    return

def zip_shp(input_shp):
    basepath, fname, base_fname = util.gen_paths_shp(input_shp)
    zip_path = os.path.join(basepath, base_fname + '.zip')

    zf = zipfile.ZipFile(zip_path, 'w', allowZip64=False)

    search = os.path.join(basepath, "*.*")
    files = glob.glob(search)

    # Will fail if the input data is > 2 GB
    # This is by design (using the allowZip64=False option
    # If a .shp is > 2 GB it will likely be corrupt anyway
    # We'll use the GDB zip process instead
    try:
        for f in files:
            bname = os.path.basename(f)
            if (base_fname in bname) and (bname != base_fname + ".zip"):
                add_to_zip(f, zf)

        zip_result = zip_path

    except:
        print 'Failed to zip SHP-- likely too large for the 2GB limit'
        zip_result = None

    zf.close()

    return zip_result

def zip_dir(input_folder):
    output_dir = os.path.dirname(input_folder)
    output_name = os.path.basename(input_folder)

    zip_path = os.path.join(output_dir, output_name)

    shutil.make_archive(zip_path, 'zip', input_folder)

    # Need to add '.zip' to zip_path; shutil doesn't want .zip on the end for the call used above
    return zip_path + '.zip'

def zip_tif(input_tif):
    basepath, fname, base_fname = util.gen_paths_shp(input_tif)
    zip_path = os.path.join(basepath, base_fname + '.zip')

    zf = zipfile.ZipFile(zip_path, 'w', allowZip64=True)
    zf.write(input_tif)
    zf.close()

    return zip_path


def zip_file(input_fc, temp_zip_dir, download_output=False, archive_output=False, sr_is_local=False):
    basepath, fname, base_fname = util.gen_paths_shp(input_fc)
    temp_dir = util.create_temp_dir(os.path.dirname(temp_zip_dir))

    data_type = arcpy.Describe(input_fc).dataType

    if data_type in ['FeatureClass', 'ShapeFile']:

        print 'trying to zip SHP-----------------'
        arcpy.FeatureClassToShapefile_conversion(input_fc, temp_dir)

        out_shp = os.path.join(temp_dir, fname)

        temp_zip = zip_shp(out_shp)

        # In case this process fails-- zip file is too large etc
        # Try it with GDB and allow > 2GB zip files
        if not temp_zip:

            print 'trying to zip GDB-----------------'

            # Delete failed shapefile conversion dir and start fresh
            temp_dir = util.create_temp_dir(os.path.dirname(temp_zip_dir))

            gdb_fc = util.fc_to_temp_gdb(input_fc, os.path.dirname(temp_zip_dir))
            print 'THIS IS THE GDB FC --------------' + gdb_fc
            gdb_dir = os.path.dirname(os.path.dirname(gdb_fc))

            temp_zip = zip_dir(gdb_dir)

    elif data_type == 'RasterDataset':
        temp_zip = zip_tif(input_fc)

    else:
        print 'Unknown data_type: {0}. Exiting the program'.format(data_type)
        sys.exit(1)

    if archive_output:
        print 'Archiving {0} in {1}'.format(base_fname, archive_output)

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        dst = os.path.splitext(archive_output)[0] + '_{0}.zip'.format(timestamp)

        shutil.copy(temp_zip, dst)

    if download_output:
        print "Copying {0} to download folder {1}".format(base_fname, download_output)

        if sr_is_local:
            dst = os.path.splitext(download_output)[0] + "_local.zip"
        else:
            dst = download_output

        shutil.copy(temp_zip, dst)


    # Cleanup
    shutil.rmtree(temp_dir)

