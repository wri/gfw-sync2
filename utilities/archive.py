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
import logging


def unzip(source_filename, dest_dir):
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)


def add_to_zip(fname, zf):

    bname = os.path.basename(fname)
    ending = os.path.splitext(bname)[1]
    if not ending == ".lock" and not ending == ".zip":
        zf.write(fname, bname)

    return


def zip_shp(input_shp):
    basepath, fname, base_fname = util.gen_paths_shp(input_shp)
    zip_path = os.path.join(basepath, base_fname + '.zip')

    zf = zipfile.ZipFile(zip_path, 'w', allowZip64=False)

    search = os.path.join(basepath, "*.*")
    files = glob.glob(search)

    for f in files:
        bname = os.path.basename(f)
        if (base_fname in bname) and (bname != base_fname + ".zip"):
            add_to_zip(f, zf)

    zip_result = zip_path

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


def dir_less_than_2gb(input_dir):
    file_size_list = []

    for list_file in os.listdir(input_dir):
        file_size = os.path.getsize(os.path.join(input_dir, list_file))
        file_size_list.append(file_size)

    total_size = sum(file_size_list)

    if total_size / 1e9 < 2.0:
        dir_is_smaller = True
    else:
        dir_is_smaller = False

    return dir_is_smaller


def zip_file(input_fc, temp_zip_dir, download_output=None, archive_output=None, sr_is_local=False):
    logging.debug('Starting archive.zip_file')

    basepath, fname, base_fname = util.gen_paths_shp(input_fc)
    temp_dir = util.create_temp_dir(os.path.dirname(temp_zip_dir))

    data_type = arcpy.Describe(input_fc).dataType

    if data_type in ['FeatureClass', 'ShapeFile']:

        logging.debug('trying to zip SHP-----------------')
        arcpy.FeatureClassToShapefile_conversion(input_fc, temp_dir)

        out_shp = os.path.join(temp_dir, fname)

        # If the dir with the shapefile is < 2GB, zip the shapefile
        if dir_less_than_2gb(temp_dir):
            temp_zip = zip_shp(out_shp)

        # In case this process fails-- zip file is too large etc
        # Try it with GDB and allow > 2GB zip files
        else:
            logging.debug('trying to zip GDB-----------------')

            # Delete failed shapefile conversion dir and start fresh
            temp_dir = util.create_temp_dir(os.path.dirname(temp_zip_dir))

            gdb_fc = util.fc_to_temp_gdb(input_fc, os.path.dirname(temp_zip_dir))
            logging.debug('THIS IS THE GDB FC --------------' + gdb_fc)
            gdb_dir = os.path.dirname(os.path.dirname(gdb_fc))

            temp_zip = zip_dir(gdb_dir)

    elif data_type == 'RasterDataset':
        temp_zip = zip_tif(input_fc)

    else:
        logging.error('Unknown data_type: {0}. Exiting the program'.format(data_type))
        sys.exit(1)

    if archive_output:
        logging.debug('Archiving {0} in {1}'.format(base_fname, archive_output))

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        dst = os.path.splitext(archive_output)[0] + '_{0}.zip'.format(timestamp)

        shutil.copy(temp_zip, dst)

    if download_output:
        logging.debug("Copying {0} to download folder {1}".format(base_fname, download_output))

        if sr_is_local:
            dst = os.path.splitext(download_output)[0] + "_local.zip"
        else:
            dst = download_output

        shutil.copy(temp_zip, dst)

    # Cleanup
    shutil.rmtree(temp_dir)
