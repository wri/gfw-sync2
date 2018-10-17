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
import subprocess


def unzip(source_filename, dest_dir):
    """
    Extract a zip file to the target dir
    :param source_filename: .zip file
    :param dest_dir: dir to extract output
    :return:
    """
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)


def add_to_zip(fname, zf):
    """
    Will grab all files matching fname except those with .lock and .zip in a dir
    :param fname: fname to match
    :param zf: zipfile object we've alrady created
    :return:
    """
    bname = os.path.basename(fname)
    ending = os.path.splitext(bname)[1]
    if not ending == ".lock" and not ending == ".zip":
        zf.write(fname, bname)

    return


def zip_shp(input_shp):
    """
    :param input_shp: path to a shapefile
    :return: zipped shapefile
    """
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
    """
    :param input_folder: path to a dir
    :return: zipped dir
    """
    output_dir = os.path.dirname(input_folder)
    output_name = os.path.basename(input_folder)

    zip_path = os.path.join(output_dir, output_name)

    shutil.make_archive(zip_path, 'zip', input_folder)

    # Need to add '.zip' to zip_path; shutil doesn't want .zip on the end for the call used above
    return zip_path + '.zip'


def zip_tif(input_tif):
    """
    :param input_tif: path to a tif
    :return: zipped tif
    """
    basepath, fname, base_fname = util.gen_paths_shp(input_tif)
    zip_path = os.path.join(basepath, base_fname + '.zip')

    zf = zipfile.ZipFile(zip_path, 'w', allowZip64=True)
    zf.write(input_tif)
    zf.close()

    return zip_path


def all_files_less_than_2gb(input_dir):
    """
    Checks a dir to see if all files are < 2.0 GB
    :param input_dir: dir to check
    :return: Boolean if all files in dir < 2.0 GB
    """
    all_less_than_2gb = True

    for list_file in os.listdir(input_dir):
        file_size = os.path.getsize(os.path.join(input_dir, list_file))

        if file_size / 1e9 < 2.0:
            pass
        else:
            all_less_than_2gb = False
            break

    return all_less_than_2gb


def zip_file(input_fc, temp_zip_dir, download_output=None, archive_output=None, sr_is_local=False):
    """
    :param input_fc: feature class/raster to zip
    :param temp_zip_dir: output zip dir
    :param download_output: path to the download output, if required
    :param archive_output: path to the archive output, if requried
    :param sr_is_local: if the spatial reference is local, will create a _local.zip in download_output
    :return: None
    """
    logging.debug('Starting archive.zip_file')

    basepath, fname, base_fname = util.gen_paths_shp(input_fc)
    temp_dir = util.create_temp_dir(temp_zip_dir)

    data_type = arcpy.Describe(input_fc).dataType

    if data_type in ['FeatureClass', 'ShapeFile']:

        # Try to create a shapefile first, knowing that the data may be too large and may have to use an FGDB instead
        logging.debug('trying to zip SHP-----------------')
        arcpy.FeatureClassToShapefile_conversion(input_fc, temp_dir)
        out_shp = os.path.join(temp_dir, fname)

        # If the dir with the shapefile is < 2GB, zip the shapefile
        if all_files_less_than_2gb(temp_dir):
            temp_zip = zip_shp(out_shp)

        else:
            logging.debug('Some components of SHP > 2 GB; now exporting to GDB instead')

            # Delete shapefile conversion dir and start fresh
            temp_dir = util.create_temp_dir(temp_zip_dir)

            gdb_fc = util.fc_to_temp_gdb(input_fc, temp_dir)
            gdb_dir = os.path.dirname(os.path.dirname(gdb_fc))

            temp_zip = zip_dir(gdb_dir)

    elif data_type == 'RasterDataset':
        temp_zip = zip_tif(input_fc)

    else:
        logging.error('Unknown data_type: {0}. Exiting the program'.format(data_type))
        sys.exit(1)

    # Define output path for archive zip file and copy temp zip there
    if archive_output:
        logging.debug('Archiving {0} in {1}'.format(base_fname, archive_output))

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        dst = os.path.splitext(archive_output)[0] + '_{0}.zip'.format(timestamp)

        if r's3://' in dst:
            subprocess.check_call(['aws', 's3', 'cp', temp_zip, dst])

        else:
            shutil.copy(temp_zip, dst)

    # Define output path for download zip file and copy temp zip there
    if download_output:
        logging.debug("Copying {0} to download folder {1}".format(base_fname, download_output))

        if sr_is_local:
            dst = os.path.splitext(download_output)[0] + "_local.zip"
        else:
            dst = download_output

        if r's3://' in dst:
            subprocess.check_call(['aws', 's3', 'cp', temp_zip, dst])
        else:
            shutil.copy(temp_zip, dst)
