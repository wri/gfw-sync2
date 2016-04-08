import arcpy
import json
import ctypes
import itertools
import os
import string
import shutil
import errno
import win32file
import sys
import cartodb
import logging


def byteify(unicode_string):
    if isinstance(unicode_string, dict):
        return {byteify(key):byteify(value) for key, value in unicode_string.iteritems()}
    elif isinstance(unicode_string, list):
        return [byteify(element) for element in unicode_string]
    elif isinstance(unicode_string, unicode):
        return unicode_string.encode('utf-8')
    else:
        return unicode_string


def gen_paths_shp(src):

    if '@localhost).sde' in src:
        basepath = os.path.split(src)[0]
        base_fname = os.path.basename(src).split('.')[-1]
        fname = base_fname + '.shp'

    else:
        basepath, fname = os.path.split(src)
        base_fname = os.path.splitext(fname)[0]

    return basepath, fname, base_fname


def list_network_drives():
    drive_bitmask = ctypes.cdll.kernel32.GetLogicalDrives()
    all_drives = list(itertools.compress(string.ascii_uppercase, map(lambda x:ord(x) - ord('0'), bin(drive_bitmask)[:1:-1])))

    network_drive_list = []

    for drive_letter in all_drives:
        drive_path = r'{0}:\\'.format(drive_letter)

        if win32file.GetDriveType(drive_path) == win32file.DRIVE_REMOTE:
            network_drive_list.append(drive_letter.lower())

    return network_drive_list


def create_temp_dir(output_dir):
    temp_dir = os.path.join(output_dir, 'temp')

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    os.mkdir(temp_dir)

    return temp_dir


def fc_to_temp_gdb(input_fc, rootdir):
    temp_dir = create_temp_dir(rootdir)

    basepath, fname, base_fname = gen_paths_shp(input_fc)

    gdb_dir = os.path.join(temp_dir, base_fname)
    os.mkdir(gdb_dir)

    # Create a gdb and build the path for the output
    arcpy.CreateFileGDB_management(gdb_dir, 'data.gdb')
    gdb_path = os.path.join(gdb_dir, 'data.gdb')

    arcpy.FeatureClassToGeodatabase_conversion(input_fc, gdb_path)

    fc_path = os.path.join(gdb_path, base_fname)

    return fc_path


def csl_to_list(csl):
    l = csl.split(',')
    result = []
    for item in l:
        result.append(item.strip())
    return result


def get_token(token_file):
    abspath = os.path.abspath(__file__)
    dir_name = os.path.dirname(os.path.dirname(abspath))
    token_path = os.path.join(dir_name, r"tokens\{0!s}".format(token_file))

    if not os.path.exists(token_path):
        raise IOError('Cannot find any token for {0!s}\n Make sure there is a file called {1!s} '
                      'in the tokens directory'.format(token_file, token_file))
    else:
        if os.path.splitext(token_path)[1] == '.json':
            return json.load(open(token_path))
        else:
            with open(token_path, "r") as f:
                for row in f:
                    return row


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def add_field_and_calculate(fc, fieldName, fieldType, fieldLength, fieldVal, gfw_env):
    logging.debug("add_field_and_calculate: Adding field {0} and calculating to {1}".format(fieldName, fieldVal))

    if fieldName not in list_fields(fc, gfw_env):
        arcpy.AddField_management(fc, fieldName, fieldType, "", "", fieldLength)

    if fieldType in ['TEXT', 'DATE']:
        fieldVal = "'{0}'".format(fieldVal)

    arcpy.CalculateField_management(fc, fieldName, fieldVal, "PYTHON")


def list_fields(input_dataset, gfw_env):
    logging.debug('starting layer.list_fields')

    if arcpy.Exists(input_dataset):
        fieldList = [x.name for x in arcpy.ListFields(input_dataset)]

    elif cartodb.cartodb_check_exists(input_dataset, gfw_env):
        fieldList = cartodb.get_column_order(input_dataset, gfw_env)

    else:
        logging.error('Input dataset type for list_fields unknown. Does not appear to be an esri fc, and '
                      'does not exist in cartodb. Exiting.')
        sys.exit(1)

    return fieldList


def build_field_map(in_fc_list, in_field_dict):

    # in_field_dict example:
    # {
    #   'field1': {'out_name':'field77', 'out_length': 23},
    #   'field2': {'out_length':17}
    #   'field3': {'out_name': 'field1214'}
    # }

    # Thank goodness for this help page
    # http://pro.arcgis.com/en/pro-app/arcpy/classes/fieldmap.htm
    # Field maps suck!

    # Create our field mappings object
    fms = arcpy.FieldMappings()

    # Iterate over all the fields we're interested in bringing to the next fc
    for field_name in in_field_dict.keys():

        # Create a fieldmap object
        fm = arcpy.FieldMap()

        # Iterate over all of our FCs of interest to see if they have that field
        # If they do, we'll add it to the field map for that field
        for fc in in_fc_list:

            # Pull out the field object if it exists. We'll modify this to set a new name and length
            # and then push it back to the field mapping
            current_field = [f for f in arcpy.ListFields(fc) if f.name == field_name][0]

            # If this field exists in the fc of interest
            if current_field:

                # Add the field from this FC to the list of fields to be merged in the output dataset
                fm.addInputField(fc, field_name)

                # If we've specified an new name, set it
                try:
                    current_field.name = in_field_dict[field_name]['out_name']
                except KeyError:
                    pass

                # If we've specified an out length, set it
                # Otherwise will go with default
                try:
                    current_field.length = in_field_dict[field_name]['out_length']
                except KeyError:
                    pass

                # Set the output field to the updated field definition
                fm.outputField = current_field

        # CRITICAL NOTE
        # Only add the fieldmap object to the field mappings object once! This is key-- otherwise
        # will get duplicate field names. Make sure you know where it is in the for loop-- one field map
        # per field of interest (duh)
        fms.addFieldMap(fm)

    return fms




