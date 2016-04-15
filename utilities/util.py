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
import uuid


def byteify(unicode_string):
    if isinstance(unicode_string, dict):
        return {byteify(key): byteify(value) for key, value in unicode_string.iteritems()}
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
    all_drives = list(itertools.compress(string.ascii_uppercase, map(lambda x: ord(x) - ord('0'),
                                                                     bin(drive_bitmask)[:1:-1])))

    network_drive_list = []

    for drive_letter in all_drives:
        drive_path = r'{0}:\\'.format(drive_letter)

        if win32file.GetDriveType(drive_path) == win32file.DRIVE_REMOTE:
            network_drive_list.append(drive_letter.lower())

    return network_drive_list


def create_temp_dir(output_dir):
    temp_dir = os.path.join(output_dir, str(uuid.uuid4()))

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


def add_field_and_calculate(fc, field_name, field_type, field_length, field_val, gfw_env):
    logging.debug("add_field_and_calculate: Adding field {0} and calculating to {1}".format(field_name, field_val))

    if field_name not in list_fields(fc, gfw_env):
        arcpy.AddField_management(fc, field_name, field_type, "", "", field_length)

    if field_type in ['TEXT', 'DATE']:
        field_val = "'{0}'".format(field_val)

    if field_val in list_fields(fc, gfw_env):
        field_val = '!{0}!'.format(field_val)

    arcpy.CalculateField_management(fc, field_name, field_val, "PYTHON")


def list_fields(input_dataset, gfw_env):

    if arcpy.Exists(input_dataset):
        field_list = [x.name for x in arcpy.ListFields(input_dataset)]

    elif cartodb.cartodb_check_exists(input_dataset, gfw_env):
        field_list = cartodb.get_column_order(input_dataset, gfw_env)

    else:
        logging.error('Input dataset type for list_fields unknown. Does not appear to be an esri fc, and '
                      'does not exist in cartodb. Exiting.')
        sys.exit(1)

    return field_list


def create_temp_id_field(input_dataset, in_gfw_env):

    temp_id_fieldname = 'temp__cartodb__id'
    oid_field = [f.name for f in arcpy.ListFields(input_dataset) if f.type == 'OID'][0]

    add_field_and_calculate(input_dataset, temp_id_fieldname, 'LONG', "", oid_field, in_gfw_env)

    return temp_id_fieldname


def replace_dict_value(in_dict, in_val, new_val):

    for key, value in in_dict.iteritems():
        if value == in_val:
            in_dict[key] = new_val
            break
        else:
            pass

    return in_dict


def copy_to_scratch_workspace(input_fc, output_workspace, field_mappings=None):

    fc_name = os.path.basename(input_fc)

    # input_fc has an extension (.tif, .shp, etc)
    # can be copied directly to a dir
    if os.path.splitext(input_fc)[1]:
        out_copied_fc = os.path.join(output_workspace, fc_name)

    else:
        gdb_name = "source_data"
        arcpy.CreateFileGDB_management(output_workspace, gdb_name)
        out_copied_fc = os.path.join(output_workspace, gdb_name + '.gdb', fc_name)

    logging.info('Input data is not local or has a fieldmap-- copying here: {0}'.format(out_copied_fc))

    if field_mappings:
        out_workspace = os.path.dirname(out_copied_fc)
        arcpy.FeatureClassToFeatureClass_conversion(input_fc, out_workspace, fc_name, "", field_mappings)

    else:
        arcpy.Copy_management(input_fc, out_copied_fc)

    return out_copied_fc
