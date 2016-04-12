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


def add_field_and_calculate(fc, field_name, field_type, field_length, field_val, gfw_env):
    logging.debug("add_field_and_calculate: Adding field {0} and calculating to {1}".format(field_name, field_val))

    if field_name not in list_fields(fc, gfw_env):
        arcpy.AddField_management(fc, field_name, field_type, "", "", field_length)

    if field_type in ['TEXT', 'DATE']:
        fieldVal = "'{0}'".format(field_val)

    arcpy.CalculateField_management(fc, field_name, fieldVal, "PYTHON")


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

        # Iterate over all of our FCs of interest to see if they have that field
        # If they do, we'll add it to the field map for that field
        for fc in in_fc_list:

            # Pull out the field object if it exists. We'll modify this to set a new name and length
            # and then push it back to the field mapping
            current_field_list = [f for f in arcpy.ListFields(fc) if f.name == field_name]

            # If this field exists in the fc of interest
            if len(current_field_list) == 1:
                current_field = current_field_list[0]

                # Create a fieldmap object
                fm = arcpy.FieldMap()

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

            else:
                logging.debug('Field {0} not found in input fc {1} during field mapping'.format(field_name, fc))

        # CRITICAL NOTE
        # Only add the fieldmap object to the field mappings object once! This is key-- otherwise
        # will get duplicate field names. Make sure you know where it is in the for loop-- one field map
        # per field of interest (duh)
        fms.addFieldMap(fm)

    return fms


def ini_fieldmap_to_fc(in_fc, ini_dict, out_workspace):
    # ini_dict example:
    # {
    #   '__joins__': 'nom_conces ON concessions.nom_conces;concessions.attributai ON societes.societe',
    #   'out_fieldname1': 'in_fieldname1',
    #   'out_fieldname2': 'in_fieldname2 * 100',
    #   'out_fieldname3': 'concessions.attributai'
    # }

    if '__joins__' in ini_dict.keys():
        join_list = ini_dict['__joins__'].split(',')

        arcpy.MakeFeatureLayer_management(in_fc, 'temp_join_fc')

        for join in join_list:
            src_info, to_info = join.split(' ON ')
            src_field = src_info.split('.')
            to_table, to_field = to_info.split('.')

            arcpy.AddJoin_management('temp_join_fc', src_field, to_table, to_field, "KEEP_COMMON")

        # Delete the __joins__ key after we've finished creating the join table
        del ini_dict['__joins__']

    else:
        arcpy.MakeFeatureLayer_management(in_fc, 'temp_join_fc')

    # Create dict obj to pass to util.build_field_map
    field_mapping_dict = {}

    # { 'field1': {'out_name':'field77', 'out_length': 23},
    #   'field2': {'out_length':17} }

    # Create dict of fields we'll process after we copy the data locally
    to_process_dict = {}

    for final_field_name, src_field_name in ini_dict.iteritems():

        # If there's a multiplication operation specified, we'll deal with it later
        # Example: area_ha = sup_adm_km2 * 100
        if '*' in src_field_name:

            # Iterate over the sup_adm_km2 and 100 (for this example, anyway)
            for x in src_field_name.split('*'):
                try:
                    # Find the value part of the expression (i.e. the 100 in field_name * 100)
                    float(x.strip())

                # Find the half of the equation that has the input field name in it
                # We'll use this as a placeholder; ultimately this will be multiplied by 100 in the local copy
                # of the dataset; just don't want to do this to the source
                except ValueError:
                    field_mapping_dict[x.strip()] = {'out_name': x.strip()}

            # We'll need to do actual multiplication for this field after we copy the dataset locally
            # Until then, create a field with the correct name and the old data, we'll multiply it later
            to_process_dict[final_field_name] = src_field_name

        # This is used if we're adding a new string field i.e. source = 'Minfof'
        # to the data table that wasn't there before
        # We'll add this after we copy the dataset locally
        elif "'" in src_field_name or '"' in src_field_name:
            to_process_dict[final_field_name] = src_field_name

        # Otherwise just add the input/output field pair to the field_mapping_dict
        else:
            field_mapping_dict[src_field_name] = {'out_name': final_field_name}

    fms = build_field_map(['temp_join_fc'], field_mapping_dict)

    out_fc = copy_to_scratch_workspace('temp_join_fc', out_workspace, fms)

    # Clean up
    arcpy.Delete_management('temp_join_fc')

    process_additional_fields_from_ini_file(to_process_dict)

    return out_fc


def process_additional_fields_from_ini_file(in_dict):

    print in_dict
    # for source_info, out_field in in_dict.iteritems():
    #
    #     pass


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




