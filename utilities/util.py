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
import urllib2


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
    """
    Grab all the drives on the current PC, returning those that are mapped through the network
    :return: list of network drives
    """
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
    """
    Current a tempdir in the root, create a gdb in the temp dir, and copy the input_fc there
    :param input_fc: the fc of interest
    :param rootdir: the dir where we'll create the temp dir
    :return: path to the input_fc as it exists in the new GDB
    """
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


def is_wgs_84(input_dataset):
    """
    Test if input dataset has SR WGS 84
    :param input_dataset: input dataset
    :return: True/False if SRS is WGS84
    """
    logging.debug('Starting layer.isWGS84')
    sr_as_string = arcpy.Describe(input_dataset).spatialReference.exporttostring()

    first_element = sr_as_string.split(',')[0]

    if 'GEOGCS' in first_element and 'GCS_WGS_1984' in first_element:
        return True
    else:
        return False


def build_update_where_clause(in_fc, input_field):
    """
    Generate a where_clause based on the unique values in a particular input field
    :param in_fc: the source feature class
    :param input_field: the field to look at
    :return: where clause in form '''field_name in ('fielval1', 'fieldval2', 'fieldval3')'''
    """
    if input_field:
        # Get unique values in specified where_clause field
        # Uses a set comprehension. Fun!
        # http://love-python.blogspot.com/2012/12/set-comprehensions-in-python.html
        unique_values = list({x[0] for x in arcpy.da.SearchCursor(in_fc, [input_field])})
        unique_values_sql = "'" + "', '".join(unique_values) + "'"

        where_clause = """{0} IN ({1})""".format(input_field, unique_values_sql)

    else:
        where_clause = None

    return where_clause


def get_token(token_file):
    """
    Grab token from the tokens\ folder in the root directory
    :param token_file: name of the file
    :return: the token value
    """
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
    """
    mkdirs that don't exist for all dirs in the path
    :param path: path we'd like to create
    :return:
    """
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def add_field_and_calculate(fc, field_name, field_type, field_length, field_val, gfw_env):
    """
    Add a field and calculate
    :param fc: input fc
    :param field_name: field_name
    :param field_type: field_type
    :param field_length: field_length
    :param field_val: value to calculate
    :param gfw_env: required because we're using the util.list_fields, which also lists for cartoDB
    :return:
    """
    logging.debug("add_field_and_calculate: Adding field {0} and calculating to {1}".format(field_name, field_val))

    if field_name not in list_fields(fc, gfw_env):
        arcpy.AddField_management(fc, field_name, field_type, "", "", field_length)

    if field_type in ['TEXT', 'DATE']:
        field_val = "'{0}'".format(field_val)

    if field_val in list_fields(fc, gfw_env):
        field_val = '!{0}!'.format(field_val)

    arcpy.CalculateField_management(fc, field_name, field_val, "PYTHON")


def list_fields(input_dataset, gfw_env):
    """
    List fields for an esri or cartoDB dataset
    :param input_dataset: either path to local esri FC or a cartoDB table name
    :param gfw_env: used to determine which cartoDB account to use
    :return: list of fields
    """

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
    """
    Used when we need a temp_id field when appending to cartoDB tables. We use this temp_id field to set
    where clauses on the data to group it into manageable chunks for upload to the API
    :param input_dataset: in dataset
    :param in_gfw_env: in gfw_env
    :return: the name of the temp_id field
    """

    temp_id_fieldname = 'c_temp_id'
    oid_field = [f.name for f in arcpy.ListFields(input_dataset) if f.type == 'OID'][0]

    add_field_and_calculate(input_dataset, temp_id_fieldname, 'LONG', "", oid_field, in_gfw_env)

    return temp_id_fieldname


def copy_to_scratch_workspace(input_fc, output_workspace, field_mappings=None):
    """
    Used when we get source data that is not local or has field mappings. This data will be copied locally and then
    the local FC will be the new source
    :param input_fc: inptu source fc
    :param output_workspace: output location
    :param field_mappings: if field mappings are included, use these to set the output fields for the local FC
    :return: path to local fc
    """

    fc_name = os.path.basename(input_fc)

    # input_fc has an extension (.tif, .shp, etc)
    # can be copied directly to a dir
    if os.path.splitext(input_fc)[1]:
        out_copied_fc = os.path.join(output_workspace, fc_name)

    else:
        gdb_name = "source_data"
        arcpy.CreateFileGDB_management(output_workspace, gdb_name)
        out_copied_fc = os.path.join(output_workspace, gdb_name + '.gdb', fc_name)

    if field_mappings:
        out_workspace = os.path.dirname(out_copied_fc)
        arcpy.FeatureClassToFeatureClass_conversion(input_fc, out_workspace, fc_name, "", field_mappings)

    else:
        arcpy.Copy_management(input_fc, out_copied_fc)

    logging.info('Input data is not local or has a fieldmap-- copied here: {0}'.format(out_copied_fc))

    return out_copied_fc


def validate_osm_source(osm_source):
    """
    Used to check that all the job uids for an osm datasource exists for our osm HOT export account
    :param osm_source: list of job uids. Example: 2c5d8ae4-940a-445b-b34a-0e922a40598c,
                                                  3b88a831-e2a0-4c80-8b25-cd3bc64ffd2f
    :return: true/false based on if all job uids are valid
    """

    osm_id_list = osm_source.split(',')

    auth_key = get_token('thomas.maschler@hot_export')
    headers = {"Content-Type": "application/json", "Authorization": "Token " + auth_key}
    url = "http://export.hotosm.org/api/runs?job_uid={0}"

    is_valid = True

    for osm_id in osm_id_list:

        request = urllib2.Request(url.format(osm_id))

        for key, value in headers.items():
            request.add_header(key, value)

        try:
            # If the input uid is in the correct format, but doesn't exist, will return an empty list
            if json.load(urllib2.urlopen(request)):
                # Success! We have a response, and can assume that the job uid exists
                pass
            else:
                is_valid = False

        # If formatted improperly/etc, will return a 500 HTTP Error
        except urllib2.HTTPError:
            is_valid = False

        if not is_valid:
            logging.error("HOT OSM job uid {0} is invalid\n".format(osm_id))
            break

    return is_valid
