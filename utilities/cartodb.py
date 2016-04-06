import json
import logging
import os
import subprocess
import sys
import urllib
from collections import OrderedDict
import arcpy

import util
import settings


def run_ogr2ogr(cmd):

    logging.debug('Running OGR:\n' + ' '.join(cmd))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    subprocessList = []

    #ogr2ogr doesn't properly fail on an error, just displays error messages
    #as a result, we need to read this output as it happens
    #http://stackoverflow.com/questions/1606795/catching-stdout-in-realtime-from-subprocess
    for line in iter(p.stdout.readline, b''):
        subprocessList.append(line.strip())

    #If ogr2ogr has complained, and ERROR in one of the messages, exit
    if subprocessList and 'error' in str(subprocessList).lower():
        logging.error("OGR2OGR threw an error: " + '\n'.join(subprocessList))
        sys.exit(1)

    elif subprocessList:
        logging.debug('\n'.join(subprocessList))

def generate_where_clause(start_id, end_id, transaction_row_limit, where_field_name):

    current_max_id = start_id

    while current_max_id < end_id:
        yield "{0} >= {1} and {0} < {2}".format(where_field_name, current_max_id, current_max_id + transaction_row_limit)

        current_max_id += transaction_row_limit

def cartodb_sql(sql, gfw_env, raise_error=True):

    logging.debug(sql)

    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])

    result = urllib.urlopen("{0!s}?api_key={1!s}&q={2!s}".format(settings.get_settings(gfw_env)["cartodb"]["sql_api"], key, sql))
    json_result = json.loads(result.readlines()[0], object_pairs_hook=OrderedDict)
    if raise_error and "error" in json_result.keys():
        raise SyntaxError("Wrong SQL syntax. {0!s}".format(json_result['error']))
    return json_result

def is_shp(file_name):
    if os.path.splitext(file_name)[1] == '.shp':
        return True
    else:
        return False

def cartodb_create(file_name, out_cartodb_name, gfw_env, raise_error=True):
    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])
    accountName = settings.get_settings(gfw_env)['cartodb']['token'].split('@')[0]

    cmd = [r'ogr2ogr',
        '--config', 'CARTODB_API_KEY', key,
        '-progress', '-skipfailures',
        '-t_srs', 'EPSG:4326',
        '-f', 'CartoDB', '-nln', out_cartodb_name,
        'CartoDB:{0}'.format(accountName)]

    if is_shp(file_name):
        cmd += [file_name]
    else:
        cmd += [os.path.dirname(file_name), os.path.basename(file_name)]

    rowCount = int(arcpy.GetCount_management(file_name).getOutput(0))
    rowAppendLimit = 500000

    #Had issues with cartoDB server timing out
    if rowCount > rowAppendLimit:

        cmd.insert(1, '-where')
        cmd.insert(2, "FID >= 0 and FID < {0}".format(rowAppendLimit))

        #Run the initial command to create the fc, using FIDs 0 - rowAppendLimit
        run_ogr2ogr(cmd)

        for wc in generate_where_clause(rowAppendLimit, rowCount, rowAppendLimit, 'FID'):

            #Build all where_clauses and pass them to cartodb_append
            cartodb_append(file_name, wc, accountName)
        

    else:
        run_ogr2ogr(cmd)

def cartodb_append(file_name, gfw_env, where_clause=None, raise_error=True):
    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])
    accountName = settings.get_settings(gfw_env)['cartodb']['token'].split('@')[0]

    cmd = [r'ogr2ogr',
            '--config', 'CARTODB_API_KEY', key,
            '-append', '-progress', '-skipfailures',
            '-t_srs', 'EPSG:4326',
            '-f', 'CartoDB',
            'CartoDB:{0}'.format(accountName)]

    if is_shp(file_name):
        cmd += [file_name]
    else:
        cmd += [os.path.dirname(file_name), os.path.basename(file_name)]

    if where_clause:
        cmd.insert(1, '-where')
        cmd.insert(2, where_clause)

    run_ogr2ogr(cmd)

def cartodb_check_exists(table_name, gfw_env):
    sql = "SELECT * FROM {0} LIMIT 1".format(table_name)

    tableExists = False
    try:
        cartodb_sql(sql, gfw_env)
        tableExists = True
    except:
        tableExists = False

    return tableExists

def get_column_order(table_name, gfw_env):
    sql = 'SELECT * FROM {0} LIMIT 1'.format(table_name)

    return cartodb_sql(sql, gfw_env)['fields'].keys()

def cartodb_push_to_production(staging_table, production_table, gfw_env):

    sql = "SELECT COUNT(*) FROM {0}".format(staging_table)
    rowCount = int(cartodb_sql(sql, gfw_env)['rows'][0]['count'])

    rowAppendLimit = 500000

    prod_columns = get_column_order(production_table, gfw_env)
    staging_columns = get_column_order(staging_table, gfw_env)

    # Find the columns they have in common
    # Exclude cartodb_id; cartodb will auto number
    final_columns = [x for x in prod_columns if x in staging_columns if x != 'cartodb_id']
    final_columns_sql = ', '.join(final_columns)

    for wc in generate_where_clause(0, rowCount, rowAppendLimit, 'cartodb_id'):
        sql = 'INSERT INTO {0} ({1}) SELECT {1} FROM {2} WHERE {3}'.format(production_table, final_columns_sql, staging_table, wc)
        cartodb_sql(sql, gfw_env)


def cartodb_sync(shp, production_table, where_clause, gfw_env, scratch_workspace):

    # ogr2ogr can't use an SDE fc as an input; must export to GDB
    if '@localhost).sde' in shp:
        shp = util.fc_to_temp_gdb(shp, scratch_workspace)

    basename = os.path.basename(shp)
    staging_table = os.path.splitext(basename)[0] + '_staging'

    logging.debug("delete staging if it exists")
    sql = 'DROP TABLE IF EXISTS {0!s} CASCADE'.format(staging_table)
    cartodb_sql(sql, gfw_env)

    logging.debug("upload data from {0} to staging table {1}".format(shp, staging_table))
    cartodb_create(shp, staging_table, gfw_env)

    logging.debug("repair geometry")
    sql = 'UPDATE {0!s} SET the_geom = ST_MakeValid(the_geom), the_geom_webmercator = ' \
          'ST_MakeValid(the_geom_webmercator) WHERE ST_IsValid(the_geom) = false'.format(staging_table)

    cartodb_sql(sql, gfw_env)

    logging.debug("delete or truncate rows from production table")
    if where_clause:
        sql = 'DELETE FROM {0!s} WHERE {1}'.format(production_table, where_clause)
    else:
        sql = 'TRUNCATE {0!s};'.format(production_table)
    cartodb_sql(sql, gfw_env)

    logging.debug("push staging to production table: {0}".format(production_table))
    cartodb_push_to_production(staging_table, production_table, gfw_env)

    logging.debug("delete staging")
    sql = 'DROP TABLE IF EXISTS {0!s} CASCADE'.format(staging_table)
    cartodb_sql(sql, gfw_env)
