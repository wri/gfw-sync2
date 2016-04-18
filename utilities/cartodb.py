import json
import logging
import os
import subprocess
import sys
import time
import urllib
from collections import OrderedDict
import arcpy

import util
import settings


def run_ogr2ogr(cmd):

    logging.debug('Running OGR:\n' + ' '.join(cmd))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    subprocess_list = []

    # ogr2ogr doesn't properly fail on an error, just displays error messages
    # as a result, we need to read this output as it happens
    # http://stackoverflow.com/questions/1606795/catching-stdout-in-realtime-from-subprocess
    for line in iter(p.stdout.readline, b''):
        subprocess_list.append(line.strip())

    # If ogr2ogr has complained, and ERROR in one of the messages, exit
    if subprocess_list and 'error' in str(subprocess_list).lower():
        logging.error("OGR2OGR threw an error: " + '\n'.join(subprocess_list))
        sys.exit(1)

    elif subprocess_list:
        logging.debug('\n'.join(subprocess_list))


def generate_where_clause(start_id, end_id, transaction_row_limit, where_field_name):

    current_max_id = start_id

    while current_max_id <= end_id:
        yield '{0} >= {1} and {0} < {2}'.format(where_field_name, current_max_id,
                                                current_max_id + transaction_row_limit)

        # Sleep for a minute every time we hit an increment of 20,000
        # Important to give the cartodb API time to recover
        if not current_max_id % 20000:
            print 'Sleepinng for a minute'
            time.sleep(60)

        current_max_id += transaction_row_limit


def cartodb_sql(sql, gfw_env, raise_error=True):

    logging.debug(sql)
    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])

    result = urllib.urlopen("{0!s}?api_key={1!s}&q={2!s}".format(settings.get_settings(gfw_env)["cartodb"]["sql_api"],
                                                                 key, sql))
    json_result = json.loads(result.readlines()[0], object_pairs_hook=OrderedDict)

    if raise_error and "error" in json_result.keys():
        raise SyntaxError(json_result['error'])

    return json_result


def is_shp(file_name):
    if os.path.splitext(file_name)[1] == '.shp':
        return True
    else:
        return False


def cartodb_create(file_name, out_cartodb_name, gfw_env):
    logging.debug("upload data from {0} to staging table {1}".format(file_name, out_cartodb_name))

    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])
    account_name = settings.get_settings(gfw_env)['cartodb']['token'].split('@')[0]

    cmd = ['ogr2ogr', '--config', 'CARTODB_API_KEY', key, '-skipfailures', '-t_srs', 'EPSG:4326',
           '-f', 'CartoDB', '-nln', out_cartodb_name, 'CartoDB:{0}'.format(account_name)]

    if is_shp(file_name):
        cmd += [file_name]
    else:
        cmd += [os.path.dirname(file_name), os.path.basename(file_name)]

    row_count = int(arcpy.GetCount_management(file_name).getOutput(0))
    row_append_limit = 500
    temp_id_field = util.create_temp_id_field(file_name, gfw_env)

    # Had issues with cartoDB server timing out
    if row_count > row_append_limit:

        cmd.insert(1, '-where')
        cmd.insert(2, '{1} >= 0 and {1} < {0}'.format(row_append_limit, temp_id_field))

        # Run the initial command to create the fc, using FIDs 0 - rowAppendLimit
        run_ogr2ogr(cmd)

        for wc in generate_where_clause(row_append_limit, row_count, row_append_limit, temp_id_field):

            # Build all where_clauses and pass them to cartodb_append
            cartodb_append(file_name, out_cartodb_name, gfw_env, wc)

    else:
        run_ogr2ogr(cmd)


def cartodb_append(file_name, out_cartodb_name, gfw_env, where_clause=None):
    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])
    account_name = settings.get_settings(gfw_env)['cartodb']['token'].split('@')[0]

    cmd = ['ogr2ogr', '--config', 'CARTODB_API_KEY', key, '-append', '-skipfailures', '-t_srs',
           'EPSG:4326', '-f', 'CartoDB', '-nln', out_cartodb_name, 'CartoDB:{0}'.format(account_name)]

    if is_shp(file_name):
        cmd += [file_name]
    else:
        cmd += [os.path.dirname(file_name), os.path.basename(file_name)]

    if where_clause:
        cmd.insert(1, '-where')
        cmd.insert(2, where_clause)

    run_ogr2ogr(cmd)


def cartodb_make_valid_geom(table_name, gfw_env):
    logging.debug("repair geometry")

    row_count = cartodb_row_count(table_name)
    row_wc_limit = 1000

    for wc in generate_where_clause(0, row_count, row_wc_limit, 'cartodb_id'):
        sql = 'UPDATE {0} SET the_geom = ST_MakeValid(the_geom), the_geom_webmercator = ' \
          'ST_MakeValid(the_geom_webmercator) WHERE (ST_IsValid(the_geom) = false AND {1})'.format(table_name, wc)

        cartodb_sql(sql, gfw_env)


def cartodb_check_exists(table_name, gfw_env):
    sql = "SELECT * FROM {0} LIMIT 1".format(table_name)

    try:
        cartodb_sql(sql, gfw_env)
        table_exists = True
    except SyntaxError:
        table_exists = False

    return table_exists


def get_column_order(table_name, gfw_env):
    sql = 'SELECT * FROM {0} LIMIT 1'.format(table_name)

    return cartodb_sql(sql, gfw_env)['fields'].keys()


def cartodb_row_count(table_name, gfw_env):
    sql = "SELECT COUNT(*) FROM {0}".format(table_name)
    row_count = int(cartodb_sql(sql, gfw_env)['rows'][0]['count'])

    print 'THIS IS THE ROW COUNT in CARTODB --------------- {0}'.format(row_count)

    return row_count


def cartodb_push_to_production(staging_table, production_table, gfw_env):
    logging.debug("push staging to production table: {0}".format(production_table))

    row_count = cartodb_row_count(staging_table, gfw_env)
    row_append_limit = 5000

    prod_columns = get_column_order(production_table, gfw_env)
    staging_columns = get_column_order(staging_table, gfw_env)

    # Find the columns they have in common
    # Exclude cartodb_id; cartodb will auto number
    final_columns = [x for x in prod_columns if x in staging_columns if x != 'cartodb_id']
    final_columns_sql = ', '.join(final_columns)

    for wc in generate_where_clause(0, row_count, row_append_limit, 'cartodb_id'):
        sql = 'INSERT INTO {0} ({1}) SELECT {1} FROM {2} WHERE {3}'.format(production_table, final_columns_sql,
                                                                           staging_table, wc)
        cartodb_sql(sql, gfw_env)


def cartodb_delete_where_clause_or_truncate_prod_table(in_prod_table, in_wc, in_gfw_env):
    logging.debug("delete or truncate rows from production table")

    if in_wc:
        sql = 'DELETE FROM {0!s} WHERE {1}'.format(in_prod_table, in_wc)
    else:
        sql = 'TRUNCATE {0!s};'.format(in_prod_table)

    cartodb_sql(sql, in_gfw_env)


def delete_staging_table_if_exists(staging_table_name, in_gfw_env):
    logging.debug("delete staging if it exists")

    sql = 'DROP TABLE IF EXISTS {0!s} CASCADE'.format(staging_table_name)
    cartodb_sql(sql, in_gfw_env)


def cartodb_sync(shp, production_table, where_clause, gfw_env, scratch_workspace):

    # ogr2ogr can't use an SDE fc as an input; must export to GDB
    if '@localhost).sde' in shp:
        shp = util.fc_to_temp_gdb(shp, scratch_workspace)

    basename = os.path.basename(shp)
    staging_table = os.path.splitext(basename)[0] + '_staging'

    delete_staging_table_if_exists(staging_table, gfw_env)

    cartodb_create(shp, staging_table, gfw_env)

    cartodb_make_valid_geom(staging_table, gfw_env)

    cartodb_delete_where_clause_or_truncate_prod_table(production_table, where_clause, gfw_env)

    cartodb_push_to_production(staging_table, production_table, gfw_env)

    delete_staging_table_if_exists(staging_table, gfw_env)
