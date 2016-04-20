import json
import logging
import os
import subprocess
import sys
import urllib
import arcpy
from collections import OrderedDict
from retrying import retry

import util
import settings


def run_ogr2ogr(cmd):
    """
    Function to run ogr2ogr in a subprocess and monitor the STDOUT. If there's an error, log it and exit
    :param cmd: a list of commands to exeecute
    :return:
    """

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


def generate_where_clause(start_row, end_row, where_field_name):
    """
    Build a series of where clauses based on a start_row, end_row, a number of ids per where_clause, and the field_name
    Also sleeps for a minute every time the current_max_id is divisible by 20,000. Important to give the API a break
    Per the cartoDB developers, we're hardcoding the transaction row limit to 500
    :param start_row: the first ID to append, usually 0
    :param end_row: the last ID in the dataset
    :param where_field_name: the field name of the where_field
    :return: where_clauses for all ids from start_id to end_id in the appropriate chunks
    """
    transaction_row_limit = 500

    # Set the max to the start_id
    current_max_id = start_row

    # Iterate until the current max is > the last id of interest
    while current_max_id <= end_row:
        yield '{0} >= {1} and {0} < {2}'.format(where_field_name, current_max_id,
                                                current_max_id + transaction_row_limit)

        # Increment the current_max_id based on the rows we just processed
        current_max_id += transaction_row_limit


def cartodb_sql(sql, gfw_env):
    """
    Execute a SQL statement using the API
    :param sql: a SQL statment
    :param gfw_env: the gfw_env-- used to grab the correct API token
    :return:
    """

    logging.debug(sql)
    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])
    api_url = settings.get_settings(gfw_env)["cartodb"]["sql_api"]

    result = urllib.urlopen("{0!s}?api_key={1!s}&q={2!s}".format(api_url, key, sql))
    json_result = json.loads(result.readlines()[0], object_pairs_hook=OrderedDict)

    if "error" in json_result.keys():
        raise SyntaxError(json_result['error'])

    return json_result


def cartodb_create(in_fc, out_cartodb_name, gfw_env):
    """
    Create a new dataset/table on cartodb
    :param in_fc: source esri FC
    :param out_cartodb_name: name of output table
    :param gfw_env: the gfw-env-- required to pick the API key
    :return:
    """
    logging.debug("upload data from {0} to staging table {1}".format(in_fc, out_cartodb_name))

    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])
    account_name = settings.get_settings(gfw_env)['cartodb']['token'].split('@')[0]

    # Help: http://www.gdal.org/ogr2ogr.html
    # The -dim 2 option ensures that only two dimensional data is created; no Z or M values
    cmd = ['ogr2ogr', '--config', 'CARTODB_API_KEY', key, '-skipfailures', '-t_srs', 'EPSG:4326',
           '-f', 'CartoDB', '-nln', out_cartodb_name, '-dim', '2', 'CartoDB:{0}'.format(account_name)]

    cmd = add_fc_to_ogr2ogr_cmd(in_fc, cmd)

    # Count dataset rows and compare them to the append limit we're using for each API transaction
    row_count = int(arcpy.GetCount_management(in_fc).getOutput(0))

    # If the row count is > the row append limit (500), need to split up the process of moving the esri FC to cartoDB
    if row_count > 500:

        # Create a temp ID field (set equal to OBJECTID) that we'll use to manage the process of incrementally
        # uploading data to the cartoDB server
        temp_id_field = util.create_temp_id_field(in_fc, gfw_env)

        where_clause = '{0} >= 0 and {0} < 500'.format(temp_id_field)
        cmd = add_where_clause_to_ogr2ogr_cmd(where_clause, cmd)

        # Run the initial command to create the fc, using FIDs 0 - rowAppendLimit
        run_ogr2ogr(cmd)

        # Use cartodb_execute_where_clause to generate where_clauses and append them with exponential backoff
        cartodb_execute_where_clause(500, row_count, temp_id_field, in_fc, out_cartodb_name, gfw_env)

        # # Build all where_clauses and pass them to cartodb_append
        # for wc in generate_where_clause(row_append_limit, row_count, row_append_limit, temp_id_field):
        #     cartodb_append(file_name, out_cartodb_name, gfw_env, wc)

    # If there are few enough rows and we don't need a set of where_clauses, upload the entire dataset at once
    else:
        run_ogr2ogr(cmd)


def add_fc_to_ogr2ogr_cmd(in_fc, cmd):
    """
    Add FC to ogr2ogr, handling issue of different format for .shp vs GDB FC
    :param in_fc: path to esri FC (shape or GDB)
    :param cmd: current list of CMD parameters
    :return: updated cmd list of parameters
    """
    if os.path.splitext(in_fc)[1] == '.shp':
        cmd += [in_fc]
    else:
        cmd += [os.path.dirname(in_fc), os.path.basename(in_fc)]

    return cmd


def add_where_clause_to_ogr2ogr_cmd(in_where_clause, cmd):
    """
    Add where clause to cmd list of parameters if it exists
    :param in_where_clause: where clause
    :param cmd: current list of CMD parameters
    :return: updated cmd list of parameters
    """
    if in_where_clause:
        cmd.insert(1, '-where')
        cmd.insert(2, in_where_clause)

    return cmd


def cartodb_append(file_name, out_cartodb_name, gfw_env, where_clause=None):
    """
    Append a local FC to a cartoDB dataset
    :param file_name: path to local esri FC
    :param out_cartodb_name: cartoDB table
    :param gfw_env: gfw_env
    :param where_clause: where_clause to apply to the dataset
    :return:
    """
    key = util.get_token(settings.get_settings(gfw_env)['cartodb']['token'])
    account_name = settings.get_settings(gfw_env)['cartodb']['token'].split('@')[0]

    # Help: http://www.gdal.org/ogr2ogr.html
    # The -dim 2 option ensures that only two dimensional data is created; no Z or M values
    cmd = ['ogr2ogr', '--config', 'CARTODB_API_KEY', key, '-append', '-skipfailures', '-t_srs', 'EPSG:4326',
           '-f', 'CartoDB',  '-nln', out_cartodb_name, '-dim', '2', 'CartoDB:{0}'.format(account_name)]

    cmd = add_fc_to_ogr2ogr_cmd(file_name, cmd)
    cmd = add_where_clause_to_ogr2ogr_cmd(where_clause, cmd)

    run_ogr2ogr(cmd)


def cartodb_make_valid_geom(table_name, gfw_env):
    """
    Iterate over all features of table_name using the where_clause and run the SQL statement below
    :param table_name:
    :param gfw_env:
    :return:
    """
    logging.debug("repair geometry")

    row_count = cartodb_row_count(table_name, gfw_env)

    sql = 'UPDATE {0} SET the_geom = ST_MakeValid(the_geom), the_geom_webmercator = ST_MakeValid(the_geom_webmercator)'\
          ' WHERE (ST_IsValid(the_geom) = false AND {1})'
    format_tuple = (table_name,)

    cartodb_execute_where_clause(0, row_count, 'cartodb_id', None, None, gfw_env, sql, format_tuple)


def cartodb_check_exists(table_name, gfw_env):
    """
    Check if the table exists by executing a LIMIT 1 query against it
    :param table_name: cartoDB table name
    :param gfw_env: gfw env
    :return: True | False
    """
    sql = "SELECT * FROM {0} LIMIT 1".format(table_name)

    try:
        cartodb_sql(sql, gfw_env)
        table_exists = True
    except SyntaxError:
        table_exists = False

    return table_exists


def get_column_order(table_name, gfw_env):
    """
    Get column order-- used to list fields for cartoDB datasets
    :param table_name: cartoDB table
    :param gfw_env: gfw env
    :return: field list
    """
    sql = 'SELECT * FROM {0} LIMIT 1'.format(table_name)

    return cartodb_sql(sql, gfw_env)['fields'].keys()


def cartodb_row_count(table_name, gfw_env):
    """
    Get row count
    :param table_name: cartoDB table
    :param gfw_env: gfw env
    :return: int value of row count
    """
    sql = "SELECT COUNT(*) FROM {0}".format(table_name)
    row_count = int(cartodb_sql(sql, gfw_env)['rows'][0]['count'])

    print 'CartoDB row count: {0}'.format(row_count)

    return row_count


def cartodb_push_to_production(staging_table, production_table, gfw_env):
    """
    Push temporary cartoDB staging table to production by selecting rows from it and inserting into production table
    :param staging_table: staging table
    :param production_table: prod table
    :param gfw_env: gfw env
    :return:
    """
    logging.debug("push staging to production table: {0}".format(production_table))

    row_count = cartodb_row_count(staging_table, gfw_env)

    prod_columns = get_column_order(production_table, gfw_env)
    staging_columns = get_column_order(staging_table, gfw_env)

    # Find the columns they have in common
    # Exclude cartodb_id; cartodb will auto number
    final_columns = [x for x in prod_columns if x in staging_columns if x != 'cartodb_id']
    final_columns_sql = ', '.join(final_columns)

    sql = 'INSERT INTO {0} ({1}) SELECT {1} FROM {2} WHERE {3}'
    format_tuple = (production_table, final_columns_sql, staging_table)

    cartodb_execute_where_clause(0, row_count, 'cartodb_id', None, None, gfw_env, sql, format_tuple)


def cartodb_execute_where_clause(start_row, end_row, id_field, src_fc, out_table, gfw_env, sql=None, format_tuple=None):
    """
    Generates a where clause and executes it to append to a cartodb table or execute SQL against the API
    :param start_row: first row ID of the where clause
    :param end_row: last row ID
    :param id_field: field to use to build integer where clauses
    :param src_fc: source FC, if appending from a local esri FC
    :param out_table: out table, if appending to a cartoDB table
    :param gfw_env: need to know which account to use
    :param sql: SQL statement to execute against the cartoDB API
    :param format_tuple: tuple to pass when formatting the SQL statement
    :return:
    """
    for wc in generate_where_clause(start_row, end_row, id_field):
        cartodb_retry(src_fc, out_table, gfw_env, sql, format_tuple, wc)


@retry(wait_exponential_multiplier=1000, wait_exponential_max=512000)
def cartodb_retry(src_fc, out_table, gfw_env, sql, format_tuple, wc):
    """
    Used to retry the ogr2ogr append/SQL query defined from by the where clause
    :param src_fc: the source fc, if appending
    :param out_table: the out table, if appending
    :param gfw_env: gfw_env to know which cartodb account
    :param sql: SQL to execute against the API
    :param format_tuple: tuple to format the sql statement if {0}/{1}/etc included
    :param wc: the where clause to add to the SQL statement
    :return:
    """
    if sql:
        format_tuple += (wc,)
        sql = sql.format(*format_tuple)
        cartodb_sql(sql, gfw_env)

    else:
        cartodb_append(src_fc, out_table, gfw_env, wc)


def cartodb_delete_where_clause_or_truncate_prod_table(in_prod_table, in_wc, in_gfw_env):
    """
    Delete features from cartoDB prod table, using a where clause if it exists
    :param in_prod_table: prod_table
    :param in_wc: where clause
    :param in_gfw_env: gfw env
    :return:
    """
    logging.debug("delete or truncate rows from production table")

    if in_wc:
        sql = 'DELETE FROM {0!s} WHERE {1}'.format(in_prod_table, in_wc)
    else:
        sql = 'TRUNCATE {0!s};'.format(in_prod_table)

    cartodb_sql(sql, in_gfw_env)


def delete_staging_table_if_exists(staging_table_name, in_gfw_env):
    """
    Delete staging table if it exists
    :param staging_table_name: table name
    :param in_gfw_env: gfw env
    :return:
    """
    logging.debug("delete staging if it exists")

    sql = 'DROP TABLE IF EXISTS {0} CASCADE'.format(staging_table_name)
    cartodb_sql(sql, in_gfw_env)


def cartodb_sync(shp, production_table, where_clause, gfw_env, scratch_workspace):
    """
    Function called by VectorLayer and other Layer objects as part of layer.update()
    Will carry out the sync process from start to finish-- pushing the shp to a staging table on cartodb, then
    to production on cartodb, using a where_clause if included
    :param shp: input feature class (can be GDB FC or SDE too)
    :param production_table: final output table in cartoDB
    :param where_clause: where_clause to use when adding/deleting from final prod table
    :param gfw_env: gfw env
    :param scratch_workspace: scratch workspace
    :return:
    """

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
