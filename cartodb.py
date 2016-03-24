import urllib
import subprocess
import os
import util
import json
import arcpy
from settings import settings

accountName = settings['cartodb']['token'].split('@')[0]

def run_ogr2ogr(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    subprocessList = []

    #ogr2ogr doesn't properly fail on an error, just displays error messages
    #as a result, we need to read this output as it happens
    #http://stackoverflow.com/questions/1606795/catching-stdout-in-realtime-from-subprocess
    for line in iter(p.stdout.readline, b''):
        subprocessList.append(line.strip())

    #If ogr2ogr has complained, and ERROR in one of the messages, exit
    if subprocessList and 'error' in str(subprocessList).lower():
        raise RuntimeError("OGR2OGR threw an error: " + '\r\n'.join(subprocessList))

    elif subprocessList:
        print '\r\n'.join(subprocessList)

def cartodb_sql(sql, raise_error=True):
    key = util.get_token(settings['cartodb']['token'])
    result = urllib.urlopen("{0!s}?api_key={1!s}&q={2!s}".format(settings["cartodb"]["sql_api"], key, sql))
    json_result = json.loads(result.readlines()[0])
    if raise_error and "error" in json_result.keys():
        raise SyntaxError("Wrong SQL syntax.\n {0!s}".format(json_result['error']))
    return json_result


def cartodb_create(file_name, raise_error=True):
    key = util.get_token(settings['cartodb']['token'])

    cmd = [r'ogr2ogr',
        '--config', 'CARTODB_API_KEY', key,
        '-progress', '-skipfailures',
        '-t_srs', 'EPSG:4326',
        '-f', 'CartoDB',
        'CartoDB:{0}'.format(accountName), file_name]

    rowCount = int(arcpy.GetCount_management(file_name).getOutput(0))
    rowAppendLimit = 1000000

    #Had issues with cartoDB server timing out
    if rowCount > rowAppendLimit:
        
        isFirst = True
        print file_name
        print rowCount
        
        for i in range(0, rowCount+1, rowAppendLimit):
            where_clause = "FID >= {0} and FID < {1}".format(i, i + rowAppendLimit)
            print where_clause

            if isFirst:
                isFirst = False
                cmd.insert(1, '-where')
                cmd.insert(2, where_clause)

                run_ogr2ogr(cmd)
            
            else:
                cartodb_append(file_name, where_clause)
        

    else:
        run_ogr2ogr(cmd)

def cartodb_append(file_name, where_clause=None, raise_error=True):
    key = util.get_token(settings['cartodb']['token'])

    cmd = [r'C:\Program Files\GDAL\ogr2ogr.exe',
            '--config', 'CARTODB_API_KEY', key,
            '-append', '-progress', '-skipfailures',
            '-t_srs', 'EPSG:4326',
            '-f', 'CartoDB',
            'CartoDB:{0}'.format(accountName), "{}".format(file_name)]

    if where_clause:
        cmd.insert(1, '-where')
        cmd.insert(2, where_clause)

    run_ogr2ogr(cmd)


def cartodb_sync(shp, production_table, where_clause):

    basename = os.path.basename(shp)
    staging_table = os.path.splitext(basename)[0]

    print "upload data from {0} to staging table {1}".format(shp, staging_table)
    cartodb_create(shp)

    print "repair geometry"
    sql = 'UPDATE {0!s} SET the_geom = ST_MakeValid(the_geom), the_geom_webmercator = ST_MakeValid(the_geom_webmercator) WHERE ST_IsValid(the_geom) = false'.format(staging_table)
    cartodb_sql(sql)

    print "push to production table: {0}".format(production_table)

    if where_clause:
        sql = 'DELETE FROM {0!s} WHERE {1};'.format(production_table, where_clause)
        print sql
    else:
        sql = 'TRUNCATE {0!s};'.format(production_table)
        
    cartodb_sql(sql)

    sql = 'INSERT INTO {0!s} SELECT * FROM {0!s}; COMMIT'.format(production_table, staging_table)
    cartodb_sql(sql)

    print "delete staging"
    sql = 'DROP TABLE IF EXISTS {0!s} CASCADE'.format(staging_table)
    cartodb_sql(sql)
