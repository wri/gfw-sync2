import os
import arcpy
import json
import requests
import logging
import subprocess
import sys
import shutil
import urlparse

from utilities import settings
from utilities import util


def find_src_mxd_and_cache_dir(map_service_path):
    """
    Use the admin REST endpoint to get info about the source MXD location and the cache directory
    :param map_service_path:
    :return:
    """

    # http://server.arcgis.com/en/server/latest/administer/linux/example-edit-service-properties.htm
    cred = settings.get_ini_file('arcgis_server_dm', 'tokens')
    token = request_token(cred)

    # Need to convert the path from our gis server (map_service_path) to its URL
    # map_service_path example: GIS Servers\arcgis on gis-gfw.wri.org (admin)\cached\temp_cached_mapservice.MapServer
    try:
        partial_path = map_service_path.split('gis-gfw.wri.org')[1]

    except IndexError:
        partial_path = map_service_path.split('localhost')[1]

    paren_index = partial_path.index(')')

    # Start at the paren index and grab the rest of the path
    map_service = partial_path[paren_index+2:]

    base_url = r'http://localhost/arcgis/admin/services/{0}'
    url = urlparse.urljoin(base_url, map_service)

    payload = {"token": token, "f": "json"}
    r = requests.get(url, params=payload)

    if r.status_code != 200:
        logging.error("Bad JSON response. Status {0}, content {1]".format(r.status_code, r.content))
        sys.exit(1)

    else:
        response = json.loads(r.content)

        source_mxd = find_src_mxd(response)
        cache_dir = find_cache_dir(response)

    return source_mxd, cache_dir


def find_src_mxd(json_response):
    server_path_to_msd = json_response['properties']['filePath']

    # Now that we have the path to the MSD, we can find the manifest.json file
    # This can be used to get the path to the source MXD
    config_dir = os.path.dirname(os.path.dirname(server_path_to_msd))
    config_json = os.path.join(config_dir, 'manifest.json')
    #
    # # This config_json file is on the D:\ of the prod server, but that's actually the P:\ drive of the DM server
    # # Use this to change the path
    # file_path = map_prod_server_path(config_json)

    with open(config_json) as data_file:
        data = json.load(data_file)

    resources = data['resources'][0]
    source_machine = resources['clientName']

    # May ultimately be able to build in a source MXD on the prod server, but shouldn't worry about this yet
    if source_machine != 'GFW-DM-Server':
        logging.error("Unknown client machine {0}. Where does the source MXD live??".format(source_machine))

    source_mxd = resources['onPremisePath']

    if not arcpy.Exists(source_mxd):
        logging.error("Found path to source MXD {0} but arcpy doesn't think it exists. Exiting")
        sys.exit(1)

    return source_mxd


def find_cache_dir(json_response):

    service_name = json_response['serviceName']
    basedir = json_response['properties']['cacheDir']

    # Looks like the path needs to include 'cached_' . . . inserted because it's the folder name on the server?
    cache_dir = os.path.join(basedir, 'cached_' + service_name, 'Layers')

    return cache_dir


def map_prod_server_path(input_path):

    driveletter, file_path = os.path.splitdrive(input_path)

    if driveletter == r'D:':
        mapped_drive = r'P:'

    else:
        logging.error("Cached map service is in unknown drive {0} on the server. Exiting".format(driveletter))
        sys.exit(1)

    mapped_file_path = os.path.join(mapped_drive, file_path)

    return mapped_file_path


def request_token(cred):

    d = {"username": cred['username'],
         "password": cred['password'],
         "client": "requestip",
         "f": "json"}

    url = "http://localhost/arcgis/admin/generateToken"
    r = requests.post(url, data=d)

    response = json.loads(r.content)

    if 'error' in response.values() or r.status_code != 200:
        raise Exception(response['code'], response['messages'])

    return response['token']


def manage_service(host_type, service_path, operation):

    if host_type == 'dev':
        host = 'http://localhost'
        cred = settings.get_ini_file('arcgis_server_dm', 'tokens')

    elif host_type == 'prod':
        host = 'http://gis-gfw.wri.org/arcgis'
        cred = settings.get_ini_file('arcgis_server_prod', 'tokens')

    else:
        logging.error('Invalid service type for manage_service. Exiting')
        sys.exit(1)

    service_utility = r"C:\Program Files\ArcGIS\Server\tools\admin\manageservice.py"

    path_split = service_path.split('\\')
    name = '/'.join(path_split[-2:]).replace('.MapServer', '')

    cmd = ['python', service_utility, '-u', cred['username'], '-p', cred['password'],
           '-s', host, '-n', name, '-o', operation]
    subprocess.check_call(cmd)


def push_to_production(src_cache_dir, out_local_cache_dir, service_path):

    out_prod_cache_dir = map_prod_server_path(out_local_cache_dir)

    # Not totally necessary to update the cache on the DM server, but helpful for troubleshooting
    for cache in [out_local_cache_dir, out_prod_cache_dir]:
        shutil.rmtree(cache)
        shutil.copytree(src_cache_dir, cache)

    logging.debug('Restarting production services')
    manage_service('prod', service_path, 'stop')
    manage_service('prod', service_path, 'start')


def update_cache(map_service_path, scratch_workspace, gfw_env):

    logging.debug('Updating tiles for cached map service {0}'.format(map_service_path))

    # Start the service on localhost-- should be shut off when not in use
    manage_service('dev', map_service_path, 'start')

    source_mxd, local_cache_dir = find_src_mxd_and_cache_dir(map_service_path)
    logging.debug("Found source MXD: {0}".format(source_mxd))

    output_dir = util.create_temp_dir(scratch_workspace)

    # Zoom levels 0 - 6
    scale_aoi = "591657527.591555;295828763.795777;147914381.897889;73957190.948944;" \
                "36978595.474472;18489297.737236;9244648.868618"

    min_scale = scale_aoi.split(';')[0]
    max_scale = scale_aoi.split(';')[-1]

    logging.debug("Generating tiles . . . ")
    cache_dir_name = 'cache'
    arcpy.ManageTileCache_management(output_dir, "RECREATE_ALL_TILES", cache_dir_name, source_mxd,
                                     "ARCGISONLINE_SCHEME", "", scale_aoi, "", "", min_scale, max_scale)

    logging.debug("Copying to local and production cache directories")
    src_cache_dir = os.path.join(output_dir, cache_dir_name, 'Layers')

    if gfw_env == 'PROD':
        push_to_production(gfw_env, src_cache_dir, local_cache_dir, map_service_path)
    else:
        logging.debug("Nothing pushed to PROD dir; just testing cache generation process")
        logging.debug('Cached tiles are here: {0}'.format(local_cache_dir))

    # Stop the map service-- no need for it to be serving on the DM machine
    manage_service('dev', map_service_path, 'stop')


