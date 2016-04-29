import os
import arcpy
import json
import requests
import logging
import sys
import urlparse

from utilities import settings
from utilities import util


def find_src_mxd_and_cache_dir(map_service_path):

    # http://server.arcgis.com/en/server/latest/administer/linux/example-edit-service-properties.htm
    token = request_token()

    # Need to convert the path from our gis server (map_service_path) to its URL
    # map_service_path example: GIS Servers\arcgis on gis-gfw.wri.org (admin)\cached\temp_cached_mapservice.MapServer
    partial_path = map_service_path.split('gis-gfw.wri.org')[1]
    paren_index = partial_path.index(')')

    # Start at the paren index and grab the rest of the path
    map_service = partial_path[paren_index+2:]

    base_url = r'http://gis-gfw.wri.org/arcgis/admin/services/{0}'
    url = urlparse.urljoin(base_url, map_service)

    payload = {"token": token, "f": "json"}
    r = requests.get(url, params=payload)

    if r.status_code != 200:
        logging.error("Bad JSON response. Status {0}, content {1]".format(r.status_code, r.content))
        sys.exit(1)

    else:
        response = json.loads(r.content)

        source_mxd = find_src_mxd(response)

        # TODO BUILD THIS OUT
        cache_dir = find_cache_dir(response)

    return source_mxd, cache_dir


def find_src_mxd(json_response):
    server_path_to_msd = json_response['properties']['filePath']

    # Now that we have the path to the MSD, we can find the manifest.json file
    # This can be used to get the path to the source MXD
    config_dir = os.path.dirname(os.path.dirname(server_path_to_msd))
    config_json = os.path.join(config_dir, 'manifest.json')

    # This config_json file is on the D:\ of the prod server, but that's actually the P:\ drive of the DM server
    # Use this to change the path
    file_path = os.path.splitdrive(config_json)[1]
    file_path_rdrive = os.path.join(r'P:', file_path)

    with open(file_path_rdrive) as data_file:
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

    # TODO BUILD OUT
    return json_response


def request_token():

    cred = settings.get_ini_file('arcgis_server', 'tokens')

    d = {"username": cred['username'],
         "password": cred['password'],
         "client": "requestip",
         "f": "json"}

    url = "http://gis-gfw.wri.org/arcgis/admin/generateToken"
    r = requests.post(url, data=d)

    response = json.loads(r.content)

    if 'error' in response.keys() or r.status_code != 200:
        raise Exception(response['message'], response['details'])

    return response['token']


def update_cache(map_service_path, scratch_workspace):

    source_mxd, cache_dir = find_src_mxd_and_cache_dir(map_service_path)
    logging.debug("Found source MXD: {0}".format(source_mxd))

    output_dir = util.create_temp_dir(scratch_workspace)
    scale_aoi = "591657527.591555;295828763.795777;147914381.897889;73957190.948944;" \
                "36978595.474472;18489297.737236;9244648.868618"

    min_scale = scale_aoi.split(';')[0]
    max_scale = scale_aoi.split(';')[-1]

    cache_dir_name = 'cache'

    arcpy.ManageTileCache_management(output_dir, "RECREATE_ALL_TILES", cache_dir_name, source_mxd, None, None,
                                     scale_aoi, None, None, min_scale, max_scale)

    src_layer_dir = os.path.join(output_dir, cache_dir_name, 'Layers')

    # TODO copy to cache dir

    sys.exit(0)
    #
    # arcpy.ManageTileCache_management(output_dir, "RECREATE_ALL_TILES", "cache", source_mxd)

