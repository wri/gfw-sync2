import os
import logging
import subprocess
from isoweek import Week

from utilities import util
from utilities import update_elastic


def post_process(layerdef):

    # start country page analysis stuff (not map related)
    logging.debug("starting country page analytics")
    cmd = ['python', 'update_country_stats.py', '-d', 'umd_landsat_alerts', '-a', 'gadm2_boundary']
    cwd = r'D:\scripts\gfw-country-pages-analysis-2'

    if layerdef.gfw_env == 'PROD':
        cmd += ['-e', 'prod']

    else:
        cmd += ['-e', 'staging']

    subprocess.check_call(cmd, cwd=cwd)

    # Running this manually for now, as no way to tell when dataset has finished saving in PROD
    # util.hit_vizz_webhook('glad-alerts')

    region_list = ['se_asia', 'africa', 'south_america']

    run_elastic_update(region_list)

    # make_climate_maps(region_list)


def run_elastic_update(region_list):

    logging.debug('starting to update elastic')
    dataset_id = r'e663eb09-04de-4f39-b871-35c6c2ed10b5'

    year_list = ['2016', '2017']

    api_version = 'prod'

    for year in year_list:
        for region in region_list:

            src_url = r'http://gfw2-data.s3.amazonaws.com/alerts-tsv/glad/{0}_{1}.csv'.format(region, year)
            delete_wc = "WHERE year = {0} AND region = '{1}'".format(year, region)

            update_elastic.delete_and_append(dataset_id, api_version, src_url, delete_wc)


def make_climate_maps(region_list):

    logging.debug('starting make_climate_maps')

    gfw_sync_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.dirname(gfw_sync_dir)
    climate_maps_dir = os.path.join(scripts_dir, 'gfw-climate-glad-maps')

    current_week = Week.thisweek()

    for i in range(1, 5):
        offset = current_week - i

        year = str(offset.year)
        week = str(offset.week)

        for region in region_list:

            cmd = ['python', 'create_map.py', '-y', year, '-w', week, '-r', region]
            logging.debug('calling subprocess:')
            logging.debug(cmd)

            subprocess.check_call(cmd, cwd=climate_maps_dir)





