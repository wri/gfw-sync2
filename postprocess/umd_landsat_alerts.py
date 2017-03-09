import logging
import subprocess

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

    util.hit_vizz_webhook('glad-alerts')

    run_elastic_update()


def run_elastic_update():

    logging.debug('starting to update elastic')
    dataset_id = r'e663eb09-04de-4f39-b871-35c6c2ed10b5'

    region_list = ['se_asia', 'africa', 'south_america']
    year_list = ['2016', '2017']

    api_version = 'prod'

    for year in year_list:
        for region in region_list:

            src_url = r'http://gfw2-data.s3.amazonaws.com/alerts-tsv/glad/{0}_{1}.csv'.format(region, year)
            delete_wc = "WHERE year = {0} AND region = '{1}'".format(year, region)

            update_elastic.delete_and_append(dataset_id, api_version, src_url, delete_wc)


