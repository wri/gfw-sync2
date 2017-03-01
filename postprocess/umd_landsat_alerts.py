import logging
import subprocess
import requests

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

    # POST to kick off GLAD Alerts subscriptions now that we've updated the country-pages data
    api_token = util.get_token('gfw-rw-api-prod')

    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer {0}'.format(api_token)}
    url = r'https://production-api.globalforestwatch.org/subscriptions/notify-updates/glad-alerts'

    r = requests.post(url, headers=headers)
    logging.debug(r.text)

    run_elastic_update()


def run_elastic_update():

    logging.debug('starting to update elastic')
    dataset_id = r'274b4818-be18-4890-9d10-eae56d2a82e5'

    region_list = ['se_asia', 'africa', 'south_america']
    year_list = ['2016', '2017']

    api_version = 'staging'

    for year in year_list:
        for region in region_list:

            src_url = r'http://gfw2-data.s3.amazonaws.com/alerts-tsv/glad/{0}_{1}.csv'.format(region, year)
            delete_wc = "WHERE year = {1} AND region = '{2}'".format(year, region)

            update_elastic.delete_and_append(dataset_id, api_version, src_url, delete_wc)


