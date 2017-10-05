import os
import logging
import subprocess
from isoweek import Week

from utilities import util
from utilities import update_elastic


def post_process(layerdef):

    if layerdef.gfw_env == 'staging':
        raise ValueError('Not running postprocess for staging currently')

    # start country page analysis stuff (not map related)
    logging.debug("starting country page analytics")

    cmd = [r'C:\PYTHON27\ArcGISx6410.5\python', 'update_country_stats.py', '-d', 'umd_landsat_alerts']
    cwd = r'D:\scripts\gfw-country-pages-analysis-2'

    cmd += ['-e', layerdef.gfw_env]
    subprocess.check_call(cmd, cwd=cwd)

    current_s3_path = update_elastic.get_current_hadoop_output('glad', 's3')
    header_text = 'long,lat,confidence,year,julian_day,country_iso,state_id,dist_id,confidence_text'

    update_elastic.add_headers_to_s3(layerdef, current_s3_path, header_text)

    # updating basically everything except RUS at this point
    country_list = ['BDI', 'BRA', 'BRN', 'CAF', 'CMR', 'COD', 'COG', 'ECU', 'GAB', 'GNQ',
                    'IDN', 'MYS', 'PER', 'PNG', 'RWA', 'SGP', 'TLS', 'UGA']

    run_elastic_update(country_list, layerdef.gfw_env)

    util.hit_vizz_webhook('glad-alerts')

    # make_climate_maps(region_list)


def run_elastic_update(country_list, api_version):

    logging.debug('starting to update elastic')

    if api_version == 'prod':
        dataset_id = r'e663eb09-04de-4f39-b871-35c6c2ed10b5'
    elif api_version == 'staging':
        dataset_id = r'a228c22c-e99a-4df3-b627-a1825e7176f2'
    else:
        raise ValueError('unknown API version supplied: {}'.format(api_version))

    year_list = ['2016', '2017']

    for year in year_list:
        for country in country_list:

            delete_wc = "WHERE year = {0} AND country_iso = '{1}'".format(year, country)
            update_elastic.delete_from_elastic(dataset_id, api_version, delete_wc)

    hadoop_output_url = update_elastic.get_current_hadoop_output('glad')
    update_elastic.append_to_elastic(dataset_id, api_version, hadoop_output_url)


def make_climate_maps(region_list):

    logging.debug('starting make_climate_maps')

    gfw_sync_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.dirname(gfw_sync_dir)
    climate_maps_dir = os.path.join(scripts_dir, 'gfw-climate-glad-maps')

    python_exe = r'C:\PYTHON27\ArcGISx6410.5\python'

    current_week = Week.thisweek()

    for i in range(1, 5):
        offset = current_week - i

        year = str(offset.year)
        week = str(offset.week)

        for region in region_list:

            cmd = [python_exe, 'create_map.py', '-y', year, '-w', week, '-r', region]
            logging.debug('calling subprocess:')
            logging.debug(cmd)

            subprocess.check_call(cmd, cwd=climate_maps_dir)





