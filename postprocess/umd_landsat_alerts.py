import os
import logging
import subprocess
import datetime
from isoweek import Week

from utilities import util
from utilities import update_elastic


def post_process(layerdef):

    # start country page analysis stuff (not map related)
    logging.debug("starting country page analytics")

    cmd = [r'C:\PYTHON27\ArcGISx6410.3\python', 'update_country_stats.py', '-d', 'umd_landsat_alerts']
    cwd = r'D:\scripts\gfw-country-pages-analysis-2'

    if layerdef.gfw_env == 'PROD':
        cmd += ['-e', 'prod']

    else:
        cmd += ['-e', 'staging']

    subprocess.check_call(cmd, cwd=cwd)

    # Running this manually for now, as no way to tell when dataset has finished saving in PROD
    # util.hit_vizz_webhook('glad-alerts')

    add_headers_to_s3()

    # region_list = ['se_asia', 'africa', 'south_america']
    country_list = 'PER'

    run_elastic_update(country_list)

    # make_climate_maps(region_list)


def get_current_hadoop_output(url_type=None):

    today = datetime.datetime.today().strftime('%Y%m%d')

    if url_type == 's3':
        return r's3://gfw2-data/alerts-tsv/temp/output-glad-summary-{}/part-'.format(today)

    else:
        return 'http://gfw2-data.s3.amazonaws.com/alerts-tsv/temp/output-glad-summary-{}/part-'.format(today)


def add_headers_to_s3():

    s3_path = get_current_hadoop_output('s3')

    today = datetime.datetime.today().strftime('%Y%m%d')
    local_path = r'I:\temp\glad_csv_download\{}.csv'.format(today)

    # download CSV without header
    cmd = ['aws', 's3', 'cp', s3_path, local_path]
    subprocess.check_call(cmd)

    temp_file = r'I:\temp\glad_csv_download\temp.csv'
    if os.path.exists(temp_file):
        os.remove(temp_file)

    # create file with header
    header_text = 'long,lat,confidence,year,julian_day,country_iso,state_id,dist_id,confidence_text'
    cmd = ['echo', header_text, '>', temp_file]
    subprocess.check_call(cmd, shell=True)

    # append GLAD CSV to it
    cmd = ['type', local_path, '>>', temp_file]
    subprocess.check_call(cmd, shell=True)

    # Copy back to s3
    cmd = ['aws', 's3', 'cp', temp_file, s3_path]
    subprocess.check_call(cmd)


def run_elastic_update(country_list):

    logging.debug('starting to update elastic')
    dataset_id = r'e663eb09-04de-4f39-b871-35c6c2ed10b5'

    year_list = ['2016', '2017']

    api_version = 'prod'

    for year in year_list:
        for country in country_list:

            delete_wc = "WHERE year = {0} AND region = '{1}'".format(year, country)
            update_elastic.delete_from_elastic(dataset_id, api_version, delete_wc)

    hadoop_output_url = get_current_hadoop_output()
    update_elastic.append_to_elastic(dataset_id, api_version, hadoop_output_url)


def make_climate_maps(region_list):

    logging.debug('starting make_climate_maps')

    gfw_sync_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.dirname(gfw_sync_dir)
    climate_maps_dir = os.path.join(scripts_dir, 'gfw-climate-glad-maps')

    python_exe = r'C:\PYTHON27\ArcGISx6410.3\python.exe'

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





