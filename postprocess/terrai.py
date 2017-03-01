import subprocess
import logging

from utilities import util, update_elastic


def post_process(layerdef):
    """
    Run the update_country_pages script to update the data in the API
    :param layerdef: the layerdef
    :return:
    """

    if layerdef.gfw_env == 'PROD':
        logging.debug('Starting country page analysis')

        country_analysis_dir = r'D:\scripts\gfw-country-pages-analysis-2'

        cmd = ['python', 'update_country_stats.py', '-d', 'terra_i_alerts', '-a', 'gadm1_boundary', '-e', 'prod']
        subprocess.check_call(cmd, cwd=country_analysis_dir)

        util.hit_vizz_webhook('terrai-alerts')

        run_elastic_update()

    else:
        logging.debug('Not running gfw-country-pages-analysis-2; gfw_env is {0}'.format(layerdef.gfw_env))


def run_elastic_update():
    logging.debug('starting to update elastic')
    dataset_id = r'05e95470-a815-4d66-b879-d185bdde4785'

    api_version = 'staging'
    src_url = r'http://gfw2-data.s3.amazonaws.com/alerts-tsv/terrai.csv'

    update_elastic.delete_and_append(dataset_id, api_version, src_url)
