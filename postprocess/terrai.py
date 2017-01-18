import subprocess
import logging


def post_process(layerdef):
    """
    Run the update_country_pages script to update the data in the API
    :param layerdef: the layerdef
    :return:
    """

    if layerdef.gfw_env == 'PROD':
        logging.debug('Starting country page analysis')

        country_analysis_dir = r'D:\scripts\gfw-country-pages-analysis-2'

        cmd = ['python', 'update_country_stats.py', '-d', 'terra_i_alerts', '-a', 'gadm1_boundary']
        subprocess.check_call(cmd, cwd=country_analysis_dir)

    else:
        logging.debug('Not running gfw-country-pages-analysis-2; gfw_env is {0}'.format(layerdef.gfw_env))
