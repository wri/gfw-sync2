import subprocess
import logging

from utilities import util, update_elastic


def post_process(layerdef):
    """
    Run the update_country_pages script to update the data in the API
    :param layerdef: the layerdef
    :return:
    """

    if layerdef.gfw_env == 'prod':
        logging.debug('Starting country page analysis')

        country_analysis_dir = r'D:\scripts\gfw-country-pages-analysis-2'
        cmd = [r'C:\PYTHON27\ArcGISx6410.5\python', 'update_country_stats.py']
        cmd += ['-d', 'terra_i_alerts', '-e', layerdef.gfw_env]

        # subprocess.check_call(cmd, cwd=country_analysis_dir)

        # Running this manually for now, as no way to tell when dataset has finished saving in PROD
        # util.hit_vizz_webhook('terrai-alerts')

        current_s3_path = update_elastic.get_current_hadoop_output('terrai', 's3')
        header_text = 'long,lat,year,day,country_iso,state_id,dist_id'

        update_elastic.add_headers_to_s3(layerdef, current_s3_path, header_text)

        run_elastic_update(layerdef.gfw_env)

    else:
        logging.debug('Not running gfw-country-pages-analysis-2; gfw_env is {0}'.format(layerdef.gfw_env))


def run_elastic_update(api_version):
    logging.debug('starting to update elastic')
    dataset_id = r'bb80312e-b514-48ad-9252-336408603591'

    src_url = update_elastic.get_current_hadoop_output('terrai')

    update_elastic.append_to_elastic(dataset_id, api_version, src_url, append_type='data-overwrite')
