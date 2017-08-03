import subprocess
import logging
import datetime
import os

from utilities import update_elastic, settings

def post_process(layerdef):
    """
    Run the forma_layer script to trigger the post process
    Postprocess will download the update output from country page analysis,
    add headers, and reupload to S3.
    :param layerdef: the layerdef
    :return:
    """
    logging.debug('Starting PostProcess')

    #Download GEE output to temp workspace
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    gee_path = 'gs://forma-2017/tmp/csv/forma_alerts_2012-01-01_{}.csvee_export.csv'.format(today)
    download_workspace = os.path.join(settings.get_settings('prod')['paths']['scratch_workspace'],
                                           'downloads', 'forma')

    gsutil_cmd = 'gsutil cp {0} {1}'.format(gee_path, download_workspace)
    subprocess.Popen(gsutil_cmd, shell=True, stderr=subprocess.PIPE)
    logging.debug('Csv copied from Google to File')

    #Copy GEE output to S3
    temp_file = download_workspace + '\\' + os.path.basename(gee_path)
    cmd = ['aws', 's3', 'cp', temp_file, 's3://gfw2-data/alerts-tsv/forma.csv']
    subprocess.check_call(cmd)
    logging.debug('File copied to S3')

    #Charlie to trigger country page analyses
    #>>>>>>>>>>>>>>country analysis<<<<<<<<<<<<<<<

    #copy output from Country pages down
    #will be replaced with: current_s3_path = update_elastic.get_current_hadoop_output('forma', 's3')
    # today_folder = datetime.datetime.today().strftime('%Y%m%d')
    # current_s3_path = 's3://gfw2-data/alerts-tsv/temp/output-forma-summary-{}/part-'.format(today_folder)
    current_s3_path = 's3://gfw2-data/alerts-tsv/temp/output-forma-summary-20170630/part-'

    #add headers to country analysis output
    header_text = 'alert_delta,lat,long,country_iso,day,value'
    update_elastic.add_headers_to_s3(layerdef, current_s3_path, header_text)
    logging.debug('headers added to analysis output')
