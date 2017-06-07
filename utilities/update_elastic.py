import os
import requests
import logging
import datetime
import subprocess

from utilities import util


def get_headers(api_version):
    if api_version == 'staging':
        api_url = r'http://staging-api.globalforestwatch.org'
        token = util.get_token('gfw-rw-api-staging')
    elif api_version == 'prod':
        api_url = r'http://production-api.globalforestwatch.org'
        token = util.get_token('gfw-rw-api-prod')
    else:
        raise ValueError('unknown api_version: {}'.format(api_version))

    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}

    return headers, api_url


def append_to_elastic(dataset_id, api_version, src_url, append_type='concat'):

    headers, api_url = get_headers(api_version)
    dataset_url = r'{0}/dataset/{1}/{2}'.format(api_url, dataset_id, append_type)

    payload = {"url": src_url,
               "provider": "csv",
               "legend": {
                   "long": "long",
                   "lat": "lat"}
               }

    logging.debug('starting {}'.format(append_type))
    logging.debug(payload)

    r = requests.post(dataset_url, headers=headers, json=payload)
    status = r.status_code

    if status == 204:
        logging.debug('Request succeeded!')
    else:
        print r.text
        logging.debug(r.text)
        raise ValueError('Request failed with code: {}'.format(status))


def delete_from_elastic(dataset_id, api_version, delete_where_clause):

    headers, api_url = get_headers(api_version)

    delete_url = r'{0}/query/{1}'.format(api_url, dataset_id)

    sql = "DELETE FROM index_{0}".format(dataset_id.replace('-', ''))

    if delete_where_clause:
        sql += ' ' + delete_where_clause

    qry_parms = {"sql": sql}

    logging.debug('starting delete request')
    logging.debug(qry_parms)

    r = requests.get(delete_url, headers=headers, params=qry_parms)

    logging.debug(r.status_code)
    logging.debug(r.json())


def delete_and_append(dataset_id, api_version, src_url, delete_where_clause=None):

    if api_version == 'staging':
        api_url = r'http://staging-api.globalforestwatch.org'
        token = util.get_token('gfw-rw-api-staging')
    elif api_version == 'prod':
        api_url = r'http://production-api.globalforestwatch.org'
        token = util.get_token('gfw-rw-api-prod')
    else:
        raise ValueError('unknown api_version: {}'.format(api_version))

    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}

    delete_url = r'{0}/query/{1}'.format(api_url, dataset_id)

    sql = "DELETE FROM index_{0}".format(dataset_id.replace('-', ''))

    if delete_where_clause:
        sql += ' ' + delete_where_clause

    qry_parms = {"sql": sql}

    logging.debug('starting delete request')
    logging.debug(qry_parms)

    r = requests.get(delete_url, headers=headers, params=qry_parms)

    logging.debug(r.status_code)
    logging.debug(r.json())

    # Temporarily remove this-- request will timeout and return a 500, but delete will execute
    # Raul is making this an async request, because it takes awhile to delete all the rows
    # if r.status_code != 200:
    #     raise ValueError('request failed with status code {}'.format(r.status_code))

    dataset_url = r'{0}/dataset/{1}/concat'.format(api_url, dataset_id)

    payload = {"url": src_url,
               "provider": "csv",
               "legend": {
                   "long": "long",
                   "lat": "lat"}
               }

    logging.debug('starting concat')
    logging.debug(payload)

    r = requests.post(dataset_url, headers=headers, json=payload)
    status = r.status_code

    if status == 204:
        logging.debug('Request succeeded!')
    else:
        print r.text
        logging.debug(r.text)
        raise ValueError('Request failed with code: {}'.format(status))


def add_headers_to_s3(layerdef, s3_url, header_csv_str):

    today = datetime.datetime.today().strftime('%Y%m%d')

    temp_s3_dir = os.path.join(layerdef.scratch_workspace, 'temp_s3_download')
    local_path = os.path.join(temp_s3_dir, '{}.csv'.format(today))

    # download CSV without header
    cmd = ['aws', 's3', 'cp', s3_url, local_path]
    subprocess.check_call(cmd)

    temp_file = os.path.join(temp_s3_dir, 'temp.csv')
    if os.path.exists(temp_file):
        os.remove(temp_file)

    # create file with header
    cmd = ['echo', header_csv_str, '>', temp_file]
    subprocess.check_call(cmd, shell=True)

    # append GLAD CSV to it
    cmd = ['type', local_path, '>>', temp_file]
    subprocess.check_call(cmd, shell=True)

    # Copy back to s3
    cmd = ['aws', 's3', 'cp', temp_file, s3_url]
    subprocess.check_call(cmd)


def get_current_hadoop_output(alert_type, url_type=None):
    today = datetime.datetime.today().strftime('%Y%m%d')

    if url_type == 's3':
        return r's3://gfw2-data/alerts-tsv/temp/output-{}-summary-{}/part-'.format(alert_type, today)

    else:
        return 'http://gfw2-data.s3.amazonaws.com/alerts-tsv/temp/output-{}-summary-{}/part-'.format(alert_type, today)
