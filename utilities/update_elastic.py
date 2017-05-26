import requests
import logging

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
