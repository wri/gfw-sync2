import requests
import logging

from utilities import util


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

    if r.status_code == 500:
        raise ValueError('request failed')

    dataset_url = r'http://staging-api.globalforestwatch.org/dataset/{0}/concat'.format(dataset_id)

    payload = {'url': src_url}

    logging.debug('starting concat')
    logging.debug(payload)

    r = requests.post(dataset_url, headers=headers, json=payload)
    status = r.status_code

    if status == 204:
        logging.debug('Request succeeded!')
    else:
        logging.debug(r.text)
        raise ValueError('Request failed with code: {}'.format(status))
