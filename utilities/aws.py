import boto
import boto.ec2
import datetime
import logging
import time
import util


def get_timestamps(bucket, filelist):

    out_dict = {}

    s3 = boto.connect_s3()
    bucket = s3.lookup(bucket)

    for key in bucket:
        if key.name in filelist:
            out_dict[key.name] = datetime.datetime.strptime(key.last_modified, '%Y-%m-%dT%H:%M:%S.000Z')

    return out_dict


def set_processing_server_state(server_name, desired_state):

    token_info = util.get_token('boto.config')
    aws_access_key = token_info[0][1]
    aws_secret_key = token_info[1][1]

    ec2_conn = boto.ec2.connect_to_region('us-east-1', aws_access_key_id=aws_access_key,
                                          aws_secret_access_key=aws_secret_key)

    reservations = ec2_conn.get_all_reservations()
    for reservation in reservations:
        for instance in reservation.instances:
            if 'Name' in instance.tags:
                if instance.tags['Name'] == server_name:

                    server_instance = instance
                    break

    if server_instance.state != desired_state:
        logging.debug('Current server state is {0}. '
                      'Setting it to {1} now.'.format(server_instance.state, desired_state))

        if desired_state == 'running':
            server_instance.start()
        else:
            server_instance.stop()

        while server_instance.state != desired_state:
            logging.debug(server_instance.state)
            time.sleep(5)

            # Need to keep checking get updated instance status
            server_instance.update()

    logging.debug('Server {0} is now {1} at {2}'.format(server_name, server_instance.state, server_instance.ip_address))
    logging.debug('Sleeping for a minute to be sure server is up')
    time.sleep(60)

    return server_instance.ip_address
