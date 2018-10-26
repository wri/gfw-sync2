import boto
import boto.ec2
import datetime
import logging
import time
import utilities.token_util

token_info = utilities.token_util.get_token('boto.config')
access_key = token_info[0][1]
secret_key = token_info[1][1]

ec2_conn = boto.ec2.connect_to_region('us-east-1', aws_access_key_id=access_key, aws_secret_access_key=secret_key)


def get_timestamps(bucket):

    out_dict = {}

    s3 = boto.connect_s3()
    bucket = s3.lookup(bucket, "/")

    for key in bucket:

        # ignore any files within folders-- only want top level
        if r'/' not in key.name:
            out_dict[key.name] = datetime.datetime.strptime(key.last_modified, '%Y-%m-%dT%H:%M:%S.000Z')

    return out_dict


def get_aws_instance(server_name):

    reservations = ec2_conn.get_all_reservations()
    for reservation in reservations:
        for instance in reservation.instances:
            if 'Name' in instance.tags:
                if instance.tags['Name'] == server_name:

                    server_instance = instance
                    break

    return server_instance


def set_server_instance_type(aws_instance_object, desired_type):

    instance_name = aws_instance_object.tags['Name']
    current_type = ec2_conn.get_instance_attribute(aws_instance_object.id, 'instanceType')['instanceType']

    if current_type != desired_type:
        logging.debug('Changing {0} from type {1} to {2}'.format(instance_name, current_type, desired_type))

        # Stop instance if it is running
        if aws_instance_object.state != 'stopped':
            set_processing_server_state(aws_instance_object, 'stopped')

        ec2_conn.modify_instance_attribute(aws_instance_object.id, 'instanceType', desired_type)

    else:
        logging.debug('Server {0} is already type {1}'.format(instance_name, desired_type))


def set_processing_server_state(aws_instance_object, desired_state):

    if aws_instance_object.state != desired_state:
        logging.debug('Current server state is {0}. '
                      'Setting it to {1} now.'.format(aws_instance_object.state, desired_state))

        if desired_state == 'running':
            aws_instance_object.start()
        elif desired_state == 'stopped':
            aws_instance_object.stop()
        else:
            raise ValueError('Unknown server status {0} requested'.format(desired_state))

        while aws_instance_object.state != desired_state:
            logging.debug(aws_instance_object.state)
            time.sleep(5)

            # Need to keep checking get updated instance status
            aws_instance_object.update()

        logging.debug('Server {0} is now '
                      '{1} at {2}'.format(aws_instance_object.tags['Name'], aws_instance_object.state,
                                          aws_instance_object.private_ip_address))

        logging.debug('Sleeping for a minute to be sure server is ready')
        time.sleep(60)

    return aws_instance_object.private_ip_address
