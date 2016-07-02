__author__ = 'Charlie.Hofmann'

import time
import logging
import boto.ec2

from layers.raster_layer import RasterLayer
from utilities import util


class GlobalForestChangeLayer(RasterLayer):
    """
    GlobalForestChange layer class. Inherits from RasterLayer
    """

    def __init__(self, layerdef):
        logging.debug('Starting global_forest_change_layer')
        super(GlobalForestChangeLayer, self).__init__(layerdef)

        self.server_name = 'TERRANLYSIS-GFW-DEV'
        self.server_ip = None

    def archive_source_rasters(self):
        """
        Create timestamped backup of source datasets
        :return:
        """
        for ras in self.source:
            self.archive_source(ras)

    def copy_to_esri_output_multiple(self):
        """
        Copy inputs downloaded from the source to the proper output location
        :return:
        """
        esri_output_list = self.esri_service_output.split(',')
        input_output_tuples = zip(self.source, esri_output_list)

        for input_ras, output_ras in input_output_tuples:
            self.copy_to_esri_output(input_ras, output_ras)

    def set_processing_server_state(self, desired_state):

        token_info = util.get_token('boto.config')
        aws_access_key = token_info[0][1]
        aws_secret_key = token_info[1][1]

        ec2_conn = boto.ec2.connect_to_region('us-east-1', aws_access_key_id=aws_access_key,
                                              aws_secret_access_key=aws_secret_key)

        reservations = ec2_conn.get_all_reservations()
        for reservation in reservations:
            for instance in reservation.instances:
                if 'Name' in instance.tags:
                    if instance.tags['Name'] == self.server_name:

                        server_instance = instance
                        break

        if server_instance.state != desired_state:
            print 'Current server state is {0}. Setting it to {1} now.'.format(server_instance.state, desired_state)

            if desired_state == 'running':
                server_instance.start()
            else:
                server_instance.stop()

            while server_instance.state != desired_state:
                print server_instance.state
                time.sleep(5)

                # Need to keep checking get updated instance status
                server_instance.update()

        self.server_ip = server_instance.ip_address

        print 'Server {0} is now {1}, IP: {2}'.format(self.server_name, server_instance.state, self.server_ip)

    def _update(self):

        # self.archive_source_rasters()
        #
        # self.copy_to_esri_output_multiple()

        pass


