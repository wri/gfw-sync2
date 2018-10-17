__author__ = 'Charlie.Hofmann'

import logging
import subprocess
import os
import sys

from layers.raster_layer import RasterLayer
from utilities import aws


class GlobalForestChangeLayer(RasterLayer):
    """
    GlobalForestChange layer class. Inherits from RasterLayer
    """

    def __init__(self, layerdef):
        logging.debug('Starting global_forest_change_layer')
        super(GlobalForestChangeLayer, self).__init__(layerdef)

        self.server_name = 'TERRANLYSIS-GFW-DEV'
        self.server_ip = None
        self.proc = None

    def archive_source_rasters(self):
        """
        Create timestamped backup of source datasets
        :return:
        """
        for ras in self.source:
            self.archive_source(ras)

    def create_tiles(self):

        server_instance = aws.get_aws_instance(self.server_name)
        aws.set_server_instance_type(server_instance, 'm4.10xlarge')

        # Required so that the machine now knows it's an m4.10xlarge
        server_instance.update()
        server_ip = aws.set_processing_server_state(server_instance, 'running')

        abspath = os.path.abspath(__file__)
        gfw_sync_dir = os.path.dirname(os.path.dirname(abspath))

        utilities_dir = os.path.join(gfw_sync_dir, 'utilities')
        tokens_dir = os.path.join(gfw_sync_dir, 'tokens')

        pem_file = os.path.join(tokens_dir, 'chofmann-wri.pem')
        host_name = 'ubuntu@{0}'.format(server_ip)

        region_str, year_str = self.lookup_region_year_from_source()

        fab_path = r"C:\PYTHON27\ArcGISx6410.6\Scripts\fab.exe"
        cmd = [fab_path, 'kickoff:{0},{1},{2},{3}'.format(self.name, region_str, year_str, self.gfw_env)]
        cmd += ['-i', pem_file, '-H', host_name]
        logging.debug('Running fabric: {0}'.format(cmd))

        has_error = False

        try:
            subprocess.check_call(cmd, cwd=utilities_dir)
        except subprocess.CalledProcessError:
            has_error = True

        server_instance = aws.get_aws_instance(self.server_name)
        aws.set_processing_server_state(server_instance, 'stopped')

        if has_error:
            logging.debug('Unsuccessful tile creation. Exiting.')
            sys.exit()

    def lookup_region_year_from_source(self):

        if self.name == 'terrai':
            region_list = ['eastern_hemi', 'latin']
            year_list = ['all']

        else:

            region_list = ['nsa', 'africa', 'se_asia']
            year_list = ['2018']

            if self.gfw_env == 'staging':
                region_list = ['nsa', 'africa', 'se_asia']

        return ';'.join(region_list), ';'.join(year_list)

    def update(self):

        if self.gfw_env == 'prod':
            self.create_tiles()

            self.archive_source_rasters()

            self.post_process()
        else:
            print 'GFW ENV is {}, not running create tiles at this time'.format(self.gfw_env)
