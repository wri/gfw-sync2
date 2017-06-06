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

        fab_path = r"C:\PYTHON27\ArcGISx6410.5\Scripts\fab.exe"
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
            # region_list = []
            # year_list = []
            #
            # lkp_dict = {'peru': 'south_america', 'brazil': 'south_america',
            #             'FE': 'russia', 'borneo': 'asia',
            #             'SEA': 'se_asia',
            #             'Africa': 'africa'}
            #
            # for output_raster in self.source:
            #     ras_name = os.path.basename(output_raster)
            #     country = ras_name.split('_')[0]
            #
            #     region = lkp_dict[country]
            #     region_list.append(region)
            #
            #     digits_only = [s for s in ras_name if s.isdigit()]
            #     year = ''.join(digits_only)
            #
            #     year_list.append(year)
            #
            # # Remove duplicates
            # region_list = list(set(region_list))
            # year_list = list(set(year_list))
            #
            # # This shouldn't happen. No way that 3 years are updated at the same time. Max is 2.
            # if len(year_list) > 2:
            #     logging.debug('Exiting. Found year list > 2:')
            #     logging.debug(year_list)
            #     sys.exit(1)

            # Only south_america being updated currently
            region_list = ['south_america']
            year_list = ['2016', '2017']

        return ';'.join(region_list), ';'.join(year_list)

    def update(self):

        self.create_tiles()

        self.archive_source_rasters()

        self.post_process()
