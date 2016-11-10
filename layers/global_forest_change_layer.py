__author__ = 'Charlie.Hofmann'

import logging
import arcpy
import subprocess
import os

from layers.raster_layer import RasterLayer
from utilities import aws
from utilities import arcgis_server


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

    def copy_to_esri_output_multiple(self):
        """
        Copy inputs downloaded from the source to the proper output location copy_to_esri_output_multiple
        """
        arcpy.env.overwriteOutput = True
        output_hash = {}
        input_output_tuples = []
        esri_output_list = self.esri_service_output.split(',')
        # input_output_tuples = zip(self.source, esri_output_list)

        for y in range(0,len(esri_output_list)):
            output_name = esri_output_list[y].split("\\")[-1].split("\\")[-1]
            output_hash[output_name] = esri_output_list[y]

        for x in range(0,len(self.source)):
            source_name = self.source[x].split("\\")[-1]
            input_output_tuples.append((self.source[x], output_hash[source_name]))

        logging.debug(input_output_tuples)

        for input_ras, output_ras in input_output_tuples:
            self.copy_to_esri_output(input_ras, output_ras)

    def calculate_stats(self):
        '''
        calculate stats on rasters and mosaics
        '''
        esri_raster_list = self.esri_service_output.split(',')
        esri_mosaic_list = self.esri_mosaics.split(',')

        for raster in esri_raster_list:
            arcpy.CalculateStatistics_management(raster, "1", "1", "", "OVERWRITE", "")
            logging.debug("stats calculated on raster")

        for mosaic in esri_mosaic_list:
            arcpy.CalculateStatistics_management(mosaic, "1", "1", "", "OVERWRITE", "")
            logging.debug("stats calculated on mosaic")

    def update_image_service(self):
        arcpy.env.overwriteOutput = True

        logging.debug("running update_image_service")
        # Copy downloaded data to R drive
        self.copy_to_esri_output_multiple()

        # Calculate stats on files and mosaics
        self.calculate_stats()

        # Will update two GFW image services
        # http://gis-gfw.wri.org/arcgis/rest/services/image_services/glad_alerts_analysis/ImageServer
        # http://gis-gfw.wri.org/arcgis/rest/services/image_services/glad_alerts_con_analysis/ImageServer
        service_dict = {'umd_landsat_alerts': 'glad_alerts_analysis', 'terrai': 'terrai_analysis'}
        service_name = service_dict[self.name]

        service = r'image_services/{0}'.format(service_name)

        for i in range(0, 2):
            arcgis_server.set_service_status(service, 'stop')
            arcgis_server.set_service_status(service, 'start')

    def start_visualization_process(self):

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

        regions_to_update = ','.join(self.lookup_regions_from_source())

        cmd = ['fab', 'kickoff:{0},{1}'.format(self.name, regions_to_update), '-i', pem_file, '-H', host_name]
        logging.debug('Running fabric: {0}'.format(cmd))

        self.proc = subprocess.Popen(cmd, cwd=utilities_dir, stdout=subprocess.PIPE)

    def finish_visualization_process(self):

        while True:
            line = self.proc.stdout.readline().rstrip()

            if line != '****FAB SUBPROCESS COMPLETE****':
                logging.debug(line)
            else:
                break

        server_instance = aws.get_aws_instance(self.server_name)
        aws.set_processing_server_state(server_instance, 'stopped')

    def lookup_regions_from_source(self):
        region_list = []

        if self.name == 'terrai':
            region_list = ['eastern_hemi', 'latin']

        else:
            lkp_dict = {'roc': 'roc',
                        'uganda': 'uganda',
                        'peru': 'south_america',
                        'brazil': 'south_america',
                        'FE': 'russia',
                        'borneo': 'asia'}

            for output_raster in self.source:
                country = os.path.basename(output_raster).split('_')[0]

                region = lkp_dict[country]
                region_list.append(region)

            # Remove duplicates
            region_list = list(set(region_list))

        return region_list

    def update(self):

        if self.gfw_env == 'DEV':
            self.update_image_service()

        else:
            self.start_visualization_process()

            self.update_image_service()

            self.finish_visualization_process()

            self.archive_source_rasters()

        self.post_process()
