__author__ = 'Charlie.Hofmann'

import logging
import arcpy
import subprocess
import os

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

    def copy_to_esri_output_multiple(self):
        """
        Copy inputs downloaded from the source to the proper output location copy_to_esri_output_multiple
        """
        arcpy.env.overwriteOutput = True
        esri_output_list = self.esri_service_output.split(',')
        input_output_tuples = zip(self.source, esri_output_list)

        for input_ras, output_ras in input_output_tuples:
            logging.debug(input_ras, output_ras)
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

    def set_service_status(self, action):
        auth_key = r'D:\scripts\gfw-sync2\tokens\arcgis_server_pass'

        service_dict = {'umd_landsat_alerts': 'glad_alerts_analysis', 'terrai': 'terrai_analysis'}
        service_name = service_dict[self.name]

        service = r'image_services/{0}'.format(service_name)

        with open(auth_key, 'r') as myfile:
            password = myfile.read().replace('\n', '')

        cwd = r"C:\Program Files\ArcGIS\Server\tools\admin"
        cmd = ['python', "manageservice.py", '-u', 'astrong', '-p', password]
        cmd += ['-s', 'http://gis-gfw.wri.org/arcgis/admin', '-n', service, '-o', action]

        # Added check_call so it will crash if the subprocess fails
        subprocess.check_call(cmd, cwd=cwd)
        logging.debug("service {0} complete".format(action))

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
        for i in range(0, 2):
            self.set_service_status('stop')
            self.set_service_status('start')

    def start_visualization_process(self):

        server_ip = aws.set_processing_server_state(self.server_name, 'running')

        abspath = os.path.abspath(__file__)
        gfw_sync_dir = os.path.dirname(os.path.dirname(abspath))

        utilities_dir = os.path.join(gfw_sync_dir, 'utilities')
        tokens_dir = os.path.join(gfw_sync_dir, 'tokens')

        pem_file = os.path.join(tokens_dir, 'chofmann-wri.pem')
        host_name = 'ubuntu@{0}'.format(server_ip)

        cmd = ['fab', 'kickoff:{0}'.format(self.name), '-i', pem_file, '-H', host_name]
        self.proc = subprocess.Popen(cmd, cwd=utilities_dir, stdout=subprocess.PIPE)

    def finish_visualization_process(self):

        while True:
            line = self.proc.stdout.readline().rstrip()

            if line != '':
                logging.debug(line)
            else:
                break

        self.set_processing_server_state('stopped')

    def update(self):

        self.start_visualization_process()

        self.update_image_service()

        self.finish_visualization_process()

        self.archive_source_rasters()
