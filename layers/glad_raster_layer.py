__author__ = 'Charlie.Hofmann'

import os
import subprocess
import time
import logging
import arcpy

from layers.global_forest_change_layer import GlobalForestChangeLayer


class GladRasterLayer(GlobalForestChangeLayer):
    """
    GladRaster layer class. Inherits from GlobalForestChangeLayer
    """

    def __init__(self, layerdef):
        logging.debug('Starting glad_raster_layer')
        super(GladRasterLayer, self).__init__(layerdef)

        self.proc = None

    def update_image_service(self):
        # Will update two GFW image services
        # http://gis-gfw.wri.org/arcgis/rest/services/image_services/glad_alerts_analysis/ImageServer
        # http://gis-gfw.wri.org/arcgis/rest/services/image_services/glad_alerts_con_analysis/ImageServer

        #step 1- copy to R drive .source to .esri_service_output
        copy_to_esri_output_multiple()

        #step 2 calculate stats on rasters
        for file in self.esri_service_output:
            arcpy.CalculateStatistics_management(file, "1", "1", "", "OVERWRITE", "")
            print "stats calculated on raster"

        #step 3 calculate stats on mosaics, possibly add to the spreadsheet?
        # self.mosaic_gdb = [r'R:\glad_alerts\glad_alerts_analysis.gdb', 

        print "Asa's stuff goes here"
        print 'Source rasters are here: ' + ', '.join(self.source)
        print 'Output rasters should be copied here:' + ', '.join(self.esri_service_output.split(','))

        # All of the above is set in the Google Doc - feel free to change if necessary
        # https://docs.google.com/spreadsheets/d/1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI/edit#gid=0


    def start_visualization_process(self):

        self.set_processing_server_state('running')

        abspath = os.path.abspath(__file__)
        gfw_sync_dir = os.path.dirname(os.path.dirname(abspath))

        utilities_dir = os.path.join(gfw_sync_dir, 'utilities')
        tokens_dir = os.path.join(gfw_sync_dir, 'tokens')

        pem_file = os.path.join(tokens_dir, 'chofmann-wri.pem')
        host_name = 'ubuntu@{0}'.format(self.server_ip)

        cmd = ['fab', 'kickoff:GLAD', '-i', pem_file, '-H', host_name]
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

        self.update_image_service() #will update the analysis

        self.finish_visualization_process()

        self._update()
