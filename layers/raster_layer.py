__author__ = 'Thomas.Maschler'

import arcpy
import logging
import os
import shutil

from layer import Layer


class RasterLayer(Layer):
    """
    Raster layer class. Inherits from Layer
    """

    def __init__(self, layerdef):
        logging.debug('Starting raster_layer')
        super(RasterLayer, self).__init__(layerdef)

        # self.wgs84_file = None
        # self.export_file = None

    def archive(self):
        logging.info('Starting raster_layer.archive')
        self._archive(self.source, self.download_output, self.archive_output)

    def archive_source(self, ras_path):
        """
        Creates an archive of the input data (listed in the config table under 'source' before the process begins
        :param ras_path: path to input raster. required given that the self.source for some raster datasets
        are multiple rasters
        :return:
        """
        logging.info('Starting raster_layer.archive source for {0}'.format(self.name))
        archive_dir = os.path.dirname(self.archive_output)
        archive_src_dir = os.path.join(archive_dir, 'src')

        if not os.path.exists(archive_src_dir):
            os.mkdir(archive_src_dir)

        output_zip_name = os.path.basename(ras_path).replace('.tif', '.zip')

        src_archive_output = os.path.join(archive_src_dir, output_zip_name)
        self._archive(ras_path, None, src_archive_output)

    @staticmethod
    def copy_to_esri_output(input_ras, output_ras):
        logging.info('Starting to copy from {0} to esri_service_output: {1}'.format(input_ras, output_ras))
        shutil.copy(input_ras, output_ras)
        #arcpy.CopyRaster_management(input_ras, output_ras)

    def update(self):
        logging.info('Starting raster_layer.update for {0}'.format(self.name))

        # Creates timestamped backup and download from source
        self.archive()

        # Exports to WGS84 if current dataset isn't already
        # self.export_2_shp()

        # Moves to esri output destination-- basis for image services etc
        self.copy_to_esri_output()
