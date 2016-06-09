__author__ = 'Charlie.Hofmann'

import arcpy
import datetime
import logging

from datasource import DataSource


class TerraiDataSource(DataSource):
    """
    Terrai datasource class. Inherits from DataSource
    Used to download the source file and calculate dates in the VAT
    """
    def __init__(self, layerdef):
        logging.debug('Starting terrai datasource')
        super(TerraiDataSource, self).__init__(layerdef)

        self.layerdef = layerdef

    def get_layer(self):
        """
        Download the terrai datasource, add VAT and calculate dates
        :return: an updated layerdef with the local source for the layer.update() process
        """

        # self.data_source = self.download_file(self.data_source, self.download_workspace)
        #
        # self.build_table()
        #
        # self.calculate_dates()
        #
        # self.layerdef['source'] = self.data_source

        return self.layerdef


    



