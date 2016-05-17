__author__ = 'Charlie.Hofmann'

import logging

from datasource import DataSource


class GladDataSource(DataSource):
    """
    GLAD datasource class. Inherits from DataSource
    Used to download the source files
    """
    def __init__(self, layerdef):
        logging.debug('Starting GLAD datasource')
        super(GladDataSource, self).__init__(layerdef)

        self.layerdef = layerdef

    def get_layer(self):
        """
        Download the GLAD datasets
        :return: an updated layerdef with the local source for the layer.update() process
        """

        raster_list = self.data_source.split(',')
        output_list = []

        for ras in raster_list:
            out_file = self.download_file(ras, self.download_workspace)
            output_list.append(out_file)

        self.layerdef['source'] = output_list

        return self.layerdef






