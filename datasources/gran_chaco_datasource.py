__author__ = 'Asa.Strong'

import logging
import urlparse
import datetime
import sys

from datasource import DataSource
from utilities import aws
from utilities import google_sheet as gs

class GranChacoDataSource(DataSource):
    """
    Gran Chaco datasource class. Inherits from DataSource
    Used to download the source files
    """
    def __init__(self, layerdef):
        logging.debug('Starting Gran Chaco datasource')
        super(GranChacoDataSource, self).__init__(layerdef)

        self.layerdef = layerdef

        def get_layer(self):
            """
            Download the Gran Chaco dataset
            :return: an updated layerdef with the local source for the layer.update() process
            """

            output_list = []
            vect_url_list = self.data_source.split(',')


            for vect in vect_url_list:
                out_file = self.download_file(vect, self.download_workspace)
                output_list.append(out_file)

            self.layerdef['source'] = output_list
