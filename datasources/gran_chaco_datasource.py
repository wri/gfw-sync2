__author__ = 'Asa.Strong'

import logging
import urlparse
import datetime
import sys
import requests

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

        def download_gran_chaco(self, data_url, download_workspace):

            #Get data from source
            logging.debug('Downloading Gran Chaco from S3')
            self.download_file(data_url, download_workspace)

        def get_layer(self):
            """
            Download the Gran Chaco dataset
            :return: an updated layerdef with the local source for the layer.update() process
            """
            #call download data method
            self.download_gran_chaco(self.data_source, self.download_workspace)

            #Trigger vector process
            self.layerdef.source = self.download_workspace + '\\gran_chaco_deforestation.shp'
