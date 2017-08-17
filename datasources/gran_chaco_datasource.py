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

        def get_layer(self):
            """
            Download the Gran Chaco dataset
            :return: an updated layerdef with the local source for the layer.update() process
            """

            #Get data from source
            logging.debug('Downloading Gran Chaco from S3')
            url = self.data_source
            z = self.download_file(url, self.download_workspace)

            #Trigger vector process
            self.layerdef.source = self.download_workspace + '\\gran_chaco_deforestation.shp'
