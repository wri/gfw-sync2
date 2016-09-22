__author__ = 'Charlie.Hofmann'

import logging
import urlparse
import datetime
import sys

from datasource import DataSource
from utilities import aws
from utilities import google_sheet as gs


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

        raster_url_list = self.data_source.split(',')

        updated_raster_url_list = self.find_updated_data(raster_url_list)

        if updated_raster_url_list:
            output_list = []

            for ras in updated_raster_url_list:
                out_file = self.download_file(ras, self.download_workspace)
                output_list.append(out_file)

            self.layerdef['source'] = output_list

        else:
            # Important for the script that reads the log file and sends an email
            # Including this 'Checked' message will show that we checked the layer but it didn't need updating
            logging.debug('Checked GLAD S3 bucket, no new data as compared to last timestamp in gfw-sync2 config')
            logging.critical('Checked | {0}'.format(self.name))
            sys.exit(0)

        return self.layerdef

    def find_updated_data(self, raster_url_list):

        updated_raster_url_list = []

        config_sheet_datetime_text = gs.get_value('tech_title', 'umd_landsat_alerts', 'last_updated', self.gfw_env)
        config_sheet_datetime = datetime.datetime.strptime(config_sheet_datetime_text, '%m/%d/%Y')

        first_url = raster_url_list[0]
        netloc = urlparse.urlparse(first_url).netloc

        bucket = netloc.split('.')[0]
        bucket_timestamps = aws.get_timestamps(bucket)

        for raster_url in raster_url_list:

            raster_name = urlparse.urlparse(raster_url).path.replace('/', '')
            raster_timestamp = bucket_timestamps[raster_name]

            if raster_timestamp > config_sheet_datetime:
                updated_raster_url_list.append(raster_url)

        return updated_raster_url_list












