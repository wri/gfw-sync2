__author__ = 'Charlie.Hofmann'

import logging
import urlparse
import datetime
import sys

from datasource import DataSource
from utilities import aws_s3
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

        if self.find_updated_data(raster_url_list):
            output_list = []

            for ras in raster_url_list:
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

        #TODO remove this
        # gs.set_value('tech_title', 'umd_landsat_alerts', 'last_updated', self.gfw_env, '12/8/1987')

        config_sheet_datetime_text = gs.get_value('tech_title', 'umd_landsat_alerts', 'last_updated', self.gfw_env)
        config_sheet_datetime = datetime.datetime.strptime(config_sheet_datetime_text, '%m/%d/%Y')

        first_url = raster_url_list[0]
        netloc = urlparse.urlparse(first_url).netloc

        bucket = netloc.split('.')[0]

        raster_name_list = []

        for raster_url in raster_url_list:

            raster_name = urlparse.urlparse(raster_url).path.replace('/', '')
            raster_name_list.append(raster_name)

        time_stamp_dict = aws_s3.get_timestamps(bucket, raster_name_list)
        min_bucket_datetime = min(time_stamp_dict.values())

        return min_bucket_datetime > config_sheet_datetime












