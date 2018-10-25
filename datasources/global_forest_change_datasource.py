__author__ = 'Charlie.Hofmann'

import logging
import urlparse
import datetime
import sys

from datasource import DataSource
from utilities import aws
from utilities import google_sheet as gs


class GlobalForestChange(DataSource):
    """
    GLAD datasource class. Inherits from DataSource
    Used to download the source files
    """
    def __init__(self, layerdef):
        logging.debug('Starting GlobalForestChange datasource')
        super(GlobalForestChange, self).__init__(layerdef)

        self.layerdef = layerdef

    def get_layer(self):
        """
        Download the source rasters from S3
        :return: an updated layerdef with the local source for the layer.update() process
        """

        # all umd_landsat_alerts updates now taken care of on terranalysis server
        if self.name == 'umd_landsat_alerts':
            pass

        else:
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
                logging.debug('Checked S3 bucket, no new data as compared to last timestamp in gfw-sync2 config')
                logging.critical('Checked | {0}'.format(self.name))
                sys.exit(0)

        return self.layerdef

    def find_updated_data(self, raster_url_list):

        updated_raster_url_list = []

        config_sheet_datetime_text = gs.get_value('tech_title', self.name, 'last_updated', self.gfw_env)
        config_sheet_datetime = datetime.datetime.strptime(config_sheet_datetime_text, '%m/%d/%Y')

        # order is important here-- key names are the same + don't want to overwrite proper timestamps
        if self.name == 'umd_landsat_alerts':
            bucket = 'gfw-gee-glad-export'
        else:
            bucket = 'terra-i'

        bucket_timestamps = {}

        output_dict = aws.get_timestamps(bucket)

        # add this to our current dict
        bucket_timestamps.update(output_dict)

        for raster_url in raster_url_list:

            raster_name = urlparse.urlparse(raster_url).path.replace('/', '')
            raster_timestamp = bucket_timestamps[raster_name]

            if raster_timestamp > config_sheet_datetime:
                updated_raster_url_list.append(raster_url)

        return updated_raster_url_list












