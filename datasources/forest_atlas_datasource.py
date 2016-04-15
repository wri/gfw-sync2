__author__ = 'Charlie.Hofmann'

import arcpy
import datetime
import logging
import sys

from datasource import DataSource


class ForestAtlasDataSource(DataSource):
    """
    ForestAtlas datasource class. Inherits from DataSource
    """

    def __init__(self, layerdef):
        logging.debug('starting forest_atlas_datasource')

        super(ForestAtlasDataSource, self).__init__(layerdef)

        self.layerdef = layerdef

    def get_update_date_src_fc(self):
        date_list = []
        with arcpy.da.SearchCursor(self.data_source, ['last_edited_date']) as cursor:
            for row in cursor:

                date_list.append(row[0])

        return max(date_list)

    def get_layer(self):
        max_fc_date = self.get_update_date_src_fc()

        config_table_date = datetime.datetime.strptime(self.layerdef['last_updated'], "%m/%d/%Y")

        if max_fc_date > config_table_date:

            return self.layerdef

        else:
            logging.info('No new data for {0}'.format(self.name))
            logging.critical('Checked | {0}'.format(self.name))
            sys.exit(0)
