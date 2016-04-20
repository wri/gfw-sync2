__author__ = 'Charlie.Hofmann'

import arcpy
import datetime
import logging
import sys

from datasource import DataSource


class ForestAtlasDataSource(DataSource):
    """
    ForestAtlas datasource class. Inherits from DataSource
    Used to find the max value in the 'last_updated' field of a source dataset and compare this to the
    date listed in the config table. If the source dataset has been updated, will kick off the update process.
    Otherwise will exit.
    """

    def __init__(self, layerdef):
        logging.debug('Starting forest_atlas_datasource')

        super(ForestAtlasDataSource, self).__init__(layerdef)

        self.layerdef = layerdef

    def get_update_date_src_fc(self):
        date_list = []
        with arcpy.da.SearchCursor(self.data_source, ['last_edited_date']) as cursor:
            for row in cursor:

                date_list.append(row[0])

        return max(date_list)

    def get_layer(self):
        """Called by layer_decision_tree.py. If it needs to be updated, returns the layerdef. Otherwise exits"""
        max_fc_date = self.get_update_date_src_fc()

        config_table_date = datetime.datetime.strptime(self.layerdef['last_updated'], "%m/%d/%Y")

        if max_fc_date > config_table_date:

            return self.layerdef

        else:
            logging.info('No new data for {0}'.format(self.name))
            logging.critical('Checked | {0}'.format(self.name))
            sys.exit(0)
