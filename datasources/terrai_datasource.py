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

    def build_table(self):
        logging.debug("building attribute table")

        arcpy.BuildRasterAttributeTable_management(self.data_source, "Overwrite")
        arcpy.AddField_management(self.data_source, "date", "DATE")

    def calculate_dates(self):
        logging.debug("calculating dates")

        with arcpy.da.UpdateCursor(self.data_source, ['Value', 'date']) as cursor:
            for row in cursor:
                gridcode = row[0]
                year = 2004+int((gridcode)/23)

                year_format = datetime.datetime.strptime(str(year) +"/01/01", '%Y/%m/%d')
                days = datetime.timedelta(days=(gridcode%23)*16)
                date_formatted = (year_format+days).strftime('%m/%d/%Y')

                row[1] = date_formatted
                cursor.updateRow(row)

    def get_layer(self):
        """
        Download the terrai datasource, add VAT and calculate dates
        :return: an updated layerdef with the local source for the layer.update() process
        """

        self.data_source = self.download_file(self.data_source, self.download_workspace)

        self.build_table()

        self.calculate_dates()

        self.layerdef['source'] = self.data_source

        return self.layerdef


    



