__author__ = 'Charlie.Hofmann'

import arcpy
import datetime
import logging

from datasource import DataSource

class TerraiDataSource(DataSource):
    """
    Terrai datasource class. Inherits from DataSource
    """

    def __init__(self, layerdef):
        logging.debug('starting terrai datasource')

        super(TerraiDataSource, self).__init__(layerdef)

        self.layerdef = layerdef


    def build_table(self):

        logging.debug("building attribute table")

        arcpy.BuildRasterAttributeTable_management(self.source, "Overwrite")
        arcpy.AddField_management(self.source, "date", "DATE")

    def calculate_dates(self):

        logging.debug("calculating dates")

        with arcpy.da.UpdateCursor(self.source, ['Value','date']) as cursor:
            for row in cursor:
                gridcode = row[0]
                year = 2004+int((gridcode)/23)

                year_format = datetime.datetime.strptime(str(year) +"/01/01",'%Y/%m/%d')
                days = datetime.timedelta(days=(gridcode%23)*16)
                date_formatted = (year_format+days).strftime('%m/%d/%Y')

                row[1]= date_formatted
                cursor.updateRow(row)

    def get_layer(self):

        local_file = self.download_file(self.source, self.download_workspace)
        self.source = local_file

        self.build_table()

        self.calculate_dates()

        self.layerdef['source'] = self.source

        return self.layerdef


    



