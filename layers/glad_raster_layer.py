__author__ = 'Thomas.Maschler'

import arcpy
import logging

from layers.raster_layer import RasterLayer

class GLADRasterLayer(RasterLayer):
    """
    GladRaster layer class. Inherits from RasterLayer
    """

    def __init__(self, layerdef):
        logging.debug('Starting glad_raster_layer')
        super(GLADRasterLayer, self).__init__(layerdef)

        # self._year_aoi = None
        # self.year_aoi = int(os.path.splitext(os.path.basename(self.source))[0][-4:])

    # # Validate name
    # @property
    # def year_aoi(self):
    #     return self._year_aoi
    #
    # @year_aoi.setter
    # def year_aoi(self, y):
    #     if not y or 1990 > y or y > 2030:
    #         warnings.warn("Input raster name does not end in a year value > 1990 and < 2030", Warning)
    #         sys.exit(2)
    #     self._year_aoi = y
    #
    # def getDate(self, date_int):
    #     if date_int == 0:
    #         date_int = 1
    #
    #     d = datetime.datetime.strptime(str(date_int), '%j')
    #     d.strftime('%Y/%m/%d')
    #     newd = d.replace(year = (self.year_aoi)).date()
    #
    #     newd_str = newd.strftime("%m/%d/%y")
    #
    #     return newd_str
    #
    # def populateDateField(self):
    #
    #     print 'Adding date field to {0}'.format(self.wgs84_file)
    #     fieldName = 'date'
    #     arcpy.AddField_management(self.wgs84_file, fieldName, 'TEXT', "", "", 10)
    #
    #     print 'Populating date field'
    #     with arcpy.da.UpdateCursor(self.wgs84_file, ['GRID_CODE', 'date']) as cursor:
    #         for row in cursor:
    #             row[1] = self.getDate(row[0])
    #
    #             cursor.updateRow(row)
    #
    # def add_iso_code(self):
    #
    #     print 'adding iso field and populating'
    #     self.iso_code = self.name.split('_')[-1].upper()
    #
    #     self.add_field_and_calculate(self.wgs84_file, 'iso', 'TEXT', 3, self.iso_code)

    def update(self):

        #Creates timestamped backup and download from source
        self.archive()

        # # Exports to WGS84 if current dataset isn't already
        # # We need this dataset to add the date info to
        # self.export_2_shp()

        #hardcoded - remove
        #self.wgs84_file = r"D:\GIS Data\GFW\temp\gfw-sync2_test\umd_landsat_alerts_idn_wgs84.shp"

        # self.populateDateField()
        #
        # self.add_iso_code()

        #hardcoded - remove
        # self.sync_cartodb("""iso = 'IDN'""")
        # self.sync_cartodb("""iso = '{0}'""".format(self.iso_code))

        self.copy_to_esri_output()


    

    

    



