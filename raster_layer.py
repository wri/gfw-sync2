__author__ = 'Thomas.Maschler'

import os
import arcpy
import cartodb
arcpy.CheckOutExtension("Spatial")

from layer import Layer

arcpy.env.overwriteOutput = True


class RasterLayer(Layer):
    """
    Raster layer class. Inherits from Layer
    """

    def __init__(self, layerdef):
        print 'starting raster_layer'
        super(RasterLayer, self).__init__(layerdef)

        self.wgs84_file = None
        self.export_file = None

    def archive(self):
        self._archive(self.source, False)

    def project_to_wgs84(self):
        self._project_to_wgs84()

    def sync_cartodb(self, where_clause):
        cartodb.cartodb_sync(self.wgs84_file, self.cartodb_service_output, where_clause)

    def copy_to_esri_output(self):
        print 'Starting to copy from {0} to esri_service_output: {1}'.format(self.source, self.esri_service_output)
        arcpy.CopyRaster_management(self.source, self.esri_service_output)
        

    def export_2_shp(self):

        self.export_file = os.path.join(self.export_folder, self.name + ".shp")

        print "Starting to convert the input raster to point"
        print "Output: {0}".format(self.export_file)

        arcpy.RasterToPoint_conversion(self.source, self.export_file, "VALUE")

        if self.isWGS84(self.export_file):
            self.wgs84_file = self.export_file
            
        else:
            self.project_to_wgs84()

##        #hardcoded - need to remove this
##        self.wgs84_file = r'D:\GIS Data\GFW\temp\gfw-sync2_test\umd_landsat_alerts__borneo.shp'
            
        return


    def update(self):

        #Creates timestamped backup and download from source
        self.archive() 

        #Exports to WGS84 if current dataset isn't already
        self.export_2_shp()

        #Moves to esri output destination-- basis for image services etc
        self.copy_to_esri_output()

    

    

    



