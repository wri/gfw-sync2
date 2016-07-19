__author__ = 'Charlie.Hofmann'

import os
import arcpy
import logging

from layers.global_forest_change_layer import GlobalForestChangeLayer


class TerraiRasterLayer(GlobalForestChangeLayer):
    """
    TerraiRasterLayer layer class. Inherits from GlobalForestChangeLayer
    """

    def __init__(self, layerdef):
        logging.debug('Starting glad_raster_layer')
        super(TerraiRasterLayer, self).__init__(layerdef)

        self.processing_dir = r'R:\glad_alerts\processing'
        self.mosaic_gdb = r'R:\glad_alerts\filter_glad_png.gdb'

        self.region_list = ['africa', 'asia', 'south_america']
        self.band_list = ['band1_day', 'band2_day', 'band3_conf_and_year', 'band4_intensity']

        self.overview_template_fc = r'R:\glad_alerts\processing\footprint\footprint.gdb\final_footprint'

    def update_image_service(self):
        #copy data to R drive
        self.copy_to_esri_output_multiple()

        #calculate stats on files and mosaic
        self.calculate_stats()

        #Restart image service
        self.stop_service('image_services/terrai_analysis')
        self.start_service('image_services/terrai_analysis')

    @staticmethod
    def extract_attribute_values(input_ras):

        out_list =[]

        with arcpy.da.SearchCursor(input_ras, ['Value']) as cursor:
            for row in cursor:
                out_list.append(row[0])

        return out_list

    @staticmethod
    def build_remap_range_table(raster_value_list, band_number):
        remap_table = []

        for input_val in raster_value_list:

            # 0 appears to be a no-data type value, certainly not an alert
            if input_val == 0:
                pass

            else:
                year = 2004 + int((input_val - 1) /23)
                day = ((input_val % 23) * 16) + 1

                if band_number == 1:
                    if day > 255:
                        result = 255
                    else:
                        result = day

                elif band_number == 2:
                    if day < 255:
                        day = 255

                    result = 255 - day

                elif band_number == 3:

                    # Include a leading '2' to say that the pixel is confirmed
                    # There is no confirmed/unconfirmed for terra-i, but this matches
                    # the schema used for GLAD
                    result = int('2' + str(year)[2:4])

                elif band_number == 4:
                    result = 255

                remap_table.append('{0} {1} {2}'.format(input_val, input_val, result))

        # Reclassify expects a semi-colon delimited list of values
        esri_formatted = ';'.join(remap_table)

        return esri_formatted

    def reclassify_rasters(self):

        source_dir = os.path.join(self.processing_dir, 'source')
        arcpy.env.workspace = source_dir

        source_list = arcpy.ListRasters()

        for source_ras in source_list:
            try:
                ras_vals = self.extract_attribute_values(source_ras)

            except RuntimeError:
                arcpy.BuildRasterAttributeTable_management(source_ras)
                ras_vals = self.extract_attribute_values(source_ras)

            for band_number in range(1, 5):

                input_ras = os.path.join(source_dir, source_ras)
                remap_table = self.build_remap_range_table(ras_vals, band_number)

                output_dir = os.path.join(source_dir, 'reclassified')
                output_name = os.path.splitext(os.path.basename(input_ras))[0] + '_band{0}.tif'.format(band_number)
                output_ras = os.path.join(output_dir, output_name)

                logging.debug('Reclassifying {0} to {1}'.format(os.path.basename(input_ras), output_name))
                arcpy.gp.Reclassify_sa(input_ras, "Value", remap_table, output_ras, "DATA")

    def project_rasters(self):

        reclass_dir = os.path.join(self.processing_dir, 'source', 'reclassified')

        pass

    def update(self):

        # self.reclassify_rasters()
        #
        # self.project_rasters()

        self.update_image_service

        # self._update()
