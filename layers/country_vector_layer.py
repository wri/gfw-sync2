__author__ = 'Thomas.Maschler'

import logging
import os
import sys
import arcpy

from utilities import google_sheet
from utilities import field_map
from utilities import util
from vector_layer import VectorLayer


class CountryVectorLayer(VectorLayer):
    """
    CountryVectorLayer layer class. Inherits from VectorLayer
    """

    def __init__(self, layerdef):
        logging.debug('starting country_vector_layer')
        super(CountryVectorLayer, self).__init__(layerdef)

    def check_country_fields(self, field_list):

        result_list = []

        for fc in field_list:

            if 'country' in util.list_fields(fc, self.gfw_env):
                result_list.append(True)

            else:
                print "No country code found for {0}".format(fc)
                result_list.append(False)

        if len(set(result_list)) == 1 and result_list[0]:
            result = True
        else:
            result = False

        return result

    @staticmethod
    def check_country_populated(in_fc):
        country_populated = True

        with arcpy.da.SearchCursor(in_fc, ['country']) as cursor:
            for row in cursor:
                if not row[0]:

                    country_populated = False
                    break

        return country_populated

    def country_field_added(self, in_fc):
        if 'country' in util.list_fields(in_fc, self.gfw_env):
            country_added = False

        else:
            util.add_field_and_calculate(in_fc, 'country', 'TEXT', 3, self.add_country_value, self.gfw_env)
            country_added = True

        return country_added

    def apply_field_map_if_exists(self, field_map_path, out_dataset_name):

        # Check if there's a fieldmap to go from country to global layer
        if field_map_path:

            ini_path = field_map_path.replace('{ISO}', self.add_country_value)
            field_map_dict = field_map.get_ini_dict(ini_path)

            out_dir = os.path.join(self.scratch_workspace, 'country_to_global_fms')

            country_src = field_map.ini_fieldmap_to_fc(self.esri_service_output, out_dataset_name,
                                                       field_map_dict, out_dir)

        # If no field map, just use the output from the country layer that we just processed
        else:
            country_src = self.esri_service_output

        return country_src

    def update_global_layer(self, global_layerdef):

        country_src = self.apply_field_map_if_exists(global_layerdef['field_map'], global_layerdef['tech_title'])
        country_added = self.country_field_added(country_src)

        # Make sure that both input and output FCs have the 'country' field
        # Otherwise can't reliably append/delete based on ISO code
        country_fc_list = [country_src, global_layerdef['esri_service_output'],
                           global_layerdef['cartodb_service_output']]

        if self.check_country_fields(country_fc_list) and self.check_country_populated(country_src):

            # Append our country-specific data to the global output
            self.append_to_esri_source(country_src, global_layerdef['esri_service_output'],
                                       global_layerdef['esri_merge_where_field'])

            # Archive the global output
            self._archive(global_layerdef['esri_service_output'], global_layerdef['download_output'],
                          global_layerdef['archive_output'], False)

            # Append our country-specific data to the global output
            self.sync_cartodb(country_src, global_layerdef['cartodb_service_output'],
                              global_layerdef['cartodb_merge_where_field'])

        else:
            logging.error("Field country not present or not populated in input/output FCs. Exiting now.")
            sys.exit(1)

        # Delete the country field if we had to add it for this purpose
        if country_added:
            arcpy.DeleteField_management(country_src, 'country')

    def update(self):

        # Update the country-specific layer-- same as for a standard vector layer
        self._update()

        # Grab the info about the global layer that we need to update
        gs = google_sheet.GoogleSheet(self.gfw_env)
        global_layerdef = gs.get_layerdef(self.global_layer)

        # Update the global layer using it's own layerdef
        self.update_global_layer(global_layerdef)
