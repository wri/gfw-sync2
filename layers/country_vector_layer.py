__author__ = 'Thomas.Maschler'

import logging
import os
import sys
import arcpy

from vector_layer import VectorLayer
from utilities import google_sheet as gs
from utilities import field_map
from utilities import util


class CountryVectorLayer(VectorLayer):
    """
    CountryVectorLayer layer class. Inherits from VectorLayer
    Uses the ._update method from VectorLayer to update the country-specific data, then
    defines update_global_layer() to update the associated global layer of which it is a component
    """
    def __init__(self, layerdef):
        logging.debug('Starting country_vector_layer')
        super(CountryVectorLayer, self).__init__(layerdef)

    def apply_field_map_if_exists(self, field_map_path, out_dataset_name):
        """
        Check if there's a field map to go from the country specific data to global data
        :param field_map_path: path to the fieldmap, should be formatted: D:\path\to\field\map.ini\{ISO}
        :param out_dataset_name: name of the output; used to create a feature layer with a unique name
        :return: the source dataset to use when appending to the global layer
        """

        # Check if there's a fieldmap to go from country to global layer
        if field_map_path:

            # Build the actual .ini path, replacing {ISO} with the country value
            ini_path = field_map_path.replace('{ISO}', self.add_country_value)
            field_map_dict = field_map.get_ini_dict(ini_path)
            out_dir = os.path.join(self.scratch_workspace, 'country_to_global_fms')

            # Apply field map, set country_src to this new FC
            country_src = field_map.ini_fieldmap_to_fc(self.esri_service_output, out_dataset_name,
                                                       field_map_dict, out_dir)

        # If no field map, just use the output from the country layer that we just processed
        else:
            country_src = self.esri_service_output

        return country_src

    def add_and_populate_country_field(self, in_fc):
        """
        Add a field for country if it doesn't exist and populate it with country value
        :param in_fc: fc to add 'country' to
        :return: True/False based on whether country added; will be used later to delete the field if added
        """
        country_added = False

        if 'country' not in util.list_fields(in_fc, self.gfw_env):
            util.add_field_and_calculate(in_fc, 'country', 'TEXT', 3, self.add_country_value, self.gfw_env)
            country_added = True

        return country_added

    def check_country_fields(self, in_fc_list):
        """
        Check the input feature classes to be sure they have a country field
        Important if we're going to use country as a way to append/delete features
        :param in_fc_list: List of FCs/cartoDB tables to check
        :return:
        """
        result_list = []

        for fc in in_fc_list:

            if 'country' in util.list_fields(fc, self.gfw_env):
                result_list.append(True)

            else:
                logging.debug("No country code found for {0}".format(fc))
                result_list.append(False)

        # If all result_list values are True, return True
        if len(set(result_list)) == 1 and result_list[0]:
            result = True
        else:
            result = False

        return result

    @staticmethod
    def check_country_populated(in_fc):
        """
        Check that the country field is populated for all records
        This is important for our source FC-- don't want to be appending records to esri_output and cartodb_output
        that can't be deleted later using country where clauses
        :param in_fc: input feature class
        :return:
        """
        country_populated = True

        with arcpy.da.SearchCursor(in_fc, ['country']) as cursor:
            for row in cursor:

                # If row[0] is NULL
                if not row[0]:
                    country_populated = False
                    break

        return country_populated

    def update_global_layer(self, global_layerdef):
        """
        Using the just-updated country vector data, grab the associated global_layer and update it, making sure to
        check that the country field is populated in the source and exists in esri and cartodb outputs
        :param global_layerdef:
        :return:
        """

        # Apply a fieldmap if necesary to get from the source to the global_vector
        country_src = self.apply_field_map_if_exists(global_layerdef['field_map'], global_layerdef['tech_title'])

        # Add country field and populate
        country_added = self.add_and_populate_country_field(country_src)

        # Make sure that both input and output FCs have the 'country' field
        # Otherwise can't reliably append/delete based on ISO code
        country_fc_list = [country_src, global_layerdef['esri_service_output'],
                           global_layerdef['cartodb_service_output']]

        # If country field exists in input/outputs and is populated in the input, great
        if self.check_country_fields(country_fc_list) and self.check_country_populated(country_src):

            # Build a where clause to add/delete where country = '{ISO}'
            where_clause = """{0} = '{1}'""".format(global_layerdef['merge_where_field'], self.add_country_value)

            # Append our country-specific data to the global output
            self.append_to_esri_source(country_src, global_layerdef['esri_service_output'], where_clause)

            # Zip the global output for archive and download
            self._archive(global_layerdef['esri_service_output'], global_layerdef['download_output'],
                          global_layerdef['archive_output'])

            # Append our country-specific data to the global output
            self.sync_cartodb(country_src, global_layerdef['cartodb_service_output'], where_clause)

        else:
            logging.error("Field country not present or not populated in input/output FCs. Exiting.")
            sys.exit(1)

        # Delete the country field if we had to add it for this purpose
        if country_added:
            arcpy.DeleteField_management(country_src, 'country')

    def update(self):
        """
        Carry out the standard VectorLayer update for the country-specific data (self._update())
        Then grab the global_layerdef and use that to update the global layer of which this is a part
        :return:
        """

        # Update the country-specific layer-- same as for a standard vector layer
        self._update()

        # Grab the info about the global layer that we need to update
        global_layerdef = gs.get_layerdef(self.global_layer, self.gfw_env)

        # Update the global layer using it's own layerdef
        self.update_global_layer(global_layerdef)
