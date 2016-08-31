import os
import sys
import logging
import arcpy
import re
import datetime
import calendar

from datasource import DataSource
from utilities import google_sheet as gs


class WDPADatasource(DataSource):
    """
    WDPA datasource class. Inherits from DataSource
    Used to download the source GDB, find the polygon FC, repair and simplify geometry
    """
    def __init__(self, layerdef):
        logging.debug('Starting simple_datasource')
        super(WDPADatasource, self).__init__(layerdef)

        self.layerdef = layerdef

    def download_wpda_to_gdb(self):
        local_file = self.download_file(self.data_source, self.download_workspace)

        self.unzip(local_file, self.download_workspace)

        unzipped_gdb = None

        for item in os.walk(self.download_workspace):
            dirname = item[0]

            if os.path.splitext(dirname)[1] == '.gdb':
                unzipped_gdb = dirname
                break

        # Compare this to what's listed as the current version in the metadata table
        # If we're up to date for this data, exit this workflow
        self.check_current_version(unzipped_gdb)

        if unzipped_gdb:
            arcpy.env.workspace = unzipped_gdb
        else:
            logging.error("Expected to find GDB somewhere in the unzipped dirs, but didn't. Exiting")
            sys.exit(1)

        poly_list = [x for x in arcpy.ListFeatureClasses() if arcpy.Describe(x).shapeType == 'Polygon']

        if len(poly_list) != 1:
            logging.error("Expected one polygon FC in the wdpa gdb. Found {0}. Exiting now.".format(len(poly_list)))
            sys.exit(1)
        else:
            self.data_source = os.path.join(unzipped_gdb, poly_list[0])

    def prep_source_fc(self):

        simplified_fc = self.data_source + '_simplified'

        logging.debug("Starting simplify_polygon")
        arcpy.SimplifyPolygon_cartography(self.data_source, simplified_fc, algorithm="POINT_REMOVE",
                                          tolerance="10 Meters", minimum_area="0 Unknown", error_option="NO_CHECK",
                                          collapsed_point_option="NO_KEEP")

        self.data_source = simplified_fc

    def check_current_version(self, wdpa_gdb):
        """
        Check the filename of the zip we've just downloaded against what we currently have in the metadata doc
        If we're up to date, exit this workflow, logging that we've 'checked' the dataset
        Otherwise continue to process
        :param wdpa_gdb: the unzipped gdb just downloaded from wdpa
        :return:
        """

        # Parameters required to check the metadata response spreadsheet
        unique_col = 'Technical Title'
        unique_val = 'wdpa_protected_areas'
        update_col = 'Frequency of Updates'
        sheet_name = 'sheet1'
        gs_key = r'1hJ48cMrADMEJ67L5hTQbT5hhV20YCJHpN1NwjXiC3pI'

        current_version_text = gs.get_value(unique_col, unique_val, update_col, sheet_name, gs_key)

        gdb_name = os.path.splitext(os.path.basename(wdpa_gdb))[0]
        download_version = gdb_name.replace('WDPA_', '').replace('_Public', '')

        download_version = self.parse_month_abbrev(download_version)

        # Format to match what's in the metadata
        download_version_text = 'Monthly. Current version: {0}.'.format(download_version)

        # Versions match; no need to update
        if current_version_text == download_version_text:

            logging.info('No new data on the wdpa site')

            # Important for the script that reads the log file and sends an email
            # Including this 'Checked' message will show that we checked the layer but it didn't need updating
            logging.critical('Checked | {0}'.format(self.name))
            sys.exit(0)

        # Update the value in the metadata table and continue processing the dataset
        else:
            logging.debug('Current WDPA version text is {0}, downloaded version is {1} Updating '
                          'dataset now.'.format(current_version_text, download_version_text))
            # gs.set_value(unique_col, unique_val, update_col, sheet_name, download_version_text, gs_key)

    def parse_month_abbrev(self, download_version):

        # Find the letter characters in the download version text
        p = re.compile(r'[a-zA-Z]+')
        m = p.match(download_version)
        month_abbrev = m.group()

        year_text = download_version.replace(month_abbrev, '')
        month_text = self.get_month_name(month_abbrev)

        if not month_text:
            logging.error("Unable to parse month_text from abbrev {0}. Exiting.".format(month_abbrev))
            sys.exit(1)

        return "{0} {1}".format(month_text, year_text)

    def get_month_name(self, month_abbrev):
        """
        Take a month_abbrev (usually a three letter code, but could be anything because it's from an external source)
        and compare it to all the possible month names. if all letters match, and they're in the correct order,
        return the full month name
        :param month_abbrev: three letter month code
        :return: full month name
        """

        # Check all month names
        for month_id in range(1, 13):
            full_month_name = calendar.month_name[month_id]

            # Remove duplicates from the month abbrev and the full month name, then list the indices of all the letters
            # that are in botb
            matching_letter_locations = [full_month_name.lower().index(x) for x in
                                         self.remove_dupes(month_abbrev).lower() if x in full_month_name.lower()]

            # If all abbrev letters are in the full month, and the order is correct, it's a match
            all_letters_match = len(matching_letter_locations) == len(self.remove_dupes(month_abbrev))
            correct_order = matching_letter_locations == sorted(matching_letter_locations)

            if all_letters_match and correct_order:
                match = full_month_name
                break
            else:
                match = False

        return match

    @staticmethod
    def remove_dupes(input_str):
        """
        http://stackoverflow.com/questions/9841303/removing-duplicate-characters-from-a-string
        :param input_str: string to remove duplicates from. if we find a duplicate, remove it and return non-dupe str
        :return:
        """
        return ''.join([j for i, j in enumerate(input_str) if j not in input_str[:i]])

    def get_layer(self):
        """
        Full process, called in layer_decision_tree.py. Downloads and preps the data
        :return: Returns and updated layerdef, used in the layer.update() process in layer_decision_tree.py
        """

        self.download_wpda_to_gdb()

        self.prep_source_fc()

        self.layerdef['source'] = self.data_source

        return self.layerdef
