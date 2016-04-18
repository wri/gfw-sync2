import gspread
import time
import sys
import logging
from oauth2client.service_account import ServiceAccountCredentials


class GoogleSheet(object):
    """
    A class to access and update a Google Spreadsheet
    :param gfw_env: Determines the tab to use on the config table(PROD | DEV)
    :rtype A :class:`GoogleSheet <GoogleSheet>`
    """

    def __init__(self, gfw_env):

        self.spreadsheet_file = r'D:\scripts\gfw-sync2\tokens\spreadsheet.json'
        self.spreadsheet_key = r'1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI'
        self.sheet_name = gfw_env

    def _open_spreadsheet(self):
        """
        Used to open the spreadsheet for read/update
        :return: a gspread wks object that can be used to edit/update a given sheet
        """

        # Updated for oauth2client
        # http://gspread.readthedocs.org/en/latest/oauth2.html
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.spreadsheet_file,
                                                                       ['https://spreadsheets.google.com/feeds'])

        gc = gspread.authorize(credentials)
        wks = gc.open_by_key(self.spreadsheet_key).worksheet(self.sheet_name)

        return wks

    def sheet_to_dict(self):
        """
        Convert the spreadsheet to a dict with {layername: {colName: colVal, colName2: colVal}
        :return: a dictionary representing the sheet
        """

        sheet_as_dict = {}

        wks = self._open_spreadsheet()
        gdoc_as_lists = wks.get_all_values()

        # Pull the header row from the Google doc
        header_row = gdoc_as_lists[0]

        # Iterate over the remaining data rows
        for dataRow in gdoc_as_lists[1:]:

            # Build a dictionary for each row with the column title
            # as the key and the value of that row as the value
            row_as_dict = {k: v for (k, v) in zip(header_row, dataRow)}

            # Grab the technical title (what we know the layer as)
            layer_name = row_as_dict['tech_title']

            # Add that as a key to the larger outDict dictionary
            sheet_as_dict[layer_name] = {}

            # For the values in each row, add them to the row-level dictionary
            for key, value in row_as_dict.iteritems():
                sheet_as_dict[layer_name][key] = value

        return sheet_as_dict

    def get_layerdef(self, layer_name):
        """
        Build a layerdef dictionary by specifying the layer of interest
        :param layer_name: the layer name of interest
        :return: a dictionary of values that define a layer (layerdef) specified in the config table
        """

        try:
            layerdef = self.sheet_to_dict()[layer_name]

            layerdef['name'] = layerdef['tech_title']
            layerdef['gfw_env'] = self.sheet_name

            return layerdef

        except KeyError:
            logging.error('Unable to find the specified layer in the Google Sheet. Exiting now.')
            sys.exit(1)

    def update_value(self, layername, colname, update_value):
        """ Basic
        :param layername:
        :param colname:
        :param update_value:
        :return:
        """

        wks = self._open_spreadsheet()
        gdoc_as_lists = wks.get_all_values()

        row_id = [x[0] for x in gdoc_as_lists].index(layername) + 1
        col_id = gdoc_as_lists[0].index(colname) + 1

        wks.update_cell(row_id, col_id, update_value)

    def update_gs_timestamp(self, layername):
        self.update_value(layername, 'last_updated', time.strftime("%m/%d/%Y"))

        # If the layer is part of a global_layer, update its last_updated timestamp as well
        associated_global_layer = self.get_layerdef(layername)['global_layer']

        if associated_global_layer:
            self.update_value(associated_global_layer, 'last_updated', time.strftime("%m/%d/%Y"))
