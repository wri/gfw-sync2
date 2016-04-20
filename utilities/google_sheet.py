import gspread
import time
import sys
import logging
from oauth2client.service_account import ServiceAccountCredentials


def _open_spreadsheet(gfw_env):
    """
    Open the spreadsheet for read/update
    :return: a gspread wks object that can be used to edit/update a given sheet
    """

    spreadsheet_file = r'D:\scripts\gfw-sync2\tokens\spreadsheet.json'
    spreadsheet_key = r'1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI'

    # Updated for oauth2client
    # http://gspread.readthedocs.org/en/latest/oauth2.html
    credentials = ServiceAccountCredentials.from_json_keyfile_name(spreadsheet_file,
                                                                   ['https://spreadsheets.google.com/feeds'])

    gc = gspread.authorize(credentials)
    wks = gc.open_by_key(spreadsheet_key).worksheet(gfw_env)

    return wks


def sheet_to_dict(gfw_env):
    """
    Convert the spreadsheet to a dict with {layername: {colName: colVal, colName2: colVal}
    :return: a dictionary representing the sheet
    """

    sheet_as_dict = {}
    wks = _open_spreadsheet(gfw_env)
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


def get_layerdef(layer_name, gfw_env):
    """
    Build a layerdef dictionary by specifying the layer of interest
    :param layer_name: the layer name of interest
    :return: a dictionary of values that define a layer (layerdef) specified in the config table
    """

    try:
        layerdef = sheet_to_dict(gfw_env)[layer_name]

        layerdef['name'] = layerdef['tech_title']
        layerdef['gfw_env'] = gfw_env

        return layerdef

    except KeyError:
        logging.error('Unable to find the specified layer in the Google Sheet. Exiting now.')
        sys.exit(1)


def update_value(layername, colname, in_update_value, gfw_env):
    """
    Update a value in the spreadsheet given the layername and column name
    :param layername: the layer name we're updating-- matches tech_title in the first column
    :param colname: the column name to update
    :param in_update_value: the value to set
    :param gfw_env: gfw env
    """

    wks = _open_spreadsheet(gfw_env)

    gdoc_as_lists = wks.get_all_values()

    row_id = [x[0] for x in gdoc_as_lists].index(layername) + 1
    col_id = gdoc_as_lists[0].index(colname) + 1

    wks.update_cell(row_id, col_id, in_update_value)


def update_gs_timestamp(layername, gfw_env):
    """
    Update the 'last_updated' column for the layer specified with the current date
    :param layername: the row to update (based on tech_title column)
    :param gfw_env: gfw env
    """
    update_value(layername, 'last_updated', time.strftime("%m/%d/%Y"), gfw_env)

    # If the layer is part of a global_layer, update its last_updated timestamp as well
    associated_global_layer = get_layerdef(layername, gfw_env)['global_layer']

    if associated_global_layer:
        update_value(associated_global_layer, 'last_updated', time.strftime("%m/%d/%Y"), gfw_env)
