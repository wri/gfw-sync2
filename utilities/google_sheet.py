import gspread
import time
import sys
import logging
from oauth2client.service_account import ServiceAccountCredentials


def _open_spreadsheet(sheet_name, custom_key=False):
    """
    Open the spreadsheet for read/update
    :return: a gspread wks object that can be used to edit/update a given sheet
    """

    spreadsheet_file = r'D:\scripts\gfw-sync2\tokens\spreadsheet.json'

    if custom_key:
        spreadsheet_key = custom_key
    else:
        spreadsheet_key = r'1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI'

    # Updated for oauth2client
    # http://gspread.readthedocs.org/en/latest/oauth2.html
    credentials = ServiceAccountCredentials.from_json_keyfile_name(spreadsheet_file,
                                                                   ['https://spreadsheets.google.com/feeds'])

    gc = gspread.authorize(credentials)
    wks = gc.open_by_key(spreadsheet_key).worksheet(sheet_name)

    return wks


def sheet_to_dict(gfw_env):
    """
    Convert the spreadsheet to a dict with {layername: {colName: colVal, colName2: colVal}
    :param gfw_env: the name of the sheet to call (PROD | DEV)
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
    :param gfw_env: the name of the sheet to call
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


def set_value(unique_id_col, unique_id_value, colname, sheet_name, in_update_value, spreadsheet_key=None):
    """
    Update a value in the spreadsheet given the layername and column name
    :param unique_id_col: the column that has unique ids (tech_title in the config table)
    :param unique_id_value: the particular value we're looking for in the unique id col
    :param colname: the column name to update
    :param sheet_name: the name of the sheet to update
    :param in_update_value: the value to set
    :param spreadsheet_key: key for the spreadsheet if not the default config table
    """

    wks, row_id, col_id = get_cell_location(unique_id_col, unique_id_value, colname, sheet_name, spreadsheet_key)

    wks.update_cell(row_id, col_id, in_update_value)


def get_value(unique_id_col, unique_id_value, colname, sheet_name, spreadsheet_key=None):
    """
    Update a value in the spreadsheet given the layername and column name
    :param unique_id_col: the column that has unique ids (tech_title in the config table)
    :param unique_id_value: the particular value we're looking for in the unique id col
    :param colname: the column name to get the value of
    :param sheet_name: the name of the sheet in the google doc
    :param spreadsheet_key: key for the spreadsheet if not the default config table
    """

    wks, row_id, col_id = get_cell_location(unique_id_col, unique_id_value, colname, sheet_name, spreadsheet_key)

    return wks.cell(row_id, col_id).value


def get_cell_location(unique_id_col, unique_id_value, colname, sheet_name, spreadsheet_key=None):
    """
    Get the row and col of a particular cell so later we can report the value or update it
    :param unique_id_col: the column that has unique ids (tech_title in the config table)
    :param unique_id_value: the particular value we're looking for in the unique id col
    :param colname: the column name to get the value of
    :param sheet_name: the name of the sheet in the google doc
    :param spreadsheet_key: json key if not the default config table
    """

    wks = _open_spreadsheet(sheet_name, spreadsheet_key)

    gdoc_as_lists = wks.get_all_values()

    unique_id_index = gdoc_as_lists[0].index(unique_id_col)

    row_id = [x[unique_id_index] for x in gdoc_as_lists].index(unique_id_value) + 1
    col_id = gdoc_as_lists[0].index(colname) + 1

    return wks, row_id, col_id


def update_gs_timestamp(layername, gfw_env):
    """
    Update the 'last_updated' column for the layer specified with the current date
    :param layername: the row to update (based on tech_title column)
    :param gfw_env: gfw env
    """
    set_value('tech_title', layername, 'last_updated', time.strftime("%m/%d/%Y"), gfw_env)

    # If the layer is part of a global_layer, update its last_updated timestamp as well
    associated_global_layer = get_layerdef(layername, gfw_env)['global_layer']

    if associated_global_layer:
        set_value('tech_title', associated_global_layer, 'last_updated', time.strftime("%m/%d/%Y"), gfw_env)
