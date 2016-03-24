import os
import gspread
import json
from oauth2client.client import SignedJwtAssertionCredentials

def open_spreadsheet(spreadsheet_file, spreadsheet_key, sheetName):

    json_key = json.load(open(spreadsheet_file))
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope)

    #authorize oauth2client credentials
    gc = gspread.authorize(credentials)

    #open the metadata entry spreadsheet
    wks = gc.open_by_key(spreadsheet_key).worksheet(sheetName)

    gdocAsLists = wks.get_all_values()

    return gdocAsLists

def gdoc_lists_to_layer_dict(inGdocAsLists, keyColumn):

    #Create emtpy spreadsheet dict
    outDict = {}

    #Pull the header row from the Google doc
    headerRow = inGdocAsLists[0]

    #Iterate over the remaining data rows
    for dataRow in inGdocAsLists[1:]:

        #Build a dictionary for each row with the column title
        #as the key and the value of that row as the value
        rowAsDict = {k: v for (k, v) in zip(headerRow, dataRow)}

        #Grab the technical title (what we know the layer as)
        layerName = rowAsDict[keyColumn]

        #Add that as a key to the larger outDict dictionary
        outDict[layerName] = {}

        #For the values in each row, add them to the row-level
        #dictionary
        for key, value in rowAsDict.iteritems():
            outDict[layerName][key] = value
            
    return outDict

