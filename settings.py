import glob
import os
import google_sheet_to_dict
from configobj import ConfigObj

def get_ini_file(folder, ini_f):
    abspath = os.path.abspath(__file__)
    dir_name = os.path.dirname(abspath)
    ini_file = os.path.join(dir_name, folder, ini_f)
    content = ConfigObj(ini_file)

    return content


def get_settings():
    return get_ini_file('config', 'settings.ini')


def get_layers_from_file(f):
    return get_ini_file('layers', f)


def get_country_iso3_list():
    return get_ini_file('config', 'country_iso3.ini')

def google_doc_dict_to_config_dict(inGoogleDocDict):
    outDict = {}

    for layerName in inGoogleDocDict.keys():
        layerdef = inGoogleDocDict[layerName]

        outDict[layerName] = {}

        for key, value in layerdef.iteritems():
            
            if ':' in key:
                subCategory = key.split(':')[0]
                subCategoryKey = key.split(':')[1]
                
                try:
                    outDict[layerName][subCategory][subCategoryKey] = value
                except:
                    outDict[layerName][subCategory] = {}
                    outDict[layerName][subCategory][subCategoryKey] = value
            else:
                outDict[layerName][key] = value

    return outDict
                

def get_layer_config_dict(inLayerList=None):
    spreadsheetJSON = r'D:\scripts\gfw-sync2\config\spreadsheet.json'
    spreadsheetKey = r'1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI'
    sheetName = 'Sheet1'

    gdoc_list = google_sheet_to_dict.open_spreadsheet(spreadsheetJSON, spreadsheetKey, sheetName)

    gdoc_dict = google_sheet_to_dict.gdoc_lists_to_layer_dict(gdoc_list, 'tech_title')

    layer_config_dict = google_doc_dict_to_config_dict(gdoc_dict)
    
    if inLayerList:
        return {key: layer_config_dict[key] for key in inLayerList}
                       
    else:
        return layer_config_dict

def get_country_aliases_to_iso_dict():
    spreadsheetJSON = r'D:\scripts\gfw-sync2\config\spreadsheet.json'
    spreadsheetKey = r'1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI'
    sheetName = 'CountryNames_to_ISO'

    gdoc_list = google_sheet_to_dict.open_spreadsheet(spreadsheetJSON, spreadsheetKey, sheetName)

    gdoc_dict = google_sheet_to_dict.gdoc_lists_to_layer_dict(gdoc_list, 'country_alias')

    country_alias_iso_dict = google_doc_dict_to_config_dict(gdoc_dict)
    
    return country_alias_iso_dict




settings = get_settings()

