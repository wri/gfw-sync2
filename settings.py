import os
from configobj import ConfigObj

from utilities import google_sheet

def get_ini_file(folder, ini_f):
    abspath = os.path.abspath(__file__)
    dir_name = os.path.dirname(abspath)
    ini_file = os.path.join(dir_name, folder, ini_f)
    content = ConfigObj(ini_file)

    return content

def get_settings(input_env):
    return get_ini_file('config', 'settings.ini')[input_env]

def get_country_iso3_list():
    return get_ini_file('config', 'country_iso3.ini')



