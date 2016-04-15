import os
from configobj import ConfigObj


def get_ini_file(ini_f, folder=None):

    if folder:
        abspath = os.path.abspath(__file__)
        dir_name = os.path.dirname(os.path.dirname(abspath))
        ini_file = os.path.join(dir_name, folder, ini_f)
    else:
        ini_file = ini_f

    content = ConfigObj(ini_file)

    return content


def get_settings(input_env):
    return get_ini_file('settings.ini', 'config')[input_env]


def get_country_iso3_list():
    return get_ini_file('country_iso3.ini', 'config')
