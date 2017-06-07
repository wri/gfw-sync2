import os
from configobj import ConfigObj


def get_ini_file(ini_f, folder=None):
    """
    :param ini_f: input ini file
    :param folder: folder where ini file lives
    :return:
    """

    if folder:
        abspath = os.path.abspath(__file__)
        dir_name = os.path.dirname(os.path.dirname(abspath))
        ini_file = os.path.join(dir_name, folder, ini_f)
    else:
        ini_file = ini_f

    # Grab the ini_file and return it's keys value pairs as a dict
    content = ConfigObj(ini_file)

    return content


def get_settings(input_env):
    """
    Get
    :param input_env: gfw_env-- either prod or staging
    :return: the config for this gfw_env
    """
    return get_ini_file('settings.ini', 'config')[input_env]


def get_country_iso3_list():
    """
    :return: the list of dict of valid iso3:country names
    """
    return get_ini_file('country_iso3.ini', 'config')
