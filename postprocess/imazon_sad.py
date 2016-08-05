import logging

from utilities import cartodb


def post_process(layerdef):
    """
    Update the date value in the layerspec table-- required for the time slider to work properly
    :param layerdef: the layerdef
    :return:
    """

    if layerdef.gfw_env == 'PROD':
        update_layerspec(layerdef)

    else:
        logging.debug('Not updating layerspec table; gfw_env is {0}'.format(layerdef.gfw_env))


def update_layerspec(layerdef):
    sql = "UPDATE layerspec set maxdate = (SELECT max(date) FROM imazon_sad) WHERE table_name='imazon_sad'"
    cartodb.cartodb_sql(sql, layerdef.gfw_env)
