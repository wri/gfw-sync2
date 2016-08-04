from utilities import cartodb


def post_process(layerdef):
    """
    Update the date value in the layerspec table-- required for the time slider to work properly
    :param layerdef: the layerdef
    :return:
    """

    sql = "UPDATE layerspec set maxdate = (SELECT max(date) FROM imazon_sad) WHERE table_name='imazon_sad'"
    cartodb.cartodb_sql(sql, layerdef.gfw_env)

