import sys
import logging

from layers.raster_layer import RasterLayer
from layers.vector_layer import VectorLayer
from layers.country_vector_layer import CountryVectorLayer
from layers.glad_raster_layer import GLADRasterLayer

from datasources.imazon_datasource import ImazonDataSource
from datasources.terrai_datasource import TerraiDataSource
from datasources.wdpa_datasource import WDPADatasource
from datasources.hot_osm_export_datasource import HotOsmExportDataSource
from datasources.forest_atlas_datasource import ForestAtlasDataSource


def build_layer(google_sheet, layername):
    """
    Used to get a layerdef for our layer of interest, then build a layer object based on the type of layer input
    :param google_sheet: a google sheet object used to layerdef
    :param layername: the name of the layer to update
    :return: a layer object to gfw-sync so that it can call the update method
    """
    layerdef = google_sheet.get_layerdef(layername)

    if layerdef["type"] == "simple_vector":
        layer = VectorLayer(layerdef)

    elif layerdef["type"] == "raster":
        layer = RasterLayer(layerdef)

    elif layerdef["type"] == "terrai_raster":
        datasource = TerraiDataSource(layerdef)
        layer = RasterLayer(datasource.get_layer())

    elif layerdef["type"] == "imazon_vector":
        datasource = ImazonDataSource(layerdef)
        layer = VectorLayer(datasource.get_layer())

    elif layerdef["type"] == "glad_raster":
        layer = GLADRasterLayer(layerdef)

    elif layerdef["type"] == "hot_osm_export":
        datasource = HotOsmExportDataSource(layerdef)
        layer = VectorLayer(datasource.get_layer())

    elif layerdef["type"] == "wdpa_vector":
        datasource = WDPADatasource(layerdef)
        layer = VectorLayer(datasource.get_layer())

    elif layerdef["type"] == "forest_atlas_vector":
        datasource = ForestAtlasDataSource(layerdef)
        layer = CountryVectorLayer(datasource.get_layer())

    elif layerdef["type"] == "country_vector":
        if layerdef['global_layer']:

            # Get the associated layerdef for the global_layer specified by our original dataset of interest
            # This is important because if we updated gab_logging, we also need to updated gfw_logging
            global_layerdef = google_sheet.get_layerdef(layerdef['global_layer'])

            logging.debug('Found a value for global_layer. Validating this input using the VectorLayer schema')

            # Use the output value as the source so that all the tests (validating that the "source" exists etc pass
            # This allows us to leave the source field in the google spreadsheet blank for this dataset, which makes
            # sense. The source for a global layer is made up of a bunch of smaller country layers
            global_layerdef['source'] = global_layerdef['esri_service_output']
            VectorLayer(global_layerdef)

            logging.debug('Global layer validation complete')

            layer = CountryVectorLayer(layerdef)

        else:
            logging.error('Expecting to find global_layer associated with country_vector but did not.'
                          'If no global_layer associated, this should be classified as simple_vector. Exiting.')
            sys.exit(1)

    elif layerdef["type"] == "global_vector":
        logging.error('Please update global vector data by updating a country_vector dataset and specifying '
                      'the global layer in the global_layer column \n Exiting now.')
        sys.exit(1)

    else:
        logging.error("Layer type {0} unknown".format(layerdef["type"]))
        sys.exit(1)

    return layer
