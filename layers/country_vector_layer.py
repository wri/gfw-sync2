__author__ = 'Thomas.Maschler'

from utilities import google_sheet
from vector_layer import VectorLayer

class CountryVectorLayer(VectorLayer):
    """
    CountryVectorLayer layer class. Inherits from VectorLayer
    """

    def __init__(self, layerdef):
        print 'starting country_vector_layer'
        super(CountryVectorLayer, self).__init__(layerdef)

    def update(self):

        # Update the country-specific layer-- same as for a standard vecotr layer
        self._update()

        # Grab the info about the global layer that we need to update
        gs = google_sheet.GoogleSheet(self.gfw_env)
        global_layerdef = gs.get_layerdef(self.global_layer)

        # Append our country-specific data to the global output
        self.append_to_esri_source(self.esri_service_output, global_layerdef['esri_service_output'],
                                   global_layerdef['esri_merge_where_field'])

        # Archive the global output
        self._archive(global_layerdef['esri_service_output'], global_layerdef['download_output'],
                      global_layerdef['archive_output'], False)

        # Append our country-specific data to the global output
        self.sync_cartodb(self.esri_service_output, global_layerdef['cartodb_service_output'],
                          global_layerdef['cartodb_merge_where_field'])





    

    

    



