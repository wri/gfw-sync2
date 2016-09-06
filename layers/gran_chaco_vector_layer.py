__author__ = 'Asa.Strong'

import os
import subprocess
import time
import logging
import arcpy

from layers.vector_layer import VectorLayer

class GranChacoDeforestation(VectorLayer):

    def update(self):

        # Update the regular vector data first
        self._update()

        # self.update_gran_chaco_vector()

        # Then add custom stuff regarding vector to raster processing . . .
        self.gran_chaco_custom_process()

    def gran_chaco_custom_process(self):

        logging.debug('Starting gran chaco custoom')
