__author__ = 'Asa.Strong'

import os
import subprocess
import time
import logging
import arcpy

from layers.vector_layer import VectorLayer

class GranChacoDeforestation(VectorLayer):

    def update_gran_chaco_vector(self):

        self.delete_and_append()

    def update(self):

        self.update_gran_chaco_vector()
