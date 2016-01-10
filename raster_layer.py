__author__ = 'Thomas.Maschler'

from layer import Layer


class RasterLayer(Layer):
    """
    Raster layer class. Inherits from Layer
    """

    def __init__(self, layerdef):

        super(RasterLayer, self).__init__(layerdef)

    def archive(self):
        self._archive(self.src, False)