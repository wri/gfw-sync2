__author__ = 'Thomas.Maschler'

import layer

layerdef = {"name": "test_layer",
            "alias": "Alias",
            "gdb_connection": "gfw",
            "fc": "test",
            "replica": "my_replica",
            "bucket": "gfw2-data",
            "folder": "land-use"
            }

l = layer.Layer(layerdef)