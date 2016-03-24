import sys
import getopt
import settings

from vector_layer import VectorLayer
from raster_layer import RasterLayer
from glad_raster_layer import GLADRasterLayer

from imazon_datasource import ImazonDataSource
##import s3_vector_layer

##from s3_vector_layer import S3VectorLayer
##from s3_vector_layer import S3CountryVectorLayer

#from osm_loggingroads_layer import OSMLoggingRoadsLayer
#from wdpa_layer import WDPALayer


def main(argv):
    print "{0!s} v{1!s}".format(settings.settings['tool_info']['name'], settings.settings['tool_info']['version'])
    print ""

    layers = []
    countries = []
    validate = False
    verbose = True
    logging = True
    try:
        opts, remainder = getopt.gnu_getopt(argv, "hl:c:", ["help", "layers=", "country="])

        #Check that we're using the correct options and argument
        if remainder:
            raise getopt.GetoptError("Bad argument: unclear what {0} is. Be sure to prefix it with an option listed below".format(remainder))
            usage()
            sys.exit(2)

    except getopt.GetoptError:
        print "Error: Invalide argument"
        usage()
        sys.exit(2)
        
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        #elif opt == "-d":
        #    global _debug
        #    _debug = 1
        if opt in ("-n", "--nonverbose"):
            verbose = False
        if opt in ("-g", "--nolog"):
            logging = False
        if opt in ("-l", "--layers"):
            layers += arg.lower().split(',')
##            layers.append(arg.lower())
        if opt in ("-c", "--country"):
            countries += arg.upper().split(',')

    try:
        layer_config_dict = settings.get_layer_config_dict(layers)
    except KeyError:
        print 'Unknown layer(s) specified {0}'.format(layers)
        print r'Check the gfw-sync2 config table for a list of options: https://docs.google.com/spreadsheets/d/1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI/edit#gid=0'
        sys.exit(2)
        

    for layerName, layerdef in layer_config_dict.iteritems():

        layerdef['name'] = layerName

        if layerdef["type"] == "simple_vector":
            layer = VectorLayer(layerdef)
            
        elif layerdef["type"] == "raster":
            layer = RasterLayer(layerdef)
            
        elif layerdef["type"] == "imazon_vector":
            datasource = ImazonDataSource(layerdef)
            layer = VectorLayer(datasource.merge_imazon_layers())
                
        elif layerdef["type"] == "glad_raster":
            layer = GLADRasterLayer(layerdef)
            
##        elif layerdef["type"] == "s3_vector":
##            layer = S3VectorLayer(layerdef)
##        elif layerdef["type"] == "s3_country_vector":
##            layer = S3CountryVectorLayer(layerdef)
       # elif layerdef["type"] == "wdpa":
       #     layer = WDPALayer(layerdef)
       # elif layerdef["type"] == "osm_loggingroads":
       #     layer = OSMLoggingRoadsLayer(layerdef)
       # elif layerdef["type"] == "wdpa_layer":
       #     layer = WDPALayer(layerdef)
        else:
            raise RuntimeError("Layer type unknown")

        layer.update()

        #Check if second level of inheritance exists


def usage():
    layers = settings.get_layer_config_dict().keys()

    print "Usage: gfw-sync.py [options]"
    print "Options:"
    print "-h, --help               Show help of GFW Sync Tool"
    print "-n, --nonverbose        Turn console messages off"
    print "-g, --nolog             Turn logging off"
    print "-c <country ISO3 code>   Country to be updated. Update will affect all selected layers."
    print "                         If left out, all countries will be selected."
    print "                         You can use this option multiple times"
    print "-l <GFW layers name>      GFW Layer, which will be updated. Update will affect all selected countries"
    print "                         If left out, all layers will be selected."
    print "                         You can use this option multiple times"
    print "                         Currently supported layers:"
    for layer in layers:
        print "                             {0!s}".format(layer)




if __name__ == "__main__":

    main(sys.argv[1:])

