import os
import arcpy


def get_srs(layer):
    desc = arcpy.Describe(layer)
    return desc.spatialReference.name


def get_token(platform):
    abspath = os.path.abspath(__file__)
    dir_name = os.path.dirname(abspath)
    token_file = os.path.join(dir_name, r"tokens\%s" % platform)
    if not os.path.exists(token_file):
        raise IOError('Cannot find any token for %s\n Make sure there is a file called %s in the tokens directory'
                      % (platform, platform))
    else:
        with open(token_file, "r") as f:
            for row in f:
                return row

