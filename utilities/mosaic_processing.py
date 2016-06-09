import os
import subprocess
import argparse

#  Used to pass *kwargs to the functions below at the command line
# http://stackoverflow.com/questions/14522539/python-passing-parameters-in-a-command-line-app
parser = argparse.ArgumentParser()
parser.add_argument('function')
parser.add_argument('arguments', nargs='*')
args = parser.parse_args()


def export_mosaic_to_tif(input_ras, output_ras, band_name):

    # Import within the function so only loadd if necessary
    import arcpy

    # Can't log to STDOUT for some reason, likely to do with calling this from subprocess
    print 'Exporting {0} to {1}'.format(input_ras, output_ras)

    # Export the raster to tif, with 0 as the value to convert to NoData
    if band_name in ['band1_day', 'band2_day', 'band3_conf_and_year']:
        no_data_value = '0'

    # Export to tif, but keep the 0 pixels as 0-- we want to include them in our resampling
    elif band_name == 'band4_intensity':
        no_data_value = '256'

    else:
        raise ValueError('Unknown raster {0}.'.format(band_name))

    arcpy.CopyRaster_management(input_ras, output_ras, "", "", no_data_value, "NONE", "NONE",
                                "8_BIT_UNSIGNED", "NONE", "NONE")


def resample_single_tif(input_ras, output_ras, band_name, cell_size):

    # Import within the function so only loadd if necessary
    import arcpy
    arcpy.CheckOutExtension("Spatial")

    if band_name in ['band1_day', 'band2_day', 'band3_conf_and_year']:
        resample_method = 'MAJORITY'
    else:
        resample_method = 'BILINEAR'

    print "Resampling {0} to cell size {1}m".format(os.path.basename(input_ras), int(float(cell_size)))
    arcpy.Resample_management(input_ras, output_ras, float(cell_size), resample_method)


def run_gdal_translate(input_ras, output_ras, ulx, uly, lrx, lry):

    # Grab cmd arguments passed in related to projwin etc
    cmd = ['gdal_translate', '-projwin', ulx, uly, lrx, lry]

    # Include TFW files for arc's sake
    cmd += ['-co', 'TFW=YES']

    # Add input tif and output filename
    cmd += [input_ras, output_ras]

    overview_dir = os.path.dirname(output_ras)

    try:
        subprocess.check_call(cmd, cwd=overview_dir)
    except subprocess.CalledProcessError:
        raise SyntaxError('Error in gdal_translate subprocess.')


func_options = {
                    'export_mosaic_to_tif': export_mosaic_to_tif,
                    'resample_single_tif': resample_single_tif,
                    'run_gdal_translate': run_gdal_translate
                }


if __name__ == '__main__':
    func_options[args.function](*args.arguments)
