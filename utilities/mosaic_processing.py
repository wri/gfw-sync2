import arcpy
import os
import subprocess
import argparse

arcpy.CheckOutExtension("Spatial")

#  Used to pass *kwargs to the functions below at the command line
# http://stackoverflow.com/questions/14522539/python-passing-parameters-in-a-command-line-app
parser = argparse.ArgumentParser()
parser.add_argument('function')
parser.add_argument('arguments', nargs='*')
args = parser.parse_args()


def export_mosaic_to_tif(input_ras, output_ras, ras_type):

    # Can't log to STDOUT for some reason, likely to do with calling this from subprocess
    print 'Exporting {0} to {1}'.format(input_ras, output_ras)

    output_dir = os.path.dirname(output_ras)
    temp_ras = ras_type + '_with_zeros.tif'
    raster_with_zeros = os.path.join(output_dir, temp_ras)

    # Export the raster to tif
    arcpy.CopyRaster_management(input_ras, raster_with_zeros, "", "", "256", "NONE", "NONE", "8_BIT_UNSIGNED", "NONE", "NONE")

    # Set the zero values to null if it's in bands 1 -3
    if ras_type in ['band1_day', 'band2_day', 'band3_conf_and_year']:

        arcpy.gp.SetNull_sa(raster_with_zeros, raster_with_zeros, output_ras, """"Value" = 0""")

    # Change pixel type to float. Keep the 0 pixels as 0-- we want to include them in our resampling
    elif ras_type == 'band4_intensity':

        arcpy.CopyRaster_management(raster_with_zeros, output_ras, "", "", "256", "NONE", "NONE",
                                    "32_BIT_FLOAT", "NONE", "NONE")
    else:
        raise ValueError('Unknown raster {0}.'.format(ras_type))


def resample_single_tif(input_ras, output_ras, ras_type, cell_size):

    if ras_type in ['band1_day', 'band2_day', 'band3_conf_and_year']:
        resample_method = 'MAJORITY'
    else:
        resample_method = 'BILINEAR'

    print "Resampling {0} to cell size {1}m".format(os.path.basename(input_ras), int(cell_size))
    arcpy.Resample_management(input_ras, output_ras, cell_size, resample_method)


def run_gdal_translate(input_ras, output_ras, source_mosaic, ulx, uly, lrx, lry):

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

    arcpy.AddRastersToMosaicDataset_management(source_mosaic, "Raster Dataset", output_ras,
                                               "UPDATE_CELL_SIZES", "UPDATE_BOUNDARY", "NO_OVERVIEWS", 0)


func_options = {
                    'export_mosaic_to_tif': export_mosaic_to_tif,
                    'resample_single_tif': resample_single_tif,
                    'run_gdal_translate': run_gdal_translate
                }


if __name__ == '__main__':
    func_options[args.function](*args.arguments)
