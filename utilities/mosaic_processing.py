import arcpy
import os
import subprocess
import sys
import argparse

arcpy.CheckOutExtension("Spatial")

#  Used to pass *kwargs to the functions below at the command line
# http://stackoverflow.com/questions/14522539/python-passing-parameters-in-a-command-line-app
parser = argparse.ArgumentParser()
parser.add_argument('function')
parser.add_argument('arguments', nargs='*')
args = parser.parse_args()


def export_mosaic_to_tif(input_ras, output_ras, ras_type):

    # Can't log to STDOUT for some reason, likely to due with calling this from subprocess
    print 'Exporting {0} to {1}'.format(input_ras, output_ras)

    # Export confidence to 8 bit
    if ras_type == 'confidence':

        arcpy.CopyRaster_management(input_ras, output_ras, "", "", "8", "NONE", "NONE",
                                    "8_BIT_UNSIGNED", "NONE", "NONE")

    # Export date to 16 bit, then set null to remove 0 values
    elif ras_type == 'filter_glad_alerts':

        date_16_bit = os.path.join(os.path.dirname(output_ras), 'filter_glad_alerts_16bit.tif')
        arcpy.CopyRaster_management(input_ras, date_16_bit, "", "", "16", "NONE", "NONE",
                                    "16_BIT_UNSIGNED", "NONE", "NONE")

        arcpy.gp.SetNull_sa(date_16_bit, date_16_bit, output_ras, """"Value" = 0""")

    # Reclass confidence to 1/0, then change pixel type to float
    elif ras_type == 'intensity':

        out_reclass = os.path.join(os.path.dirname(output_ras), 'intensity_4bit.tif')
        arcpy.gp.Reclassify_sa(input_ras, "Value", "2 1;3 1;NODATA 0", out_reclass)

        arcpy.CopyRaster_management(out_reclass, output_ras, "", "", "15", "NONE", "NONE",
                                    "32_BIT_FLOAT", "NONE", "NONE")
    else:
        print 'Unknown raster {0}. Exiting'.format(ras_type)
        sys.exit(1)


def resample_single_tif(input_ras, output_ras, ras_type, cell_size):

    if ras_type in ['date', 'confidence']:
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
        print 'Error in gdal_translate subprocess. Exiting.'
        sys.exit(1)

    arcpy.AddRastersToMosaicDataset_management(source_mosaic, "Raster Dataset", output_ras,
                                               "UPDATE_CELL_SIZES", "UPDATE_BOUNDARY", "NO_OVERVIEWS", 0)


func_options = {
                    'export_mosaic_to_tif': export_mosaic_to_tif,
                    'resample_single_tif': resample_single_tif,
                    'run_gdal_translate': run_gdal_translate
                }


if __name__ == '__main__':
    func_options[args.function](*args.arguments)
