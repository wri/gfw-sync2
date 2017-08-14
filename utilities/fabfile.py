import fabric.api
import util
import datetime


def kickoff(proc_name, regions, years, gfw_env):

    token_info = util.get_token('s3_read_write.config')
    aws_access_key = token_info[0][1]
    aws_secret_key = token_info[1][1]

    lkp_proc_name = {'umd_landsat_alerts': 'glad', 'terrai': 'terrai'}
    tile_layer_name = lkp_proc_name[proc_name]

    region_str = ' '.join(regions.split(';'))
    year_str = ' '.join(years.split(';'))

    # Generate the mapnik tiles and push to s3
    tile_cmd = 'python /home/ubuntu/mapnik-forest-change-tiles/generate-tiles.py'
    tile_cmd += ' -l {0} -r {1} -y {2} --world'.format(tile_layer_name, region_str, year_str)

    # Write the rasters to point and push to s3
    point_cmd = 'python /home/ubuntu/raster-vector-to-tsv/processing/utilities/weekly_updates.py'
    point_cmd += ' -l {0} -r {1} -y {2}'.format(tile_layer_name, region_str, year_str)

    # add staging flags if necessary
    if gfw_env == 'staging':
        tile_cmd += ' --staging'
        point_cmd += ' --staging'

    ptw_cmd = 'python /home/ubuntu/gfw-places-to-watch/update-ptw.py -r all --threads 25'

    # Required, even though these are set for ubuntu in .bashrc
    with fabric.api.shell_env(S3_ACCESS_KEY=aws_access_key, S3_SECRET_KEY=aws_secret_key):

        cmd_list = [tile_cmd, point_cmd]

        # If today's date is >= 4 and <= 10 and south_america is to be processed, run ptw
        if tile_layer_name == 'glad' and run_ptw() and 'south_america' in region_str and gfw_env != 'staging':
            cmd_list += [ptw_cmd]

        # required because fabric will wait if process is not actively connected to this machine
        # can't do multiple fabric.api.run calls for some reason
        # http://docs.fabfile.org/en/1.6/faq.html#my-cd-workon-export-etc-calls-don-t-seem-to-work
        final_cmd = ' && '.join(cmd_list)
        fabric.api.run(final_cmd)

        # Important to signal the global_forest_change_layer to kill the subprocess
        print '****FAB SUBPROCESS COMPLETE****'


def run_ptw():
    today = datetime.datetime.today()

    # PTW on hold until we get GLAD fully back online
    # return today.day in range(4, 11)
    return False
