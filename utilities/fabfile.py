import fabric.api
import util
import datetime
import calendar


def kickoff(proc_name, *regions):

    token_info = util.get_token('s3_read_write.config')
    aws_access_key = token_info[0][1]
    aws_secret_key = token_info[1][1]

    lkp_proc_name = {'umd_landsat_alerts': 'glad', 'terrai': 'terrai'}
    tile_layer_name = lkp_proc_name[proc_name]

    region_str = ' '.join(regions)

    tile_cmd = 'python /home/ubuntu/mapnik-forest-change-tiles/generate-tiles.py'
    tile_cmd += ' -l {0} -r {1} --world'.format(tile_layer_name, region_str)

    point_cmd = 'python /home/ubuntu/raster-vector-to-tsv/processing/utilities/weekly_updates.py'
    point_cmd += ' -l {0}'.format(lkp_proc_name)

    ptw_cmd = 'python /home/ubuntu/gfw-places-to-watch/update-ptw.py -r all'

    # # Required, even though these are set for ubuntu in .bashrc
    # # Set for both tilestache and s4cmd . . . annoyingly different
    # # Previouly used AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as well for tilestache
    with fabric.api.shell_env(S3_ACCESS_KEY=aws_access_key, S3_SECRET_KEY=aws_secret_key):

        # Generate the mapnik tiles and push to s3
        fabric.api.run(tile_cmd)

        # Write the rasters to point and push to s3
        fabric.api.run(point_cmd)

        # If it's the last week of the month, run ptw
        if proc_name == 'umd_landsat_alerts' and run_ptw():
            fabric.api.run(ptw_cmd)

        # Important to signal the global_forest_change_layer to kill the subprocess
        print '****FAB SUBPROCESS COMPLETE****'


def run_ptw():

    today = datetime.datetime.today()
    all_weeks = calendar.monthcalendar(today.year, today.month)

    full_weeks = [w for w in all_weeks if len(set(w)) == 7]

    last_full_week = full_weeks[-1]

    return today.day in last_full_week
