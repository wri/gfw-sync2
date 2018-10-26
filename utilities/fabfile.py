import fabric.api

import token_util


def kickoff(proc_name, regions, years, gfw_env):

    token_info = token_util.get_token('s3_read_write.config')
    aws_access_key = token_info[0][1]
    aws_secret_key = token_info[1][1]

    region_str = ' '.join(regions.split(';'))
    year_str = ' '.join(years.split(';'))

    if proc_name == 'umd_landsat_alerts':

        final_cmd = 'python /home/ubuntu/staging/glad-vt-analysis/update_glad_data.py ' \
                    '-r {} -y {}'.format(region_str, year_str)

        if gfw_env == 'staging':
            final_cmd += ' --staging'

    else:
        # Generate the mapnik tiles and push to s3
        tile_cmd = 'python /home/ubuntu/mapnik-forest-change-tiles/generate-tiles.py'
        tile_cmd += ' -l terrai -r {} -y {} --world'.format(region_str, year_str)

        # Write the rasters to point and push to s3
        point_cmd = 'python /home/ubuntu/raster-vector-to-tsv/processing/utilities/weekly_updates.py'
        point_cmd += ' -l terrai -r {} -y {}'.format(region_str, year_str)

        # add staging flags if necessary
        if gfw_env == 'staging':
            tile_cmd += ' --staging'
            point_cmd += ' --staging'

        # required because fabric will wait if process is not actively connected to this machine
        # can't do multiple fabric.api.run calls for some reason
        # http://docs.fabfile.org/en/1.6/faq.html#my-cd-workon-export-etc-calls-don-t-seem-to-work
        final_cmd = ' && '.join([tile_cmd, point_cmd])

    # Required, even though these are set for ubuntu in .bashrc
    with fabric.api.shell_env(S3_ACCESS_KEY=aws_access_key, S3_SECRET_KEY=aws_secret_key):
        fabric.api.run(final_cmd)

        # Important to signal the global_forest_change_layer to kill the subprocess
        print '****FAB SUBPROCESS COMPLETE****'

