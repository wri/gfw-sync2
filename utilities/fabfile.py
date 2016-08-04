import fabric.api
import util


def kickoff(proc_name):

    token_info = util.get_token('s3_read_write.config')
    aws_access_key = token_info[0][1]
    aws_secret_key = token_info[1][1]

    lkp_proc_name = {'umd_landsat_alerts': 'glad', 'terrai': 'terrai'}
    tile_layer_name = lkp_proc_name[proc_name]

    # Required, even though these are set for ubuntu in .bashrc
    # Set for both tilestache and s4cmd . . . annoyingly different
    # Previouly used AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as well for tilestache
    with fabric.api.shell_env(S3_ACCESS_KEY=aws_access_key, S3_SECRET_KEY=aws_secret_key):

        # cmd = 'python /home/ubuntu/mapnik-forest-change-tiles/generate-tiles.py -l {0} -r all'.format(proc_name)
        cmd = 'python /home/ubuntu/mapnik-forest-change-tiles/generate-tiles.py -l {0} -r all'.format(tile_layer_name)
        fabric.api.run(cmd)
