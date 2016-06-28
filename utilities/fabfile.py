import fabric.api
import util


def kickoff(proc_name):

    token_info = util.get_token('s3_read_write.config')
    aws_access_key = token_info[0][1]
    aws_secret_key = token_info[1][1]

    # Required, even though these are set for ubuntu in .bashrc
    with fabric.api.shell_env(AWS_ACCESS_KEY_ID=aws_access_key, AWS_SECRET_ACCESS_KEY=aws_secret_key):
        if proc_name == 'GLAD':

            cmd = 'python /home/ubuntu/glad/glad-processing-gdal/process_glad.py -r sa_test'
            fabric.api.run(cmd)

        else:
            raise ValueError("Unknown process name in fabfile.py")
