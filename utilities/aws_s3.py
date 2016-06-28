import boto
import datetime


def get_timestamps(bucket, filelist):

    out_dict = {}

    s3 = boto.connect_s3()
    bucket = s3.lookup(bucket)

    for key in bucket:
        if key.name in filelist:
            out_dict[key.name] = datetime.datetime.strptime(key.last_modified, '%Y-%m-%dT%H:%M:%S.000Z')

    return out_dict
