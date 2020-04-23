#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
Author: Prasanna Parasurama
"""

import boto3
import yaml
from joblib import Parallel, delayed

with open('secrets.yaml') as f:
    secrets = yaml.safe_load(f)

session = boto3.Session()

resource = boto3.resource("s3",
                          aws_access_key_id=secrets['aws_access_key_id'],
                          aws_secret_access_key=secrets['aws_secret_access_key'])

bucket = resource.Bucket('out-nyu')
files = list(bucket.objects.all())
keys = [f.key for f in files]

client = boto3.client("s3",
                      aws_access_key_id=secrets['aws_access_key_id'],
                      aws_secret_access_key=secrets['aws_secret_access_key'])


def download_file(key):
    file_name = '_'.join(key.split('/'))

    client.download_file(Bucket='out-nyu',
                         Key=key,
                         Filename="/scratch/pp1994/venpath_covid/{}".format(file_name))

    return True


# download files in parallel
res = Parallel(n_jobs=16,
               backend='threading',
               verbose=True)(delayed(download_file)(k) for k in keys)
