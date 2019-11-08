#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from subprocess import PIPE,Popen, call
import os
import errno

proc = Popen(['hadoop', 'fs', '-ls', '/data/share/venpath/data/pings/*/*'], stdout=PIPE)
out = proc.communicate()[0].split('\n')
files = [x.split()[-1] for x in out if '/data/share/' in x]


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


for i, f in enumerate(files):
    print(i, f)
    year_month_date = "/".join(f.split("/")[-3:])
    cusp_dest_path = "/home/cusp/pp1994/pings/" + year_month_date
    if not os.path.exists(cusp_dest_path):
        mkdir_p(cusp_dest_path)

    # copy file to local
    os.system("hadoop fs -copyToLocal %s %s" % (f, cusp_dest_path))

    # rsync to hpc
    os.system("rsync -a -z -v --relative -e ssh %s pp1994@dtn.hpc.nyu.edu:/scratch/pp1994/" % cusp_dest_path)

    # remove
    os.system("rm -r %s" % cusp_dest_path)
