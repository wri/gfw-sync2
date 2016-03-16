__author__ = 'Thomas.Maschler'

import zipfile
import os
import glob


def unzip(source_filename, dest_dir):
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)


def gen_paths(src):
    basepath, fname = os.path.split(src)
    base_fname = os.path.splitext(fname)[0]

    return basepath, fname, base_fname


def add_to_zip(fname, zf):

    bname = os.path.basename(fname)
    ending = os.path.splitext(bname)[1]
    if not ending ==  ".lock" and not ending == ".zip":
        #print 'Writing %s to archive' % ending
        # flatten zipfile
        zf.write(fname, bname)

    return

def zip_file(src, dst, local):

    basepath, fname, base_fname = gen_paths(src)

    if local:
        zip_name = base_fname + "_local.zip"
    else:
        zip_name = base_fname + ".zip"

    zip_path = os.path.join(dst, zip_name)
    zf = zipfile.ZipFile(zip_path, 'w', allowZip64=False)

    search = os.path.join(basepath, "*.*")
    files = glob.glob(search)
    for f in files:
        bname = os.path.basename(f)
        if (base_fname in bname) and (bname != zip_name):
            add_to_zip(f, zf)

    zf.close()

    return zip_name
