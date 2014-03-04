#!/bin/bash

# get the site-packages directory
base_dir=$(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")
pth_file=$base_dir/gae.pth

echo '/usr/local/google_appengine' > $pth_file
python -c 'import dev_appserver; print("\n".join(dev_appserver.EXTRA_PATHS))' >> $pth_file
