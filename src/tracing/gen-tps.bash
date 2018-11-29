#!/bin/bash

python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BlueStore.cc -t c --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BlueStore.cc -t tp --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BlueStore.cc -t h --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BitmapAllocator.cc -t c --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BitmapAllocator.cc -t tp --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BitmapAllocator.cc -t h --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BitmapFreelistManager.cc -t c --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BitmapFreelistManager.cc -t tp --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/os/bluestore/BitmapFreelistManager.cc -t h --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/msg/async/AsyncConnection.cc -t h --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/msg/async/AsyncConnection.cc -t tp --outdir ../../build/
python tracetool/parsetool.py -f /var/data/ceph/src/msg/async/AsyncConnection.cc -t c --outdir ../../build/
touch CMakeLists.txt

