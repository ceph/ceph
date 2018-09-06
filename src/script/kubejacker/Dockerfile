
from BASEIMAGE

# Some apt-get commands fail in docker builds because they try
# and do interactive prompts
ENV TERM linux

# Baseline rook images may be from before the `rook` ceph-mgr module,
# so let's install the dependencies of that
RUN yum install -y python-pip
RUN pip install kubernetes==6.0.0

# New RGW dependency since luminous
RUN yum install -y liboath

# For the dashboard, if the rook images are pre-Mimic
RUN yum install -y python-bcrypt librdmacm

ADD bin.tar.gz /usr/bin/
ADD lib.tar.gz /usr/lib64/

# Assume developer is using default paths (i.e. /usr/local), so
# build binaries will be looking for libs there.
ADD eclib.tar.gz /usr/local/lib64/ceph/erasure-code/
ADD mgr_plugins.tar.gz /usr/local/lib64/ceph/mgr

