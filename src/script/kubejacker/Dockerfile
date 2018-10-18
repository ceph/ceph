from BASEIMAGE

# Some apt-get commands fail in docker builds because they try
# and do interactive prompts
ENV TERM linux

# Baseline rook images may be from before the `rook` ceph-mgr module,
# so let's install the dependencies of that
# New RGW dependency since luminous: liboath
# For the dashboard, if the rook images are pre-Mimic: ython-bcrypt librdmacm

RUN (grep -q rhel /etc/os-release && ( \
        yum install -y python-pip && \
        yum install -y liboath && \
        yum install -y python-bcrypt librdmacm && \
        pip install kubernetes==6.0.0 \
    )) || (grep -q suse /etc/os-release && ( \
        zypper --non-interactive --gpg-auto-import-keys install --no-recommends --auto-agree-with-licenses --replacefiles --details \
            python3-kubernetes \
            liboauth-devel \
            python-bcrypt \
            lz4 \
            librdmacm1 \
            libopenssl1_1 \
    ))

ADD bin.tar.gz /usr/bin/
ADD lib.tar.gz /usr/lib64/

# Assume developer is using default paths (i.e. /usr/local), so
# build binaries will be looking for libs there.
ADD eclib.tar.gz /usr/local/lib64/ceph/erasure-code/
ADD mgr_plugins.tar.gz /usr/local/lib64/ceph/mgr
