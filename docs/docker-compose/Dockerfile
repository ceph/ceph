FROM ubuntu:latest
ENV DEBIAN_FRONTEND=nonintercative
COPY . /teuthology
COPY .teuthology.yaml ./root
WORKDIR /teuthology
RUN chmod +x /teuthology/bootstrap && \
    mkdir ../archive_dir && \
    mkdir log
RUN apt-get update && \
    apt-get install -y \
    git \
    qemu-utils \
    python3-dev \
    libssl-dev \
    ipmitool \
    python3-pip \
    python3-virtualenv \
    vim \
    libev-dev \
    libvirt-dev \
    libffi-dev \
    libyaml-dev \
    lsb-release && \
    apt-get clean all
CMD  ./bootstrap && ./virtualenv/bin/teuthology-suite \
-v --ceph-repo https://github.com/ceph/ceph.git \
--suite-repo https://github.com/ceph/ceph.git \
-c master -m smithi --subset 9000/100000 --limit 1 \
--suite dummy \
--suite-branch master -p 75 --force-priority -n 100 \
&& tail -f /dev/null
