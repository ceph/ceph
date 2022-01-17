FROM ubuntu:latest
ENV DEBIAN_FRONTEND=nonintercative
RUN \
    mkdir $HOME/.ssh && \
    touch $HOME/.ssh/id_ed25519 && \
    chmod 600 $HOME/.ssh/id_ed25519 && \
    echo "StrictHostKeyChecking=no" > $HOME/.ssh/config && \
    echo "UserKnownHostsFile=/dev/null" >> $HOME/.ssh/config
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
COPY . /teuthology
COPY .teuthology.yaml ./root
WORKDIR /teuthology
RUN \
    mkdir ../archive_dir && \
    mkdir log && \
    chmod +x /teuthology/bootstrap && \
    ./bootstrap
CMD echo "$SSH_PRIVKEY" > $HOME/.ssh/id_ed25519 && \
    ./virtualenv/bin/teuthology-suite \
    -v --ceph-repo https://github.com/ceph/ceph.git \
    --suite-repo https://github.com/ceph/ceph.git \
    -c master -m testnode --subset 9000/100000 --limit 1 \
    --suite dummy \
    --suite-branch master -p 75 --force-priority -n 100 && \
    ./virtualenv/bin/teuthology-dispatcher --log-dir ./log --tube testnode ; \
    tail -f /dev/null
