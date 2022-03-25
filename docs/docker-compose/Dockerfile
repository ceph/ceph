FROM ubuntu:latest
ARG SSH_PRIVKEY_FILE=id_ed25519
ENV DEBIAN_FRONTEND=noninteractive
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
RUN \
    mkdir $HOME/.ssh && \
    touch $HOME/.ssh/${SSH_PRIVKEY_FILE} && \
    chmod 600 $HOME/.ssh/${SSH_PRIVKEY_FILE} && \
    echo "StrictHostKeyChecking=no" > $HOME/.ssh/config && \
    echo "UserKnownHostsFile=/dev/null" >> $HOME/.ssh/config
COPY teuthology.sh /
RUN mkdir -p /etc/ansible
COPY ansible_inventory/hosts ansible_inventory/secrets /etc/ansible/
ENTRYPOINT /teuthology.sh