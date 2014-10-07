#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
# Environment variables are substituted via envsubst(1)
# 
# user_id=$(id -u)
# os_version= the desired REPOSITORY TAG
#
FROM ubuntu:%%os_version%%

RUN useradd -M --uid %%user_id%% %%USER%%
RUN apt-get update
# build dependencies
RUN apt-get install -y autoconf automake autotools-dev libbz2-dev debhelper default-jdk git javahelper junit4 libaio-dev libatomic-ops-dev libbabeltrace-ctf-dev libbabeltrace-dev libblkid-dev libboost-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev libcurl4-gnutls-dev libedit-dev libexpat1-dev libfcgi-dev libfuse-dev libgoogle-perftools-dev libkeyutils-dev libleveldb-dev libnss3-dev libsnappy-dev liblttng-ust-dev libtool libudev-dev libxml2-dev pkg-config python python-argparse python-nose uuid-dev uuid-runtime xfslibs-dev yasm 
# development tools
RUN apt-get install -y ccache valgrind gdb
# make check dependencies
RUN apt-get install -y python-virtualenv gdisk kpartx
