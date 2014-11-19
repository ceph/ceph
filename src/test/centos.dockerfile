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
FROM centos:%%os_version%%

RUN useradd -M --uid %%user_id%% %%USER%%
# http://jperrin.github.io/centos/2014/09/25/centos-docker-and-systemd/
RUN yum -y swap -- remove fakesystemd -- install systemd systemd-libs
RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == systemd-tmpfiles-setup.service ] || rm -f $i; done)
RUN rm -f /lib/systemd/system/multi-user.target.wants/*
RUN rm -f /etc/systemd/system/*.wants/*
RUN rm -f /lib/systemd/system/local-fs.target.wants/*
RUN rm -f /lib/systemd/system/sockets.target.wants/*udev*
RUN rm -f /lib/systemd/system/sockets.target.wants/*initctl*
RUN rm -f /lib/systemd/system/basic.target.wants/*
RUN rm -f /lib/systemd/system/anaconda.target.wants/*
RUN yum install -y redhat-lsb-core
# build dependencies from spec.in
RUN yum install -y wget 
RUN wget http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-2.noarch.rpm
RUN rpm -ivh epel-release-7-2.noarch.rpm
RUN yum install -y make gcc-c++ libtool boost-devel bzip2-devel libedit-devel perl gdbm pkgconfig python python-nose python-argparse libaio-devel libcurl-devel libxml2-devel libuuid-devel libblkid-devel libudev-devel leveldb-devel xfsprogs-devel yasm snappy-devel sharutils gperftools-devel google-perftools-devel mozilla-nss-devel keyutils-devel libatomic-ops-devel fdupes nss-devel keyutils-libs-devel libatomic_ops-devel gperftools-devel fuse-devel libexpat-devel FastCGI-devel expat-devel fcgi-devel lttng-ust-devel libbabeltrace-devel java-devel java-devel junit4 
# development tools
RUN yum install -y ccache valgrind gdb git
# make check dependencies
RUN yum install -y python-virtualenv gdisk kpartx
