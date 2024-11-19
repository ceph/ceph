#!/bin/sh -ex

# This script is intended to run within centos-9 container
# image name: quay.io/centos/centos:stream9
# docker run --rm -v "$(pwd):/src" -w /src quay.io/centos/centos:stream9 /bin/bash -c "./do_containerized_build.sh"
# Based on https://docs.ceph.com/en/reef/install/build-ceph/#:~:text=You%20can%20get%20Ceph%20software,packages%20and%20install%20the%20packages.


# Before you can build Ceph source code, you need to install several libraries and tools:
./install-deps.sh

# The container root ownership issue
#fatal: detected dubious ownership in repository at '/src'
# To add an exception for this directory, call:
git config --global --add safe.directory /src

# Ceph is built using cmake.
./do_cmake.sh
cd build
ninja

# return the ownership to the user
#sudo chown -R $(id -u):$(id -g)  .
