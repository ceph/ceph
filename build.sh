#!/bin/bash
./install-deps.sh
./do_cmake.sh
cd build
make check -j$(nproc)
