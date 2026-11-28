# Ceph - scalable distributed file system
#
# Copyright (C) 2026 International Business Machines Corp. (IBM)
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation. See file COPYING.

# Mini-example for libfdb

CXX?=g++
CXXFLAGS+=-std=c++23 -Wno-unused-parameter -Wall -Wextra -Werror -g

CEPH_SRC?=../../src
CEPH_BUILD?=../../build

INCLUDES=-I$(CEPH_SRC) -I$(CEPH_BUILD)/_deps/zpp_bits-src
LIBRARY_PATH=-L$(CEPH_BUILD)/lib/ -Wl,-rpath,$(CEPH_BUILD)/lib
LIBRARIES=-lfdb_c -lfmtd
HEADERS=$(wildcard $(CEPH_SRC)/rgw/fdb/*.h)

all: mini_example

.PHONY: all clean

mini_example: mini_example.cc $(HEADERS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(LIBRARY_PATH) -o mini_example mini_example.cc $(LIBRARIES)

clean:
	rm -f mini_example
