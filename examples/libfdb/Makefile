# Mini-example for libfdb

CXX?=g++
CXXFLAGS+=-std=c++23 -Wno-unused-parameter -Wall -Wextra -Werror -g

CEPH_SRC?=../../src
CEPH_BUILD?=../../build

INCLUDES=-I$(CEPH_SRC) -I$(CEPH_BUILD)/_deps/zpp_bits-src
LIBRARY_PATH=-L$(CEPH_BUILD)/lib/ -Wl,-rpath,$(CEPH_BUILD)/lib
LIBRARIES=-lfdb_c -lfmtd

all: mini_example

.PHONY: clean

mini_example: mini_example.cc
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(LIBRARY_PATH) -o mini_example mini_example.cc $(LIBRARIES)

clean:
	rm -f mini_example
