
CXX?=g++
CXX_FLAGS?=-Wno-unused-parameter -Wall -Wextra -Werror -g
CXX_LIBS?=-lboost_system -lrbd -lrados
CXX_INC?=$(LOCAL_LIBRADOS_INC)
CXX_CC=$(CXX) $(CXX_FLAGS) $(CXX_INC) $(LOCAL_LIBRADOS)

# Relative path to the Ceph source:
CEPH_SRC_HOME?=../../src
CEPH_BLD_HOME?=../../build

LOCAL_LIBRADOS?=-L$(CEPH_BLD_HOME)/lib/ -Wl,-rpath,$(CEPH_BLD_HOME)/lib
LOCAL_LIBRADOS_INC?=-I$(CEPH_SRC_HOME)/include

all: hello_world_cpp 

# Build against the system librados instead of the one in the build tree:
all-system: LOCAL_LIBRADOS=
all-system: LOCAL_LIBRADOS_INC=
all-system: all

hello_world_cpp: hello_world.cc
	$(CXX_CC) -o hello_world_cpp hello_world.cc $(CXX_LIBS)

clean:
	rm -f hello_world_cpp

