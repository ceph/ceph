
CXX?=g++
CXX_FLAGS?=-std=c++11 -Wall -Wextra -Werror -g
CXX_LIBS?=-lboost_system -lrados -lradosstriper
CXX_INC?=$(LOCAL_LIBRADOS_INC)
CXX_CC=$(CXX) $(CXX_FLAGS) $(CXX_INC) $(LOCAL_LIBRADOS) $(CXX_LIBS)

CC?=gcc
CC_FLAGS=-Wall -Wextra -Werror -g
CC_INC=$(LOCAL_LIBRADOS_INC)
CC_LIBS?=-lrados
CC_CC=$(CC) $(CC_FLAGS) $(CC_INC) $(LOCAL_LIBRADOS) $(CC_LIBS)

# Relative path to the Ceph source:
CEPH_SRC_HOME?=../../src
CEPH_BLD_HOME?=../../build

LOCAL_LIBRADOS?=-L$(CEPH_BLD_HOME)/lib/ -Wl,-rpath,$(CEPH_BLD_HOME)/lib
LOCAL_LIBRADOS_INC?=-I$(CEPH_SRC_HOME)/include

all: hello_world_cpp hello_radosstriper_cpp hello_world_c

# Build against the system librados instead of the one in the build tree:
all-system: LOCAL_LIBRADOS=
all-system: LOCAL_LIBRADOS_INC=
all-system: all

hello_world_cpp: hello_world.cc
	$(CXX_CC) -o hello_world_cpp hello_world.cc

hello_radosstriper_cpp: hello_radosstriper.cc
	$(CXX_CC) -o hello_radosstriper_cpp hello_radosstriper.cc

hello_world_c: hello_world_c.c
	$(CC_CC) -o hello_world_c hello_world_c.c

clean:
	rm -f hello_world_cpp hello_radosstriper_cpp hello_world_c

