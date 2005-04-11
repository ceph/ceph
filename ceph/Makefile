
CC=g++
#CC=distcc g++
CFLAGS=-g -I. -pg
#CFLAGS=-D __gnu_cxx=std -g -I.  #-I/usr/lib/mpi/include -L/usr/lib/mpi/lib
LIBS=

MPICC=g++
MPICFLAGS=${CFLAGS} -I/usr/lib/mpi/include -L/usr/lib/mpi/mpi_gnu/lib
MPILIBS= -lelan -lmpi  ${LIBS}

LEAKTRACER=
#LEAKTRACER=$(HOME)/lib/LeakTracer.o

SRCS=*.cc */*.cc
OBJS=osd/OSD.o\
 msg/Messenger.o\
 mds/MDBalancer.o\
 mds/MDS.o\
 mds/CDentry.o\
 mds/CDir.o\
 mds/CInode.o\
 mds/MDCache.o\
 mds/MDStore.o\
 mds/LogStream.o\
 mds/IdAllocator.o\
 mds/MDLog.o\
 mds/MDCluster.o\
 fakeclient/FakeClient.o\
 Logger.o\
 clock.o\
 config.o

TARGETS=test import pmds

all: depend ${TARGETS}


test: test.cc msg/FakeMessenger.o pmds.o
	test ! -e leak.out || rm leak.out
	${CC} ${CFLAGS} ${LIBS} pmds.o msg/FakeMessenger.o test.cc ${LEAKTRACER} -o test

import: pmds.o msg/FakeMessenger.o import.cc
	${CC} ${CFLAGS} ${LIBS} pmds.o msg/FakeMessenger.o import.cc ${LEAKTRACER} -o import

pmds: mpitest.cc msg/MPIMessenger.cc pmds.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} mpitest.cc msg/MPIMessenger.cc pmds.o  -o pmds


clean:
	rm *.o */*.o ${TARGETS}

%.o: %.cc
	${CC} ${CFLAGS} -c $< -o $@

pmds.o: ${OBJS}
	ld -i -o pmds.o ${OBJS} 

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null

# now add a line to include the dependency list.
include .depend
