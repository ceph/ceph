
CC=g++
#CC=mpiCC
CFLAGS=-D __gnu_cxx=std -g -I.  -I/usr/lib/mpi/include -L/usr/lib/mpi/lib
LIBS=

MPICC=g++
MPILIBS=-lelan -lmpi  ${LIBS}

LEAKTRACER=
LEAKTRACER=$(HOME)/lib/LeakTracer.o

SRCS=*.cc */*.cc
OBJS=osd/OSD.o Messenger.o Logger.o mds/MDBalancer.o mds/MDS.o mds/CDentry.o mds/CDir.o mds/CInode.o mds/MDCache.o mds/MDStore.o clock.o mds/LogStream.o mds/MDLog.o client/Client.o mds/MDCluster.o
TARGETS=test import

all: depend ${TARGETS}


test: test.cc FakeMessenger.o pmds.o
	test ! -e leak.out || rm leak.out
	${CC} ${CFLAGS} ${LIBS} pmds.o FakeMessenger.o ${LEAKTRACER} test.cc -o test

import: pmds.o FakeMessenger.o import.cc
	${CC} ${CFLAGS} ${LIBS} pmds.o FakeMessenger.o import.cc -o import

mpitest: mpitest.cc MPIMessenger.o pmds.o
	${MPICC} ${CFLAGS} ${MPILIBS} mpitest.cc MPIMessenger.o pmds.o  -o mpitest


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
