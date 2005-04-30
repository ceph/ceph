
CC=g++
#CC=distcc g++
CFLAGS=-g -I. -pg -D_FILE_OFFSET_BITS=64
LIBS=-lpthread

#
# i give up, use MPI provided compile scripts, like you're supposed to.
#
MPIDIR=/usr/local/mpich-1.2.6
MPICC=${MPIDIR}/bin/mpicxx
MPICFLAGS=${CFLAGS} -I${MPIDIR}/include -L${MPIDIR}/lib
MPILIBS=${LIBS}

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

TARGETS=test import mpitest

all: depend ${TARGETS}


test: test.cc msg/FakeMessenger.o pmds.o msg/MTMessenger.o msg/support.o
	test ! -e leak.out || rm leak.out
	${CC} ${CFLAGS} ${LIBS} pmds.o msg/FakeMessenger.o test.cc ${LEAKTRACER} -o test

import: pmds.o msg/FakeMessenger.o import.cc
	${CC} ${CFLAGS} ${LIBS} pmds.o msg/FakeMessenger.o import.cc ${LEAKTRACER} -o import

mpitest: mpitest.o msg/MPIMessenger.cc pmds.o
	${MPICC} ${CFLAGS} ${MPILIBS} mpitest.o msg/MPIMessenger.cc pmds.o  -o mpitest

singleclient: pmds.o fakesingleclient.o client/Client.o msg/CheesySerializer.o msg/FakeMessenger.o fsck.o
	${CC} ${CFLAGS} ${LIBS} pmds.o client/Client.o msg/FakeMessenger.o msg/CheesySerializer.o fakesingleclient.o fsck.o ${LEAKTRACER} -o singleclient

fuseclient: client/Client.o client/fuse.o msg/CheesySerializer.o msg/FakeMessenger.o 
	${CC} ${CFLAGS} ${LIBS} pmds.o client/fuse.o client/Client.o msg/FakeMessenger.o msg/CheesySerializer.o ${LEAKTRACER} -o fuseclient

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
