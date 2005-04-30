
# mpicxx must be on your path; on Szilard, this means that
# /usr/local/mpich2-1.0.1/bin must be on your path.
CC=mpicxx
MPICC=mpicxx
CFLAGS=-g -I. -pg -D_FILE_OFFSET_BITS=64 -DMPICH_IGNORE_CXX_SEEK
LIBS=-lpthread

PMDS_OBJS= \
	osd/OSD.o\
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

TARGETS=test import mpitest singleclient

all: depend ${TARGETS}


test: test.cc msg/FakeMessenger.o pmds.o  msg/MTMessenger.o msg/support.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

import: pmds.o msg/FakeMessenger.o import.cc
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

mpitest: mpitest.o msg/MPIMessenger.cc pmds.o
	${MPICC} ${CFLAGS} $^ -o $@

singleclient: pmds.o fakesingleclient.o client/Client.o \
msg/CheesySerializer.o msg/FakeMessenger.o 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

fuseclient: client/Client.o client/fuse.o msg/CheesySerializer.o \
msg/FakeMessenger.o 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

clean:
	rm -f *.o */*.o ${TARGETS}

%.o: %.cc
	${CC} ${CFLAGS} -c $< -o $@

pmds.o: ${PMDS_OBJS}
	ld -i -o pmds.o $^

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null

# now add a line to include the dependency list.
include .depend
