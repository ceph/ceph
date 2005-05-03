
# mpicxx must be on your path; on Szilard, this means that
# /usr/local/mpich2-1.0.1/bin must be on your path.

# For now, use g++ most of the time.
# When compiling MPI stuff, specify myfile.cc instead of myfile.o so that ${MPICC} is 
# invoked instead of the generic .o rule (or it'll use g++).
# This makes it less annoying to build on non-mpi hosts for dev work, and seems to 
# behave just fine...  change ${CC} back to mpicxx if you get paranoid.
CC = g++
MPICC = mpicxx
CFLAGS = -g -I. -pg -D_FILE_OFFSET_BITS=64 -DMPICH_IGNORE_CXX_SEEK -D_REENTRANT -D_THREAD_SAFE
LIBS = -lpthread -lfuse

MDS_OBJS= \
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
	mds/MDCluster.o

COMMON_OBJS= \
	msg/Messenger.o\
	msg/error.o\
	Logger.o\
	clock.o\
	config.o

TEST_TARGETS = fakemds mpitest mttest
TARGETS = import singleclient

SRCS=*.cc */*.cc

all: depend ${TARGETS}

test: depend ${TEST_TARGETS}



import: mds/mds.o osd/OSD.o msg/FakeMessenger.o import.cc ${COMMON_OBJS} 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

singleclient: mds/mds.o osd/OSD.o fakesingleclient.o client/Client.o \
msg/CheesySerializer.o msg/FakeMessenger.o fsck.o ${COMMON_OBJS} 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

fuseclient: client/Client.o client/fuse.o msg/CheesySerializer.o \
msg/FakeMessenger.o ${COMMON_OBJS} 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

fakemds: test/fakemds.cc msg/FakeMessenger.o fakeclient/FakeClient.o osd/OSD.o mds/mds.o ${COMMON_OBJS} 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

mpitest: test/mpitest.o msg/MPIMessenger.cc mds/mds.o osd/OSD.o fakeclient/FakeClient.o ${COMMON_OBJS} 
	${MPICC} ${CFLAGS} $^ -o $@

mttest: test/mttest.cc msg/MTMessenger.cc ${COMMON_OBJS}
	${MPICC} ${CFLAGS} ${LIBS} $^ -o $@

mpifuse: mpifuse.cc mds/mds.o client/Client.o osd/OSD.o client/fuse.o msg/MPIMessenger.cc ${COMMON_OBJS}
	${MPICC} ${CFLAGS} ${LIBS} $^ -o $@

fakefuse: fakefuse.cc mds/mds.o client/Client.o osd/OSD.o client/fuse.o msg/FakeMessenger.cc msg/CheesySerializer.o ${COMMON_OBJS}
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

clean:
	rm -f *.o */*.o ${TARGETS}

mds/mds.o: ${MDS_OBJS}
	ld -i -o mds/mds.o $^

%.o: %.cc
	${CC} ${CFLAGS} -c $< -o $@

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null

# now add a line to include the dependency list.
include .depend
