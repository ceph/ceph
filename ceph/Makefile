
# mpicxx must be on your path; on Szilard and googoo, this means that
# /usr/local/mpich2-1.0.2/bin must be on your path.

# For now, use g++ most of the time.
# When compiling MPI stuff, specify myfile.cc instead of myfile.o so that ${MPICC} is 
# invoked instead of the generic .o rule (or it'll use g++).
# This makes it less annoying to build on non-mpi hosts for dev work, and seems to 
# behave just fine...  change ${CC} back to mpicxx if you get paranoid.
CC = g++
CFLAGS = -g -Wall -I. -D_FILE_OFFSET_BITS=64 -DMPICH_IGNORE_CXX_SEEK -D_REENTRANT -D_THREAD_SAFE
LIBS = -lpthread  -lrt -ldb

#for normal mpich2 machines
MPICC = mpicxx
MPICFLAGS = ${CFLAGS}
MPILIBS = ${LIBS}

#for LLNL boxes without mpicxx
#MPICC = g++
#MPICFLAGS = ${CFLAGS} -I/usr/lib/mpi/include -L/usr/lib/mpi/mpi_gnu/lib
#MPILIBS = ${LIBS} -lelan -lmpi

EBOFS_OBJS= \
	ebofs/BlockDevice.o\
	ebofs/BufferCache.o\
	ebofs/Ebofs.o\
	ebofs/Allocator.o

MDS_OBJS= \
	mds/MDS.o\
	mds/MDCache.o\
	mds/MDBalancer.o\
	mds/CDentry.o\
	mds/CDir.o\
	mds/CInode.o\
	mds/AnchorTable.o\
	mds/MDStore.o\
	mds/LogStream.o\
	mds/IdAllocator.o\
	mds/MDLog.o\
	mds/MDCluster.o\
	mds/OSDMonitor.o

COMMON_OBJS= \
	msg/Messenger.o\
	msg/Dispatcher.o\
	msg/error.o\
	msg/HostMonitor.o\
	osd/FakeStore.o\
	osd/Filer.o\
	osd/OSDMap.o\
	osd/PG.o\
	osd/rush.o\
	common/Logger.o\
	common/Clock.o\
	common/Timer.o\
	config.o

SYN_OBJS = \
	client/SyntheticClient.o\
	client/Trace.o

TCP_OBJS = \
	msg/TCPMessenger.o\
	msg/TCPDirectory.o

TEST_TARGETS = fakemds 
TARGETS = fakefuse fakesyn tcpsyn tcpfuse

SRCS=*.cc */*.cc

all: depend ${TARGETS}

test: depend ${TEST_TARGETS}

obfs: depend obfstest

gprof-helper.so: test/gprof-helper.c
	gcc -shared -fPIC test/gprof-helper.c -o gprof-helper.so -lpthread -ldl 


# old test crap
import: mds/allmds.o osd/OSD.o msg/FakeMessenger.o import.cc ${COMMON_OBJS} 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

singleclient: mds/allmds.o osd/OSD.o fakesingleclient.o client/Client.o client/Buffercache.o msg/FakeMessenger.o fsck.o ${COMMON_OBJS} 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

tp: osd/tp.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

fuseclient: client/Client.o client/Buffercache.o client/fuse.o msg/FakeMessenger.o ${COMMON_OBJS} 
	${CC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@

fakemds: test/fakemds.cc msg/FakeMessenger.o fakeclient/FakeClient.o osd/OSD.o mds/allmds.o ${COMMON_OBJS} 
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

mpitest: test/mpitest.o msg/MPIMessenger.cc mds/allmds.o osd/OSD.o fakeclient/FakeClient.o ${COMMON_OBJS} 
	${MPICC} ${CFLAGS} $^ -o $@

mttest: test/mttest.cc msg/MTMessenger.cc ${COMMON_OBJS}
	${MPICC} ${CFLAGS} ${LIBS} $^ -o $@


# fuse
fakefuse: fakefuse.cc mds/allmds.o client/Client.o client/Buffercache.o osd/OSD.o client/fuse.o msg/FakeMessenger.cc ${COMMON_OBJS}
	${CC} -pg ${CFLAGS} ${LIBS} -lfuse $^ -o $@

tcpfuse: tcpfuse.cc mds/allmds.o client/Client.o client/Buffercache.o osd/OSD.o client/fuse.o ${TCP_OBJS} ${COMMON_OBJS}
	${MPICC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@

mpifuse: mpifuse.cc mds/allmds.o client/Client.o client/Buffercache.o osd/OSD.o client/fuse.o ${TCP_OBJS} ${COMMON_OBJS}
	${MPICC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@


# synthetic workload
fakesyn: fakesyn.cc mds/allmds.o client/Client.o client/Buffercache.o osd/OSD.o msg/FakeMessenger.o ${COMMON_OBJS} ${SYN_OBJS}
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@

mpisyn: mpisyn.cc mds/allmds.o client/Client.o client/Buffercache.o osd/OSD.o msg/MPIMessenger.cc ${COMMON_OBJS} ${SYN_OBJS}
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

tcpsyn: tcpsyn.cc mds/allmds.o client/Client.o client/Buffercache.o osd/OSD.o ${TCP_OBJS} ${COMMON_OBJS} ${SYN_OBJS}
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

# obfs + synthetic
obfstest: tcpsyn.cc mds/allmds.o client/Client.o client/Buffercache.o osd/OSD.cc osd/OBFSStore.o msg/TCPMessenger.cc ${COMMON_OBJS} $(SYN_OBJS)
	${MPICC} -DUSE_OBFS ${MPICFLAGS} ${MPILIBS} $^ -o $@ ../uofs/uofs.a

# ebofs
ebofs: mkfs.ebofs test.ebofs

mkfs.ebofs: ebofs/mkfs.ebofs.cc config.cc common/Clock.o ebofs/ebo.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@

test.ebofs: ebofs/test.ebofs.cc config.cc common/Clock.o ebofs/ebo.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@




testmpi: test/testmpi.cc msg/MPIMessenger.cc config.o common/Timer.o common/clock.o msg/Messenger.o msg/Dispatcher.o msg/error.o
	${MPICC} ${CFLAGS} ${LIBS} $^ -o $@


clean:
	rm -f *.o */*.o ${TARGETS} ${TEST_TARGETS}

ebofs/ebo.o: ${EBOFS_OBJS}
	ld -i -o $@ $^

mds/allmds.o: ${MDS_OBJS}
	ld -i -o $@ $^

%.o: %.cc
	${CC} ${CFLAGS} -c $< -o $@

count:
	cat *.cc */*.cc */*.h */*/*.h | wc -l
	cat *.cc */*.cc */*.h */*/*.h | grep -c \;

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null

# now add a line to include the dependency list.
include .depend
