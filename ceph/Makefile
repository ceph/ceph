
# mpicxx must be on your path; on Szilard and googoo, this means that
# /usr/local/mpich2-1.0.2/bin must be on your path.

# For now, use g++ most of the time.
# When compiling MPI stuff, specify myfile.cc instead of myfile.o so that ${MPICC} is 
# invoked instead of the generic .o rule (or it'll use g++).
# This makes it less annoying to build on non-mpi hosts for dev work, and seems to 
# behave just fine...  change ${CC} back to mpicxx if you get paranoid.
CC = g++
CFLAGS = -g -Wall -I. -D_FILE_OFFSET_BITS=64 -DMPICH_IGNORE_CXX_SEEK -D_REENTRANT -D_THREAD_SAFE
LIBS = -lpthread -lrt 

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
	mds/OSDMonitor.o

OSD_OBJS= \
	osd/PG.o\
	osd/Ager.o\
	osd/FakeStore.o\
	ebofs.o\
	osd/OSD.o

COMMON_OBJS= \
	msg/Messenger.o\
	msg/Dispatcher.o\
	msg/HostMonitor.o\
	osd/Filer.o\
	osd/ObjectCacher.o\
	osd/Objecter.o\
	osd/OSDMap.o\
	mds/MDCluster.o\
	common/Logger.o\
	common/Clock.o\
	common/Timer.o\
	config.o

CLIENT_OBJS= \
	client/Client.o\
	client/SyntheticClient.o\
	client/Trace.o

TCP_OBJS = \
	msg/TCPMessenger.o\
	msg/TCPDirectory.o

TARGETS = fakefuse fakesyn tcpsyn tcpfuse libceph.o

SRCS=*.cc */*.cc

all: depend ${TARGETS}

test: depend ${TEST_TARGETS}

obfs: depend obfstest


# misc
gprof-helper.so: test/gprof-helper.c
	gcc -shared -fPIC test/gprof-helper.c -o gprof-helper.so -lpthread -ldl 



# fuse
fakefuse: fakefuse.cc mds.o client.o osd.o client/fuse.o msg/FakeMessenger.cc common.o
	${CC} -pg ${CFLAGS} ${LIBS} -lfuse $^ -o $@

tcpfuse: tcpfuse.cc mds.o client.o client/fuse.o ${TCP_OBJS} common.o
	${MPICC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@

mpifuse: mpifuse.cc mds.o client.o client/fuse.o ${TCP_OBJS} common.o
	${MPICC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@


# synthetic workload
fakesyn: fakesyn.cc mds.o client.o osd.o msg/FakeMessenger.o common.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@

mpisyn: mpisyn.cc mds.o client.o osd.o msg/MPIMessenger.cc common.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

tcpsyn: tcpsyn.cc mds.o client.o osd.o ${TCP_OBJS} common.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

# + obfs
fakesynobfs: fakesyn.cc mds.o client.o osd_obfs.o msg/FakeMessenger.o common.o
	${CC} -DUSE_OBFS ${CFLAGS} ${LIBS} $^ -o $@

tcpsynobfs: tcpsyn.cc mds.o client.o osd_obfs.o ${TCP_OBJS} common.o
	${MPICC} -DUSE_OBFS ${MPICFLAGS} ${MPILIBS} $^ -o $@ 


# ebofs

mkfs.ebofs: ebofs/mkfs.ebofs.cc config.cc common/Clock.o ebofs.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@

test.ebofs: ebofs/test.ebofs.cc config.cc common/Clock.o ebofs.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@

ebofs: mkfs.ebofs test.ebofs



# libceph
libceph.o: client/ldceph.o client/Client.o ${TCP_OBJS} ${COMMON_OBJS} ${SYN_OBJS}
	ld -i $^ -o $@

bench/mdtest/mdtest.o: bench/mdtest/mdtest.c
	mpicc -c $^ -o $@

mdtest: bench/mdtest/mdtest.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

mdtest.ceph: bench/mdtest/mdtest.o libceph.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

#

%.so: %.cc
	${CC} -shared -fPIC ${CFLAGS} $< -o $@


testmpi: test/testmpi.cc msg/MPIMessenger.cc config.o common/Timer.o common/clock.o msg/Messenger.o msg/Dispatcher.o msg/error.o
	${MPICC} ${CFLAGS} ${LIBS} $^ -o $@


clean:
	rm -f *.o */*.o ${TARGETS} ${TEST_TARGETS}

common.o: ${COMMON_OBJS}
	ld -i -o $@ $^

ebofs.o: ${EBOFS_OBJS}
	ld -i -o $@ $^

client.o: ${CLIENT_OBJS}
	ld -i -o $@ $^

osd.o: ${OSD_OBJS}
	ld -i -o $@ $^

osd_obfs.o: osd/OBFSStore.o osd/OSD.ccosd/PG.o osd/ObjectStore.o osd/FakeStore.o
	${MPICC} -DUSE_OBFS ${MPICFLAGS} ${MPILIBS} $^ -o $@ ../uofs/uofs.a

mds.o: ${MDS_OBJS}
	ld -i -o $@ $^

%.o: %.cc
	${CC} ${CFLAGS} -c $< -o $@

%.po: %.cc
	${CC} -fPIC ${CFLAGS} -c $< -o $@

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
