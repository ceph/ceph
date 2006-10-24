
# mpicxx must be on your path; on googoo, this means that
# /usr/local/mpich2-1.0.2/bin must be on your path.

# For now, use g++ most of the time.
# When compiling MPI stuff, specify myfile.cc instead of myfile.o so that ${MPICC} is 
# invoked instead of the generic .o rule (or it'll use g++).
# This makes it less annoying to build on non-mpi hosts for dev work, and seems to 
# behave just fine...  change ${CC} back to mpicxx if you get paranoid.

CC = g++
CFLAGS = -g -Wall -I. -D_FILE_OFFSET_BITS=64 -DMPICH_IGNORE_CXX_SEEK -D_REENTRANT -D_THREAD_SAFE
LIBS = -lpthread  

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
	mds/Server.o\
	mds/MDCache.o\
	mds/Migrator.o\
	mds/Renamer.o\
	mds/MDBalancer.o\
	mds/CDentry.o\
	mds/CDir.o\
	mds/CInode.o\
	mds/AnchorTable.o\
	mds/AnchorClient.o\
	mds/MDStore.o\
	mds/LogEvent.o\
	mds/IdAllocator.o\
	mds/MDLog.o

OSD_OBJS= \
	osd/PG.o\
	osd/Ager.o\
	osd/FakeStore.o\
	osd/OSD.o

OSDC_OBJS= \
	osdc/Objecter.o\
	osdc/ObjectCacher.o\
	osdc/Filer.o\
	osdc/Journaler.o

MON_OBJS= \
	mon/Monitor.o\
	mon/OSDMonitor.o\
	mon/MDSMonitor.o\
	mon/Elector.o

COMMON_OBJS= \
	msg/Messenger.o\
	msg/Message.o\
	msg/HostMonitor.o\
	common/Logger.o\
	common/Clock.o\
	common/Timer.o\
	config.o


CLIENT_OBJS= \
	client/FileCache.o\
	client/Client.o\
	client/SyntheticClient.o\
	client/Trace.o

TCP_OBJS = \
	msg/TCPMessenger.o\
	msg/TCPDirectory.o

TARGETS = cosd cfuse newsyn fakesyn

SRCS=*.cc */*.cc *.h */*.h */*/*.h

all: depend ${TARGETS}

test: depend ${TEST_TARGETS}

obfs: depend obfstest


# real bits
cmon: cmon.cc mon.o ebofs.o msg/NewerMessenger.o common.o
	${CC} ${CFLAGS} ${MPILIBS} $^ -o $@

cosd: cosd.cc osd.o ebofs.o msg/NewerMessenger.o common.o
	${CC} ${CFLAGS} ${MPILIBS} $^ -o $@

cmds: cmds.cc mds.o osdc.o msg/NewerMessenger.o common.o
	${CC} ${CFLAGS} ${MPILIBS} $^ -o $@

cfuse: cfuse.cc client.o osdc.o client/fuse.o msg/NewerMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@


# misc
gprof-helper.so: test/gprof-helper.c
	gcc -shared -fPIC test/gprof-helper.c -o gprof-helper.so -lpthread -ldl 



# fuse
fakefuse: fakefuse.cc mon.o mds.o client.o osd.o osdc.o ebofs.o client/fuse.o msg/FakeMessenger.cc common.o
	${CC} -pg ${CFLAGS} ${LIBS} -lfuse $^ -o $@

tcpfuse: tcpfuse.cc mds.o client.o client/fuse.o ${TCP_OBJS} common.o
	${MPICC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@

mpifuse: mpifuse.cc mds.o client.o client/fuse.o ${TCP_OBJS} common.o
	${MPICC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@


# synthetic workload
fakesyn: fakesyn.o mon.o mds.o client.o osd.o ebofs.o osdc.o msg/FakeMessenger.o common.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@

tcpsyn: tcpsyn.cc mon.o mds.o client.o osd.o ebofs.o osdc.o ${TCP_OBJS} common.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

newsyn: newsyn.cc mon.o mds.o client.o osd.o ebofs.o osdc.o msg/NewerMessenger.o common.o
	${MPICC} -pg ${MPICFLAGS} ${MPILIBS} $^ -o $@

newsyn.nopg: newsyn.cc mon.o mds.o client.o osd.o ebofs.o osdc.o msg/NewerMessenger.o common.o
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




# libceph
libceph.o: client/ldceph.o client/Client.o ${TCP_OBJS} ${COMMON_OBJS} ${SYN_OBJS} ${OSDC_OBJS}
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

osdc.o: ${OSDC_OBJS}
	ld -i -o $@ $^

osd_obfs.o: osd/OBFSStore.o osd/OSD.ccosd/PG.o osd/ObjectStore.o osd/FakeStore.o
	${MPICC} -DUSE_OBFS ${MPICFLAGS} ${MPILIBS} $^ -o $@ ../uofs/uofs.a

mds.o: ${MDS_OBJS}
	ld -i -o $@ $^

mon.o: ${MON_OBJS}
	ld -i -o $@ $^

%.o: %.cc
	${CC} ${CFLAGS} -c $< -o $@

%.po: %.cc
	${CC} -fPIC ${CFLAGS} -c $< -o $@

count:
	cat ${SRCS} | wc -l
	cat ${SRCS} | grep -c \;

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null

# now add a line to include the dependency list.
include .depend
