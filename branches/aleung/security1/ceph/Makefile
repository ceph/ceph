
# mpicxx must be on your path; on googoo, this means that
# /usr/local/mpich2-1.0.2/bin must be on your path.

# For now, use g++ most of the time.
# When compiling MPI stuff, specify myfile.cc instead of myfile.o so that ${MPICC} is 
# invoked instead of the generic .o rule (or it'll use g++).
# This makes it less annoying to build on non-mpi hosts for dev work, and seems to 
# behave just fine...  change ${CC} back to mpicxx if you get paranoid.

#CC = g++
#CFLAGS = -g -Wall -I. -D_FILE_OFFSET_BITS=64 -DMPICH_IGNORE_CXX_SEEK -D_REENTRANT -D_THREAD_SAFE
#LIBS = -lpthread  

# Hook for extra -I options, etc.
EXTRA_CFLAGS = 

ifeq ($(target),darwin)
# For Darwin
CFLAGS = -ggdb3 -Wall -I. -D_FILE_OFFSET_BITS=64 -DMPICH_IGNORE_CXX_SEEK -D_REENTRANT -D_THREAD_SAFE -DDARWIN -D__FreeBSD__=10 ${EXTRA_CFLAGS}
else
# For linux
CFLAGS = -ggdb3 -Wall -I. -D_FILE_OFFSET_BITS=64 -DMPICH_IGNORE_CXX_SEEK -D_REENTRANT -D_THREAD_SAFE 
endif

CC = g++
LIBS = -lpthread -lcrypto++5.2
#CRYPTOLIBS = /usr/lib/libcrypto++5.2.so

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
	mds/journal.o\
	mds/Server.o\
	mds/MDCache.o\
	mds/Locker.o\
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
	mon/ClientMonitor.o\
	mon/Elector.o

COMMON_OBJS= \
	msg/Messenger.o\
	msg/Message.o\
	common/Logger.o\
	common/Clock.o\
	common/Timer.o\
	config.o


CLIENT_OBJS= \
	client/FileCache.o\
	client/Client.o\
	client/SyntheticClient.o\
	client/Trace.o

CRYPTO_OBJS = \
	crypto/CryptoLib.o

TARGETS = cmon cosd cmds cfuse newsyn fakesyn

SRCS=*.cc */*.cc *.h */*.h */*/*.h

all: depend ${TARGETS}

test: depend ${TEST_TARGETS}

obfs: depend obfstest


# real bits
mkmonmap: mkmonmap.cc common.o crypto.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

cmon: cmon.cc mon.o ebofs.o msg/SimpleMessenger.o common.o crypto.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

cosd: cosd.cc osd.o ebofs.o msg/SimpleMessenger.o common.o crypto.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

cmds: cmds.cc mds.o osdc.o msg/SimpleMessenger.o common.o crypto.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

csyn: csyn.cc client.o osdc.o msg/SimpleMessenger.o common.o crypto.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

cfuse: cfuse.cc client.o osdc.o client/fuse.o msg/SimpleMessenger.o common.o crypto.o
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
fakesyn: fakesyn.o mon.o mds.o client.o osd.o ebofs.o osdc.o msg/FakeMessenger.o common.o crypto.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@

tcpsyn: tcpsyn.cc mon.o mds.o client.o osd.o ebofs.o osdc.o ${TCP_OBJS} common.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

newsyn: newsyn.cc mon.o mds.o client.o osd.o ebofs.o osdc.o msg/SimpleMessenger.o common.o crypto.o
	${MPICC} -pg ${MPICFLAGS} ${MPILIBS} $^ -o $@

newsyn.nopg: newsyn.cc mon.o mds.o client.o osd.o ebofs.o osdc.o msg/NewerMessenger.o common.o crypto.o
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
libceph.o: client/ldceph.o client/Client.o ${COMMON_OBJS} ${SYN_OBJS} ${OSDC_OBJS}
	ar -rc $@ $^

bench/mdtest/mdtest.o: bench/mdtest/mdtest.c
	mpicc -c $^ -o $@

mdtest: bench/mdtest/mdtest.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

mdtest.ceph: bench/mdtest/mdtest.o libceph.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

#

%.so: %.cc
	${CC} -shared -fPIC ${CFLAGS} $< -o $@

clean:
	rm -f *.o */*.o ${TARGETS} ${TEST_TARGETS}

common.o: ${COMMON_OBJS}
	ar -rc $@ $^

ebofs.o: ${EBOFS_OBJS}
	ar -rc $@ $^

client.o: ${CLIENT_OBJS} 
	ar -rc $@ $^

osd.o: ${OSD_OBJS}
	ar -rc $@ $^

osdc.o: ${OSDC_OBJS}
	ar -rc $@ $^

crypto.o: ${CRYPTO_OBJS}
	ld -i -o $@ $^

osd_obfs.o: osd/OBFSStore.o osd/OSD.cc osd/PG.o osd/ObjectStore.o osd/FakeStore.o
	${MPICC} -DUSE_OBFS ${MPICFLAGS} ${MPILIBS} $^ -o $@ ../uofs/uofs.o

mds.o: ${MDS_OBJS}
	ar -rc $@ $^

mon.o: ${MON_OBJS}
	ar -rc $@ $^

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
