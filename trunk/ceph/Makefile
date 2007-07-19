
# mpicxx must be on your path to build newsyn.  
#  on googoo, this means that /usr/local/mpich2-1.0.2/bin must be on your path.
#  on issdm, it's /usr/local/mpich2/bin.

# Hook for extra -I options, etc.
EXTRA_CFLAGS = 

ifeq ($(target),darwin)
# For Darwin
CFLAGS = -g -Wall -I. -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -DDARWIN -D__FreeBSD__=10 ${EXTRA_CFLAGS}
LDINC = ar -rc
else
# For linux
CFLAGS = -pg -g -Wall -I. -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE 
LDINC = ld -i -o
endif

CC = g++
LIBS = -lpthread

ifeq ($(want_bdb),yes)
CFLAGS += -DUSE_OSBDB
OSBDB_LIBS = -ldb_cxx
endif

#for normal mpich2 machines
MPICC = mpicxx
MPICFLAGS = -DMPICH_IGNORE_CXX_SEEK ${CFLAGS}
MPILIBS = ${LIBS}

#for LLNL boxes without mpicxx
#MPICC = g++
#MPICFLAGS = ${CFLAGS} -I/usr/lib/mpi/include -L/usr/lib/mpi/mpi_gnu/lib
#MPILIBS = ${LIBS} -lelan -lmpi

EBOFS_OBJS= \
	ebofs/BlockDevice.o\
	ebofs/BufferCache.o\
	ebofs/Ebofs.o\
	ebofs/Allocator.o\
	ebofs/FileJournal.o

MDS_OBJS= \
	mds/MDS.o\
	mds/journal.o\
	mds/Server.o\
	mds/MDCache.o\
	mds/Locker.o\
	mds/Migrator.o\
	mds/MDBalancer.o\
	mds/CDentry.o\
	mds/CDir.o\
	mds/CInode.o\
	mds/AnchorTable.o\
	mds/AnchorClient.o\
	mds/LogEvent.o\
	mds/IdAllocator.o\
	mds/ClientMap.o\
	mds/MDLog.o

OSD_OBJS= \
	osd/PG.o\
	osd/ReplicatedPG.o\
	osd/RAID4PG.o\
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
	mon/Paxos.o\
	mon/PaxosService.o\
	mon/OSDMonitor.o\
	mon/MDSMonitor.o\
	mon/ClientMonitor.o\
	mon/PGMonitor.o\
	mon/Elector.o\
	mon/MonitorStore.o

COMMON_OBJS= \
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


ifeq ($(want_bdb),yes)
OSBDB_OBJS = \
	osbdb/OSBDB.o

OSBDB_OBJ = osbdb.o
endif

TARGETS = cmon cosd cmds csyn newsyn fakesyn mkmonmap cmonctl cfuse fakefuse
NO_FUSE = cmon cosd cmds csyn newsyn fakesyn mkmonmap


SRCS=*.cc */*.cc *.h */*.h */*/*.h

all: depend ${TARGETS}

nofuse: depend ${NO_FUSE}

test: depend ${TEST_TARGETS}


# real bits
mkmonmap: mkmonmap.cc common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

cmon: cmon.cc mon.o msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

cmonctl: cmonctl.cc msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

cosd: cosd.cc osd.o ebofs.o ${OSBDB_OBJ} msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} ${OSBDB_LIBS} $^ -o $@

cmds: cmds.cc mds.o osdc.o msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

csyn: csyn.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

cfuse: cfuse.cc client.o osdc.o client/fuse.o msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} -lfuse $^ -o $@

activemaster: active/activemaster.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

activeslave: active/activeslave.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

echotestclient: active/echotestclient.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

msgtestclient: active/msgtestclient.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@




# misc
gprof-helper.so: test/gprof-helper.c
	gcc -shared -fPIC test/gprof-helper.c -o gprof-helper.so -lpthread -ldl 


# fake*
fakefuse: fakefuse.cc mon.o mds.o client.o osd.o osdc.o ebofs.o ${OSBDB_OBJ} client/fuse.o msg/FakeMessenger.o common.o
	${CC} -pg ${CFLAGS} ${LIBS} ${OSBDB_LIBS} -lfuse $^ -o $@

fakesyn: fakesyn.cc mon.o mds.o client.o osd.o ebofs.o ${OSBDB_OBJ} osdc.o msg/FakeMessenger.o common.o
	${CC} -pg ${CFLAGS} ${LIBS} ${OSBDB_LIBS} $^ -o $@


# mpi startup
newsyn: newsyn.cc mon.o mds.o client.o osd.o ebofs.o ${OSBDB_OBJ} osdc.o msg/SimpleMessenger.o common.o
	${MPICC} -pg ${MPICFLAGS} ${MPILIBS} ${OSBDB_LIBS} $^ -o $@

newsyn.nopg: newsyn.cc mon.o mds.o client.o osd.o ebofs.o ${OSBDB_OBJ} osdc.o msg/SimpleMessenger.o common.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} ${OSBDB_LIBS} $^ -o $@


# ebofs
mkfs.ebofs: ebofs/mkfs.ebofs.cc config.cc common/Clock.o ebofs.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@

test.ebofs: ebofs/test.ebofs.cc config.cc common/Clock.o ebofs.o
	${CC} -pg ${CFLAGS} ${LIBS} $^ -o $@


# hadoop
libhadoopcephfs.so: client/hadoop/CephFSInterface.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CC} -fPIC -shared -Wl,-soname,$@.1 ${CFLAGS} -I/usr/local/java/include -I/usr/local/java/include/linux  ${LIBS} $^ -o $@

# libceph
libceph.o: client/ldceph.o client/Client.o msg/SimpleMessenger.o ${COMMON_OBJS} ${SYN_OBJS} ${OSDC_OBJS}
	${LDINC} $^ -o $@

bench/mdtest/mdtest.o: bench/mdtest/mdtest.c
	mpicc -c $^ -o $@

mdtest: bench/mdtest/mdtest.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

mdtest.ceph: bench/mdtest/mdtest.o libceph.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

# OSD test

testos: test/testos.o ebofs.o osbdb.o common.o
	${CC} ${CFLAGS} ${LIBS} ${OSBDB_LIBS} -o $@ $^

#

%.so: %.cc
	${CC} -shared -fPIC ${CFLAGS} $< -o $@

clean:
	rm -f *.o */*.o ${TARGETS} ${TEST_TARGETS}

common.o: ${COMMON_OBJS}
	${LDINC} $@ $^

ebofs.o: ${EBOFS_OBJS}
	${LDINC} $@ $^

client.o: ${CLIENT_OBJS} 
	${LDINC} $@ $^

osd.o: ${OSD_OBJS}
	${LDINC} $@ $^

osdc.o: ${OSDC_OBJS}
	${LDINC} $@ $^

mds.o: ${MDS_OBJS}
	${LDINC} $@ $^

mon.o: ${MON_OBJS}
	${LDINC} $@ $^

osbdb.o: ${OSBDB_OBJS}
	${LDINC} $@ $^

%.o: %.cc
	${CC} ${CFLAGS} -c $< -o $@

%.po: %.cc
	${CC} -fPIC ${CFLAGS} -c $< -o $@

count:
	cat ${SRCS} | wc -l
	cat ${SRCS} | grep -c \;

TAGS:
	etags `find . -name "*.[h|cc]"`

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null

# now add a line to include the dependency list.
include .depend
