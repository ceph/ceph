#
# until autoconf is set up, here are the options i understand:
#
#  darwin=yes    -- build on darwin
#  fuse=no       -- don't build anything requiring FUSE
#  mpi=no        -- don't build newsyn (require MPI)
#  use_ccpp=yes  -- use Common C++ for buffer.h reference counting
#  want_bdb=yes  -- build berkelydb objectstore
# 

# mpicxx must be on your path to build newsyn.  
#  on googoo, this means that /usr/local/mpich2-1.0.2/bin must be on your path.
#  on issdm, it's /usr/local/mpich2/bin.

# Hook for extra -I options, etc.
EXTRA_CFLAGS = #-I${HOME}/include -L${HOME}/lib
EXTRA_CFLAGS += -g
EXTRA_CFLAGS += -pg
#EXTRA_CFLAGS += -O3

# base
CFLAGS = -Wall -I. -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE ${EXTRA_CFLAGS}
LDINC = ld -i -o
CXX = g++
CC = gcc
LIBS = -pthread

# darwin?
ifeq ($(target),darwin)
CFLAGS += -DDARWIN -D__FreeBSD__=10
LDINC = ar -rc
endif

# use Common C++ (for buffer.h)?
ifeq ($(use_ccpp),yes)
CFLAGS += -D_GNU_SOURCE -DBUFFER_USE_CCPP
LIBS += -lccgnu2 -ldl
endif


#for normal mpich2 machines
MPICC = mpicxx
MPICFLAGS = -DMPICH_IGNORE_CXX_SEEK ${CFLAGS}
MPILIBS = ${LIBS}

#for LLNL boxes without mpicxx
#MPICC = g++
#MPICFLAGS = ${CFLAGS} -I/usr/lib/mpi/mpi_gnu/include -L/usr/lib/mpi/mpi_gnu/lib
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


# bdbstore?
ifeq ($(want_bdb),yes)
CFLAGS += -DUSE_OSBDB
LIBS = -ldb_cxx
OSD_OBJS += osbdb/OSBDB.o
OSBDB_OBJS = \
	osbdb/OSBDB.o
endif


# targets
TARGETS = cmon cosd cmds csyn mkmonmap cmonctl fakesyn dupstore
SRCS=*.cc */*.cc *.h */*.h */*/*.h

ifneq ($(fuse),no)
TARGETS += cfuse fakefuse
endif

ifneq ($(mpi),no)
TARGETS += newsyn
endif

all: depend ${TARGETS}

test: depend ${TEST_TARGETS}


# real bits
mkmonmap: mkmonmap.cc common.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

extractosdmaps: extractosdmaps.cc common.o osd.o mon.o ebofs.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

cmon: cmon.o mon.o msg/SimpleMessenger.o common.o crush.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

cmonctl: cmonctl.cc msg/SimpleMessenger.o common.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

cosd: cosd.o osd.o ebofs.o msg/SimpleMessenger.o common.o crush.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

cmds: cmds.o mds.o osdc.o msg/SimpleMessenger.o common.o crush.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

csyn: csyn.o client.o osdc.o msg/SimpleMessenger.o common.o crush.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

cfuse: cfuse.o client.o osdc.o client/fuse.o client/fuse_ll.o msg/SimpleMessenger.o common.o crush.o
	${CXX} ${CFLAGS} ${LIBS} -lfuse $^ -o $@


# code shipping experiments
activemaster: active/activemaster.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

activeslave: active/activeslave.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CXX} -ldl ${CFLAGS} ${LIBS} $^ -o $@

echotestclient: active/echotestclient.cc
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

msgtestclient: active/msgtestclient.o client.o osdc.o msg/SimpleMessenger.o common.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

#libtrivialtask.so: active/trivial_task.cc client.o osdc.o msg/SimpleMessenger.o common.o
#	${CXX} -fPIC -shared -Wl,-soname,$@.1 ${CFLAGS}  ${LIBS} $^ -o $@

libtrivialtask.so: active/trivial_task_ipc.cc ceph_ipc/ipc_client.o
	${CXX} -fPIC -shared -Wl,-soname,$@.1 ${CFLAGS}  ${LIBS} $^ -o $@


libgreptask.so: active/grep_task.cc ceph_ipc/ipc_client.o
	${CXX} -fPIC -shared -Wl,-soname,$@.1 ${CFLAGS}  ${LIBS} $^ -o $@


#libhadoopcephfs.so: client/hadoop/CephFSInterface.cc client.o osdc.o msg/SimpleMessenger.o common.o
#	${CXX} -fPIC -shared -Wl,-soname,$@.1 ${CFLAGS}  ${LIBS} $^ -o $@


# IPC interface
ipc_server: ceph_ipc/ipc_server.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

ipc_testclient: ceph_ipc/ipc_testclient.cc ceph_ipc/ipc_client.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@


# fake*
fakefuse: fakefuse.o mon.o mds.o client.o osd.o osdc.o ebofs.o client/fuse.o client/fuse_ll.o msg/FakeMessenger.o common.o crush.o
	${CXX} ${CFLAGS} ${LIBS} -lfuse $^ -o $@

fakesyn: fakesyn.o mon.o mds.o client.o osd.o ebofs.o osdc.o msg/FakeMessenger.o common.o crush.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@


# mpi startup
newsyn: newsyn.cc mon.o mds.o client.o osd.o ebofs.o osdc.o msg/SimpleMessenger.o common.o crush.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@


# ebofs
mkfs.ebofs: ebofs/mkfs.ebofs.cc config.cc common/Clock.o ebofs.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

test.ebofs: ebofs/test.ebofs.cc config.cc common/Clock.o ebofs.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

dupstore: dupstore.cc config.cc ebofs.o common/Clock.o common/Timer.o osd/FakeStore.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@


# hadoop
libhadoopcephfs.so: client/hadoop/CephFSInterface.cc client.o osdc.o msg/SimpleMessenger.o common.o
	${CXX} -fPIC -shared -Wl,-soname,$@.1 ${CFLAGS} -I/usr/local/java/include -I/usr/local/java/include/linux  ${LIBS} $^ -o $@

# libceph
libceph.o: client/ldceph.o client/Client.o msg/SimpleMessenger.o ${COMMON_OBJS} ${SYN_OBJS} ${OSDC_OBJS}
	${LDINC} $^ -o $@

# some benchmarking tools
bench/mdtest/mdtest.o: bench/mdtest/mdtest.c
	mpicc -c $^ -o $@

mdtest: bench/mdtest/mdtest.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

mdtest.ceph: bench/mdtest/mdtest.o libceph.o
	${MPICC} ${MPICFLAGS} ${MPILIBS} $^ -o $@

testos: test/testos.o ebofs.o osbdb.o common.o
	${CXX} ${CFLAGS} ${LIBS} ${OSBDB_LIBS} -o $@ $^


# misc
gprof-helper.so: test/gprof-helper.c
	gcc -shared -fPIC test/gprof-helper.c -o gprof-helper.so -lpthread -ldl 

test_disk_bw: test/test_disk_bw.cc common.o
	${CXX} ${CFLAGS} ${LIBS} $^ -o $@

# crush
# lameness: use .co extension for .c files
%.co: %.c
	${CC} ${CFLAGS} -c $< -o $@

crush.o: crush/builder.co crush/mapper.co crush/crush.co
	${LDINC} $@ $^

# bits
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

# generic rules
%.so: %.cc
	${CXX} -shared -fPIC ${CFLAGS} $< -o $@

%.o: %.cc
	${CXX} ${CFLAGS} -c $< -o $@

%.po: %.cc
	${CXX} -fPIC ${CFLAGS} -c $< -o $@


# handy
clean:
	rm -f *.o */*.o crush/*.co ${TARGETS} ${TEST_TARGETS}

count:
	cat ${SRCS} | wc -l
	cat ${SRCS} | grep -c \;

TAGS:
	etags `find . -name "*.[h|c|cc]"|grep -v '\.\#'`

tags:
	ctags `find . -name "*.[h|c|cc]"|grep -v '\.\#'`

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null
#	for f in $(SRCS) ; do cpp -MM $(CFLAGS) $$f 2> /dev/null >> .depend ; done


# now add a line to include the dependency list.
include .depend
# DO NOT DELETE
