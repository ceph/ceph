
EXTRA_CFLAGS += -g
EXTRA_CFLAGS += -pg
#EXTRA_CFLAGS += -O3

# base
CFLAGS = -Wall -I. -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE ${EXTRA_CFLAGS}
LDINC = ld -i -o
CXX = g++
CC = gcc
LIBS = -pthread

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
	mon/MonMap.o\
	config.o

#
perl: common.o crush.o
	swig -perl5 -c++ -shadow crush/CrushWrapper.i
	${CXX} ${CFLAGS} -c crush/CrushWrapper_wrap.cxx -fno-strict-aliasing -pipe
	${CXX} -shared crush/CrushWrapper_wrap.o common.o crush.o -o CrushWrapper.so



#/usr/local/include -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -I/usr/lib/perl/5.8/CORE -I. -I/home/michael/ceph/src/

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

# crush
# lameness: use .co extension for .c files
%.co: %.c
	${CC} ${CFLAGS} -c $< -o $@

crush.o: crush/builder.co crush/mapper.co crush/crush.co
	${LDINC} $@ $^

# bits
common.o: ${COMMON_OBJS}
	${LDINC} $@ $^


# generic rules
%.so: %.cc
	${CXX} -shared -fPIC ${CFLAGS} $< -o $@

%.o: %.cc
	${CXX} -fPIC ${CFLAGS} -c $< -o $@

%.po: %.cc
	${CXX} -fPIC ${CFLAGS} -c $< -o $@


# handy
clean:
	rm -f *.o */*.o crush/*.co ${TARGETS}

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
