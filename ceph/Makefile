
CC=g++
CFLAGS=-D __gnu_cxx=std -g -I.
LIBS=

SRCS=*.cc */*.cc
OBJS=Logger.o mds/MDBalancer.o mds/MDS.o mds/CDentry.o mds/CDir.o mds/CInode.o mds/MDCache.o mds/MDStore.o clock.o FakeMessenger.o mds/LogStream.o mds/MDLog.o osd/OSD.o client/Client.o mds/MDCluster.o
TARGETS=test import

all: depend ${TARGETS}


test: test.cc pmds.o
	${CC} ${CFLAGS} ${LIBS} pmds.o test.cc -o test

import: pmds.o import.cc
	${CC} ${CFLAGS} ${LIBS} pmds.o import.cc -o import

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
