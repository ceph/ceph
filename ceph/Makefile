
CC=g++
CFLAGS=-D __gnu_cxx=std -g

SRCS=*.cc
OBJS=CDentry.o CDir.o CInode.o MDCache.o MDStore.o clock.o MDS.o FakeMessenger.o LogStream.o MDLog.o OSD.o
TARGETS=test import

all: depend ${TARGETS}


test: test.cc pmds.o
	${CC} ${CFLAGS} pmds.o test.cc -o test

import: pmds.o import.cc
	${CC} ${CFLAGS} pmds.o import.cc -o import

clean:
	rm *.o ${TARGETS}

%.o: %.cc
	${CC} ${CFLAGS} -c $<

pmds.o: ${OBJS}
	ld -i -o pmds.o ${OBJS} 

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null

# now add a line to include the dependency list.
include .depend
