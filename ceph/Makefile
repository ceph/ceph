
CC=g++
CFLAGS=-D __gnu_cxx=std -g

SRCS=*.cc
OBJS=CDentry.o CDir.o CInode.o MDCache.o MDStore.o clock.o mds.o osd.o


all: test import


test: test.cc pmds.o
	${CC} ${CFLAGS} pmds.o test.cc -o test

import: pmds.o import.cc
	${CC} ${CFLAGS} pmds.o import.cc -o import


%.o: %.cc
	${CC} ${CFLAGS} -c $<

pmds.o: ${OBJS}
	ld -i -o pmds.o ${OBJS} 

depend: .depend

.depend: ${SRCS}
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null

# now add a line to include the dependency list.
include .depend
