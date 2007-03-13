# Makefile for Ceph crypto library

CC = g++
CFLAGS = -g -O3 -Wall -I.
LIBS = -lpthread -lcryptopp
#CRYPTOLIBS = /usr/lib/libcrypto++5.2.so
#CRYPTOLIBS = /usr/lib/libcryptopp.a
LIBDIR = -L /usr/lib/

SRCS=*.cc */*.cc *.h */*.h */*/*.h

TARGETS = cryptolib driver
CRYPT_TARGETS = crypt
TEST_TARGETS = test

all: depend ${TARGETS}

driver: driver.cc CryptoLib.o
	${CC} ${CFLAGS} $^ -o $@ ${LIBS}

cryptolib: CryptoLib.cc
	${CC} ${CFLAGS} $^ -c

crypt: cryptTest.cc
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

encrypt: encTest.cc CryptoLib.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

retest: retest.cc CryptoLib.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

multiplex: multiplex.cc CryptoLib.o
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

test: test.cc
	${CC} ${CFLAGS} ${LIBS} $^ -o $@

clean:
	rm -f *.o */*.o *~ *.core ${TARGETS} ${TEST_TARGETS}

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null
