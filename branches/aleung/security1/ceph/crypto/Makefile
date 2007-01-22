# Makefile for Ceph crypto library

CC = g++
CFLAGS = -g -Wall -I.
LIBS = -lpthread
CRYPTOLIBS = /usr/lib/libcrypto++5.2.so

SRCS=*.cc */*.cc *.h */*.h */*/*.h

TARGETS = cryptolib driver
CRYPT_TARGETS = crypt
TEST_TARGETS = test

all: depend ${TARGETS}

driver: driver.cc CryptoLib.o
	${CC} ${CFLAGS} ${CRYPTOLIBS} ${LIBS} $^ -o $@

cryptolib: CryptoLib.cc
	${CC} ${CFLAGS} $^ -c

crypt: cryptTest.cc
	${CC} ${CFLAGS} ${CRYPTOLIBS} ${LIBS} $^ -o $@

encrypt: encTest.cc CryptoLib.o
	${CC} ${CFLAGS} ${CRYPTOLIBS} ${LIBS} $^ -o $@

retest: retest.cc CryptoLib.o
	${CC} ${CFLAGS} ${CRYPTOLIBS} ${LIBS} $^ -o $@

multiplex: multiplex.cc CryptoLib.o
	${CC} ${CFLAGS} ${CRYPTOLIBS} ${LIBS} $^ -o $@

test: test.cc
	${CC} ${CFLAGS} ${CRYPTOLIBS} ${LIBS} $^ -o $@

clean:
	rm -f *.o */*.o *~ *.core ${TARGETS} ${TEST_TARGETS}

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- $(SRCS) > .depend 2>/dev/null