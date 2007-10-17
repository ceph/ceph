
CC = gcc
CFLAGS = -Wall
CFLAGS += -O3 -g
LD = ld
RM = rm

all: depend builder.o libcrush.o

clean:
	rm -f *.o libcrush.o

%.o: %.c
	${CC} ${CFLAGS} -c $< -o $@

libcrush.o: crush.o buckets.o
	$(LD) -i -o $@ $^

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- *.c > .depend 2>/dev/null

include .depend
