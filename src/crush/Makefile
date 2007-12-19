
CC = gcc
CFLAGS = -Wall
CFLAGS += -g
#CFLAGS += -O3
LD = ld
RM = rm

all: depend libcrush.o test

clean:
	rm -f *.o libcrush.o

%.o: %.c
	${CC} ${CFLAGS} -c $< -o $@

libcrush.o: builder.o crush.o mapper.o
	$(LD) -i -o $@ $^

test: test.c libcrush.o
	$(CC) ${CFLAGS} -lm $^ -o $@

.depend:
	touch .depend

depend:
	$(RM) .depend
	makedepend -f- -- $(CFLAGS) -- *.c > .depend 2>/dev/null

include .depend
