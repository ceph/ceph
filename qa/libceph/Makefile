CFLAGS = -Wall -Wextra -D_GNU_SOURCE -lcephfs -L../../build/lib

TARGETS = trivial_libceph 

.c:
	$(CC) $(CFLAGS) $@.c -o $@

all:	$(TARGETS)

clean:
	rm $(TARGETS)
