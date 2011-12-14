CFLAGS = -Wall -Wextra -D_GNU_SOURCE -lcephfs -L../../src/.libs

TARGETS = trivial_libceph 

.c:
	$(CC) $(CFLAGS) $@.c -o $@

all:	$(TARGETS)

clean:
	rm $(TARGETS)
