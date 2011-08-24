CFLAGS = -Wall -Wextra -D_GNU_SOURCE

TARGETS = clone_range

.c:
	$(CC) $(CFLAGS) $@.c -o $@

all:	$(TARGETS)

clean:
	rm $(TARGETS)
