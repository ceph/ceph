CFLAGS = -Wall -Wextra -D_GNU_SOURCE

TARGETS = clone_range test_async_snap create_async_snap

.c:
	$(CC) $(CFLAGS) $@.c -o $@

all:	$(TARGETS)

clean:
	rm $(TARGETS)
