CFLAGS = -Wall -Wextra -D_GNU_SOURCE

TARGETS = test_o_trunc

.c:
	$(CC) $(CFLAGS) $@.c -o $@

all:	$(TARGETS)

clean:
	rm $(TARGETS)
