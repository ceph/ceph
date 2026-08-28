CFLAGS = -O2 -g -Wall -Wextra -Wno-format-truncation -D_GNU_SOURCE
LDFLAGS = -pthread

TARGETS = mdsc_stress

.c:
	$(CC) $(CFLAGS) $@.c -o $@ $(LDFLAGS)

all: $(TARGETS)

clean:
	rm -f $(TARGETS)
