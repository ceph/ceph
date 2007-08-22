
CFLAGS = -g -I../ceph -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -I$(HOME)/include -L$(HOME)/lib
LIBS = -lfuse
CC = g++

fusetrace_ll: fusetrace_ll.cc
	$(CC) $(CFLAGS) $(LIBS) $^ -o $@
