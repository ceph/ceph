
CFLAGS=-I../ceph -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE
CC=g++

fusetrace_ll: fusetrace_ll.cc
	$(CC) $(CFLAGS) $^ -o $@