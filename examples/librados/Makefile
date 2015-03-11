ifdef WITH_BLKIN
all: librados_hello_world librados_hello_world_c librados_hello_blkin
else
all: librados_hello_world librados_hello_world_c
endif

librados_hello_world: hello_world.cc
	g++ -g -c hello_world.cc -o hello_world.o $(CFLAGS)
	g++ -g hello_world.o -lrados -o librados_hello_world $(LDFLAGS)

librados_hello_world_c: hello_world_c.c
	cc -g -c hello_world_c.c -o hello_world_c.o $(CFLAGS)
	cc -g hello_world_c.o -lrados -o librados_hello_world_c $(LDFLAGS)

librados_hello_blkin: hello_blkin.c
	cc -g -c hello_blkin.c -o hello_blkin.o $(CFLAGS)
	cc -g hello_blkin.o -lrados -lblkin-front -o librados_hello_blkin $(LDFLAGS)

clean:
	rm hello_world.o librados_hello_world
	rm hello_world_c.o librados_hello_world_c
	rm hello_blkin.o librados_hello_blkin
