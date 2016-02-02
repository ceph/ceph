CXXFLAGS=-std=c++0x -I../../src/include
CFLAGS=-I../../src/include
all: librados_hello_world librados_hello_world_c

librados_hello_world: hello_world.cc
	g++ -g -c hello_world.cc -o hello_world.o $(CXXFLAGS)
	g++ -g hello_world.o -lrados -o librados_hello_world $(LDFLAGS)

librados_hello_world_c: hello_world_c.c
	cc -g -c hello_world_c.c -o hello_world_c.o $(CFLAGS)
	cc -g hello_world_c.o -lrados -o librados_hello_world_c $(LDFLAGS)

clean:
	rm hello_world.o librados_hello_world
	rm hello_world_c.o librados_hello_world_c
