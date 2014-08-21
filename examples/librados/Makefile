all: hello_world.cc
	g++ -g -c hello_world.cc -o hello_world.o
	g++ -g hello_world.o -lrados -o librados_hello_world
clean:
	rm hello_world.o librados_hello_world