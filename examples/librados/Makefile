all: hello_world.cc hello_world.c
	gcc hello_world.c -o hello -lrados -lblkin-front
	g++ -g -c hello_world.cc -o hello_world.o
	g++ -g hello_world.o -lrados -o librados_hello_world
clean:
	rm hello_world.o librados_hello_world hello
