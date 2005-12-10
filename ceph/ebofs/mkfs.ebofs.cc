
#include <iostream>
#include "ebofs/Ebofs.h"


int main(int argc, char **argv)
{
  // args
  char *filename = 0;
  if (argc > 1) filename = argv[1];
  if (!filename) return -1;

  // device
  BlockDevice dev(filename);
  if (dev.open() < 0) {
	cerr << "couldn't open " << filename << endl;
	return -1;
  }

  // mkfs
  Ebofs mfs(dev);
  mfs.mkfs();

  // test-o-rama!
  Ebofs fs(dev);
  fs.mount();

  if (0) {  // test
	bufferlist bl;
	char crap[10000];
	memset(crap, 0, 10000);
	bl.append(crap, 10000);
 	fs.write(10, bl.length(), 200, bl, (Context*)0);
	fs.trim_buffer_cache();
 	fs.write(10, bl.length(), 3222, bl, (Context*)0);
	fs.trim_buffer_cache();
 	fs.write(10, 5000, 3222, bl, (Context*)0);
  }

  // test small writes
  if (1) {
	char crap[10000];
	memset(crap, 0, 10000);
	bufferlist bl;
	bl.append(crap, 10000);

	// write
	srand(0);
	for (int i=0; i<100; i++) {
	  off_t off = rand() % 1000000;
	  size_t len = 100;
	  cout << "writing bit at " << off << " len " << len << endl;
	  fs.write(10, len, off, bl, (Context*)0);
	}
	
	if (0) {
	// read
	srand(0);
	for (int i=0; i<100; i++) {
	  bufferlist bl;
	  off_t off = rand() % 1000000;
	  size_t len = 100;
	  cout << "read bit at " << off << " len " << len << endl;
	  int r = fs.read(10, len, off, bl);
	  assert(bl.length() == len);
	  assert(r == 0);
	}
	}

	// flush
	fs.flush_all();
	fs.trim_buffer_cache();

	if (0) {
	// read again
	srand(0);
	for (int i=0; i<100; i++) {
	  bufferlist bl;
	  off_t off = rand() % 1000000;
	  size_t len = 100;
	  cout << "read bit at " << off << " len " << len << endl;
	  int r = fs.read(10, len, off, bl);
	  assert(bl.length() == len);
	  assert(r == 0);
	}

	// flush
	fs.trim_buffer_cache();
	}

	// write on empty cache
	srand(0);
	for (int i=0; i<100; i++) {
	  off_t off = rand() % 1000000;
	  size_t len = 100;
	  cout << "writing bit at " << off << " len " << len << endl;
	  fs.write(10, len, off, bl, (Context*)0);
	}

  }

  fs.flush_all();
  fs.trim_buffer_cache();
  fs.trim_onode_cache();

  fs.umount();
  dev.close();
}

	
