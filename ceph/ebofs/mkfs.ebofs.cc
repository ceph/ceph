
#include <iostream>
#include "ebofs/Ebofs.h"


int main(int argc, char **argv)
{
  // args
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  if (args.size() < 1) {
	cerr << "usage: mkfs.ebofs [options] <device file>" << endl;
	return -1;
  }
  char *filename = args[0];

  // mkfs
  Ebofs mfs(filename);
  int r = mfs.mkfs();
  if (r < 0) exit(r);

  if (0) {
	// test-o-rama!
	Ebofs fs(filename);
	fs.mount();

	if (1) { // big write speed test
	  bufferlist bl;
	  char crap[1024*1024];
	  memset(crap, 0, 1024*1024);
	  bl.append(crap, 1024*1024);
	  
	  int megs = 1000;

	  utime_t start = g_clock.now();

	  for (off_t m=0; m<megs; m++) {
		//if (m%100 == 0)
		  cout << m << " / " << megs << endl;
		fs.write(10, bl.length(), 1024LL*1024LL*m, bl, (Context*)0);
	  }	  
	  fs.sync();

	  utime_t end = g_clock.now();
	  end -= start;

	  dout(1) << "elapsed " << end << endl;
	  
	  float mbs = (float)megs / (float)end;
	  dout(1) << "mb/s " << mbs << endl;
	}
	
	if (0) {  // test
	  bufferlist bl;
	  char crap[10000];
	  memset(crap, 0, 10000);
	  bl.append(crap, 10000);
	  fs.write(10, bl.length(), 200, bl, (Context*)0);
	  fs.trim_buffer_cache();
	  fs.write(10, bl.length(), 5222, bl, (Context*)0);
	  sleep(1);
	  fs.trim_buffer_cache();
	  fs.write(10, 5000, 3222, bl, (Context*)0);
	}
	
	// test small writes
	if (0) {
	  char crap[1024*1024];
	  memset(crap, 0, 1024*1024);
	  bufferlist bl;
	  bl.append(crap, 1024*1024);
	  
	  // reandom write
	  if (1) {
		srand(0);
		for (int i=0; i<10000; i++) {
		  off_t off = rand() % 1000000;
		  size_t len = 1+rand() % 10000;
		  cout << endl << i << " writing bit at " << off << " len " << len << endl;
		  fs.write(10, len, off, bl, (Context*)0);
		  //fs.sync();
		  //fs.trim_buffer_cache();
		}
		fs.remove(10);
		for (int i=0; i<100; i++) {
		  off_t off = rand() % 1000000;
		  size_t len = 1+rand() % 10000;
		  cout << endl << i << " writing bit at " << off << " len " << len << endl;
		  fs.write(10, len, off, bl, (Context*)0);
		  //fs.sync();
		  //fs.trim_buffer_cache();
		}
	  }
	  
	  if (0) {
		// sequential write
		srand(0);
		off_t off = 0;
		for (int i=0; i<10000; i++) {
		  size_t len = 1024*1024;//1+rand() % 10000;
		  cout << endl << i << " writing bit at " << off << " len " << len << endl;
		  fs.write(10, len, off, bl, (Context*)0);
		  off += len;
		}

	  }
	  
	  
	  if (0) {
		// read
		srand(0);
		for (int i=0; i<100; i++) {
		  bufferlist bl;
		  off_t off = rand() % 1000000;
		  size_t len = rand() % 1000;
		  cout << endl << "read bit at " << off << " len " << len << endl;
		  int r = fs.read(10, len, off, bl);
		  assert(bl.length() == len);
		  assert(r == (int)len);
		}
	  }
	  
	  // flush
	  fs.sync();
	  fs.trim_buffer_cache();
	  //fs.trim_buffer_cache();
	  
	  if (0) {
		// read again
		srand(0);
		for (int i=0; i<100; i++) {
		  bufferlist bl;
		  off_t off = rand() % 1000000;
		  size_t len = 100;
		  cout << endl << "read bit at " << off << " len " << len << endl;
		  int r = fs.read(10, len, off, bl);
		  assert(bl.length() == len);
		  assert(r == (int)len);
		}
		
		// flush
		fs.sync();
		fs.trim_buffer_cache();
	  }
	  
	  if (0) {
		// write on empty cache
		srand(0);
		for (int i=0; i<100; i++) {
		  off_t off = rand() % 1000000;
		  size_t len = 100;
		  cout << endl <<  "writing bit at " << off << " len " << len << endl;
		  fs.write(10, len, off, bl, (Context*)0);
		}
	  }
	  
	}
	
	fs.sync();
	fs.trim_buffer_cache();
	
	fs.umount();
  }

  return 0;
}

	
