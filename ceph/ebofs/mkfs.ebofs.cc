
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
  Ebofs fs(dev);
  fs.mkfs();

  if (1) {  // test
	bufferlist bl;
	char crap[10000];
	memset(crap, 0, 10000);
	bl.append(crap, 10000);
 	fs.write(10, bl.length(), 200, bl, 0);
	fs.trim_buffer_cache();
 	fs.write(10, bl.length(), 3222, bl, 0);
	fs.trim_buffer_cache();
 	fs.write(10, 5000, 3222, bl, 0);
  }

  fs.flush_all();
  fs.trim_buffer_cache();

  fs.umount();
}

	
