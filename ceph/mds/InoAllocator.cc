
#include "InoAllocator.h"
#include "MDS.h"

#include <cassert>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <cassert>

char ifn[100];

char *InoAllocator::get_filename() {
  sprintf(ifn,"osddate/ino.%d", mds->get_nodeid());
  return ifn;
}

void InoAllocator::save()
{
  int fd;
  fd = open(get_filename(), O_CREAT|O_WRONLY);
  if (fd >= 0) {
	int mapsize = free.map_size();
	write(fd, (char*)&mapsize, sizeof(mapsize));

	for (map<inodeno_t,inodeno_t>::iterator it = free.map_begin();
		 it != free.map_end();
		 it++) {
	  inodeno_t a = it->first;
	  inodeno_t b = it->second;
	  write(fd, &a, sizeof(a));
	  write(fd, &b, sizeof(b));
	}
	close(fd);
  } else 
	assert(0);
}


void InoAllocator::load()
{  
  int fd;
  fd = open(get_filename(), O_RDONLY);
  if (fd >= 0) {
	int mapsize = 0;
	read(fd, &mapsize, sizeof(mapsize));
	for (int i=0; i<mapsize; i++) {
	  inodeno_t a,b;
	  read(fd, &a, sizeof(a));
	  read(fd, &b, sizeof(b));
	  free.map_insert(a,b);
	}
	close(fd);
  }
  else {
	// use generic range
	free.map_insert(1000000000000 * mds->get_nodeid(),
					1000000000000 * (mds->get_nodeid()+1));
  }
}
