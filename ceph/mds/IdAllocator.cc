
#include "IdAllocator.h"
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

char *IdAllocator::get_filename() {
  sprintf(ifn,"osddata/idalloc.%d", mds->get_nodeid());
  return ifn;
}

void IdAllocator::save()
{
  int fd;
  fd = open(get_filename(), O_CREAT|O_WRONLY);
  if (fd >= 0) {
	fchmod(fd, 0644);
	
	int ntypes = free.size();
	write(fd, (char*)&ntypes, sizeof(ntypes));
	
	// over types
	for (map<char, map<idno_t,idno_t> >::iterator ty = free.begin();
		 ty != free.end(); 
		 ty++) {
	  char type = *ty;
	  write(fd, &type, 1);
	  
	  int mapsize = free[type].map_size();
	  write(fd, (char*)&mapsize, sizeof(mapsize));
	  
	  // over entries
	  for (map<idno_t,idno_t>::iterator it = free[type]..map_begin();
		   it != free[type].map_end();
		   it++) {
		idno_t a = it->first;
		idno_t b = it->second;
		write(fd, &a, sizeof(a));
		write(fd, &b, sizeof(b));
	  }
	}
	close(fd);
  } else 
	assert(0);
}


void IdAllocator::load()
{  
  int fd;
  fd = open(get_filename(), O_RDONLY);
  if (fd >= 0) {
	int ntypes;
	read(fd, &ntypes, sizeof(ntypes));

	for (int ty = 0; ty < ntypes; ty++) {
	  char type;
	  read(fd, &type, 1);

	  int mapsize = 0;
	  read(fd, &mapsize, sizeof(mapsize));
	  for (int i=0; i<mapsize; i++) {
		idno_t a,b;
		read(fd, &a, sizeof(a));
		read(fd, &b, sizeof(b));
		free[type].map_insert(a,b);
	  }
	  close(fd);
	}
  }
  else {
	// use generic range
	free[ID_INO].map_insert((long long)1000000000000LL * (mds->get_nodeid()+1),
							(long long)1000000000000LL * (mds->get_nodeid()+2) - 1);
	free[ID_FH].map_insert((long long)1000000000000LL * (mds->get_nodeid()+1),
						   (long long)1000000000000LL * (mds->get_nodeid()+2) - 1);
  }
}
