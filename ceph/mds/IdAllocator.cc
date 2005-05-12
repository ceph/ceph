
#include "include/config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "idalloc:"

#define DBLEVEL  20

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


idno_t IdAllocator::get_id(int type) 
{
  dout(DBLEVEL) << "idalloc " << this << ": type " << type << " dump:" << endl;
  //free[type].dump();
  idno_t id = free[type].first();
  free[type].erase(id);
  dout(DBLEVEL) << "idalloc " << this << ": getid type " << type << " is " << id << endl;
  //free[type].dump();
  save();
  return id;
}

void IdAllocator::reclaim_id(int type, idno_t id) 
{
  dout(DBLEVEL) << "idalloc " << this << ": reclaim type " << type << " id " << id << endl;
  free[type].insert(id);
  //free[type].dump();
  save();
}




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
	for (map<int, rangeset<idno_t> >::iterator ty = free.begin();
		 ty != free.end(); 
		 ty++) {
	  int type = ty->first;
	  write(fd, (char*)&type, sizeof(type));
	  
	  int mapsize = free[type].map_size();
	  write(fd, (char*)&mapsize, sizeof(mapsize));
	  dout(DBLEVEL) << "type " << type << " num " << mapsize << endl;
	  
	  // over entries
	  for (map<idno_t,idno_t>::iterator it = free[type].map_begin();
		   it != free[type].map_end();
		   it++) {
		idno_t a = it->first;
		idno_t b = it->second;
		write(fd, &a, sizeof(a));
		write(fd, &b, sizeof(b));
		mapsize--;
	  }
	  assert(mapsize == 0);
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
	dout(DBLEVEL) << "ntypes " << ntypes << endl;

	for (int ty = 0; ty < ntypes; ty++) {
	  int type;
	  read(fd, &type, sizeof(type));

	  int mapsize = 0;
	  read(fd, &mapsize, sizeof(mapsize));

	  dout(DBLEVEL) << "type " << type << " num " << mapsize << endl;
	  for (int i=0; i<mapsize; i++) {
		idno_t a,b;
		read(fd, &a, sizeof(a));
		read(fd, &b, sizeof(b));
		free[type].map_insert(a,b);
	  }
	}
	close(fd);
  }
  else {
	// use generic range FIXME THIS IS CRAP
	free[ID_INO].map_insert((long long)1000000LL * (mds->get_nodeid()+1),
							(long long)1000000LL * (mds->get_nodeid()+2) - 1);
	//free[ID_INO].dump();

	free[ID_FH].map_insert(1000000LL * (mds->get_nodeid()+1),
						   1000000LL * (mds->get_nodeid()+2) - 1);
	//free[ID_FH].dump();
  }
}
