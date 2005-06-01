#ifndef __IDALLOCATOR_H
#define __IDALLOCATOR_H

#include "include/types.h"
#include "include/rangeset.h"

class MDS;

#define ID_INO    1  // inode
#define ID_FH     2  // file handle

typedef __uint64_t idno_t;

class IdAllocator {
  MDS *mds;

  map< int, rangeset<idno_t> > free;  // type -> rangeset

  bool opened, opening;
  
 public:
  IdAllocator();
  IdAllocator(MDS *mds) {
	this->mds = mds;
	opened = false;
	
	load();
	free[ID_INO].dump();
	free[ID_FH].dump();
  }
  //~InoAllocator();

  idno_t get_id(int type);
  void reclaim_id(int type, idno_t id);

  // load/save from disk (hack)
  void save();
  void load();
  void load_2(int, char *, char*);

 private:
  char *get_filename();

};

#endif
