#ifndef __INOALLOCATOR_H
#define __INOALLOCATOR_H

#include "include/types.h"
#include "rangeset.h"

class MDS;

#define ID_INO    1  // inode
#define ID_FH     2  // file handle

typedef __uint64_t id_t

class IdAllocator {
  MDS *mds;

  map< char, rangeset<id_t> > free;  // type -> rangeset
  
 public:
  IdAllocator();
  IdAllocator(MDS *mds) {
	this->mds = mds;
	load();
  }
  //~InoAllocator();

  
  id_t get_id(char type) {
	free[type].dump();
	id_t ino = free[type].first();
	free[type].erase(ino);
	cout << "id type " << type << " is " << ino << endl;
	free[type].dump();
	save();
	return id;
  }
  void reclaim_id(char type, id_t ino) {
	free[type].insert(ino);
	save();
  }

  // load/save from disk (hack)
  void load();
  void save();

 private:
  char *get_filename();

};

#endif
