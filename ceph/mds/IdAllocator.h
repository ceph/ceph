#ifndef __IDALLOCATOR_H
#define __IDALLOCATOR_H

#include "include/types.h"
#include "rangeset.h"

class MDS;

#define ID_INO    1  // inode
#define ID_FH     2  // file handle

typedef __uint64_t idno_t;

class IdAllocator {
  MDS *mds;

  map< char, rangeset<idno_t> > free;  // type -> rangeset
  
 public:
  IdAllocator();
  IdAllocator(MDS *mds) {
	this->mds = mds;
	load();
  }
  //~InoAllocator();

  
  idno_t get_id(char type) {
	free[type].dump();
	idno_t id = free[type].first();
	free[type].erase(id);
	cout << "id type " << type << " is " << id << endl;
	free[type].dump();
	save();
	return id;
  }
  void reclaim_id(char type, idno_t id) {
	free[type].insert(id);
	save();
  }

  // load/save from disk (hack)
  void load();
  void save();

 private:
  char *get_filename();

};

#endif
