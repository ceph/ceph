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

  map< int, rangeset<idno_t> > free;  // type -> rangeset
  
 public:
  IdAllocator();
  IdAllocator(MDS *mds) {
	this->mds = mds;
	load();
	cout << "idalloc init " << this << endl;
	free[ID_INO].dump();
	free[ID_FH].dump();
  }
  //~InoAllocator();

  
  idno_t get_id(int type) {
	cout << "idalloc " << this << ": type " << type << " dump:" << endl;
	free[type].dump();
	idno_t id = free[type].first();
	free[type].erase(id);
	cout << "idalloc " << this << ": getid type " << type << " is " << id << endl;
	free[type].dump();
	save();
	return id;
  }
  void reclaim_id(int type, idno_t id) {
	cout << "idalloc " << this << ": reclaim type " << type << " id " << id << endl;
	free[type].insert(id);
	free[type].dump();
	save();
  }

  // load/save from disk (hack)
  void load();
  void save();

 private:
  char *get_filename();

};

#endif
