#ifndef __IDALLOCATOR_H
#define __IDALLOCATOR_H

#include "include/types.h"
#include "include/rangeset.h"
#include "include/bufferlist.h"
#include "include/Context.h"

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
  }
  //~InoAllocator();

  idno_t get_id(int type);
  void reclaim_id(int type, idno_t id);

  // load/save from disk (hack)
  bool is_open() { return opened; }
  bool is_opening() { return opening; }

  void save();
  void load(Context *onfinish);
  void load_2(int, bufferlist&, Context *onfinish);

};

#endif
