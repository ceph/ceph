#ifndef __INOALLOCATOR_H
#define __INOALLOCATOR_H

#include "include/types.h"
#include "rangeset.h"

class MDS;

class InoAllocator {
  MDS *mds;

  rangeset<inodeno_t> free;

 public:
  InoAllocator();
  InoAllocator(MDS *mds) {
	this->mds = mds;
	load();
  }
  //~InoAllocator();

  inodeno_t get_ino() {
	free.dump();
	inodeno_t ino = free.first();
	free.erase(ino);
	cout << "ino is " << ino << endl;
	free.dump();
	save();
	return ino;
  }
  void reclaim_ino(inodeno_t ino) {
	free.insert(ino);
	save();
  }

  // load/save from disk (hack)
  void load();
  void save();

 private:
  char *get_filename();

};

#endif
