#ifndef __EBOFS_ONODE_H
#define __EBOFS_ONODE_H


#include "include/lru.h"

#include "types.h"
#include "BufferCache.h"


class Onode : public LRUObject {
private:
  int ref;

public:
  object_t object_id;

  // data
  Extent   onode_loc;
  off_t    object_size;
  unsigned object_blocks;

  // onode
  map<string, pair<int, void*> > attr;
  vector<Extent>                 extents;

  ObjectCache  *oc;


 public:
  Onode(object_t oid) : ref(0), object_id(oid),
	object_size(0), object_blocks(0), oc(0) { 
	onode_loc.length = 0;
  }

  block_t get_onode_id() { return onode_loc.start; }
  int get_onode_len() { return onode_loc.length; }

  void get() {
	if (ref == 0) lru_pin();
	ref++;
  }
  void put() {
	ref--;
	if (ref == 0) lru_unpin();
  }

  // attr
  int getxattr(char *name, void *value, int size) {
	return 0;
  }
  int setxattr(char *name, void *value, int size) {
	return 0;	
  }
  int removexattr(char *name) {
	return 0;
  }


  // pack/unpack
  int get_attr_bytes() {
	int s = 0;
	for (map<string, pair<int, void*> >::iterator i = attr.begin();
		 i != attr.end();
		 i++) {
	  s += i->first.length() + 1;
	  s += i->second.first + sizeof(int);
	}
	return s;
  }
  int get_extent_bytes() {
	return sizeof(Extent) * extents.size();
  }


  

};



#endif
