#ifndef __EBOFS_ONODE_H
#define __EBOFS_ONODE_H


#include "include/lru.h"

#include "types.h"
#include "BufferCache.h"

class OnodeAttrVal {
 public:
  char *data;
  int len;
  OnodeAttrVal() : data(0), len(0) {}
  OnodeAttrVal(char *from, int l) : 
	len(l) {
	data = new char[len];
	memcpy(data, from, len);
  }
  OnodeAttrVal(const OnodeAttrVal &other) {
	len = other.len;
	data = new char[len];
	memcpy(data, other.data, len);
  }
  OnodeAttrVal& operator=(const OnodeAttrVal &other) {
	if (data) delete[] data;
	len = other.len;
	data = new char[len];
	memcpy(data, other.data, len);
	return *this;
  }
  ~OnodeAttrVal() {
	delete[] data;
  }
};

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
  map<string, OnodeAttrVal > attr;
  vector<Extent>             extents;

  ObjectCache  *oc;


 public:
  Onode(object_t oid) : ref(0), object_id(oid),
	object_size(0), object_blocks(0), oc(0) { 
	onode_loc.length = 0;
  }
  ~Onode() {
	delete oc;
	// lose the attrs
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

  // allocation
  int map_extents(block_t start, block_t len, vector<Extent>& ls) {
	block_t cur = 0;
	for (unsigned i=0; i<extents.size(); i++) {
	  if (cur >= start+len) break;
	  if (cur + extents[i].length > start) {
		Extent ex;
		block_t headskip = start-cur;
		ex.start = extents[i].start + headskip;
		ex.length = MIN(len, extents[i].length - headskip);
		ls.push_back(ex);
		start += ex.length;
		len -= ex.length;
		if (len == 0) break;
	  }
	  cur += extents[i].length;
	}
	return 0;
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
	for (map<string, OnodeAttrVal >::iterator i = attr.begin();
		 i != attr.end();
		 i++) {
	  s += i->first.length() + 1;
	  s += i->second.len + sizeof(int);
	}
	return s;
  }
  int get_extent_bytes() {
	return sizeof(Extent) * extents.size();
  }


};


inline ostream& operator<<(ostream& out, Onode& on)
{
  out << "onode(" << hex << on.object_id << dec << " len=" << on.object_size << ")";
  return out;
}



#endif
