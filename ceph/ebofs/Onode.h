#ifndef __EBOFS_ONODE_H
#define __EBOFS_ONODE_H


#include "include/lru.h"

#include "types.h"
#include "BufferCache.h"

class AttrVal {
 public:
  char *data;
  int len;
  AttrVal() : data(0), len(0) {}
  AttrVal(char *from, int l) : 
	len(l) {
	data = new char[len];
	memcpy(data, from, len);
  }
  AttrVal(const AttrVal &other) {
	len = other.len;
	data = new char[len];
	memcpy(data, other.data, len);
  }
  AttrVal& operator=(const AttrVal &other) {
	if (data) delete[] data;
	len = other.len;
	data = new char[len];
	memcpy(data, other.data, len);
	return *this;
  }
  ~AttrVal() {
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
  map<string, AttrVal > attr;
  vector<Extent>        extents;

  ObjectCache  *oc;

  bool          dirty;

 public:
  Onode(object_t oid) : ref(0), object_id(oid),
	object_size(0), object_blocks(0), oc(0),
	dirty(false) { 
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
	cout << "onode.get " << ref << endl;
  }
  void put() {
	ref--;
	if (ref == 0) lru_unpin();
	cout << "onode.put " << ref << endl;
  }

  void mark_dirty() {
	if (!dirty) {
	  dirty = true;
	  get();
	}
  }
  void mark_clean() {
	if (dirty) {
	  dirty = false;
	  put();
	}
  }
  bool is_dirty() { return dirty; }

  
  ObjectCache *get_oc(BufferCache *bc) {
	if (!oc) {
	  oc = new ObjectCache(object_id, bc);
	  get();
	}
	return oc;
  }
  void close_oc() {
	assert(oc);
	delete oc;
	oc = 0;
	put();
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
	for (map<string, AttrVal >::iterator i = attr.begin();
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
