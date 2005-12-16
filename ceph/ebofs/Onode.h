#ifndef __EBOFS_ONODE_H
#define __EBOFS_ONODE_H

#include "include/lru.h"

#include "types.h"
#include "BufferCache.h"

#include "include/interval_set.h"


/*
 * object node (like an inode)
 *
 * holds object metadata, including
 *  size
 *  allocation (extent list)
 *  attributes
 *
 */

class Onode : public LRUObject {
private:
  int ref;

public:
  object_t object_id;
  version_t version;      // incremented on each modify.

  // data
  Extent   onode_loc;
  off_t    object_size;
  unsigned object_blocks;

  // onode
  map<string, AttrVal>  attr;
  vector<Extent>        extents;

  interval_set<block_t> uncommitted;

  ObjectCache  *oc;

  bool          dirty;
  bool          deleted;

  list<Context*>   commit_waiters;

 public:
  Onode(object_t oid) : ref(0), object_id(oid), version(0),
	object_size(0), object_blocks(0), oc(0),
	dirty(false), deleted(false) { 
	onode_loc.length = 0;
  }
  ~Onode() {
	if (oc) delete oc;
  }

  block_t get_onode_id() { return onode_loc.start; }
  int get_onode_len() { return onode_loc.length; }

  int get_ref_count() { return ref; }
  void get() {
	if (ref == 0) lru_pin();
	ref++;
	//cout << "ebofs.onode.get " << ref << endl;
  }
  void put() {
	ref--;
	if (ref == 0) lru_unpin();
	//cout << "ebofs.onode.put " << ref << endl;
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
  void _append_extent(Extent& ex) {
	if (extents.size() &&
		extents[extents.size()-1].end() == ex.start)
	  extents[extents.size()-1].length += ex.length;
	else
	  extents.push_back(ex);		
  }
  void verify_extents() {
	block_t count = 0;
	interval_set<block_t> is;	

	if (0) {  // do crazy stupid sanity checking
	  set<block_t> s;
	  cout << "verifying" << endl;
	  for (unsigned i=0; i<extents.size(); i++) {
		//cout << "verify_extents " << i << " off " << count << " " << extents[i] << endl;
		count += extents[i].length;
		
		//assert(!is.contains(extents[i].start, extents[i].length));
		//is.insert(extents[i].start, extents[i].length);
		
		for (unsigned j=0;j<extents[i].length;j++) {
		  assert(s.count(extents[i].start+j) == 0);
		  s.insert(extents[i].start+j);
		}
	  }
	  cout << "verified " << extents.size() << " extents" << endl;
	  assert(s.size() == count);
	  assert(count == object_blocks);
	}
  }
  void set_extent(block_t offset, Extent ex) {
	//cout << "set_extent " << offset << " " << ex << " ... " << object_blocks << endl;
	assert(offset <= object_blocks);
	verify_extents();

	// at the end?
	if (offset == object_blocks) {
	  _append_extent(ex);
	  object_blocks += ex.length;
	  return;
	}

	// nope.  ok, rebuild the extent list.
	vector<Extent> old;
	old.swap(extents);
	assert(extents.empty());

	unsigned oldex = 0;
	block_t oldoff = 0;
	block_t cur = 0;

	// copy up to offset
	while (cur < offset) {
	  Extent t;
	  t.start = old[oldex].start+oldoff;
	  t.length = MIN(offset-cur, old[oldex].length-oldoff);
	  _append_extent(t);

	  cur += t.length;
	  oldoff += t.length;
	  if (oldoff == old[oldex].length) {
		oldex++;
		oldoff = 0;
	  }	  
	}

	// add our new extent
	_append_extent(ex);
	if (offset + ex.length > object_blocks)
	  object_blocks = offset + ex.length;	

	// skip past it in the old stuff
	block_t sleft = ex.length;
	while (sleft > 0) {
	  block_t skip = MIN(sleft, old[oldex].length-oldoff);
	  sleft -= skip;
	  oldoff += skip;
	  if (oldoff == old[oldex].length) {
		oldex++;
		oldoff = 0;
		if (oldex == old.size()) break;
	  }
	}

	// copy anything left?
	while (oldex < old.size()) {
	  if (oldoff) {
		Extent t;
		t.start = old[oldex].start+oldoff;
		t.length = old[oldex].length-oldoff;
		_append_extent(t);
		oldoff = 0;
		oldex++;
	  } else {
		_append_extent(old[oldex++]);
	  }
	}

	verify_extents();
  }
  

  /* map_extents(start, len, ls)
   *  map teh given page range into extents on disk.
   */
  int map_extents(block_t start, block_t len, vector<Extent>& ls) {
	verify_extents();
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


  /* map_alloc_regions(start, len, map)
   *  map range into regions that need to be (re)allocated on disk
   *  because they overlap "safe" (or unallocated) parts of the object
   */
  void map_alloc_regions(block_t start, block_t len, 
						 interval_set<block_t>& alloc) {
	interval_set<block_t> already_uncom;

	alloc.insert(start, len);   // start with whole range
	already_uncom.intersection_of(alloc, uncommitted);
	alloc.subtract(already_uncom);   // take out the bits that aren't yet committed
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
