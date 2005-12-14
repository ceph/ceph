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

  // data
  Extent   onode_loc;
  off_t    object_size;
  unsigned object_blocks;

  // onode
  map<string, AttrVal > attr;
  vector<Extent>        extents;

  interval_set<block_t> uncommitted;

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
  void set_extent(block_t offset, Extent ex) {
	assert(offset <= object_blocks);

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
	  block_t skip = MIN(ex.length, old[oldex].length-oldoff);
	  sleft -= skip;
	  oldoff += skip;
	  if (oldoff == old[oldex].length) {
		oldex++;
		oldoff = 0;
		if (oldex == old.size()) break;
	  }
	}

	// copy anything left?
	if (oldex < old.size()) {
	  if (oldoff) {
		Extent t;
		t.start = old[oldex].start+oldoff;
		t.length = old[oldex].length-oldoff;
		_append_extent(t);
	  } else {
		_append_extent(old[oldex++]);
	  }
	}
  }
  

  /* map_extents(start, len, ls)
   *  map teh given page range into extents on disk.
   */
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



  /*
  // (un)committed ranges
  void dirty_range(block_t start, block_t len) {
	// frame affected area (used for simplify later)
	block_t first = start;
	block_t last = start+len;

	// put in uncommitted range
	map<block_t,block_t>::iterator p = uncommitted.lower_bound(start);
	if (p != uncommitted.begin() &&
		(p->first > start || p == uncommitted.end())) {
	  p--;
	  first = p->first;
	  if (p->first + p->second <= start) 
		p++;
	}

	for (; len>0; p++) {
	  if (p == uncommitted.end()) {
		uncommitted[start] = len;
		break;
	  }
	  
	  if (p->first <= start) {
		block_t skip = start - p->first;
		block_t overlap = MIN( p->second - skip, len );
		start += overlap;
		len -= overlap;
	  } else if (p->first > start) { 
		block_t add = MIN(len, p->first - start);
		uncommitted[start] = add;
		start += add;
		len -= add;
	  }		
	}

	// simplify uncommitted
	map<block_t,block_t>::iterator p = uncommitted.lower_bound(first);
	block_t prevstart = p->first;
	block_t prevend = p->first + p->second;
	p++;
	while (p != uncommitted.end()) {
	  if (prevend == p->first) {
		uncommitted[prevstart] += p->second;
		prevend += p->second;
		block_t t = p->first;
		p++;
		uncommitted.erase(t);
	  } else {
		prevstart = p->first;
		prevend = p->first + p->second;
		if (prevend >= last) break;        // we've gone past our updates
		p++;
	  }
	}
  }
  void mark_committed() {
	uncommitted.clear();
  }

  bool is_uncommitted(block_t b) {
	
  }
  */


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
