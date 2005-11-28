
#include "BufferCache.h"


/*********** BufferHead **************/











/************ ObjectCache **************/


int ObjectCache::map_read(block_t start, block_t len, 
						  map<block_t, BufferHead*>& hits,
						  map<block_t, BufferHead*>& missing,
						  map<block_t, BufferHead*>& rx) {
  
  map<off_t, BufferHead*>::iterator p = data.lower_bound(start);
  // p->first >= start

  block_t cur = start;
  block_t left = len;
  
  if (p != data.begin() && p->first < cur) {
	p--;     // might overlap!
	if (p->first + p->second->object_loc.length <= cur) 
	  p++;   // doesn't overlap.
  }

  for (; left > 0; p++) {
	// at end?
	if (p == data.end()) {
	  // rest is a miss.
	  BufferHead *n = new BufferHead();
	  bc->add_bh(n);
	  n->object_loc.start = cur;
	  n->object_loc.length = left;
	  data[cur] = n;
	  bc->mark_rx(n);
	  missing[cur] = n;
	  break;
	}
	
	if (p->first <= cur) {
	  // have it (or part of it)
	  BufferHead *e = p->second;
	  
	  if (e->get_state() == BufferHead::STATE_CLEAN ||
		  e->get_state() == BufferHead::STATE_DIRTY ||
		  e->get_state() == BufferHead::STATE_TX) {
		hits[cur] = e;     // readable!
	  } 
	  else if (e->get_status() == BufferHead::STATE_RX) {
		rx[cur] = e;       // missing, not readable.
	  }
	  
	  block_t lenfromcur = e->object_loc.length;
	  if (e->object_loc.start < cur)
		lenfromcur -= cur - e->object_loc.start;
	  
	  if (lenfromcur < left) {
		cur += lenfromcur;
		left -= lenfromcur;
		continue;  // more!
	  } else {
		break;     // done.
	  }		
	} else if (p->first > cur) {
	  // gap.. miss
	  block_t next = p->first;
	  BufferHead *n = new BufferHead();
	  bc->add_bh(n);
	  n->object_loc.start = cur;
	  if (next - cur < left)
		n->object_loc.length = next - cur;
	  else
		n->object_loc.length = left;
	  data[cur] = n;
	  bc->mark_rx(n);
	  missing[cur] = n;
	  
	  cur += object_loc.length;
	  left -= object_loc.length;
	  continue;    // more?
	}
	else 
	  assert(0);
  }

  return 0;  
}

int ObjectCache::map_write(block_t start, block_t len,
						   map<block_t, BufferHead*>& hits) 
{
  map<off_t, BufferHead*>::iterator p = data.lower_bound(start);
  // p->first >= start
  
  block_t cur = start;
  block_t left = len;
  
  if (p != data.begin() && p->first < cur) {
	p--;     // might overlap!
	if (p->first + p->second->object_loc.length <= cur) 
	  p++;   // doesn't overlap.
  }

  for (; left > 0; p++) {
	// at end?
	if (p == data.end()) {
	  BufferHead *n = new BufferHead();
	  bc->add_bh(n);
	  n->object_loc.start = cur;
	  n->object_loc.length = left;
	  data[cur] = n;
	  bc->mark_dirty(n);
	  hits[cur] = n;
	  break;
	}
	
	if (p->first <= cur) {
	  // have it (or part of it)
	  BufferHead *e = p->second;
	  
	  if (p->first == cur && p->second->object_loc.length <= left) {
		// whole bufferhead, piece of cake.		
	  } else {
		if (e->get_status() == BufferHead::STATUS_CLEAN) {
		  // we'll need to cut the buffer!  :(
		  if (p->first == cur && p->second->object_loc.length > left) {
			// we want left bit (one splice)
			data[cur+left] = bc->split(e, cur + left);
		  }
		  else if (p->first < cur && cur+left >= p->first+p->second->object_loc.length) {
			// we want right bit (one splice)
			e = bc->split(e, cur);
			data[cur] = e;
			p++;
			assert(p->second == e);
		  } else {
			// we want middle bit (two splices)
			e = bc->split(e, cur);
			data[cur] = e;
			p++;
			assert(p->second == e);
			data[cur+left] = bc->split(e, cur+left);
		  }
		}		
	  }
	  
	  // FIXME
	  e->set_status(BufferHead::STATUS_DIRTY);
	  hits[cur] = e;
	  	  
	  // keep going.
	  block_t lenfromcur = e->object_loc.length;
	  if (e->object_loc.start < cur)
		lenfromcur -= cur - e->object_loc.start;

	  if (lenfromcur < left) {
		cur += lenfromcur;
		left -= lenfromcur;
		continue;  // more!
	  } else {
		break;     // done.
	  }		
	} else {
	  // gap!
	  block_t next = p->first;
	  BufferHead *n = new BufferHead();
	  bc->add_bh(n);
	  n->object_loc.start = cur;
	  if (next - cur < left)
		n->object_loc.length = next - cur;
	  else
		n->object_loc.length = left;
	  data[cur] = n;
	  bc->mark_dirty(n);
	  hits[cur] = n;
	  
	  cur += object_loc.length;
	  left -= object_loc.length;
	  continue;    // more?
	}
  }

  return 0;
}



/************** BufferCache ***************/


BufferHead *BufferCache::split(BufferHead *orig, block_t after) 
{
  BufferHead *right = new BufferHead;
  right->version(orig->get_version());
  right->set_state(orig->get_state());
  
  block_t mynewlen = after - orig->object_loc.start;
  right->object_loc.start = after;
  right->object_loc.length = orig->object_loc.length - mynewlen;

  add_bh(right);

  stat_sub(orig);
  orig->object_loc.length = mynewlen;
  stat_add(orig);
  

  // FIXME: waiters?
  
  return right;
}

