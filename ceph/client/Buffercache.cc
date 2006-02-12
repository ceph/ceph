// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Carlos Maltzahn <carlosm@soe.ucsc.edu>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#include "Buffercache.h"
#include "Client.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "client" << "." << pthread_self() << " "

// -- Bufferhead methods

Bufferhead::Bufferhead(Inode *inode, Buffercache *bc) : 
  ref(0), miss_len(0), dirty_since(0), visited(false) {
  dout(10) << "bc: new bufferhead ino: " << hex << inode->ino() << dec << endl;
  this->inode = inode;
  inode->get();
  state = BUFHD_STATE_CLEAN;
  this->bc = bc;
  fc = bc->get_fc(inode);
  bc->lru.lru_insert_top(this); //FIXME: parameterize whether top or mid
  // buffers are allocated later
}

Bufferhead::Bufferhead(Inode *inode, off_t off, Buffercache *bc) : 
  ref(0), miss_len(0), dirty_since(0), visited(false) {
  dout(10) << "bc: new bufferhead ino: " << hex << inode->ino() << dec << " offset: " << off << endl;
  this->inode = inode;
  inode->get();
  state = BUFHD_STATE_CLEAN;
  this->bc = bc;
  fc = bc->get_fc(inode);
  bc->lru.lru_insert_top(this); //FIXME: parameterize whether top or mid
  set_offset(off);
  // buffers are allocated later
}

Bufferhead::~Bufferhead()
{
  dout(10) << "bc: destroying bufferhead ino: " << hex << inode->ino() << dec << " size: " << bl.length() << " offset: " << offset << endl;
  assert(state == BUFHD_STATE_CLEAN);
  assert(ref == 0);
  assert(lru_is_expireable());
  assert(read_waiters.empty());
  assert(write_waiters.empty());
  assert(!fc->buffer_map.count(offset));
  inode->put();
  bc->lru.lru_remove(this);   
  // debug segmentation fault
  if (bl.buffers().empty()) {
    dout(10) << "bc: bufferlist is empty" << endl;
#if 0
  } else {
    for (list<bufferptr>::iterator it = bl.buffers().begin();
         it != bl.buffers().end();
	 it++) {
      //dout(10) << "bc: bufferptr len: " << it->length() << " off: " << it->offset() << endl;
      dout(10) << "bc: bufferptr: " << *it << endl; 
    }
    dout(10) <<"bc: listed all bufferptrs" << endl;
#endif
  }   
}

void Bufferhead::set_offset(off_t offset)
{
  this->offset = offset;
  assert(!fc->buffer_map.count(offset)); // fail loudly if offset already exists!
  fc->insert(offset, this); 
}

void Bufferhead::alloc_buffers(off_t size)
{
  dout(10) << "bc: allocating buffers size: " << size << endl;
  assert(size > 0);
  while (size > 0) {
    if (size <= (unsigned)g_conf.client_bcache_alloc_maxsize) {
          off_t k = g_conf.client_bcache_alloc_minsize;
          off_t asize = size - size % k + (size % k > 0) * k;
	  buffer *b = new buffer(asize);
	  b->set_length(size);
	  bl.push_back(b);
	  bc->increase_size(size);
          dout(10) << "bc: new buffer(" << asize << "), total: " << bl.length() << endl;
	  break;
	}
        buffer *b = new buffer(g_conf.client_bcache_alloc_maxsize);
	b->set_length(g_conf.client_bcache_alloc_maxsize);
	bl.push_back(b);
        dout(10) << "bc: new buffer(" << g_conf.client_bcache_alloc_maxsize << "), total: " << bl.length() << endl;
	size -= g_conf.client_bcache_alloc_maxsize;
	bc->increase_size(g_conf.client_bcache_alloc_maxsize);
  }
  dout(6) << "bc: allocated " << bl.buffers().size() << " buffers (" << bl.length() << " bytes) " << endl;
  assert(bl.length() == size);
}

void Bufferhead::miss_start(off_t miss_len) 
{
  assert(state == BUFHD_STATE_CLEAN);
  get();
  state = BUFHD_STATE_RX;
  this->miss_len = miss_len;
  bc->lru.lru_touch(this);
}

void Bufferhead::miss_finish() 
{
  assert(state == BUFHD_STATE_RX);
  state = BUFHD_STATE_CLEAN;
  if (bl.length() == 0) {
#if 0
    alloc_buffers(miss_len); 
    bl.zero();
    dout(6) << "bc: miss_finish: allocated zeroed buffer len: " << bl.length() << endl;
#endif
  } else {
    bc->increase_size(bl.length());
  }
  dout(6) << "bc: miss_finish: clean_size: " << bc->get_clean_size() << " dirty_size: " << bc->get_dirty_size() << " rx_size: " << bc->get_rx_size() << " tx_size: " << bc->get_tx_size() << " age: " << bc->dirty_buffers->get_age() << endl;
  //assert(bl.length() == miss_len);
  wakeup_read_waiters();
  wakeup_write_waiters();
  put();
}

void Bufferhead::dirty() 
{
  if (state == BUFHD_STATE_CLEAN) {
    get();
    dout(6) << "bc: dirtying clean buffer size: " << bl.length() << endl;
    state = BUFHD_STATE_DIRTY;
    dirty_since = time(NULL); // start clock for dirty buffer here
    bc->lru.lru_touch(this);
    dout(6) << "bc: dirty before: clean_size: " << bc->get_clean_size() << " dirty_size: " << bc->get_dirty_size() << " rx_size: " << bc->get_rx_size() << " tx_size: " << bc->get_tx_size() << " age: " << bc->dirty_buffers->get_age() << endl;
    bc->clean_to_dirty(bl.length());
    dout(6) << "bc: dirty after: clean_size: " << bc->get_clean_size() << " dirty_size: " << bc->get_dirty_size() << " rx_size: " << bc->get_rx_size() << " tx_size: " << bc->get_tx_size() << " age: " << bc->dirty_buffers->get_age() << endl;
    assert(!bc->dirty_buffers->exist(this));
    bc->dirty_buffers->insert(this); 
    assert(!fc->dirty_buffers.count(this));
    fc->dirty_buffers.insert(this);
  } else {
    dout(10) << "bc: dirtying dirty buffer size: " << bl.length() << endl;
  }
}

void Bufferhead::dirtybuffers_erase() 
{
  dout(7) << "bc: erase in dirtybuffers offset: " << offset << " size: " << length() << endl;
  assert(bc->dirty_buffers->exist(this));
  bc->dirty_buffers->erase(this);
  assert(fc->dirty_buffers.count(this));
  fc->dirty_buffers.erase(this);
  put();
}

void Bufferhead::flush_start() 
{
  dout(10) << "bc: flush_start" << endl;
  assert(state == BUFHD_STATE_DIRTY);
  get();
  state = BUFHD_STATE_TX;
  dirtybuffers_erase();
  assert(!bc->inflight_buffers.count(this));
  bc->inflight_buffers.insert(this);
  bc->dirty_to_tx(bl.length());
  dout(6) << "bc: flush_start: clean_size: " << bc->get_clean_size() << " dirty_size: " << bc->get_dirty_size() << " rx_size: " << bc->get_rx_size() << " tx_size: " << bc->get_tx_size() << " age: " << bc->dirty_buffers->get_age() << endl;
  assert(!fc->inflight_buffers.count(this));
  fc->inflight_buffers.insert(this);
}

void Bufferhead::flush_finish() 
{
  dout(10) << "bc: flush_finish" << endl;
  assert(state == BUFHD_STATE_TX);
  state = BUFHD_STATE_CLEAN;
  assert(bc->inflight_buffers.count(this));
  bc->inflight_buffers.erase(this);
  bc->tx_to_clean(bl.length());
  visited = 0;
  dout(6) << "bc: flush_finish: clean_size: " << bc->get_clean_size() << " dirty_size: " << bc->get_dirty_size() << " rx_size: " << bc->get_rx_size() << " tx_size: " << bc->get_tx_size() << " age: " << bc->dirty_buffers->get_age() << endl;
  assert(fc->inflight_buffers.count(this));
  fc->inflight_buffers.erase(this);
  wakeup_write_waiters(); // readers never wait on flushes
  put();
}

void Bufferhead::claim_append(Bufferhead *other) 
{
  dout(10) << "bc: claim_append old bl size: " << bl.buffers().size() << " length " << bl.length() << endl;
  bl.claim_append(other->bl);
  dout(10) << "bc: claim_append new bl size: " << bl.buffers().size() << " length: " << bl.length() << endl;
  // keep older time stamp
  if (other->dirty_since < dirty_since) {
    dirty_since = other->dirty_since;
    bc->dirty_buffers->insert(this); // update Dirtybuffers index!
  }
  other->bl.clear();
}

void Bufferhead::splice(off_t rel_off, off_t length, Bufferhead *claim_by)
{
  dout(10) << "bc: Bufferhead::splice rel_off: " << rel_off << " length: " << length << " claim_by: " << claim_by << endl;
  assert(length <= this->length());
  if (is_hole()) {
    claim_by->alloc_buffers(length);
    miss_len -= length;
  } else {
    bl.splice(rel_off, length, &(claim_by->bl));
  }
}

// -- Dirtybuffers methods

multimap<time_t, Bufferhead*>::iterator Dirtybuffers::find(Bufferhead *bh)
{
  for (multimap<time_t, Bufferhead*>::iterator it = _dbufs.lower_bound(_revind[bh]);
       it != _dbufs.upper_bound(_revind[bh]);
       it++) {
    Bufferhead *bh1 = it->second;
    if (bh1 == bh) {
      assert(bh1->inode->ino() == bh->inode->ino() && 
             bh1->offset == bh->offset &&
	     bh1->length() == bh->length());
      return it;
    }
    assert(bh1->inode->ino() != bh->inode->ino() || 
           bh1->offset != bh->offset ||
	   bh1->length() != bh->length());
  }
  return _dbufs.end();
}

void Dirtybuffers::erase(Bufferhead* bh) 
{
  dout(6) << "dirtybuffer: erase bh->ino: " << bh->inode->ino() << " offset: " << bh->offset << " len: " << bh->length() << endl;
  assert(exist(bh));
  unsigned osize = _dbufs.size();
  _dbufs.erase(find(bh));
  assert(_dbufs.size() == osize - 1);
  _revind.erase(bh);
  assert(_revind.size() == osize - 1);
  assert(_revind.size() == _dbufs.size());
}

void Dirtybuffers::insert(Bufferhead* bh) 
{
  dout(6) << "dirtybuffer: insert bh->ino: " << bh->inode->ino() << " offset: " << bh->offset << " len: " << bh->length() << endl;
  if (_revind.count(bh)) {
    _dbufs.erase(find(bh));
  }
  _revind[bh] = bh->dirty_since;
  _dbufs.insert(pair<time_t, Bufferhead*> (bh->dirty_since, bh));
  assert(_revind.size() == _dbufs.size());
}

bool Dirtybuffers::exist(Bufferhead* bh) 
{
  return _revind.count(bh);
#if 0
  /*
  for (multimap<time_t, Bufferhead*>::iterator it = _dbufs.lower_bound(bh->dirty_since);
      it != _dbufs.upper_bound(bh->dirty_since);
      it++ ) {
   */
  // FIXME: DEBUG
  for (multimap<time_t, Bufferhead*>::iterator it = _dbufs.begin();
      it != _dbufs.end();
      it++ ) {
    if (it->second == bh) {
      dout(10) << "dirtybuffer: found bh->ino: " << bh->inode->ino() << " offset: " << bh->offset << endl;
      assert(it->first == it->second->dirty_since);
      return true;
    } 
    Bufferhead *bh1 = it->second;
    //assert(bh1->inode->ino() != bh->inode->ino() || bh1->offset != bh->offset);
    assert(bh1->inode->ino() != bh->inode->ino() || 
           bh1->offset != bh->offset ||
	   bh1->length() != bh->length());
  }
  return false;
#endif
}


void Dirtybuffers::get_expired(time_t ttl, off_t left_dirty, set<Bufferhead*>& to_flush) 
{
  dout(6) << "bc: get_expired ttl: " << ttl << " left_dirty: " << left_dirty << endl;
  if (_dbufs.empty() || left_dirty >= bc->get_dirty_size()) {
    dout(6) << "bc: get_expired: empty or already less dirty than left_dirty!" << endl;
    assert(_dbufs.size() == _revind.size());
    return;
  }
  map<Inode*, map<off_t, list<off_t> > > consolidation_map;
  off_t cleaned = 0;
  if (cleaned < bc->get_dirty_size() - left_dirty) {
    time_t now = time(NULL);
    for (multimap<time_t, Bufferhead*>::iterator it = _dbufs.begin();
		 it != _dbufs.end();
		 it++) {
      if (!it->second->visited) {
		if (ttl > now - it->second->dirty_since &&
			cleaned >= bc->get_dirty_size() - left_dirty) break;
		
		// find consolidation opportunity
        it->second->visited = true;
	Filecache* fc = bc->get_fc(it->second->inode);
	if (fc->buffer_map.empty()) {
	  cout << *this << endl;
	  assert(!fc->buffer_map.empty());
	}
	assert(fc->buffer_map.count(it->second->offset));
	list<off_t> offlist;
	off_t length = fc->consolidation_opp(
			  now - ttl, 
			  bc->get_dirty_size() - left_dirty - cleaned, 
			  it->second->offset, 
			  offlist);
	if (offlist.size() > 1) {
	  offlist.sort();
	  off_t start_off = offlist.front();
	  offlist.pop_front();
	  consolidation_map[it->second->inode][start_off] = offlist;
	  to_flush.insert(fc->buffer_map[start_off]);
	} else {
	  to_flush.insert(it->second);
	}
	cleaned += length;
      }
    }
	
    // consolidate
    bc->consolidate(consolidation_map);
    dout(6) << "bc: get_expired to_flush.size(): " << to_flush.size() << endl;
  }
  assert(_dbufs.size() == _revind.size());
}


// -- Filecache methods

void Filecache::insert(off_t offset, Bufferhead* bh)
{
  pair<map<off_t, Bufferhead*>::iterator, bool> rvalue;
  rvalue = buffer_map.insert(pair<off_t, Bufferhead*> (offset, bh));

  // The following is just to get the pieces for the last two assertions 
  map<off_t, Bufferhead*>::iterator next_buf = buffer_map.upper_bound(offset);

  map<off_t, Bufferhead*>::iterator prev_buf = rvalue.first;
  if (prev_buf != buffer_map.begin()) {
    prev_buf--;
  } else {
    prev_buf = buffer_map.end();
  }

  // raise if there is any overlap!
  assert(next_buf == buffer_map.end() || 
         (unsigned)next_buf->first >= (unsigned)offset + bh->length());
  assert(prev_buf == buffer_map.end() || 
         (unsigned)prev_buf->first + prev_buf->second->length() <= (unsigned)offset);
}

void Filecache::splice(off_t offset, off_t size)
{
  // insert Bufferhead at offset with size. only works if all overlapping
  // buffers are clean. Creates at most two new bufferheads at (offset, size)
  // and (offset + size, whatsleft). 
  // align splices to g_conf.client_bcache_align depending on stripe size

  dout(6) << "bc: splice off: " << offset << " len: " << size << endl;

  // align
  // FIXME: does not work with sparse files
#if 0
  dout(1) << "bc: before align offset: " << offset << " size: " << size << endl;
  off_t align = g_conf.client_bcache_align;
  while (inode->inode.layout.stripe_size % align) align >>= 1;
  offset = offset / align * align;
  size = (size / align + (size % align > 0)) * align;
  dout(1) << "bc: after align offset: " << offset << " size: " << size << endl;
#endif
  
  // get current buffer
  map<off_t, Bufferhead*>::iterator curbuf = get_buf(offset);
  assert(curbuf != buffer_map.end());
  off_t orig_len = curbuf->second->length();

  // insert new buffer leaving front part to original buffer
  if (curbuf->second->state == BUFHD_STATE_CLEAN && curbuf->first < offset ) {
    dout(6) << "bc: splice 1st buf off: " << curbuf->first << " len: " << orig_len << endl;
    if (offset + size < curbuf->first + curbuf->second->length()) {
      // split off tail first if within this Bufferhead
      unsigned new_off = offset - curbuf->first + size;
      unsigned new_len = curbuf->second->length() - new_off;
      dout(6) << "bc: splice tail of 1st off: " << new_off << " len: " << new_len << endl;
      Bufferhead* bh = new Bufferhead(inode, bc);
      curbuf->second->splice(new_off, new_len, bh);
      bh->set_offset(offset + size);
      assert(bh->length() == new_len);
      assert(curbuf->second->length() == new_off);
    }

    // create new bufferhead at offset
    unsigned new_off = offset - curbuf->first;
    unsigned new_len = curbuf->second->length() - new_off; // length() takes tail split-off into account
    dout(6) << "bc: splice head of 1st off: " << new_off << " len: " << new_len << endl;
    Bufferhead* bh = new Bufferhead(inode, bc);
    curbuf->second->splice(new_off, new_len, bh);
    bh->set_offset(offset);
    if (bh->length() != new_len) {
      cout << *bh << endl;
      assert(bh->length() == new_len);
    }
    assert(curbuf->second->length() == new_off);
  }

  // insert another new buffer if splice end is not aligned 
  if (orig_len < size) {
    curbuf = get_buf(offset + size); 
    if (curbuf != buffer_map.end() && 
        curbuf->second->state == BUFHD_STATE_CLEAN &&
        (unsigned)curbuf->first < (unsigned)offset + size) {
      dout(6) << "bc: splice last buf off: " << curbuf->first << " len: " << curbuf->second->length() << endl;
      unsigned new_off = offset + size - curbuf->first;
      unsigned new_len = curbuf->second->length() - new_off;
      dout(6) << "bc: splice tail of last off: " << new_off << " len: " << new_len << endl;
      Bufferhead *bh = new Bufferhead(inode, bc);
      curbuf->second->splice(new_off, new_len, bh);
      bh->set_offset(offset + size);
      assert(bh->length() == new_len);
      assert(curbuf->second->length() == new_off);
    }
  }
}


map<off_t, Bufferhead*>::iterator Filecache::get_buf(off_t off)
{
  map<off_t, Bufferhead*>::iterator curbuf = buffer_map.lower_bound(off);
  if (curbuf == buffer_map.end() || curbuf->first > off) {
    if (curbuf == buffer_map.begin()) {
      return buffer_map.end();
    } else {
      curbuf--;
      if ((unsigned)curbuf->first + curbuf->second->length() > (unsigned)off) {
        return curbuf;
      } else {
        return buffer_map.end();
      }
    }
  } else {
    return curbuf;
  }
}

map<off_t, Bufferhead*>::iterator Filecache::overlap(off_t len, off_t off)
{
  // returns iterator to buffer overlapping specified extent or end() if no overlap exists
  dout(7) << "bc: overlap " << len << " " << off << endl;

  if (buffer_map.empty()) return buffer_map.end();

  // find first buffer with offset >= off
  map<off_t, Bufferhead*>::iterator it = buffer_map.lower_bound(off);

  // Found buffer with exact offset
  if (it != buffer_map.end() && it->first == off) {
    dout(6) << "bc: overlap -- found buffer with exact offset" << endl;
    return it;
  }

  // examine previous buffer (< off) first in case of two overlaps
  if (it != buffer_map.begin()) {
    it--;
    if ((unsigned)it->first + it->second->length() > (unsigned)off) {
      dout(6) << "bc: overlap -- found overlap with previous buffer" << endl;
      return it;
    } else {
      dout(6) << "bc: overlap -- no overlap with previous buffer" << endl;
      it++;
    }
  }

  // then examine current buffer (> off)
  if (it != buffer_map.end() && (unsigned)it->first < (unsigned)off + len) {
    dout(6) << "bc: overlap -- overlap found" << endl;
    return it;
  } 

  // give up
  dout(6) << "bc: overlap -- no overlap found" << endl;
  return buffer_map.end();
}

map<off_t, Bufferhead*>::iterator 
Filecache::map_existing(off_t len, 
			off_t start_off,
			map<off_t, Bufferhead*>& hits, 
			map<off_t, Bufferhead*>& rx, 
			map<off_t, Bufferhead*>& tx, 
			map<off_t, off_t>& holes)
{
  dout(7) << "bc: map_existing len: " << len << " off: " << start_off << endl;
  off_t need_off = start_off;
  off_t actual_off = start_off;
  map<off_t, Bufferhead*>::iterator existing, rvalue = overlap(len, start_off);
  for (existing = rvalue;
       existing != buffer_map.end() && (unsigned)existing->first < (unsigned)start_off + len;
       existing++) {
    dout(7) << "bc: map: found overlap at offset " << actual_off << endl;
    actual_off = existing->first;
    Bufferhead *bh = existing->second;

    if (actual_off > need_off) {
      assert(buffer_map.count(need_off) == 0);
      holes[need_off] = actual_off - need_off;
      dout(6) << "bc: map: hole " << need_off << " " << holes[need_off] << endl;
      need_off = actual_off;
    } 

    // raise if this buffer overlaps with previous buffer
    assert(existing == rvalue || actual_off == need_off);

    // sort buffer into maps
    if (bh->state == BUFHD_STATE_RX) {
      rx[actual_off] = bh;
      dout(6) << "bc: map: rx " << actual_off << " " << rx[actual_off]->length() << endl;
    } else if (bh->state == BUFHD_STATE_TX) {
      tx[actual_off] = bh;
      dout(6) << "bc: map: tx " << actual_off << " " << tx[actual_off]->length() << endl;
    } else if (bh->is_hole()) {
      holes[actual_off] = bh->length();
      dout(6) << "bc: map: hit is hole " << actual_off << " " << holes[actual_off] << endl;
    } else if (bh->state == BUFHD_STATE_CLEAN || bh->state == BUFHD_STATE_DIRTY) {
      hits[actual_off] = bh;
      dout(6) << "bc: map: hit " << actual_off << " " << hits[actual_off]->length() << endl;
    } else {
      dout(1) << "map_existing: Unknown state!" << endl;
      assert(0);
    }
    need_off = actual_off + bh->length();
    assert(bh->length() > 0);
  }

  // no buffers or no buffers at tail
  if ((unsigned)need_off < (unsigned)start_off + len) {
    holes[need_off] = start_off + len - need_off;
    dout(6) << "bc: map: last hole " << need_off << " " << holes[need_off] << endl;
    assert(buffer_map.count(need_off) == 0);
  }
  return rvalue;
}

off_t 
Filecache::consolidation_opp(time_t max_dirty_since, off_t clean_goal, 
			     off_t offset, list<off_t>& offlist)
{
  dout(6) << "bc: consolidation_opp max_dirty_since: " << max_dirty_since << " clean_goal: " << clean_goal << " offset: " << offset << endl;
  off_t length = 0;
  map<off_t, Bufferhead*>::iterator cur, orig = buffer_map.find(offset);
  assert(orig != buffer_map.end());
  length += orig->second->length();
  offlist.push_back(offset);

  // search left
  cur = orig;
  off_t need_off = offset;
  while (cur != buffer_map.begin()) {
    cur--;
    if (cur->second->state != BUFHD_STATE_DIRTY  ||
        (unsigned)cur->first + cur->second->length() != (unsigned)need_off ||
        (cur->second->dirty_since > max_dirty_since &&
	 length >= clean_goal)) break;
    offlist.push_back(cur->first);
    length += cur->second->length();
    need_off = cur->first;
    cur->second->visited = true;
  }

  // search right
  cur = orig;
  need_off = offset + cur->second->length();
  cur++;
  while(cur != buffer_map.end()) {
    if (cur->second->state != BUFHD_STATE_DIRTY  ||
        cur->first != need_off ||
        (cur->second->dirty_since > max_dirty_since &&
	 length >= clean_goal)) break;
    offlist.push_back(cur->first);
    length += cur->second->length();
    need_off = cur->first + cur->second->length();
    cur->second->visited = true;
    cur++;
  }
  dout(6) << "length: " << length << " offlist size: " << offlist.size() << endl;
  return length;
}

void Filecache::get_dirty(set<Bufferhead*>& to_flush)
{
  dout(6) << "bc: fc.get_dirty" << endl;
  map<Inode*, map<off_t, list<off_t> > > consolidation_map;
  for (set<Bufferhead*>::iterator it = dirty_buffers.begin();
       it != dirty_buffers.end();
       it++) {
    if (!(*it)->visited) {
      (*it)->visited = true;
      list<off_t> offlist;
      consolidation_opp( time(NULL), bc->get_dirty_size(), (*it)->offset, offlist);
      if (offlist.size() > 1) {
        offlist.sort();
	off_t start_off = offlist.front();
	consolidation_map[inode][start_off] = offlist;
	to_flush.insert(buffer_map[start_off]);
      } else {
        to_flush.insert(*it);
      }
    }

    // consolidate
    bc->consolidate(consolidation_map);
    dout(6) << "bc: fc.get_dirty to_flush.size(): " << to_flush.size() << endl;
  }
}

#if 0  
void Filecache::simplify()
{
  dout(7) << "bc: simplify" << endl;
  list<Bufferhead*> removed;
  map<off_t, Bufferhead*>::iterator start, next;
  start = buffer_map.begin();
  next = buffer_map.begin();
  int count = 0;
  while (start != buffer_map.end()) {
    next++;
    while (next != buffer_map.end() &&
           start->second->state != BUFHD_STATE_RX &&
           start->second->state != BUFHD_STATE_TX &&
	   start->second->state == next->second->state &&
	   start->second->offset + start->second->bl.length() == next->second->offset &&
	   next->second->read_waiters.empty() &&
	   next->second->write_waiters.empty()) {
      dout(10) << "bc: simplify start: " << start->first << " next: " << next->first << endl;
      Bufferhead *bh = next->second;
      start->second->claim_append(bh);
      if (bh->state == BUFHD_STATE_DIRTY) {
        bh->dirtybuffers_erase(); 
	bh->state = BUFHD_STATE_CLEAN; 
      }
      removed.push_back(bh);
      count++;
      next++;
    }
    if (next != buffer_map.end()) {
      dout(10) << "bc: simplify failed, start state: " << start->second->state << " next state: " << next->second->state << endl;
      dout(10) << "bc: simplify failed, start offset + len " << start->second->offset + start->second->bl.length() << " next offset: " << next->second->offset << endl;
      dout(10) << "bc: simplify failed, " << next->second->read_waiters.size() << " read waiters" << endl;
      dout(10) << "bc: simplify failed, " << next->second->write_waiters.size() << " write waiters" << endl;
    }
    start = next;
  }
  dout(7) << "bc: simplified " << count << " buffers" << endl;
  for (list<Bufferhead*>::iterator it = removed.begin();
       it != removed.end();
       it++) {
    buffer_map.erase((*it)->offset);
    delete *it;
  }
  assert(!buffer_map.empty());
}
#endif

int Filecache::copy_out(off_t size, off_t offset, char *dst) 
{
  dout(7) << "bc: copy_out size: " << size << " offset: " << offset << endl;
  assert(offset >= 0);
  //assert(offset + size <= length()); doesn't hold after trim_bcache
  int rvalue = size;
  
  map<off_t, Bufferhead*>::iterator curbuf = buffer_map.lower_bound(offset);
  if (curbuf == buffer_map.end() || curbuf->first > offset) {
    if (curbuf == buffer_map.begin()) {
      return -1;
    } else {
      curbuf--;
    }
  }
  offset -= curbuf->first;
  dout(6) << "bc: copy_out: curbuf offset: " << curbuf->first << " offset: " << offset << endl;
  assert(offset >= 0);
  
  while (size > 0) {
    Bufferhead *bh = curbuf->second;
    if (offset + size <= bh->length()) {
      dout(6) << "bc: copy_out bh len: " << bh->length() << " size: " << size << endl;
      dout(10) << "bc: want to copy off: " << offset << " size: " << size << endl;
      bh->bl.copy(offset, size, dst);
      size = 0;
      break;
    }
    
    int howmuch = bh->length() - offset;
    dout(6) << "bc: copy_out bh len: " << bh->length() << " size: " << size << endl;
    dout(10) << "bc: want to copy off: " << offset << " size: " << howmuch << endl;
    bh->bl.copy(offset, howmuch, dst);
    
    dst += howmuch;
    size -= howmuch;
    offset = 0;
    curbuf++;
    if (curbuf == buffer_map.end()) {
      dout(5) << "bc: copy_out size: " << size << endl;
      assert(curbuf != buffer_map.end());
    }
  }
  return rvalue - size;
}

// -- Buffercache methods

void Buffercache::dirty(Inode *inode, off_t size, off_t offset, const char *src) 
{
  dout(6) << "bc: dirty ino: " << hex << inode->ino() << dec << " size: " << size << " offset: " << offset << endl;
  assert(bcache_map.count(inode->ino())); // filecache has to be already allocated!!
  Filecache *fc = get_fc(inode);
  assert(offset >= 0);
  
  map<off_t, Bufferhead*>::iterator curbuf = fc->get_buf(offset);
  assert(curbuf != fc->buffer_map.end());

  if (curbuf->second->state == BUFHD_STATE_CLEAN) {
    // leave unused buffer space clean
    fc->splice(offset, size);
    curbuf = fc->get_buf(offset);
  }
  offset -= curbuf->first;

  while (size > 0) {
    Bufferhead *bh = curbuf->second;
    if (offset + size <= bh->length()) {
      bh->bl.copy_in(offset, size, src); // last bit
      bh->dirty();
      break;
    }
    
    int howmuch = bh->length() - offset;
    bh->bl.copy_in(offset, howmuch, src);
    bh->dirty();    
    src += howmuch;
    size -= howmuch;
    offset = 0;
    curbuf++;
    assert(curbuf != fc->buffer_map.end());
  }
}

off_t Buffercache::touch_continuous(map<off_t, Bufferhead*>& hits, off_t size, off_t offset)
{
  dout(7) << "bc: touch_continuous size: " << size << " offset: " << offset << endl;
  if (hits.empty()) return 0;

  off_t next_off = offset;
  if (hits.begin()->first > offset ||
      (unsigned)hits.begin()->first + hits.begin()->second->length() <= (unsigned)offset) {
    return 0;
  }
  for (map<off_t, Bufferhead*>::iterator curbuf = hits.begin(); 
       curbuf != hits.end();
       curbuf++) {
    if (curbuf == hits.begin()) {
      next_off = curbuf->first;
    } else if (curbuf->first != next_off) {
      break;
    }
    lru.lru_touch(curbuf->second);
    next_off += curbuf->second->length();
  }
  return (next_off - offset) >= size ? size : (next_off - offset);
}

void Buffercache::map_or_alloc(Inode *inode, off_t size, off_t offset, 
                               map<off_t, Bufferhead*>& buffers, 
                               map<off_t, Bufferhead*>& rx,
			       map<off_t, Bufferhead*>& tx)
{
  dout(7) << "bc: map_or_alloc len: " << size << " off: " << offset << endl;
  Filecache *fc = get_fc(inode);
  map<off_t, off_t> holes;
  holes.clear();
  fc->map_existing(size, offset, buffers, rx, tx, holes);
  // stuff buffers into holes
  for (map<off_t, off_t>::iterator hole = holes.begin();
       hole != holes.end();
       hole++) {
    Bufferhead *bh;
    if (fc->buffer_map.count(hole->first)) {
      dout(10) << "bc: use hole bh " << hole->first << " " << hole->second << endl;
      bh = fc->buffer_map[hole->first];
      assert(bh->is_hole());
      assert(hole->second <= bh->length());
    } else {
      dout(10) << "bc: allocate hole " << hole->first << " " << hole->second << endl;
      assert(buffers.count(hole->first) == 0);
      bh = new Bufferhead(inode, hole->first, this);
    }
    buffers[hole->first] = bh;
    bh->alloc_buffers(hole->second);
  }
}

void Buffercache::consolidate(map<Inode*, map<off_t, list<off_t> > > cons_map)
{
  dout(6) << "bc: consolidate" << endl;
  int deleted = 0;
  for (map<Inode*, map<off_t, list<off_t> > >::iterator it_ino = cons_map.begin();
       it_ino != cons_map.end();
       it_ino++) {
    Filecache *fc = get_fc(it_ino->first);
    for (map<off_t, list<off_t> >::iterator it_off = it_ino->second.begin();
         it_off != it_ino->second.end();
	 it_off++) {
      Bufferhead *first_bh = fc->buffer_map[it_off->first];
      for (list<off_t>::iterator it_list = it_off->second.begin();
           it_list != it_off->second.end();
	   it_list++) {
	Bufferhead *bh = fc->buffer_map[*it_list];
	first_bh->claim_append(bh);
	bh->dirtybuffers_erase();
	bh->state = BUFHD_STATE_CLEAN;
	fc->buffer_map.erase(bh->offset);
	assert(!fc->buffer_map.empty());
	assert(!dirty_buffers->exist(bh));
	delete bh;
	deleted++;
      }
    }
  }
  dout(6) << "bc: consolidate: deleted: " << deleted << endl;
}

void Buffercache::get_reclaimable(off_t min_size, list<Bufferhead*>& reclaimed)
{
  while (min_size > 0) {
    if (Bufferhead *bh = (Bufferhead*)lru.lru_expire()) {
      reclaimed.push_back(bh);
      min_size -= bh->length();
    } else {
      break;
    }
  }
}


off_t Buffercache::reclaim(off_t min_size)
{
  dout(7) << "bc: reclaim min_size: " << min_size << endl;
  off_t freed_size = 0;
  while (freed_size < min_size) {
    Bufferhead *bh = (Bufferhead*)lru.lru_expire();
    if (!bh) {
      dout(6) << "bc: nothing more to reclaim -- freed_size: " << freed_size << endl;
      break; // nothing more to reclaim
    } else {
      dout(6) << "bc: reclaim: offset: " << bh->offset << " len: " << bh->length() << endl;
      assert(bh->state == BUFHD_STATE_CLEAN);
      assert(!bh->bc->dirty_buffers->exist(bh));
      freed_size += bh->length();

      decrease_size(bh->length());

      dout(6) << "bc: reclaim: clean_size: " << get_clean_size() << " dirty_size: " << get_dirty_size() << " rx_size: " << get_rx_size() << " tx_size: " << get_tx_size() << " age: " << dirty_buffers->get_age() << endl;
      assert(clean_size >= 0);
      Filecache *fc = bh->fc;
      inodeno_t ino = bh->inode->ino();
      fc->buffer_map.erase(bh->offset);
      assert(!dirty_buffers->exist(bh));
      delete bh;
      if (fc->buffer_map.empty()) {
        bcache_map.erase(ino);
		dout(6) << "bc: delete fc of ino: " << hex << ino << dec << endl;
	delete fc;
      }
    }
  }
  return freed_size;
}

