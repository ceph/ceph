#include "Buffercache.h"

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "client" << "." << pthread_self() << " "

// -- Bufferhead methods

Bufferhead::Bufferhead(inodeno_t ino, off_t off, Buffercache *bc) : 
  ref(0) {
  dout(10) << "bc: new bufferhead ino: " << ino << " offset: " << off << endl;
  this->ino = ino;
  offset = off;
  state = BUFHD_STATE_CLEAN;
  this->bc = bc;
  fc = bc->get_fc(ino);
  bc->lru.lru_insert_top(this); //FIXME: parameterize whether top or mid
  assert(!fc->buffer_map.count(offset)); // fail loudly if offset already exists!
  fc->buffer_map[offset] = this; 
  dirty_since = 0; // meaningless when clean or inflight
  // buffers are allocated later
}

Bufferhead::~Bufferhead()
{
  dout(10) << "bc: destroying bufferhead ino: " << ino << " size: " << bl.length() << " offset: " << offset << endl;
  assert(state == BUFHD_STATE_CLEAN);
  assert(ref == 0);
  assert(lru_is_expireable());
  assert(read_waiters.empty());
  assert(write_waiters.empty());
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

void Bufferhead::alloc_buffers(size_t size)
{
  dout(10) << "bc: allocating buffers size: " << size << endl;
  while (size > 0) {
    if (size <= g_conf.client_bcache_alloc_maxsize) {
          size_t k = g_conf.client_bcache_alloc_minsize;
          size_t asize = size - size % k + (size % k > 0) * k;
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
  dout(7) << "bc: allocated " << bl.buffers().size() << " buffers (" << bl.length() << " bytes) " << endl;
}

void Bufferhead::dirty() {
  if (state == BUFHD_STATE_CLEAN) {
    dout(10) << "bc: dirtying clean buffer size: " << bl.length() << endl;
    state = BUFHD_STATE_DIRTY;
    dirty_since = time(NULL); // start clock for dirty buffer here
    bc->clean_to_dirty(bl.length());
    dout(10) << "bc: clean_size: " << bc->get_clean_size() << " dirty_size: " << bc->get_dirty_size() << " flushing_size: " << bc->get_flushing_size() << endl;
    assert(!bc->dirty_buffers.exist(this));
    bc->dirty_buffers.insert(this); 
    get();
    assert(!fc->dirty_buffers.exist(this));
    fc->dirty_buffers.insert(this);
    get();
  } else {
    dout(10) << "bc: dirtying dirty buffer size: " << bl.length() << endl;
  }
}

void Bufferhead::leave_dirtybuffers() {
  dout(10) << "bc: erase in dirtybuffers size: " << bl.length() << " in state " << state << endl;
  assert(bc->dirty_buffers.exist(this));
  bc->dirty_buffers.erase(this);
  put();
  assert(fc->dirty_buffers.exist(this));
  fc->dirty_buffers.erase(this);
  put();
}

void Bufferhead::flush_start() {
  dout(10) << "bc: flush_start" << endl;
  assert(state == BUFHD_STATE_DIRTY);
  state = BUFHD_STATE_INFLIGHT;
  leave_dirtybuffers();
  bc->dirty_to_flushing(bl.length());
  dout(10) << "bc: clean_size: " << bc->get_clean_size() << " dirty_size: " << bc->get_dirty_size() << " flushing_size: " << bc->get_flushing_size() << endl;
}

void Bufferhead::flush_finish() {
  dout(10) << "bc: flush_finish" << endl;
  assert(state == BUFHD_STATE_INFLIGHT);
  state = BUFHD_STATE_CLEAN;
  bc->flushing_to_clean(bl.length());
  dout(10) << "bc: clean_size: " << bc->get_clean_size() << " dirty_size: " << bc->get_dirty_size() << " flushing_size: " << bc->get_flushing_size() << endl;
  wakeup_write_waiters(); // readers never wait on flushes
}

void Bufferhead::claim_append(Bufferhead *other) 
{
  dout(10) << "bc: claim_append old bl size: " << bl.buffers().size() << " length " << bl.length() << endl;
  bl.claim_append(other->bl);
  dout(10) << "bc: claim_append new bl size: " << bl.buffers().size() << " length: " << bl.length() << endl;
  // keep older time stamp
  if (other->dirty_since < dirty_since) dirty_since = other->dirty_since;
  other->bl.clear();
}

// -- Dirtybuffers methods

void Dirtybuffers::get_expired(time_t ttl, size_t left_dirty, list<Bufferhead*>& to_flush) 
{
  time_t now = time(NULL);
  for (multimap<time_t, Bufferhead*>::iterator it = _dbufs.begin();
    it != _dbufs.end();
    it++) {
    if (ttl > now - it->second->dirty_since &&
        left_dirty <= it->second->bc->get_dirty_size()) break;
    to_flush.push_back(it->second);
    left_dirty -= it->second->bl.length();
  }
}
                                                                                  
// -- Filecache methods

map<off_t, Bufferhead*>::iterator Filecache::overlap(size_t len, off_t off)
{
  // returns iterator to buffer overlapping specified extent or end() if no overlap exists
  dout(7) << "bc: overlap " << len << " " << off << endl;
  map<off_t, Bufferhead*>::iterator it = buffer_map.lower_bound(off);
  if (it == buffer_map.end() || it->first < off + len) {
    dout(10) << "bc: overlap -- either no lower bound or overlap found" << endl;
    return it;
  } else if (it == buffer_map.begin()) {
    dout(10) << "bc: overlap -- extent is below where buffer_map begins" << endl;
    return buffer_map.end();
  } else {
    dout(10) << "bc: overlap -- examining previous buffer" << endl;
    it--;
    if (it->first + it->second->bl.length() > off) {
      dout(10) << "bc: overlap -- found overlap with previous buffer" << endl;
      return it;
    } else {
      dout(10) << "bc: overlap -- no overlap with previous buffer" << endl;
      return buffer_map.end();
    }
  }
}

map<off_t, Bufferhead*>::iterator 
Filecache::map_existing(size_t len, 
			off_t start_off,
			map<off_t, Bufferhead*>& hits, 
			map<off_t, Bufferhead*>& inflight, 
			map<off_t, size_t>& holes)
{
  dout(7) << "bc: map_existing len: " << len << " off: " << start_off << endl;
  off_t need_off = start_off;
  off_t actual_off = start_off;
  map<off_t, Bufferhead*>::iterator existing, rvalue = overlap(len, start_off);
  for (existing = rvalue;
       existing != buffer_map.end() && existing->first < start_off + len;
       existing++) {
    dout(7) << "bc: map: found overlap at offset " << actual_off << endl;
    actual_off = existing->first;
    Bufferhead *bh = existing->second;
    if (actual_off > need_off) {
      holes[need_off] = (size_t) (actual_off - need_off);
      dout(10) << "bc: map: hole " << need_off << " " << holes[need_off] << endl;
    }
    if (bh->state == BUFHD_STATE_INFLIGHT) {
      inflight[actual_off] = bh;
      dout(10) << "bc: map: inflight " << actual_off << " " << inflight[actual_off]->miss_len << endl;
    } else {
      hits[actual_off] = bh;
      dout(10) << "bc: map: hits " << actual_off << " " << hits[actual_off]->bl.length() << endl;
    }
    need_off = actual_off + bh->bl.length();
  }
  if (need_off < actual_off + len) {
    holes[need_off] = (size_t) (actual_off + len - need_off);
    dout(10) << "bc: map: hole " << need_off << " " << holes[need_off] << endl;
  }
  return rvalue;
}

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
           start->second->state != BUFHD_STATE_INFLIGHT &&
	   start->second->state == next->second->state &&
	   start->second->offset + start->second->bl.length() == next->second->offset &&
	   next->second->read_waiters.empty() &&
	   next->second->write_waiters.empty()) {
      dout(10) << "bc: simplify start: " << start->first << " next: " << next->first << endl;
      Bufferhead *bh = next->second;
      start->second->claim_append(bh);
      if (bh->state == BUFHD_STATE_DIRTY) {
        bh->leave_dirtybuffers(); 
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

int Filecache::copy_out(size_t size, off_t offset, char *dst) 
{
  dout(7) << "bc: copy_out size: " << size << " offset: " << offset << endl;
  assert(offset >= 0);
  assert(offset + size <= length());
  int rvalue = size;
  
  map<off_t, Bufferhead*>::iterator curbuf = overlap(size, offset);
  if (curbuf == buffer_map.end()) {
    return -1;
  }
  offset -= curbuf->first;
  if (offset < 0) dout(10) << "bc: copy_out: curbuf offset: " << curbuf->first << endl;
  assert(offset >= 0);
  
  while (size > 0) {
    Bufferhead *bh = curbuf->second;
    if (offset + size <= bh->bl.length()) {
      dout(10) << "bc: copy_out bh len: " << bh->bl.length() << endl;
      dout(10) << "bc: want to copy off: " << offset << " size: " << size << endl;
      bh->bl.copy(offset, size, dst);
      break;
    }
    
    int howmuch = bh->bl.length() - offset;
    dout(10) << "bc: copy_out bh len: " << bh->bl.length() << endl;
    dout(10) << "bc: want to copy off: " << offset << " size: " << howmuch << endl;
    bh->bl.copy(offset, howmuch, dst);
    
    dst += howmuch;
    size -= howmuch;
    offset = 0;
    curbuf++;
    assert(curbuf != buffer_map.end());
  }
  return rvalue;
}

// -- Buffercache methods

void Buffercache::dirty(inodeno_t ino, size_t size, off_t offset, const char *src) 
{
  dout(7) << "bc: dirty ino: " << ino << " size: " << size << " offset: " << offset << endl;
  assert(bcache_map.count(ino)); // filecache has to be already allocated!!
  Filecache *fc = get_fc(ino);
  assert(offset >= 0);
  assert(offset + size <= fc->length());
  
  map<off_t, Bufferhead*>::iterator curbuf = fc->overlap(size, offset);
  offset -= curbuf->first;
  assert(offset >= 0);
  
  while (size > 0) {
    Bufferhead *bh = curbuf->second;
    if (offset + size <= bh->bl.length()) {
      bh->bl.copy_in(offset, size, src); // last bit
      bh->dirty();
      break;
    }
    
    int howmuch = bh->bl.length() - offset;
    bh->bl.copy_in(offset, howmuch, src);
    bh->dirty();    
    src += howmuch;
    size -= howmuch;
    offset = 0;
    curbuf++;
    assert(curbuf != fc->buffer_map.end());
  }
}


int Buffercache::touch_continuous(map<off_t, Bufferhead*>& hits, size_t size, off_t offset)
{
  dout(7) << "bc: touch_continuous size: " << size << " offset: " << offset << endl;
  off_t next_off = offset;
  for (map<off_t, Bufferhead*>::iterator curbuf = hits.begin(); 
       curbuf != hits.end(); 
       curbuf++) {
    if (curbuf == hits.begin()) {
      next_off = curbuf->first;
    } else if (curbuf->first != next_off) {
      break;
    }
    lru.lru_touch(curbuf->second);
    next_off += curbuf->second->bl.length();
  }
  return (int)(next_off - offset) >= size ? size : (next_off - offset);
}

void Buffercache::map_or_alloc(inodeno_t ino, size_t size, off_t offset, 
                               map<off_t, Bufferhead*>& buffers, 
                               map<off_t, Bufferhead*>& inflight)
{
  dout(7) << "bc: map_or_alloc len: " << size << " off: " << offset << endl;
  Filecache *fc = get_fc(ino);
  map<off_t, size_t> holes;
  fc->map_existing(size, offset, buffers, inflight, holes);
  // stuff buffers into holes
  for (map<off_t, size_t>::iterator hole = holes.begin();
       hole != holes.end();
       hole++) {
    dout(10) << "bc: allocate hole " << hole->first << " " << hole->second << endl;
    assert(buffers.count(hole->first) == 0);
    Bufferhead *bh = new Bufferhead(ino, hole->first, this);
    buffers[hole->first] = bh;
    bh->alloc_buffers(hole->second);
  }
  // split buffers
  // FIXME: not implemented yet
}

void Buffercache::release_file(inodeno_t ino) 
{
  dout(7) << "bc: release_file ino: " << ino << endl;
  assert(bcache_map.count(ino));
  Filecache *fc = bcache_map[ino];
  for (map<off_t, Bufferhead*>::iterator it = fc->buffer_map.begin();
       it != fc->buffer_map.end();
       it++) {

    decrease_size(it->second->bl.length());

    dout(10) << "bc: clean_size: " << get_clean_size() << " dirty_size: " << get_dirty_size() << " flushing_size: " << get_flushing_size() << endl;
    assert(clean_size >= 0);
    delete it->second;    
  }
  fc->buffer_map.clear();
  bcache_map.erase(ino);
  delete fc;  
}

size_t Buffercache::reclaim(size_t min_size)
{
  dout(7) << "bc: reclaim min_size: " << min_size << endl;
  size_t freed_size = 0;
  while (freed_size >= min_size) {
    Bufferhead *bh = (Bufferhead*)lru.lru_expire();
    if (!bh) {
      break; // nothing more to reclaim
    } else {
      assert(bh->state == BUFHD_STATE_CLEAN);
      freed_size += bh->bl.length();

      decrease_size(bh->bl.length());

      dout(10) << "bc: clean_size: " << get_clean_size() << " dirty_size: " << get_dirty_size() << " flushing_size: " << get_flushing_size() << endl;
      assert(clean_size >= 0);
      bh->fc->buffer_map.erase(bh->offset);
      if (bh->fc->buffer_map.empty()) {
        bcache_map.erase(bh->ino);
	delete bh->fc;
      }
      delete bh;
    }
  }
  return freed_size;
}

