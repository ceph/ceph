#include "Buffercache.h"

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "client" << "." << pthread_self() << " "

// -- Bufferhead methods

Bufferhead::Bufferhead(inodeno_t ino, off_t off, size_t len, Buffercache *bc) : 
  ref(0) {
  dout(10) << "bc: new bufferhead ino: " << ino << " len: " << len << " offset: " << off << endl;
  this->ino = ino;
  this->offset = off;
  this->len = len;
  this->state = BUFHD_STATE_CLEAN;
  this->bc = bc;
  if (bc->bcache_map.count(ino)) {
    fc = bc->bcache_map[ino];
  } else {
    fc = new Filecache(bc);
    bc->bcache_map[ino] = fc;
  }
  bc->lru.lru_insert_top(this); //FIXME: parameterize whether top or mid
  bc->clean_size += len;
  dout(10) << "bc: clean_size: " << bc->clean_size << " dirty_size: " << bc->dirty_size << " flushing_size: " << bc->flushing_size << endl;
  assert(!fc->buffer_map.count(offset)); // fail loudly if offset already exists!
  fc->buffer_map[offset] = this; 
  last_written = time(NULL);
  // buffers are allocated later
}

Bufferhead::~Bufferhead()
{
  dout(10) << "bc: destroying bufferhead ino: " << ino << " len: " << len << " offset: " << offset << endl;
  assert(state == BUFHD_STATE_CLEAN);
  assert(ref == 0);
  assert(lru_is_expireable());
  assert(read_waiters.empty());
  assert(write_waiters.empty());
  bc->lru.lru_remove(this);   
  fc->buffer_map.erase(offset); 
  if (fc->buffer_map.empty()) {
    bc->bcache_map.erase(ino);
    delete fc;  
  }
}

void Bufferhead::alloc_buffers()
{
  dout(10) << "bc: allocating buffers len: " << len << endl;
  size_t size = this->len;
  while (size > 0) {
    if (size <= BUFC_ALLOC_MAXSIZE) {
          size_t k = BUFC_ALLOC_MINSIZE;
          size_t asize = size - size % k + (size % k > 0) * k;
	  buffer *b = new buffer(asize);
	  b->set_length(size);
	  bl.push_back(b);
          dout(10) << "bc: new buffer(" << asize << "), total: " << bl.length() << endl;
	  break;
	}
        buffer *b = new buffer(BUFC_ALLOC_MAXSIZE);
	b->set_length(BUFC_ALLOC_MAXSIZE);
	bl.push_back(b);
        dout(10) << "bc: new buffer(" << BUFC_ALLOC_MAXSIZE << "), total: " << bl.length() << endl;
	size -= BUFC_ALLOC_MAXSIZE;
  }
  dout(7) << "bc: allocated " << bl.buffers().size() << " buffers (" << bl.length() << " bytes) " << endl;
}

void Bufferhead::dirty() {
  if (state == BUFHD_STATE_CLEAN) {
    dout(10) << "bc: dirtying clean buffer of len " << len << endl;
    state = BUFHD_STATE_DIRTY;
    bc->dirty_size += len;
    bc->clean_size -= len;
    dout(10) << "bc: clean_size: " << bc->clean_size << " dirty_size: " << bc->dirty_size << " flushing_size: " << bc->flushing_size << endl;
    assert(!bc->dirty_buffers.count(this));
    bc->dirty_buffers.insert(this); 
    get();
    assert(!fc->dirty_buffers.count(this));
    fc->dirty_buffers.insert(this); 
    get();
  } else {
    dout(10) << "bc: dirtying dirty buffer of len " << len << endl;
  }
}

void Bufferhead::clean() {
  dout(10) << "bc: cleaning buffer of len " << len << " in state " << state << endl;
  state = BUFHD_STATE_CLEAN;
  assert(bc->dirty_buffers.count(this));
  bc->dirty_buffers.erase(this);
  put();
  assert(fc->dirty_buffers.count(this));
  fc->dirty_buffers.erase(this);
  put();
}

void Bufferhead::flush_start() {
  dout(10) << "bc: flush_start" << endl;
  assert(state == BUFHD_STATE_DIRTY);
  state = BUFHD_STATE_INFLIGHT;
  bc->dirty_size -= len;
  bc->flushing_size += len;
  dout(10) << "bc: clean_size: " << bc->clean_size << " dirty_size: " << bc->dirty_size << " flushing_size: " << bc->flushing_size << endl;
}

void Bufferhead::flush_finish() {
  dout(10) << "flush_finish" << endl;
  assert(state == BUFHD_STATE_INFLIGHT);
  last_written = time(NULL);
  clean();
  bc->flushing_size -= len;
  bc->clean_size += len;
  dout(10) << "bc: clean_size: " << bc->clean_size << " dirty_size: " << bc->dirty_size << " flushing_size: " << bc->flushing_size << endl;
  wakeup_write_waiters(); // readers never wait on flushes
}

void Bufferhead::claim_append(Bufferhead *other) 
{
  dout(10) << "bc: claim_append old bl size: " << bl.buffers().size() << " len: " << len << endl;
  bl.claim_append(other->bl);
  len += other->len;
  dout(10) << "bc: claim_append new bl size: " << bl.buffers().size() << " len: " << len << endl;
  if (other->last_written < last_written) last_written = other->last_written;
  other->bl.clear();
  other->len = 0;
}

// -- Filecache methods

map<off_t, Bufferhead*>::iterator Filecache::overlap(size_t len, off_t off)
{
  // returns iterator to buffer overlapping specified extent or end() if no overlap exists
  dout(7) << "bc: overlap " << len << " " << off << endl;
  map<off_t, Bufferhead*>::iterator it = buffer_map.lower_bound(off);
  if (it == buffer_map.end() || it->first < off + len) {
    return it;
  } else if (it == buffer_map.begin()) {
    return buffer_map.end();
  } else {
    it--;
    if (it->first + it->second->bl.length() > off) {
      return it;
    } else {
      return buffer_map.end();
    }
  }
}

void Filecache::map_existing(size_t len, 
                             off_t start_off,
                             map<off_t, Bufferhead*>& hits, 
                             map<off_t, Bufferhead*>& inflight, 
                             map<off_t, size_t>& holes)
{
  dout(7) << "bc: map_existing len: " << len << " off: " << start_off << endl;
  off_t need_off = start_off;
  off_t actual_off = start_off;
  for (map<off_t, Bufferhead*>::iterator existing = overlap(len, start_off);
       existing != buffer_map.end() && existing->first < start_off + len;
       existing++) {
    actual_off = existing->first;
    dout(7) << "bc: map: found overlap at offset " << actual_off << endl;
    Bufferhead *bh = existing->second;
    bc->lru.lru_touch(bh);
    if (actual_off > need_off) {
      holes[need_off] = (size_t) (actual_off - need_off);
      dout(10) << "bc: map: hole " << need_off << " " << holes[need_off] << endl;
    }
    if (bh->state == BUFHD_STATE_INFLIGHT) {
      inflight[actual_off] = bh;
      dout(10) << "bc: map: inflight " << actual_off << " " << inflight[actual_off] << endl;
    } else {
      hits[actual_off] = bh;
      dout(10) << "bc: map: hits " << actual_off << " " << hits[actual_off] << endl;
    }
    need_off = actual_off + bh->len;
  }
  if (need_off < actual_off + len) {
    holes[need_off] = (size_t) (actual_off + len - need_off);
    dout(10) << "bc: map: hole " << need_off << " " << holes[need_off] << endl;
  }
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
    dout(10) << "bc: simplify next: " << next->first << " " << next->second->len << endl;
    while (next != buffer_map.end() &&
           start->second->state != BUFHD_STATE_INFLIGHT &&
	   start->second->state == next->second->state &&
	   start->second->offset + start->second->len == next->second->offset &&
	   next->second->read_waiters.empty() &&
	   next->second->write_waiters.empty()) {
      dout(10) << "bc: simplify start: " << start->first << " next: " << next->first << endl;
      Bufferhead *bh = next->second;
      start->second->claim_append(bh);
      if (bh->state == BUFHD_STATE_DIRTY) bh->clean(); 
      removed.push_back(bh);
      count++;
      next++;
    }
    if (next != buffer_map.end()) {
      dout(10) << "bc: simplify failed, start state: " << start->second->state << " next state: " << next->second->state << endl;
      dout(10) << "bc: simplify failed, start offset + len " << start->second->offset + start->second->len << " next offset: " << next->second->offset << endl;
      dout(10) << "bc: simplify failed, " << next->second->read_waiters.size() << " read waiters" << endl;
      dout(10) << "bc: simplify failed, " << next->second->write_waiters.size() << " write waiters" << endl;
    }
    start = next;
  }
  dout(7) << "bc: simplified " << count << " buffers" << endl;
  for (list<Bufferhead*>::iterator it = removed.begin();
       it != removed.end();
       it++) {
    delete *it;
  }
}

void Filecache::copy_out(size_t size, off_t offset, char *dst) 
{
  dout(7) << "bc: copy_out size: " << size << " offset: " << offset << endl;
  assert(offset >= 0);
  assert(offset + size <= length());
  
  map<off_t, Bufferhead*>::iterator curbuf = overlap(size, offset);
  offset -= curbuf->first;
  assert(offset >= 0);
  
  while (size > 0) {
    Bufferhead *bh = curbuf->second;
    if (offset + size <= bh->len) {
      bh->bl.copy(offset, size, dst);
      break;
    }
    
    int howmuch = bh->len - offset;
    bh->bl.copy(offset, howmuch, dst);
    
    dst += howmuch;
    size -= howmuch;
    offset = 0;
    curbuf++;
    assert(curbuf != buffer_map.end());
  }
}

// -- Buffercache methods

void Buffercache::dirty(inodeno_t ino, size_t size, off_t offset, const char *src) 
{
  dout(7) << "bc: dirty ino: " << ino << " size: " << size << " offset: " << offset << endl;
  assert(bcache_map.count(ino));
  Filecache *fc = bcache_map[ino];
  assert(offset >= 0);
  assert(offset + size <= fc->length());
  
  map<off_t, Bufferhead*>::iterator curbuf = fc->overlap(size, offset);
  offset -= curbuf->first;
  assert(offset >= 0);
  
  while (size > 0) {
    Bufferhead *bh = curbuf->second;
    if (offset + size <= bh->len) {
      bh->bl.copy_in(offset, size, src); // last bit
      bh->dirty();
      break;
    }
    
    int howmuch = bh->len - offset;
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
  off_t next_off;
  for (map<off_t, Bufferhead*>::iterator curbuf = hits.begin(); 
       curbuf != hits.end(); 
       curbuf++) {
    if (curbuf != hits.begin() &&
        curbuf->first != next_off) {
      break;
    }
    lru.lru_touch(curbuf->second);
    next_off += curbuf->second->len;
  }
  return (int)(next_off - offset) >= size ? size : (next_off - offset);
}

void Buffercache::map_or_alloc(inodeno_t ino, size_t size, off_t offset, 
                               map<off_t, Bufferhead*>& buffers, 
                               map<off_t, Bufferhead*>& inflight)
{
  dout(7) << "bc: map_or_alloc len: " << size << " off: " << offset << endl;
  if (!bcache_map.count(ino)) bcache_map[ino] = new Filecache(this);
  Filecache *fc = bcache_map[ino];
  map<off_t, size_t> holes;
  fc->map_existing(size, offset, buffers, inflight, holes);
  // stuff buffers into holes
  for (map<off_t, size_t>::iterator hole = holes.begin();
       hole != holes.end();
       hole++) {
    dout(10) << "bc: allocate hole " << hole->first << " " << hole->second << endl;
    assert(buffers.count(hole->first) == 0);
    Bufferhead *bh = new Bufferhead(ino, hole->first, hole->second, this);
    buffers[hole->first] = bh;
    bh->alloc_buffers();
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
    clean_size -= it->second->len;
    dout(10) << "bc: clean_size: " << clean_size << " dirty_size: " << dirty_size << " flushing_size: " << flushing_size << endl;
    delete it->second;    
  }
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
      clean_size -= bh->len;
      dout(10) << "bc: clean_size: " << clean_size << " dirty_size: " << dirty_size << " flushing_size: " << flushing_size << endl;
      delete bh;
    }
  }
  return freed_size;
}

