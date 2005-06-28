#include "Buffercache.h"

// -- Filecache methods

map<off_t, Bufferhead*>::iterator Filecache::overlap(size_t len, off_t off)
{
  // returns iterator to buffer overlapping specified extent or end() if no overlap exists
  map<off_t, Bufferhead*>::iterator it = buffer_map.lower_bound(off);
  if (it == buffer_map.end() || (*it)->first < off + len) {
    return it;
  } else if (it == buffer_map.begin()) {
    return buffer_map.end();
  } else {
    --it;
    if ((*it)->first + (*it)->second->bl.length() > off) {
      return it;
    } else {
      return buffer_map.end();
    }
  }
}

void Filecache::map_existing(size_t len, 
                             off_t start_off,
                             map<off_t, Bufferhead>& hits, 
                             map<off_t, Bufferhead>& inflight, 
                             map<off_t, size_t>& holes)
{
  off_t need_off = start_off;
  for (map<off_t, Bufferhead*>::iterator existing = overlap(len, start_off);
       existing != buffer_map.end() && (*existing)->first < start_off + len;
       existing++) {
    off_t actual_off = (*existing)->first;
    Bufferhead *bh = (*existing)->second;
    lru.lru_touch(bh);
    if (actual_off > need_off) {
      holes[need_off] = (size_t) (actual_off - need_off);
    }
    if (bh->state == BUFHD_STATE_INFLIGHT) {
      inflight[actual_off] = bh;
    } else {
      hits[actual_off] = bh;
    }
    need_off = actual_off + bh->length();
  }
  if (next_off < off + len) {
    holes[next_off] = (size_t) (off + len - next_off);
  }
}

void Filecache::simplify(list<Bufferhead*>& removed)
{
  map<off_t, Bufferhead*>::iterator start, next;
  start = buffer_map.begin();
  while (start != buffer_map.end()) {
	next = start + 1;
	while (next != buffer_map.end()) {
	  if ((*start)->second->state != BUFHD_STATE_INFLIGHT &&
		  (*start)->second->state == (*next)->second->state &&
		  (*start)->second->offset + (*start)->second->len == (*next)->second->offset) {
		(*start)->second->claim_append((*next)->second);
		buffer_map.erase((*next)->first); (*next)->second->put();
		removed.push_back((*next)->second);
		next++;
	  } else {
		break;
	  }
	}
	start = next;
  }
}

void Filecache::copy_out(size_t size, off_t offset, char *dst) 
{
  assert(offset >= 0);
  assert(offset + size <= length());
  
  map<off_t, Bufferhead*>::iterator curbuf = overlap(size, offset);
  offset -= (*curbuf)->first;
  assert(offset >= 0);
  
  while (size > 0) {
    Bufferhead *bh = (*curbuf)->second;
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

void Buffercache::insert(Bufferhead *bh) {
  Filecache *fc;
  if (bcache_map.count(bh->ino)) {
    fc = bcache_map[bh->ino];
  } else {
    fc = new Filecache();
    bcache_map[bh->ino] = fc;
  }
  if (fc->buffermap.count(bh->offset)) assert(0); // fail loudly if offset already exists!
  fc->buffer_map[bh->offset] = bh; bh->get();
  lru.lru_insert_top(bh); bh->get();
  clean_size += bh->len;
}

void Buffercache::dirty(inodeno_t ino, size_t size, off_t offset, char *src) 
{
  Filecache *fc = bcache_map[ino];
  assert(offset >= 0);
  assert(offset + size <= fc->length());
  
  map<off_t, Bufferhead*>::iterator curbuf = fc->overlap(size, offset);
  offset -= (*curbuf)->first;
  assert(offset >= 0);
  
  while (size > 0) {
    Bufferhead *bh = (*curbuf)->second;
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
    assert(curbuf != buffer_map.end());
  }
}


size_t Buffercache::touch_continuous(map<off_t, Bufferhead*>& hits, size_t size, off_t offset)
{
  off_t next_off;
  for (map<off_t, Bufferhead*>::iterator curbuf = hits->begin(); 
       curbuf != hits->end(); 
       curbuf++) {
    if (curbuf != hits.begin() &&
        (*curbuf)->first != next_off) {
      break;
    }
    lru.lru_touch((*curbuf)->second);
    next_off += (*curbuf)->second->len;
  }
  return (next_off - offset) >= size ? size : (next_off - offset);
}

void Buffercache::simplify(inodeno_t ino)
{
  Filecache *fc = bcache_map[ino];
  list<Bufferhead*> removed;
  fc->simplify(&removed);
  for (list<Bufferhead*>::iterator it = removed.begin();
	   it != removed.end();
	   it++) {
	lru.lru_remove(*it); (*it)->put();
    if (dirty_buffers.count(*it)) {
      dirty_buffers.erase(*it);
      (*it)->put();
    }
    if (bcache_map[ino]->dirty_buffers.count(*it)) {
      bcache_map[ino]->dirty_buffers.erase(*it)
      (*it)->put();
    }
  }
}

Bufferhead *Buffercache::alloc_buffers(ino, offset, size)
{
  Bufferhead *bh = new Bufferhead(ino, offset, size, this, BUFHD_STATE_CLEAN);
  clean_size += size;
  while (size > 0) {
    if (size <= BUFC_ALLOC_MAXSIZE) {
	  bh->bl.push_back(new buffer(size));
	  break;
	}
	bh->bl.push_back(new buffer(BUFC_ALLOC_MAXSIZE));
	size -= BUFC_ALLOC_MAXSIZE;
  }
  return bh;
}


void Buffercache::map_or_alloc(inodeno_t ino, size_t len, off_t off, 
                               map<off_t, Bufferhead*>& buffers, 
                               map<off_t, Bufferhead*>& inflight)
{
  Filecache *fc = bcache_map[ino];
  map<off_t, size_t> holes;
  fc->map_existing(len, off, buffers, inflight, &holes);
  // stuff buffers into holes
  for (map<off_t, size_t>::iterator hole = holes.begin();
       hole != holes.end();
       hole++) {
	assert(buffers->count((*hole)->first) == 0);
    Bufferhead *bh = alloc_buffers(ino, (*hole)->first, (*hole)->second);
    buffers[(*hole)->first] = bh;
    insert(bh); //FIXME: for prefetching we will need more flexible allocation
  }
  // split buffers
  // FIXME: not implemented yet
}

void Buffercache::free_buffers(Bufferhead *bh) 
{
  assert(bh->state == BUFH_STATE_CLEAN);
  assert(bh->lru_is_expirable());
  clean_size -= bh->len;
  lru.lru_remove(bh); bh->put();
  assert(bh->ref == 1); // next put is going to delete it
  bcache_map[bh->ino]->buffer_map.erase(bh->offset); bh->put();
}

void Buffercache::release_file(inodeno_t ino) 
{
  Filecache *fc = bcache_map[ino];
  for (map<off_t, Bufferhead*>::iterator it = fc->begin();
       it != fc->end();
       it++) {
    free_buffers((*it)->second);    
  }
  bcache_map.erase(ino);
  delete fc;
}

size_t Buffercache::reclaim(size_t min_size, set<Bufferhead)
{
  size_t freed_size = 0;
  while (freed_size >= target_size) {
    Bufferhead *bh = (Bufferhead*)lru.lru_expire();
    if (bh) {
      assert(bh->state == BUFHD_STATE_CLEAN);
      freed_size += bh->bl.length();
      free_buffers(bh);
    } else {
      break; // nothing more that can be expired!
    }
  }
  return freed_size;
}

