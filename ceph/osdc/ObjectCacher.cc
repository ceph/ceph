
#include "ObjectCacher.h"
#include "Objecter.h"

#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_client) cout << "ocacher." << pthread_self() << " "


/*** ObjectCacher::BufferHead ***/


/*** ObjectCacher::Object ***/

ObjectCacher::BufferHead *ObjectCacher::Object::split(BufferHead *bh, off_t off)
{
  dout(20) << "split " << *bh << " at " << off << endl;
  
  // split off right
  ObjectCacher::BufferHead *right = new BufferHead();
  right->last_write_tid = bh->last_write_tid;
  right->set_state(bh->get_state());
  
  off_t newleftlen = off - bh->start();
  right->set_start( off );
  right->set_length( bh->length() - newleftlen );
  
  // shorten left
  oc->bh_stat_sub(bh);
  bh->set_length( newleftlen );
  oc->bh_stat_add(bh);
  
  // add right
  oc->bh_add(this, right);
  
  // split buffers too
  bufferlist bl;
  bl.claim(bh->bl);
  if (bl.length()) {
	assert(bl.length() == (bh->length() + right->length()));
	right->bl.substr_of(bl, bh->length(), right->length());
	bh->bl.substr_of(bl, 0, bh->length());
  }
  
  // move read waiters
  if (!bh->waitfor_read.empty()) {
	map<off_t, list<Context*> >::iterator o, p = bh->waitfor_read.end();
	p--;
	while (p != bh->waitfor_read.begin()) {
	  if (p->first < right->start()) break;	  
	  dout(0) << "split  moving waiters at byte " << p->first << " to right bh" << endl;
	  right->waitfor_read[p->first].swap( p->second );
	  o = p;
	  p--;
	  bh->waitfor_read.erase(o);
	}
  }
  
  dout(20) << "split    left is " << *bh << endl;
  dout(20) << "split   right is " << *right << endl;
  return right;
}


void ObjectCacher::Object::merge(BufferHead *left, BufferHead *right)
{
  assert(left->end() == right->start());
  assert(left->get_state() == right->get_state());

  dout(10) << "merge " << *left << " + " << *right << endl;
  oc->bh_remove(this, right);
  oc->bh_stat_sub(left);
  left->set_length( left->length() + right->length());
  oc->bh_stat_add(left);

  // data
  left->bl.claim_append(right->bl);
  
  // version 
  // note: this is sorta busted, but shouldn't be used, cuz we're pbly about to write.. right?
  left->last_write_tid =  MAX( left->last_write_tid, right->last_write_tid );

  // waiters
  for (map<off_t, list<Context*> >::iterator p = right->waitfor_read.begin();
	   p != right->waitfor_read.end();
	   p++) 
	left->waitfor_read[p->first].splice( left->waitfor_read[p->first].begin(),
										 p->second );
  
  // hose right
  delete right;

  dout(10) << "merge result " << *left << endl;
}

/*
 * map a range of bytes into buffer_heads.
 * - create missing buffer_heads as necessary.
 */
int ObjectCacher::Object::map_read(Objecter::OSDRead *rd,
                                   map<off_t, BufferHead*>& hits,
                                   map<off_t, BufferHead*>& missing,
                                   map<off_t, BufferHead*>& rx)
{
  for (list<ObjectExtent>::iterator ex_it = rd->extents.begin();
       ex_it != rd->extents.end();
       ex_it++) {
    
    if (ex_it->oid != oid) continue;
    
    dout(10) << "map_read " << hex << ex_it->oid << dec 
			 << " " << ex_it->start << "~" << ex_it->length << endl;
    
    map<off_t, BufferHead*>::iterator p = data.lower_bound(ex_it->start);
    // p->first >= start
    
    off_t cur = ex_it->start;
    off_t left = ex_it->length;
    
    if (p != data.begin() && 
        (p == data.end() || p->first > cur)) {
      p--;     // might overlap!
      if (p->first + p->second->length() <= cur) 
        p++;   // doesn't overlap.
    }
    
    while (left > 0) {
      // at end?
      if (p == data.end()) {
        // rest is a miss.
		BufferHead *n = new BufferHead();
		n->set_start( cur );
		n->set_length( left );
		oc->bh_add(this, n);
		missing[cur] = n;
		dout(20) << "map_read miss " << left << " left, " << *n << endl;
		cur += left;
		left -= left;
        assert(left == 0);
        assert(cur == ex_it->start + ex_it->length);
        break;  // no more.
      }
      
      if (p->first <= cur) {
        // have it (or part of it)
        BufferHead *e = p->second;
        
        if (e->is_clean() ||
            e->is_dirty() ||
            e->is_tx()) {
          hits[cur] = e;     // readable!
          dout(20) << "map_read hit " << *e << endl;
        } 
        else if (e->is_rx()) {
          rx[cur] = e;       // missing, not readable.
          dout(20) << "map_read rx " << *e << endl;
        }
        else assert(0);
        
        off_t lenfromcur = MIN(e->end() - cur, left);
        cur += lenfromcur;
        left -= lenfromcur;
        p++;
        continue;  // more?
        
      } else if (p->first > cur) {
        // gap.. miss
        off_t next = p->first;
		BufferHead *n = new BufferHead();
		n->set_start( cur );
		n->set_length( MIN(next - cur, left) );
		oc->bh_add(this,n);
		missing[cur] = n;
		cur += MIN(left, n->length());
		left -= MIN(left, n->length());
		dout(20) << "map_read gap " << *n << endl;
        continue;    // more?
      }
      else 
        assert(0);
    }
  }
  return(0);
}

/*
 * map a range of extents on an object's buffer cache.
 * - combine any bh's we're writing into one
 * - break up bufferheads that don't fall completely within the range
 */
ObjectCacher::BufferHead *ObjectCacher::Object::map_write(Objecter::OSDWrite *wr)
{
  BufferHead *final = 0;

  for (list<ObjectExtent>::iterator ex_it = wr->extents.begin();
       ex_it != wr->extents.end();
       ex_it++) {
	
    if (ex_it->oid != oid) continue;
    
    dout(10) << "map_write oex " << hex << ex_it->oid << dec
			 << " " << ex_it->start << "~" << ex_it->length << endl;
    
    map<off_t, BufferHead*>::iterator p = data.lower_bound(ex_it->start);
    // p->first >= start
    
    off_t cur = ex_it->start;
    off_t left = ex_it->length;
    
    if (p != data.begin() && 
        (p == data.end() || p->first > cur)) {
      p--;     // might overlap!
      if (p->first + p->second->length() <= cur) 
        p++;   // doesn't overlap.
    }    
    
    while (left > 0) {
      off_t max = left;

      // at end ?
      if (p == data.end()) {
        if (final == NULL) {
          final = new BufferHead();
          final->set_start( cur );
          final->set_length( max );
          oc->bh_add(this, final);
        } else {
          final->set_length( final->length() + max );
        }
        left -= max;
        cur += max;
        continue;
      }
      
      dout(10) << "p is " << *p->second << endl;

      if (p->first <= cur) {
        BufferHead *bh = p->second;
        dout(10) << "map_write bh " << *bh << " intersected" << endl;
        
        if (p->first < cur) {
		  assert(final == 0);
          if (cur + max >= p->first + p->second->length()) {
            // we want right bit (one splice)
			final = split(bh, cur);   // just split it, take right half.
            p++;
            assert(p->second == final);
          } else {
            // we want middle bit (two splices)
			final = split(bh, cur);
			p++;
			assert(p->second == final);
			split(final, cur+max);
		  }
        } else if (p->first == cur) {
          if (p->second->length() <= max) {
            // whole bufferhead, piece of cake.
          } else {
            // we want left bit (one splice)
			split(bh, cur + max);        // just split
		  }
		  if (final) 
			merge(final,bh);
		  else
			final = bh;
        }
        
        // keep going.
        off_t lenfromcur = final->end() - cur;
        cur += lenfromcur;
        left -= lenfromcur;
        p++;
        continue; 
      } else {
        // gap!
        off_t next = p->first;
        off_t glen = MIN(next - cur, max);
        dout(10) << "map_write gap " << cur << "~" << glen << endl;
		if (final) {
		  final->set_length( final->length() + glen );
		} else {
		  final = new BufferHead();
		  final->set_start( cur );
		  final->set_length( glen );
		  oc->bh_add(this, final);
		}
        
        cur += glen;
        left -= glen;
        continue;    // more?
      }
    }
  }
  
  // set versoin
  assert(final);
  dout(10) << "map_write final is " << *final << endl;

  return final;
}

/*** ObjectCacher ***/

/* private */

void ObjectCacher::bh_read(Object *ob, BufferHead *bh)
{
  dout(7) << "bh_read on " << *bh << endl;

  // finisher
  C_ReadFinish *onfinish = new C_ReadFinish(this, ob->get_oid(), bh->start(), bh->length());

  // go
  objecter->read(ob->get_oid(), bh->start(), bh->length(), &onfinish->bl,
				 onfinish);
}

void ObjectCacher::bh_read_finish(object_t oid, off_t start, size_t length, bufferlist &bl)
{
  //lock.Lock();
  dout(7) << "bh_read_finish " 
		  << hex << oid << dec 
		  << " " << start << "~" << length
		  << endl;
  
  if (objects.count(oid) == 0) {
	dout(7) << "bh_read_finish no object cache" << endl;
  } else {
	Object *ob = objects[oid];
	
	// apply to bh's!
	off_t opos = start;
	map<off_t, BufferHead*>::iterator p = ob->data.lower_bound(opos);
	
	while (p != ob->data.end() &&
		   opos < start+length) {
	  BufferHead *bh = p->second;
	  
	  if (bh->start() > opos) {
		dout(1) << "weirdness: gap when applying read results, " 
				<< opos << "~" << bh->start() - opos 
				<< endl;
		opos = bh->start();
		p++;
		continue;
	  }
	  
	  if (!bh->is_rx()) {
		dout(10) << "bh_read_finish skipping non-rx " << *bh << endl;
		continue;
	  }
	  
	  assert(bh->start() == opos);   // we don't merge rx bh's... yet!
	  assert(bh->length() < start+length-opos);
	  
	  bh->bl.substr_of(bl,
					   start+length-opos,
					   bh->length());
	  mark_clean(bh);
	  dout(10) << "bh_read_finish read " << *bh << endl;

	  // finishers?
	  // called with lock held.
	  list<Context*> ls;
	  for (map<off_t, list<Context*> >::iterator p = bh->waitfor_read.begin();
		   p != bh->waitfor_read.end();
		   p++)
		ls.splice(ls.end(), p->second);
	  bh->waitfor_read.clear();
	  finish_contexts(ls);
	}
  }
  //lock.Unlock();
}


void ObjectCacher::bh_write(Object *ob, BufferHead *bh)
{
  dout(7) << "bh_write " << *bh << endl;
  
  // finishers
  C_WriteAck *onack = new C_WriteAck(this, ob->get_oid(), bh->start(), bh->length());
  C_WriteCommit *oncommit = new C_WriteCommit(this, ob->get_oid(), bh->start(), bh->length());

  // go
  tid_t tid = objecter->write(ob->get_oid(), bh->start(), bh->length(), bh->bl,
							  onack, oncommit);

  // set bh last_write_tid
  onack->tid = tid;
  oncommit->tid = tid;
  ob->last_write_tid = tid;
  bh->last_write_tid = tid;
}

void ObjectCacher::bh_write_ack(object_t oid, off_t start, size_t length, tid_t tid)
{
  //lock.Lock();
  
  dout(7) << "bh_write_ack " 
		  << hex << oid << dec 
		  << " tid " << tid
		  << " " << start << "~" << length
		  << endl;
  if (objects.count(oid) == 0) {
	dout(7) << "bh_write_ack no object cache" << endl;
	assert(0);
  } else {
	Object *ob = objects[oid];
	
	// apply to bh's!
	off_t opos = start;
	map<off_t, BufferHead*>::iterator p = ob->data.lower_bound(opos);
	
	while (p != ob->data.end() &&
		   opos < start+length) {
	  BufferHead *bh = p->second;
	  
	  if (bh->start() < start &&
		  bh->end() > start+length) {
		dout(20) << "bh_write_ack skipping " << *bh << endl;
		continue;
	  }
	  
	  // make sure bh is tx
	  if (!bh->is_tx()) {
		dout(10) << "bh_write_ack skipping non-tx " << *bh << endl;
		continue;
	  }
	  
	  // make sure bh tid matches
	  if (bh->last_write_tid != tid) {
		assert(bh->last_write_tid > tid);
		dout(10) << "bh_write_ack newer tid on " << *bh << endl;
		continue;
	  }
	  
	  // ok!  mark bh clean.
	  mark_clean(bh);
	}
	
	// update object last_ack.
	assert(ob->last_ack_tid < tid);
	ob->last_ack_tid = tid;
	
	// waiters?
	if (ob->waitfor_ack.count(tid)) {
	  list<Context*> ls;
	  ls.splice(ls.begin(), ob->waitfor_ack[tid]);
	  ob->waitfor_ack.erase(tid);
	  finish_contexts(ls);
	}
  }
  //lock.Unlock();
}

void ObjectCacher::bh_write_commit(object_t oid, off_t start, size_t length, tid_t tid)
{
  //lock.Lock();
  
  // update object last_commit
  dout(7) << "bh_write_commit " 
		  << hex << oid << dec 
		  << " tid " << tid
		  << " " << start << "~" << length
		  << endl;
  if (objects.count(oid) == 0) {
	dout(7) << "bh_write_commit no object cache" << endl;
	assert(0);
  } else {
	Object *ob = objects[oid];
	
	// update last_commit.
	ob->last_commit_tid = tid;
	
	// waiters?
	if (ob->waitfor_commit.count(tid)) {
	  list<Context*> ls;
	  ls.splice(ls.begin(), ob->waitfor_commit[tid]);
	  ob->waitfor_commit.erase(tid);
	  finish_contexts(ls);
	}
  }

  //  lock.Unlock();
}


/* public */

int ObjectCacher::readx(Objecter::OSDRead *rd, inodeno_t ino, Context *onfinish)
{
  bool success = true;
  list<BufferHead*> hit_ls;
  map<size_t, bufferlist> stripe_map;  // final buffer offset -> substring

  for (list<ObjectExtent>::iterator ex_it = rd->extents.begin();
       ex_it != rd->extents.end();
       ex_it++) {
	dout(10) << "readx " << *ex_it << endl;

	// get Object cache
	Object *o;
    if (objects.count(ex_it->oid) == 0) {
	  // create it.
      Object *o = new Object(this, ex_it->oid, ino);
      objects[ex_it->oid] = o;
      objects_by_ino[ino].insert(o);
    } else {
	  // had it.
	  o = objects[ex_it->oid];
	}
	
	// map extent into bufferheads
	map<off_t, BufferHead*> hits, missing, rx;

    o->map_read(rd, hits, missing, rx);
	
	if (!missing.empty() && !rx.empty()) {
	  // read missing
	  for (map<off_t, BufferHead*>::iterator bh_it = missing.begin();
		   bh_it != missing.end();
		   bh_it++) {
		bh_read(o, bh_it->second);
		if (success) {
		  dout(10) << "readx missed, waiting on " << *bh_it->second 
				   << " off " << bh_it->first << endl;
		  success = false;
		  bh_it->second->waitfor_read[bh_it->first].push_back( new C_RetryRead(this, rd, ino, onfinish) );
		}
	  }

	  // bump rx
	  for (map<off_t, BufferHead*>::iterator bh_it = rx.begin();
		   bh_it != rx.end();
		   bh_it++) {
		touch_bh(bh_it->second);		// bump in lru, so we don't lose it.
		if (success) {
		  dout(10) << "readx missed, waiting on " << *bh_it->second 
				   << " off " << bh_it->first << endl;
		  success = false;
		  bh_it->second->waitfor_read[bh_it->first].push_back( new C_RetryRead(this, rd, ino, onfinish) );
		}
	  }	  
	} else {
	  // make a plain list
	  for (map<off_t, BufferHead*>::iterator bh_it = hits.begin();
		   bh_it != hits.end();
		   bh_it++) 
		hit_ls.push_back(bh_it->second);

	  // create reverse map of buffer offset -> object for the eventual result.
	  // this is over a single ObjectExtent, so we know that
	  //  - the bh's are contiguous
	  //  - the buffer frags need not be (and almost certainly aren't)
	  map<off_t, BufferHead*>::iterator bh_it = hits.begin();
	  size_t bhoff = 0;
	  map<size_t,size_t>::iterator f_it = ex_it->buffer_extents.begin();
	  size_t foff = 0;
	  off_t opos = ex_it->start;
	  while (1) {
		BufferHead *bh = bh_it->second;
		assert(opos == bh->start() + bhoff);

		dout(10) << "readx rmap opos " << opos
				 << ": " << *bh << " +" << bhoff
				 << " frag " << f_it->first << "~" << f_it->second << " +" << foff
				 << endl;

		size_t len = MIN(f_it->second - foff,
						 bh->length() - bhoff);
		stripe_map[f_it->first].substr_of(bh->bl,
										  opos - bh->start(),
										  len);
		opos += len;
		bhoff += len;
		foff += len;
		if (opos == bh->end()) {
		  bh_it++;
		  bhoff = 0;
		}
		if (foff == f_it->second) {
		  f_it++;
		  foff = 0;
		}
		if (bh_it == hits.end()) break;
		if (f_it == ex_it->buffer_extents.end()) break;
	  }
	  assert(f_it == ex_it->buffer_extents.end());
	  assert(bh_it == hits.end());
	  assert(opos == ex_it->start + ex_it->length);
	}
  }
  
  // bump hits in lru
  for (list<BufferHead*>::iterator bhit = hit_ls.begin();
	   bhit != hit_ls.end();
	   bhit++) 
	touch_bh(*bhit);
  
  if (!success) return 0;  // wait!

  // no misses... success!  do the read.
  assert(!hit_ls.empty());
  dout(10) << "readx has all buffers" << endl;
  
  // ok, assemble into result buffer.
  rd->bl->clear();
  size_t pos = 0;
  for (map<size_t,bufferlist>::iterator i = stripe_map.begin();
	   i != stripe_map.end();
	   i++) {
	assert(pos == i->first);
	pos += i->second.length();
	rd->bl->claim_append(i->second);
  }
  
  return pos;
}


int ObjectCacher::writex(Objecter::OSDWrite *wr, inodeno_t ino)
{
  for (list<ObjectExtent>::iterator ex_it = wr->extents.begin();
	   ex_it != wr->extents.end();
       ex_it++) {
	// create object cache?
	Object *o = 0;
    if (objects.count(ex_it->oid) == 0) {
      o = new Object(this, ex_it->oid, ino);
      objects[ex_it->oid] = o;
      objects_by_ino[ino].insert(o);
    } else {
	  o = objects[ex_it->oid];
	}

	// map into a single bufferhead.
    BufferHead *bh = o->map_write(wr);
	
	// adjust buffer pointers (ie "copy" data into my cache)
	// this is over a single ObjectExtent, so we know that
	//  - there is one contiguous bh
	//  - the buffer frags need not be (and almost certainly aren't)
	// note: i assume striping is monotonic... no jumps backwards, ever!
	off_t opos = ex_it->start;
	for (map<size_t,size_t>::iterator f_it = ex_it->buffer_extents.begin();
		 f_it != ex_it->buffer_extents.end();
		 f_it++) {
	  size_t bhoff = bh->start() - opos;
	  assert(f_it->second <= bh->length() - bhoff);

	  bufferlist frag; 
	  frag.substr_of(wr->bl, 
					 f_it->first, f_it->second);

	  bh->bl.claim_append(frag);
	  opos += f_it->second;
	}

	mark_dirty(bh);
  }
  return 0;
}
 

// blocking wait for write.
void ObjectCacher::wait_for_write(size_t len, Mutex& lock)
{
  while (get_stat_dirty() + len > g_conf.client_oc_max_dirty) {
	dout(10) << "wait_for_write waiting" << endl;
	stat_waiter++;
	stat_cond.Wait(lock);
	stat_waiter--;
	dout(10) << "wait_for_write woke up" << endl;
  }
}

  
// blocking.  atomic+sync.
int ObjectCacher::atomic_sync_readx(Objecter::OSDRead *rd, inodeno_t ino, Mutex& lock)
{
  assert(0);
  return 0;
}

int ObjectCacher::atomic_sync_writex(Objecter::OSDWrite *wr, inodeno_t ino, Mutex& lock)
{
  assert(0);
  return 0;
}
 


// flush.  non-blocking, takes callback.
// returns true if already flushed, and deletes the callback.
bool ObjectCacher::flush_set(inodeno_t ino, Context *onfinish)
{
  if (objects.count(ino) == 0) {
	dout(10) << "flush_set on " << hex << ino << dec << " dne" << endl;
	delete onfinish;
	return true;
  }

  Object *ob = objects[ino];
  dout(10) << "flush_set " << *ob << endl;

  bool any_tx = false;
  for (map<off_t,BufferHead*>::iterator p = ob->data.begin();
	   p != ob->data.end();
	   p++) {
	BufferHead *bh = p->second;
	if (bh->is_tx()) {
	  any_tx = true;
	  continue;
	}
	if (!bh->is_dirty()) continue;

	bh_write(ob, bh);
  }

  if (!any_tx) {
	dout(10) << "flush_set " << *ob << " has no dirty|tx bhs" << endl;
	delete onfinish;
	return true;
  }

  dout(10) << "flush_set " << *ob << " will finish on ack tid " << ob->last_write_tid << endl;
  ob->waitfor_ack[ob->last_write_tid].push_back(onfinish);
  return false;
}


// commit.  non-blocking, takes callback.
// return true if already flushed.
bool ObjectCacher::commit_set(inodeno_t ino, Context *onfinish)
{
  assert(onfinish);  // doesn't make any sense otherwise.

  if (objects.count(ino) == 0) {
	dout(10) << "commit_set on " << hex << ino << dec << " dne" << endl;
	delete onfinish;
	return true;
  }

  Object *ob = objects[ino];
  dout(10) << "commit_set " << *ob << endl;

  // make sure it's flushing.
  flush_set(ino);

  if (ob->last_write_tid < ob->last_commit_tid) {
	dout(10) << "commit_set " << *ob << " will finish on commit tid " << ob->last_write_tid << endl;
	ob->waitfor_commit[ob->last_write_tid].push_back(onfinish);
	return false;
  } else {
	dout(10) << "commit_set " << *ob << " all committed" << endl;
	delete onfinish;
	return true;
  }
}


int ObjectCacher::release_set(inodeno_t ino)
{
  // return # bytes not clean (and thus not released).
  int unclean = 0;

  if (objects.count(ino) == 0) {
	dout(10) << "release_set on " << hex << ino << dec << " dne" << endl;
	return 0;
  }

  Object *ob = objects[ino];
  dout(10) << "release_set " << *ob << endl;

  for (map<off_t,BufferHead*>::iterator p = ob->data.begin();
	   p != ob->data.end();
	   p++) {
	BufferHead *bh = p->second;
	if (bh->is_clean()) 
	  bh_remove(ob, bh);
	else 
	  unclean += bh->length();
  }

  if (unclean) {
	dout(10) << "release_set " << *ob 
			 << ", " << unclean << " bytes left" << endl;
  }

  return unclean;
}
