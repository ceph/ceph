// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "msg/Messenger.h"
#include "ObjectCacher.h"
#include "Objecter.h"



/*** ObjectCacher::BufferHead ***/


/*** ObjectCacher::Object ***/

#define DOUT_SUBSYS objectcacher
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << oc->objecter->messenger->get_myname() << ".objectcacher.object(" << oid << ") "



ObjectCacher::BufferHead *ObjectCacher::Object::split(BufferHead *left, loff_t off)
{
  dout(20) << "split " << *left << " at " << off << dendl;
  
  // split off right
  ObjectCacher::BufferHead *right = new BufferHead(this);
  right->last_write_tid = left->last_write_tid;
  right->set_state(left->get_state());
  right->snapc = left->snapc;
  
  loff_t newleftlen = off - left->start();
  right->set_start(off);
  right->set_length(left->length() - newleftlen);
  
  // shorten left
  oc->bh_stat_sub(left);
  left->set_length(newleftlen);
  oc->bh_stat_add(left);
  
  // add right
  oc->bh_add(this, right);
  
  // split buffers too
  bufferlist bl;
  bl.claim(left->bl);
  if (bl.length()) {
    assert(bl.length() == (left->length() + right->length()));
    right->bl.substr_of(bl, left->length(), right->length());
    left->bl.substr_of(bl, 0, left->length());
  }
  
  // move read waiters
  if (!left->waitfor_read.empty()) {
    map<loff_t, list<Context*> >::iterator o, p = left->waitfor_read.end();
    p--;
    while (p != left->waitfor_read.begin()) {
      if (p->first < right->start()) break;      
      dout(0) << "split  moving waiters at byte " << p->first << " to right bh" << dendl;
      right->waitfor_read[p->first].swap( p->second );
      o = p;
      p--;
      left->waitfor_read.erase(o);
    }
  }
  
  dout(20) << "split    left is " << *left << dendl;
  dout(20) << "split   right is " << *right << dendl;
  return right;
}


void ObjectCacher::Object::merge_left(BufferHead *left, BufferHead *right)
{
  assert(left->end() == right->start());
  assert(left->get_state() == right->get_state());

  dout(10) << "merge_left " << *left << " + " << *right << dendl;
  oc->bh_remove(this, right);
  oc->bh_stat_sub(left);
  left->set_length(left->length() + right->length());
  oc->bh_stat_add(left);

  // data
  left->bl.claim_append(right->bl);
  
  // version 
  // note: this is sorta busted, but should only be used for dirty buffers
  left->last_write_tid =  MAX( left->last_write_tid, right->last_write_tid );
  left->last_write = MAX( left->last_write, right->last_write );

  // waiters
  for (map<loff_t, list<Context*> >::iterator p = right->waitfor_read.begin();
       p != right->waitfor_read.end();
       p++) 
    left->waitfor_read[p->first].splice( left->waitfor_read[p->first].begin(),
                                         p->second );
  
  // hose right
  delete right;

  dout(10) << "merge_left result " << *left << dendl;
}

void ObjectCacher::Object::try_merge_bh(BufferHead *bh)
{
  dout(10) << "try_merge_bh " << *bh << dendl;

  // to the left?
  map<loff_t,BufferHead*>::iterator p = data.find(bh->start());
  assert(p->second == bh);
  if (p != data.begin()) {
    p--;
    if (p->second->end() == bh->start() &&
	p->second->get_state() == bh->get_state()) {
      merge_left(p->second, bh);
      bh = p->second;
    } else 
      p++;
  }
  // to the right?
  assert(p->second == bh);
  p++;
  if (p != data.end() &&
      p->second->start() == bh->end() &&
      p->second->get_state() == bh->get_state()) 
    merge_left(bh, p->second);
}

/*
 * count bytes we have cached in given range
 */
bool ObjectCacher::Object::is_cached(loff_t cur, loff_t left)
{
  map<loff_t, BufferHead*>::iterator p = data.lower_bound(cur);
  
  if (p != data.begin() && 
      (p == data.end() || p->first > cur)) {
    p--;     // might overlap!
    if (p->first + p->second->length() <= cur) 
      p++;   // doesn't overlap.
  }
  
  while (left > 0) {
    if (p == data.end())
      return false;

    if (p->first <= cur) {
      // have part of it
      loff_t lenfromcur = MIN(p->second->end() - cur, left);
      cur += lenfromcur;
      left -= lenfromcur;
      p++;
      continue;
    } else if (p->first > cur) {
      // gap
      return false;
    } else
      assert(0);
  }

  return true;
}

/*
 * map a range of bytes into buffer_heads.
 * - create missing buffer_heads as necessary.
 */
int ObjectCacher::Object::map_read(OSDRead *rd,
                                   map<loff_t, BufferHead*>& hits,
                                   map<loff_t, BufferHead*>& missing,
                                   map<loff_t, BufferHead*>& rx)
{
  for (vector<ObjectExtent>::iterator ex_it = rd->extents.begin();
       ex_it != rd->extents.end();
       ex_it++) {
    
    if (ex_it->oid != oid.oid) continue;
    
    dout(10) << "map_read " << ex_it->oid 
             << " " << ex_it->offset << "~" << ex_it->length << dendl;
    
    map<loff_t, BufferHead*>::iterator p = data.lower_bound(ex_it->offset);
    // p->first >= start
    
    loff_t cur = ex_it->offset;
    loff_t left = ex_it->length;
    
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
        BufferHead *n = new BufferHead(this);
        n->set_start(cur);
        n->set_length(left);
        oc->bh_add(this, n);
        missing[cur] = n;
        dout(20) << "map_read miss " << left << " left, " << *n << dendl;
        cur += left;
        left -= left;
        assert(left == 0);
        assert(cur == ex_it->offset + (loff_t)ex_it->length);
        break;  // no more.
      }
      
      if (p->first <= cur) {
        // have it (or part of it)
        BufferHead *e = p->second;
        
        if (e->is_clean() ||
            e->is_dirty() ||
            e->is_tx()) {
          hits[cur] = e;     // readable!
          dout(20) << "map_read hit " << *e << dendl;
        } 
        else if (e->is_rx()) {
          rx[cur] = e;       // missing, not readable.
          dout(20) << "map_read rx " << *e << dendl;
        }
        else assert(0);
        
        loff_t lenfromcur = MIN(e->end() - cur, left);
        cur += lenfromcur;
        left -= lenfromcur;
        p++;
        continue;  // more?
        
      } else if (p->first > cur) {
        // gap.. miss
        loff_t next = p->first;
        BufferHead *n = new BufferHead(this);
        n->set_start( cur );
        n->set_length( MIN(next - cur, left) );
        oc->bh_add(this,n);
        missing[cur] = n;
        cur += MIN(left, n->length());
        left -= MIN(left, n->length());
        dout(20) << "map_read gap " << *n << dendl;
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
 * //no! - return a bh that includes the write.  may also include other dirty data to left and/or right.
 */
ObjectCacher::BufferHead *ObjectCacher::Object::map_write(OSDWrite *wr)
{
  BufferHead *final = 0;

  for (vector<ObjectExtent>::iterator ex_it = wr->extents.begin();
       ex_it != wr->extents.end();
       ex_it++) {
    
    if (ex_it->oid != oid.oid) continue;
    
    dout(10) << "map_write oex " << ex_it->oid
             << " " << ex_it->offset << "~" << ex_it->length << dendl;
    
    map<loff_t, BufferHead*>::iterator p = data.lower_bound(ex_it->offset);
    // p->first >= start
    
    loff_t cur = ex_it->offset;
    loff_t left = ex_it->length;
    
    if (p != data.begin() && 
        (p == data.end() || p->first > cur)) {
      p--;     // might overlap or butt up!

      /*// dirty and butts up?
      if (p->first + p->second->length() == cur &&
          p->second->is_dirty()) {
        dout(10) << "map_write will append to tail of " << *p->second << dendl;
        final = p->second;
      }
      */
      if (p->first + p->second->length() <= cur) 
        p++;   // doesn't overlap.
    }    
    
    while (left > 0) {
      loff_t max = left;

      // at end ?
      if (p == data.end()) {
        if (final == NULL) {
          final = new BufferHead(this);
          final->set_start( cur );
          final->set_length( max );
          oc->bh_add(this, final);
          dout(10) << "map_write adding trailing bh " << *final << dendl;
        } else {
	  oc->bh_stat_sub(final);
          final->set_length(final->length() + max);
	  oc->bh_stat_add(final);
        }
        left -= max;
        cur += max;
        continue;
      }
      
      dout(10) << "cur is " << cur << ", p is " << *p->second << dendl;
      //oc->verify_stats();

      if (p->first <= cur) {
        BufferHead *bh = p->second;
        dout(10) << "map_write bh " << *bh << " intersected" << dendl;
        
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
          if (final) {
	    oc->mark_dirty(bh);
	    oc->mark_dirty(final);
	    p--;  // move iterator back to final
	    assert(p->second == final);
            merge_left(final, bh);
	  } else
            final = bh;
        }
        
        // keep going.
        loff_t lenfromcur = final->end() - cur;
        cur += lenfromcur;
        left -= lenfromcur;
        p++;
        continue; 
      } else {
        // gap!
        loff_t next = p->first;
        loff_t glen = MIN(next - cur, max);
        dout(10) << "map_write gap " << cur << "~" << glen << dendl;
        if (final) {
	  oc->bh_stat_sub(final);
          final->set_length(final->length() + glen);
	  oc->bh_stat_add(final);
        } else {
          final = new BufferHead(this);
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
  dout(10) << "map_write final is " << *final << dendl;

  return final;
}


void ObjectCacher::Object::truncate(loff_t s)
{
  dout(10) << "truncate to " << s << dendl;
  
  while (!data.empty()) {
	BufferHead *bh = data.rbegin()->second;
	if (bh->end() <= s) 
	  break;
	
	// split bh at truncation point?
	if (bh->start() < s) {
	  split(bh, s);
	  continue;
	}

	// remove bh entirely
	assert(bh->start() >= s);
	oc->bh_remove(this, bh);
	delete bh;
  }
}





/*** ObjectCacher ***/

#undef dout_prefix
#define dout_prefix *_dout << dbeginl << objecter->messenger->get_myname() << ".objectcacher "


/* private */

void ObjectCacher::close_object(Object *ob) 
{
  dout(10) << "close_object " << *ob << dendl;
  assert(ob->can_close());
  
  // ok!
  objects.erase(ob->get_soid());
  delete ob;
}




void ObjectCacher::bh_read(BufferHead *bh)
{
  dout(7) << "bh_read on " << *bh << dendl;

  mark_rx(bh);

  // finisher
  C_ReadFinish *onfinish = new C_ReadFinish(this, bh->ob->get_soid(), bh->start(), bh->length());

  ObjectSet *oset = bh->ob->oset;

  // go
  objecter->read_trunc(bh->ob->get_oid(), bh->ob->get_layout(), 
		 bh->start(), bh->length(), bh->ob->get_snap(),
		 &onfinish->bl, 0,
		 oset->truncate_size, oset->truncate_seq,
		 onfinish);
}

void ObjectCacher::bh_read_finish(sobject_t oid, loff_t start, uint64_t length, bufferlist &bl)
{
  //lock.Lock();
  dout(7) << "bh_read_finish " 
          << oid
          << " " << start << "~" << length
	  << " (bl is " << bl.length() << ")"
          << dendl;

  if (bl.length() < length) {
    bufferptr bp(length - bl.length());
    bp.zero();
    dout(7) << "bh_read_finish " << oid << " padding " << start << "~" << length 
	    << " with " << bp.length() << " bytes of zeroes" << dendl;
    bl.push_back(bp);
  }
  
  if (objects.count(oid) == 0) {
    dout(7) << "bh_read_finish no object cache" << dendl;
  } else {
    Object *ob = objects[oid];
    
    // apply to bh's!
    loff_t opos = start;
    map<loff_t, BufferHead*>::iterator p = ob->data.lower_bound(opos);
    
    while (p != ob->data.end() &&
           opos < start+(loff_t)length) {
      BufferHead *bh = p->second;
      
      if (bh->start() > opos) {
        dout(1) << "weirdness: gap when applying read results, " 
                << opos << "~" << bh->start() - opos 
                << dendl;
        opos = bh->start();
        continue;
      }
      
      if (!bh->is_rx()) {
        dout(10) << "bh_read_finish skipping non-rx " << *bh << dendl;
        opos = bh->end();
        p++;
        continue;
      }
      
      assert(opos >= bh->start());
      assert(bh->start() == opos);   // we don't merge rx bh's... yet!
      assert(bh->length() <= start+(loff_t)length-opos);
      
      bh->bl.substr_of(bl,
                       opos-bh->start(),
                       bh->length());
      mark_clean(bh);
      dout(10) << "bh_read_finish read " << *bh << dendl;
      
      opos = bh->end();
      p++;

      // finishers?
      // called with lock held.
      list<Context*> ls;
      for (map<loff_t, list<Context*> >::iterator p = bh->waitfor_read.begin();
           p != bh->waitfor_read.end();
           p++)
        ls.splice(ls.end(), p->second);
      bh->waitfor_read.clear();
      finish_contexts(ls);

      // clean up?
      ob->try_merge_bh(bh);
    }
  }
  //lock.Unlock();
}


void ObjectCacher::bh_write(BufferHead *bh)
{
  dout(7) << "bh_write " << *bh << dendl;
  
  // finishers
  C_WriteAck *onack = new C_WriteAck(this, bh->ob->get_soid(), bh->start(), bh->length());
  C_WriteCommit *oncommit = new C_WriteCommit(this, bh->ob->get_soid(), bh->start(), bh->length());

  ObjectSet *oset = bh->ob->oset;

  // go
  tid_t tid = objecter->write_trunc(bh->ob->get_oid(), bh->ob->get_layout(),
			      bh->start(), bh->length(),
			      bh->snapc, bh->bl, bh->last_write, 0,
			      oset->truncate_size, oset->truncate_seq,
			      onack, oncommit);

  // set bh last_write_tid
  onack->tid = tid;
  oncommit->tid = tid;
  bh->ob->last_write_tid = tid;
  bh->last_write_tid = tid;
  if (commit_set_callback)
    oset->uncommitted.push_back(&bh->ob->uncommitted_item);

  mark_tx(bh);
}

void ObjectCacher::lock_ack(list<sobject_t>& oids, tid_t tid)
{
  for (list<sobject_t>::iterator i = oids.begin();
       i != oids.end();
       i++) {
    sobject_t oid = *i;

    if (objects.count(oid) == 0) {
      dout(7) << "lock_ack no object cache" << dendl;
      assert(0);
    } 
    
    Object *ob = objects[oid];

    list<Context*> ls;

    // waiters?
    if (ob->waitfor_ack.count(tid)) {
      ls.splice(ls.end(), ob->waitfor_ack[tid]);
      ob->waitfor_ack.erase(tid);
    }
    
    assert(tid <= ob->last_write_tid);
    if (ob->last_write_tid == tid) {
      dout(10) << "lock_ack " << *ob
               << " tid " << tid << dendl;

      switch (ob->lock_state) {
      case Object::LOCK_RDUNLOCKING: 
      case Object::LOCK_WRUNLOCKING: 
        ob->lock_state = Object::LOCK_NONE; 
        break;
      case Object::LOCK_RDLOCKING: 
      case Object::LOCK_DOWNGRADING: 
        ob->lock_state = Object::LOCK_RDLOCK; 
        ls.splice(ls.begin(), ob->waitfor_rd);
        break;
      case Object::LOCK_UPGRADING: 
      case Object::LOCK_WRLOCKING: 
        ob->lock_state = Object::LOCK_WRLOCK; 
        ls.splice(ls.begin(), ob->waitfor_wr);
        ls.splice(ls.begin(), ob->waitfor_rd);
        break;

      default:
        assert(0);
      }
      
      ob->last_ack_tid = tid;
      
      if (ob->can_close())
        close_object(ob);
    } else {
      dout(10) << "lock_ack " << *ob 
               << " tid " << tid << " obsolete" << dendl;
    }

    finish_contexts(ls);

  }
}

void ObjectCacher::bh_write_ack(sobject_t oid, loff_t start, uint64_t length, tid_t tid)
{
  //lock.Lock();
  
  dout(7) << "bh_write_ack " 
          << oid 
          << " tid " << tid
          << " " << start << "~" << length
          << dendl;
  if (objects.count(oid) == 0) {
    dout(7) << "bh_write_ack no object cache" << dendl;
    assert(0);
  } else {
    Object *ob = objects[oid];
    
    // apply to bh's!
    for (map<loff_t, BufferHead*>::iterator p = ob->data.lower_bound(start);
         p != ob->data.end();
         p++) {
      BufferHead *bh = p->second;
      
      if (bh->start() > start+(loff_t)length) break;

      if (bh->start() < start &&
          bh->end() > start+(loff_t)length) {
        dout(20) << "bh_write_ack skipping " << *bh << dendl;
        continue;
      }
      
      // make sure bh is tx
      if (!bh->is_tx()) {
        dout(10) << "bh_write_ack skipping non-tx " << *bh << dendl;
        continue;
      }
      
      // make sure bh tid matches
      if (bh->last_write_tid != tid) {
        assert(bh->last_write_tid > tid);
        dout(10) << "bh_write_ack newer tid on " << *bh << dendl;
        continue;
      }
      
      // ok!  mark bh clean.
      mark_clean(bh);
      dout(10) << "bh_write_ack clean " << *bh << dendl;
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

    // is the entire object set now clean?
    if (flush_set_callback && ob->oset->dirty_tx == 0) {
      flush_set_callback(flush_set_callback_arg, ob->oset);
    }
  }
  //lock.Unlock();
}

void ObjectCacher::bh_write_commit(sobject_t oid, loff_t start, uint64_t length, tid_t tid)
{
  //lock.Lock();
  
  // update object last_commit
  dout(7) << "bh_write_commit " 
          << oid 
          << " tid " << tid
          << " " << start << "~" << length
          << dendl;
  if (objects.count(oid) == 0) {
    dout(7) << "bh_write_commit no object cache" << dendl;
    //assert(0);
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

    // is the entire object set now clean and fully committed?
    if (commit_set_callback &&
	ob->last_commit_tid == ob->last_write_tid) {
      ob->uncommitted_item.remove_myself();
      ObjectSet *oset = ob->oset;
      if (ob->can_close())
	close_object(ob);
      if (oset->uncommitted.empty()) {  // no uncommitted in flight
	if (oset->dirty_tx == 0)        // AND nothing dirty/tx
	  commit_set_callback(flush_set_callback_arg, oset);      
      }
    }
  }

  //  lock.Unlock();
}


void ObjectCacher::flush(loff_t amount)
{
  utime_t cutoff = g_clock.now();
  //cutoff.sec_ref() -= g_conf.client_oc_max_dirty_age;

  dout(10) << "flush " << amount << dendl;
  
  /*
   * NOTE: we aren't actually pulling things off the LRU here, just looking at the
   * tail item.  Then we call bh_write, which moves it to the other LRU, so that we
   * can call lru_dirty.lru_get_next_expire() again.
   */
  loff_t did = 0;
  while (amount == 0 || did < amount) {
    BufferHead *bh = (BufferHead*) lru_dirty.lru_get_next_expire();
    if (!bh) break;
    if (bh->last_write > cutoff) break;

    did += bh->length();
    bh_write(bh);
  }    
}


void ObjectCacher::trim(loff_t max)
{
  if (max < 0) 
    max = g_conf.client_oc_size;
  
  dout(10) << "trim  start: max " << max 
           << "  clean " << get_stat_clean()
           << dendl;

  while (get_stat_clean() > max) {
    BufferHead *bh = (BufferHead*) lru_rest.lru_expire();
    if (!bh) break;
    
    dout(10) << "trim trimming " << *bh << dendl;
    assert(bh->is_clean());
    
    Object *ob = bh->ob;
    bh_remove(ob, bh);
    delete bh;
    
    if (ob->can_close()) {
      dout(10) << "trim trimming " << *ob << dendl;
      close_object(ob);
    }
  }
  
  dout(10) << "trim finish: max " << max 
           << "  clean " << get_stat_clean()
           << dendl;
}



/* public */

bool ObjectCacher::is_cached(ObjectSet *oset, vector<ObjectExtent>& extents, snapid_t snapid)
{
  for (vector<ObjectExtent>::iterator ex_it = extents.begin();
       ex_it != extents.end();
       ex_it++) {
    dout(10) << "is_cached " << *ex_it << dendl;

    // get Object cache
    sobject_t soid(ex_it->oid, snapid);
    Object *o = get_object_maybe(soid, ex_it->layout);
    if (!o)
      return false;
    if (!o->is_cached(ex_it->offset, ex_it->length))
      return false;
  }
  return true;
}


/*
 * returns # bytes read (if in cache).  onfinish is untouched (caller must delete it)
 * returns 0 if doing async read
 */
int ObjectCacher::readx(OSDRead *rd, ObjectSet *oset, Context *onfinish)
{
  bool success = true;
  list<BufferHead*> hit_ls;
  map<uint64_t, bufferlist> stripe_map;  // final buffer offset -> substring

  for (vector<ObjectExtent>::iterator ex_it = rd->extents.begin();
       ex_it != rd->extents.end();
       ex_it++) {
    dout(10) << "readx " << *ex_it << dendl;

    // get Object cache
    sobject_t soid(ex_it->oid, rd->snap);
    Object *o = get_object(soid, oset, ex_it->layout);
    
    // map extent into bufferheads
    map<loff_t, BufferHead*> hits, missing, rx;
    o->map_read(rd, hits, missing, rx);
    
    if (!missing.empty() || !rx.empty()) {
      // read missing
      for (map<loff_t, BufferHead*>::iterator bh_it = missing.begin();
           bh_it != missing.end();
           bh_it++) {
        bh_read(bh_it->second);
        if (success && onfinish) {
          dout(10) << "readx missed, waiting on " << *bh_it->second 
                   << " off " << bh_it->first << dendl;
	  bh_it->second->waitfor_read[bh_it->first].push_back( new C_RetryRead(this, rd, oset, onfinish) );
        }
	success = false;
      }

      // bump rx
      for (map<loff_t, BufferHead*>::iterator bh_it = rx.begin();
           bh_it != rx.end();
           bh_it++) {
        touch_bh(bh_it->second);        // bump in lru, so we don't lose it.
        if (success && onfinish) {
          dout(10) << "readx missed, waiting on " << *bh_it->second 
                   << " off " << bh_it->first << dendl;
	  bh_it->second->waitfor_read[bh_it->first].push_back( new C_RetryRead(this, rd, oset, onfinish) );
        }
	success = false;
      }      
    } else {
      assert(!hits.empty());

      // make a plain list
      for (map<loff_t, BufferHead*>::iterator bh_it = hits.begin();
           bh_it != hits.end();
           bh_it++) {
	dout(10) << "readx hit bh " << *bh_it->second << dendl;
        hit_ls.push_back(bh_it->second);
      }

      // create reverse map of buffer offset -> object for the eventual result.
      // this is over a single ObjectExtent, so we know that
      //  - the bh's are contiguous
      //  - the buffer frags need not be (and almost certainly aren't)
      loff_t opos = ex_it->offset;
      map<loff_t, BufferHead*>::iterator bh_it = hits.begin();
      assert(bh_it->second->start() <= opos);
      uint64_t bhoff = opos - bh_it->second->start();
      map<__u32,__u32>::iterator f_it = ex_it->buffer_extents.begin();
      uint64_t foff = 0;
      while (1) {
        BufferHead *bh = bh_it->second;
        assert(opos == (loff_t)(bh->start() + bhoff));

        dout(10) << "readx rmap opos " << opos
                 << ": " << *bh << " +" << bhoff
                 << " frag " << f_it->first << "~" << f_it->second << " +" << foff
                 << dendl;

        uint64_t len = MIN(f_it->second - foff,
                         bh->length() - bhoff);
	bufferlist bit;  // put substr here first, since substr_of clobbers, and
	                 // we may get multiple bh's at this stripe_map position
	bit.substr_of(bh->bl,
		      opos - bh->start(),
		      len);
        stripe_map[f_it->first].claim_append(bit);

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
      assert(opos == ex_it->offset + (loff_t)ex_it->length);
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
  dout(10) << "readx has all buffers" << dendl;
  
  // ok, assemble into result buffer.
  uint64_t pos = 0;
  if (rd->bl) {
    rd->bl->clear();
    for (map<uint64_t,bufferlist>::iterator i = stripe_map.begin();
	 i != stripe_map.end();
	 i++) {
      assert(pos == i->first);
      dout(10) << "readx  adding buffer len " << i->second.length() << " at " << pos << dendl;
      pos += i->second.length();
      rd->bl->claim_append(i->second);
      assert(rd->bl->length() == pos);
    }
    dout(10) << "readx  result is " << rd->bl->length() << dendl;
  } else {
    dout(10) << "readx  no bufferlist ptr (readahead?), done." << dendl;
  }

  // done with read.
  delete rd;

  trim();
  
  return pos;
}


int ObjectCacher::writex(OSDWrite *wr, ObjectSet *oset)
{
  utime_t now = g_clock.now();
  
  for (vector<ObjectExtent>::iterator ex_it = wr->extents.begin();
       ex_it != wr->extents.end();
       ex_it++) {
    // get object cache
    sobject_t soid(ex_it->oid, CEPH_NOSNAP);
    Object *o = get_object(soid, oset, ex_it->layout);

    // map it all into a single bufferhead.
    BufferHead *bh = o->map_write(wr);
    bh->snapc = wr->snapc;
    
    // adjust buffer pointers (ie "copy" data into my cache)
    // this is over a single ObjectExtent, so we know that
    //  - there is one contiguous bh
    //  - the buffer frags need not be (and almost certainly aren't)
    // note: i assume striping is monotonic... no jumps backwards, ever!
    loff_t opos = ex_it->offset;
    for (map<__u32,__u32>::iterator f_it = ex_it->buffer_extents.begin();
         f_it != ex_it->buffer_extents.end();
         f_it++) {
      dout(10) << "writex writing " << f_it->first << "~" << f_it->second << " into " << *bh << " at " << opos << dendl;
      uint64_t bhoff = bh->start() - opos;
      assert(f_it->second <= bh->length() - bhoff);

      // get the frag we're mapping in
      bufferlist frag; 
      frag.substr_of(wr->bl, 
                     f_it->first, f_it->second);

      // keep anything left of bhoff
      bufferlist newbl;
      if (bhoff)
	newbl.substr_of(bh->bl, 0, bhoff);
      newbl.claim_append(frag);
      bh->bl.swap(newbl);

      opos += f_it->second;
    }

    // ok, now bh is dirty.
    mark_dirty(bh);
    touch_bh(bh);
    bh->last_write = now;

    o->try_merge_bh(bh);
  }

  delete wr;

  //verify_stats();
  trim();
  return 0;
}
 

// blocking wait for write.
bool ObjectCacher::wait_for_write(uint64_t len, Mutex& lock)
{
  int blocked = 0;

  // wait for writeback?
  while (get_stat_dirty() + get_stat_tx() >= g_conf.client_oc_max_dirty) {
    dout(10) << "wait_for_write waiting on " << len << ", dirty|tx " 
	     << (get_stat_dirty() + get_stat_tx()) 
	     << " >= " << g_conf.client_oc_max_dirty 
	     << dendl;
    flusher_cond.Signal();
    stat_waiter++;
    stat_cond.Wait(lock);
    stat_waiter--;
    blocked++;
    dout(10) << "wait_for_write woke up" << dendl;
  }

  // start writeback anyway?
  if (get_stat_dirty() > g_conf.client_oc_target_dirty) {
    dout(10) << "wait_for_write " << get_stat_dirty() << " > target "
	     << g_conf.client_oc_target_dirty << ", nudging flusher" << dendl;
    flusher_cond.Signal();
  }
  return blocked;
}

void ObjectCacher::flusher_entry()
{
  dout(10) << "flusher start" << dendl;
  lock.Lock();
  while (!flusher_stop) {
    while (!flusher_stop) {
      loff_t all = get_stat_tx() + get_stat_rx() + get_stat_clean() + get_stat_dirty();
      dout(11) << "flusher "
               << all << " / " << g_conf.client_oc_size << ":  "
               << get_stat_tx() << " tx, "
               << get_stat_rx() << " rx, "
               << get_stat_clean() << " clean, "
               << get_stat_dirty() << " dirty ("
	       << g_conf.client_oc_target_dirty << " target, "
	       << g_conf.client_oc_max_dirty << " max)"
               << dendl;
      if (get_stat_dirty() > g_conf.client_oc_target_dirty) {
        // flush some dirty pages
        dout(10) << "flusher " 
                 << get_stat_dirty() << " dirty > target "
		 << g_conf.client_oc_target_dirty
                 << ", flushing some dirty bhs" << dendl;
        flush(get_stat_dirty() - g_conf.client_oc_target_dirty);
      }
      else {
        // check tail of lru for old dirty items
        utime_t cutoff = g_clock.now();
        cutoff.sec_ref()--;
        BufferHead *bh = 0;
        while ((bh = (BufferHead*)lru_dirty.lru_get_next_expire()) != 0 &&
               bh->last_write < cutoff) {
          dout(10) << "flusher flushing aged dirty bh " << *bh << dendl;
          bh_write(bh);
        }
        break;
      }
    }
    if (flusher_stop) break;
    flusher_cond.WaitInterval(lock, utime_t(1,0));
  }
  lock.Unlock();
  dout(10) << "flusher finish" << dendl;
}


  
// blocking.  atomic+sync.
int ObjectCacher::atomic_sync_readx(OSDRead *rd, ObjectSet *oset, Mutex& lock)
{
  dout(10) << "atomic_sync_readx " << rd
           << " in " << oset
           << dendl;

  if (rd->extents.size() == 1) {
    // single object.
    // just write synchronously.
    Mutex flock("ObjectCacher::atomic_sync_readx flock 1");
    Cond cond;
    bool done = false;
    objecter->read_trunc(rd->extents[0].oid, rd->extents[0].layout, 
		   rd->extents[0].offset, rd->extents[0].length,
		   rd->snap, rd->bl, 0,
		   oset->truncate_size, oset->truncate_seq,
		   new C_SafeCond(&flock, &cond, &done));

    // block
    while (!done) cond.Wait(lock);
  } else {
    // spans multiple objects, or is big.

    // sort by object...
    map<object_t,ObjectExtent> by_oid;
    for (vector<ObjectExtent>::iterator ex_it = rd->extents.begin();
         ex_it != rd->extents.end();
         ex_it++) 
      by_oid[ex_it->oid] = *ex_it;
    
    // lock
    for (map<object_t,ObjectExtent>::iterator i = by_oid.begin();
         i != by_oid.end();
         i++) {
      sobject_t soid(i->first, rd->snap);
      Object *o = get_object(soid, oset, i->second.layout);
      rdlock(o);
    }

    // readx will hose rd
    vector<ObjectExtent> extents = rd->extents;

    // do the read, into our cache
    Mutex flock("ObjectCacher::atomic_sync_readx flock 2");
    Cond cond;
    bool done = false;
    readx(rd, oset, new C_SafeCond(&flock, &cond, &done));
    
    // block
    while (!done) cond.Wait(lock);
    
    // release the locks
    for (vector<ObjectExtent>::iterator ex_it = extents.begin();
         ex_it != extents.end();
         ex_it++) {
      sobject_t soid(ex_it->oid, rd->snap);
      assert(objects.count(soid));
      Object *o = objects[soid];
      rdunlock(o);
    }
  }

  return 0;
}

int ObjectCacher::atomic_sync_writex(OSDWrite *wr, ObjectSet *oset, Mutex& lock)
{
  dout(10) << "atomic_sync_writex " << wr
           << " in " << oset
           << dendl;

  if (wr->extents.size() == 1 &&
      wr->extents.front().length <= g_conf.client_oc_max_sync_write) {
    // single object.
    
    // make sure we aren't already locking/locked...
    sobject_t oid(wr->extents.front().oid, CEPH_NOSNAP);
    Object *o = 0;
    if (objects.count(oid))
      o = get_object(oid, oset, wr->extents.front().layout);
    if (!o || 
        (o->lock_state != Object::LOCK_WRLOCK &&
         o->lock_state != Object::LOCK_WRLOCKING &&
         o->lock_state != Object::LOCK_UPGRADING)) {
      // just write synchronously.
      dout(10) << "atomic_sync_writex " << wr
               << " in " << oset
               << " doing sync write"
               << dendl;

      Mutex flock("ObjectCacher::atomic_sync_writex flock");
      Cond cond;
      bool done = false;
      objecter->sg_write_trunc(wr->extents, wr->snapc, wr->bl, wr->mtime, 0,
			 oset->truncate_size, oset->truncate_seq,
			 new C_SafeCond(&flock, &cond, &done), 0);
      
      // block
      while (!done) cond.Wait(lock);
      return 0;
    }
  } 

  // spans multiple objects, or is big.
  // sort by object...
  map<object_t,ObjectExtent> by_oid;
  for (vector<ObjectExtent>::iterator ex_it = wr->extents.begin();
       ex_it != wr->extents.end();
       ex_it++) 
    by_oid[ex_it->oid] = *ex_it;
  
  // wrlock
  for (map<object_t,ObjectExtent>::iterator i = by_oid.begin();
       i != by_oid.end();
       i++) {
    sobject_t soid(i->first, CEPH_NOSNAP);
    Object *o = get_object(soid, oset, i->second.layout);
    wrlock(o);
  }
  
  // writex will hose wr
  vector<ObjectExtent> extents = wr->extents;

  // do the write, into our cache
  writex(wr, oset);
  
  // flush 
  // ...and release the locks?
  for (vector<ObjectExtent>::iterator ex_it = extents.begin();
       ex_it != extents.end();
       ex_it++) {
    sobject_t soid(ex_it->oid, CEPH_NOSNAP);
    assert(objects.count(soid));
    Object *o = objects[soid];
    
    wrunlock(o);
  }

  return 0;
}
 


// locking -----------------------------

void ObjectCacher::rdlock(Object *o)
{
  // lock?
  if (o->lock_state == Object::LOCK_NONE ||
      o->lock_state == Object::LOCK_RDUNLOCKING ||
      o->lock_state == Object::LOCK_WRUNLOCKING) {
    dout(10) << "rdlock rdlock " << *o << dendl;
    
    o->lock_state = Object::LOCK_RDLOCKING;
    
    C_LockAck *ack = new C_LockAck(this, o->get_soid());
    C_WriteCommit *commit = new C_WriteCommit(this, o->get_soid(), 0, 0);
    
    commit->tid = 
      ack->tid = 
      o->last_write_tid = objecter->lock(o->get_oid(), o->get_layout(), CEPH_OSD_OP_RDLOCK, 0, ack, commit);
  }
  
  // stake our claim.
  o->rdlock_ref++;  
  
  // wait?
  if (o->lock_state == Object::LOCK_RDLOCKING ||
      o->lock_state == Object::LOCK_WRLOCKING) {
    dout(10) << "rdlock waiting for rdlock|wrlock on " << *o << dendl;
    Mutex flock("ObjectCacher::rdlock flock");
    Cond cond;
    bool done = false;
    o->waitfor_rd.push_back(new C_SafeCond(&flock, &cond, &done));
    while (!done) cond.Wait(lock);
  }
  assert(o->lock_state == Object::LOCK_RDLOCK ||
         o->lock_state == Object::LOCK_WRLOCK ||
         o->lock_state == Object::LOCK_UPGRADING ||
         o->lock_state == Object::LOCK_DOWNGRADING);
}

void ObjectCacher::wrlock(Object *o)
{
  // lock?
  if (o->lock_state != Object::LOCK_WRLOCK &&
      o->lock_state != Object::LOCK_WRLOCKING &&
      o->lock_state != Object::LOCK_UPGRADING) {
    dout(10) << "wrlock wrlock " << *o << dendl;
    
    int op = 0;
    if (o->lock_state == Object::LOCK_RDLOCK) {
      o->lock_state = Object::LOCK_UPGRADING;
      op = CEPH_OSD_OP_UPLOCK;
    } else {
      o->lock_state = Object::LOCK_WRLOCKING;
      op = CEPH_OSD_OP_WRLOCK;
    }
    
    C_LockAck *ack = new C_LockAck(this, o->get_soid());
    C_WriteCommit *commit = new C_WriteCommit(this, o->get_soid(), 0, 0);
    
    commit->tid = 
      ack->tid = 
      o->last_write_tid = objecter->lock(o->get_oid(), o->get_layout(), op, 0, ack, commit);
  }
  
  // stake our claim.
  o->wrlock_ref++;  
  
  // wait?
  if (o->lock_state == Object::LOCK_WRLOCKING ||
      o->lock_state == Object::LOCK_UPGRADING) {
    dout(10) << "wrlock waiting for wrlock on " << *o << dendl;
    Mutex flock("ObjectCacher::wrlock flock");
    Cond cond;
    bool done = false;
    o->waitfor_wr.push_back(new C_SafeCond(&flock, &cond, &done));
    while (!done) cond.Wait(lock);
  }
  assert(o->lock_state == Object::LOCK_WRLOCK);
}


void ObjectCacher::rdunlock(Object *o)
{
  dout(10) << "rdunlock " << *o << dendl;
  assert(o->lock_state == Object::LOCK_RDLOCK ||
         o->lock_state == Object::LOCK_WRLOCK ||
         o->lock_state == Object::LOCK_UPGRADING ||
         o->lock_state == Object::LOCK_DOWNGRADING);

  assert(o->rdlock_ref > 0);
  o->rdlock_ref--;
  if (o->rdlock_ref > 0 ||
      o->wrlock_ref > 0) {
    dout(10) << "rdunlock " << *o << " still has rdlock|wrlock refs" << dendl;
    return;
  }

  release(o);  // release first

  o->lock_state = Object::LOCK_RDUNLOCKING;

  C_LockAck *lockack = new C_LockAck(this, o->get_soid());
  C_WriteCommit *commit = new C_WriteCommit(this, o->get_soid(), 0, 0);
  commit->tid = 
    lockack->tid = 
    o->last_write_tid = objecter->lock(o->get_oid(), o->get_layout(), CEPH_OSD_OP_RDUNLOCK, 0, lockack, commit);
}

void ObjectCacher::wrunlock(Object *o)
{
  dout(10) << "wrunlock " << *o << dendl;
  assert(o->lock_state == Object::LOCK_WRLOCK);

  assert(o->wrlock_ref > 0);
  o->wrlock_ref--;
  if (o->wrlock_ref > 0) {
    dout(10) << "wrunlock " << *o << " still has wrlock refs" << dendl;
    return;
  }

  flush(o);  // flush first

  int op = 0;
  if (o->rdlock_ref > 0) {
    dout(10) << "wrunlock rdlock " << *o << dendl;
    op = CEPH_OSD_OP_DNLOCK;
    o->lock_state = Object::LOCK_DOWNGRADING;
  } else {
    dout(10) << "wrunlock wrunlock " << *o << dendl;
    op = CEPH_OSD_OP_WRUNLOCK;
    o->lock_state = Object::LOCK_WRUNLOCKING;
  }

  C_LockAck *lockack = new C_LockAck(this, o->get_soid());
  C_WriteCommit *commit = new C_WriteCommit(this, o->get_soid(), 0, 0);
  commit->tid = 
    lockack->tid = 
    o->last_write_tid = objecter->lock(o->get_oid(), o->get_layout(), op, 0, lockack, commit);
}


// -------------------------------------------------


bool ObjectCacher::set_is_cached(ObjectSet *oset)
{
  if (oset->objects.empty())
    return false;
  
  for (xlist<Object*>::iterator p = oset->objects.begin();
       !p.end(); ++p) {
    Object *ob = *p;
    for (map<loff_t,BufferHead*>::iterator q = ob->data.begin();
         q != ob->data.end();
         q++) {
      BufferHead *bh = q->second;
      if (!bh->is_dirty() && !bh->is_tx()) 
        return true;
    }
  }

  return false;
}

bool ObjectCacher::set_is_dirty_or_committing(ObjectSet *oset)
{
  if (oset->objects.empty())
    return false;
  
  for (xlist<Object*>::iterator i = oset->objects.begin();
       !i.end(); ++i) {
    Object *ob = *i;
    
    for (map<loff_t,BufferHead*>::iterator p = ob->data.begin();
         p != ob->data.end();
         p++) {
      BufferHead *bh = p->second;
      if (bh->is_dirty() || bh->is_tx()) 
        return true;
    }
  }  
  
  return false;
}


// purge.  non-blocking.  violently removes dirty buffers from cache.
void ObjectCacher::purge(Object *ob)
{
  dout(10) << "purge " << *ob << dendl;

  while (!ob->data.empty()) {
    BufferHead *bh = ob->data.begin()->second;
    if (!bh->is_clean())
      dout(0) << "purge forcibly removing " << *ob << " " << *bh << dendl;
    bh_remove(ob, bh);
    delete bh;
  }
  
  if (ob->can_close()) {
    dout(10) << "trim trimming " << *ob << dendl;
    close_object(ob);
  }
}

// flush.  non-blocking.  no callback.
// true if clean, already flushed.  
// false if we wrote something.
bool ObjectCacher::flush(Object *ob)
{
  bool clean = true;
  for (map<loff_t,BufferHead*>::iterator p = ob->data.begin();
       p != ob->data.end();
       p++) {
    BufferHead *bh = p->second;
    if (bh->is_tx()) {
      clean = false;
      continue;
    }
    if (!bh->is_dirty()) continue;
    
    bh_write(bh);
    clean = false;
  }
  return clean;
}

// flush.  non-blocking, takes callback.
// returns true if already flushed
bool ObjectCacher::flush_set(ObjectSet *oset, Context *onfinish)
{
  if (oset->objects.empty()) {
    dout(10) << "flush_set on " << oset << " dne" << dendl;
    return true;
  }

  dout(10) << "flush_set " << oset << dendl;

  C_Gather *gather = 0; // we'll need to wait for all objects to flush!

  bool safe = true;
  for (xlist<Object*>::iterator i = oset->objects.begin();
       !i.end(); ++i) {
    Object *ob = *i;

    if (!flush(ob)) {
      // we'll need to gather...
      if (!gather && onfinish) 
        gather = new C_Gather(onfinish);
      safe = false;

      dout(10) << "flush_set " << oset << " will wait for ack tid " 
               << ob->last_write_tid 
               << " on " << *ob
               << dendl;
      if (gather)
        ob->waitfor_ack[ob->last_write_tid].push_back(gather->new_sub());
    }
  }
  
  if (safe) {
    dout(10) << "flush_set " << oset << " has no dirty|tx bhs" << dendl;
    return true;
  }
  return false;
}


// commit.  non-blocking, takes callback.
// return true if already flushed.
bool ObjectCacher::commit_set(ObjectSet *oset, Context *onfinish)
{
  assert(onfinish);  // doesn't make any sense otherwise.

  if (oset->objects.empty()) {
    dout(10) << "commit_set on " << oset << " dne" << dendl;
    return true;
  }

  dout(10) << "commit_set " << oset << dendl;

  // make sure it's flushing.
  flush_set(oset);

  C_Gather *gather = 0; // we'll need to wait for all objects to commit

  bool safe = true;
  for (xlist<Object*>::iterator i = oset->objects.begin();
       !i.end(); ++i) {
    Object *ob = *i;
    
    if (ob->last_write_tid > ob->last_commit_tid) {
      dout(10) << "commit_set " << oset << " " << *ob 
               << " will finish on commit tid " << ob->last_write_tid
               << dendl;
      if (!gather && onfinish) gather = new C_Gather(onfinish);
      safe = false;
      if (gather)
        ob->waitfor_commit[ob->last_write_tid].push_back( gather->new_sub() );
    }
  }

  if (safe) {
    dout(10) << "commit_set " << oset << " all committed" << dendl;
    return true;
  }
  return false;
}

void ObjectCacher::purge_set(ObjectSet *oset)
{
  if (oset->objects.empty()) {
    dout(10) << "purge_set on " << oset << " dne" << dendl;
    return;
  }

  dout(10) << "purge_set " << oset << dendl;

  for (xlist<Object*>::iterator i = oset->objects.begin();
       !i.end(); ++i) {
    Object *ob = *i;
	purge(ob);
  }
}


loff_t ObjectCacher::release(Object *ob)
{
  list<BufferHead*> clean;
  loff_t o_unclean = 0;

  for (map<loff_t,BufferHead*>::iterator p = ob->data.begin();
       p != ob->data.end();
       p++) {
    BufferHead *bh = p->second;
    if (bh->is_clean()) 
	  clean.push_back(bh);
    else 
      o_unclean += bh->length();
  }

  for (list<BufferHead*>::iterator p = clean.begin();
	   p != clean.end();
	   p++) {
	bh_remove(ob, *p);
	delete *p;
  }

  if (ob->can_close()) {
	dout(10) << "trim trimming " << *ob << dendl;
	close_object(ob);
  }

  return o_unclean;
}

loff_t ObjectCacher::release_set(ObjectSet *oset)
{
  // return # bytes not clean (and thus not released).
  loff_t unclean = 0;

  if (oset->objects.empty()) {
    dout(10) << "release_set on " << oset << " dne" << dendl;
    return 0;
  }

  dout(10) << "release_set " << oset << dendl;

  xlist<Object*>::iterator q;
  for (xlist<Object*>::iterator p = oset->objects.begin();
       !p.end(); ) {
    q = p;
    ++q;
    Object *ob = *p;

    loff_t o_unclean = release(ob);
    unclean += o_unclean;

    if (o_unclean) 
      dout(10) << "release_set " << oset << " " << *ob 
               << " has " << o_unclean << " bytes left"
               << dendl;
    p = q;
  }

  if (unclean) {
    dout(10) << "release_set " << oset
             << ", " << unclean << " bytes left" << dendl;
  }

  return unclean;
}


uint64_t ObjectCacher::release_all()
{
  dout(10) << "release_all" << dendl;
  uint64_t unclean = 0;
  
  hash_map<sobject_t, Object*>::iterator p = objects.begin();
  while (p != objects.end()) {
    hash_map<sobject_t, Object*>::iterator n = p;
    n++;

    Object *ob = p->second;

    loff_t o_unclean = release(ob);
    unclean += o_unclean;

    if (o_unclean) 
      dout(10) << "release_all " << *ob 
               << " has " << o_unclean << " bytes left"
               << dendl;
  }

  if (unclean) {
    dout(10) << "release_all unclean " << unclean << " bytes left" << dendl;
  }

  return unclean;
}




void ObjectCacher::truncate_set(ObjectSet *oset, vector<ObjectExtent>& exls)
{
  if (oset->objects.empty()) {
    dout(10) << "truncate_set on " << oset << " dne" << dendl;
    return;
  }
  
  dout(10) << "truncate_set " << oset << dendl;

  for (vector<ObjectExtent>::iterator p = exls.begin();
       p != exls.end();
       ++p) {
    ObjectExtent &ex = *p;
    sobject_t soid(ex.oid, CEPH_NOSNAP);
    if (objects.count(soid) == 0)
      continue;
    Object *ob = objects[soid];
    
    // purge or truncate?
    if (ex.offset == 0) {
      dout(10) << "truncate_set purging " << *ob << dendl;
      purge(ob);
    } else {
      // hrm, truncate object
      dout(10) << "truncate_set truncating " << *ob << " at " << ex.offset << dendl;
      ob->truncate(ex.offset);
      
      if (ob->can_close()) {
	dout(10) << "truncate_set trimming " << *ob << dendl;
	close_object(ob);
      }
    }
  }
}


void ObjectCacher::kick_sync_writers(ObjectSet *oset)
{
  if (oset->objects.empty()) {
    dout(10) << "kick_sync_writers on " << oset << " dne" << dendl;
    return;
  }

  dout(10) << "kick_sync_writers on " << oset << dendl;

  list<Context*> ls;

  for (xlist<Object*>::iterator i = oset->objects.begin();
       !i.end(); ++i) {
    Object *ob = *i;
    
    ls.splice(ls.begin(), ob->waitfor_wr);
  }

  finish_contexts(ls);
}

void ObjectCacher::kick_sync_readers(ObjectSet *oset)
{
  if (oset->objects.empty()) {
    dout(10) << "kick_sync_readers on " << oset << " dne" << dendl;
    return;
  }

  dout(10) << "kick_sync_readers on " << oset << dendl;

  list<Context*> ls;

  for (xlist<Object*>::iterator i = oset->objects.begin();
       !i.end(); ++i) {
    Object *ob = *i;
    
    ls.splice(ls.begin(), ob->waitfor_rd);
  }

  finish_contexts(ls);
}



void ObjectCacher::verify_stats() const
{
  dout(10) << "verify_stats" << dendl;

  loff_t clean = 0, dirty = 0, rx = 0, tx = 0, missing = 0;

  for (hash_map<sobject_t, Object*>::const_iterator p = objects.begin();
       p != objects.end();
       p++) {
    Object *ob = p->second;
    for (map<loff_t, BufferHead*>::const_iterator q = ob->data.begin();
	 q != ob->data.end();
	 q++) {
      BufferHead *bh = q->second;
      switch (bh->get_state()) {
      case BufferHead::STATE_MISSING:
	missing += bh->length();
	break;
      case BufferHead::STATE_CLEAN:
	clean += bh->length();
	break;
      case BufferHead::STATE_DIRTY: 
	dirty += bh->length(); 
	break;
      case BufferHead::STATE_TX: 
	tx += bh->length(); 
	break;
      case BufferHead::STATE_RX:
	rx += bh->length();
	break;
      default:
	assert(0);
      }
    }
  }

  dout(10) << " clean " << clean
	   << " rx " << rx 
	   << " tx " << tx
	   << " dirty " << dirty
	   << " missing " << missing
	   << dendl;
  assert(clean == stat_clean);
  assert(rx == stat_rx);
  assert(tx == stat_tx);
  assert(dirty == stat_dirty);
  assert(missing == stat_missing);
}

