// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <limits.h>

#include "msg/Messenger.h"
#include "ObjectCacher.h"
#include "WritebackHandler.h"
#include "common/errno.h"
#include "common/perf_counters.h"

#include "include/assert.h"

#define MAX_FLUSH_UNDER_LOCK 20  ///< max bh's we start writeback on

using std::chrono::seconds;
				 /// while holding the lock

/*** ObjectCacher::BufferHead ***/


/*** ObjectCacher::Object ***/

#define dout_subsys ceph_subsys_objectcacher
#undef dout_prefix
#define dout_prefix *_dout << "objectcacher.object(" << oid << ") "



ObjectCacher::BufferHead *ObjectCacher::Object::split(BufferHead *left,
						      loff_t off)
{
  assert(oc->lock.is_locked());
  ldout(oc->cct, 20) << "split " << *left << " at " << off << dendl;

  // split off right
  ObjectCacher::BufferHead *right = new BufferHead(this);

  //inherit and if later access, this auto clean.
  right->set_dontneed(left->get_dontneed());
  right->set_nocache(left->get_nocache());

  right->last_write_tid = left->last_write_tid;
  right->last_read_tid = left->last_read_tid;
  right->set_state(left->get_state());
  right->snapc = left->snapc;
  right->set_journal_tid(left->journal_tid);

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
    map<loff_t, list<Context*> >::iterator start_remove
      = left->waitfor_read.begin();
    while (start_remove != left->waitfor_read.end() &&
	   start_remove->first < right->start())
      ++start_remove;
    for (map<loff_t, list<Context*> >::iterator p = start_remove;
	 p != left->waitfor_read.end(); ++p) {
      ldout(oc->cct, 20) << "split  moving waiters at byte " << p->first
			 << " to right bh" << dendl;
      right->waitfor_read[p->first].swap( p->second );
      assert(p->second.empty());
    }
    left->waitfor_read.erase(start_remove, left->waitfor_read.end());
  }

  ldout(oc->cct, 20) << "split    left is " << *left << dendl;
  ldout(oc->cct, 20) << "split   right is " << *right << dendl;
  return right;
}


void ObjectCacher::Object::merge_left(BufferHead *left, BufferHead *right)
{
  assert(oc->lock.is_locked());
  assert(left->end() == right->start());
  assert(left->get_state() == right->get_state());
  assert(left->can_merge_journal(right));

  ldout(oc->cct, 10) << "merge_left " << *left << " + " << *right << dendl;
  if (left->get_journal_tid() == 0) {
    left->set_journal_tid(right->get_journal_tid());
  }
  right->set_journal_tid(0);

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

  left->set_dontneed(right->get_dontneed() ? left->get_dontneed() : false);
  left->set_nocache(right->get_nocache() ? left->get_nocache() : false);

  // waiters
  for (map<loff_t, list<Context*> >::iterator p = right->waitfor_read.begin();
       p != right->waitfor_read.end();
       ++p)
    left->waitfor_read[p->first].splice(left->waitfor_read[p->first].begin(),
					p->second );

  // hose right
  delete right;

  ldout(oc->cct, 10) << "merge_left result " << *left << dendl;
}

void ObjectCacher::Object::try_merge_bh(BufferHead *bh)
{
  assert(oc->lock.is_locked());
  ldout(oc->cct, 10) << "try_merge_bh " << *bh << dendl;

  // do not merge rx buffers; last_read_tid may not match
  if (bh->is_rx())
    return;

  // to the left?
  map<loff_t,BufferHead*>::iterator p = data.find(bh->start());
  assert(p->second == bh);
  if (p != data.begin()) {
    --p;
    if (p->second->end() == bh->start() &&
	p->second->get_state() == bh->get_state() &&
	p->second->can_merge_journal(bh)) {
      merge_left(p->second, bh);
      bh = p->second;
    } else {
      ++p;
    }
  }
  // to the right?
  assert(p->second == bh);
  ++p;
  if (p != data.end() &&
      p->second->start() == bh->end() &&
      p->second->get_state() == bh->get_state() &&
      p->second->can_merge_journal(bh))
    merge_left(bh, p->second);
}

/*
 * count bytes we have cached in given range
 */
bool ObjectCacher::Object::is_cached(loff_t cur, loff_t left)
{
  assert(oc->lock.is_locked());
  map<loff_t, BufferHead*>::iterator p = data_lower_bound(cur);
  while (left > 0) {
    if (p == data.end())
      return false;

    if (p->first <= cur) {
      // have part of it
      loff_t lenfromcur = MIN(p->second->end() - cur, left);
      cur += lenfromcur;
      left -= lenfromcur;
      ++p;
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
 * all cached data in this range[off, off+len]
 */
bool ObjectCacher::Object::include_all_cached_data(loff_t off, loff_t len)
{
  assert(oc->lock.is_locked());
  if (data.empty())
      return true;
  map<loff_t, BufferHead*>::iterator first = data.begin();
  map<loff_t, BufferHead*>::reverse_iterator last = data.rbegin();
  if (first->second->start() >= off && last->second->end() <= (off + len))
    return true;
  else
    return false;
}

/*
 * map a range of bytes into buffer_heads.
 * - create missing buffer_heads as necessary.
 */
int ObjectCacher::Object::map_read(ObjectExtent &ex,
                                   map<loff_t, BufferHead*>& hits,
                                   map<loff_t, BufferHead*>& missing,
                                   map<loff_t, BufferHead*>& rx,
				   map<loff_t, BufferHead*>& errors)
{
  assert(oc->lock.is_locked());
  ldout(oc->cct, 10) << "map_read " << ex.oid 
      	       << " " << ex.offset << "~" << ex.length
      	       << dendl;
  
  loff_t cur = ex.offset;
  loff_t left = ex.length;

  map<loff_t, BufferHead*>::iterator p = data_lower_bound(ex.offset);
  while (left > 0) {
    // at end?
    if (p == data.end()) {
      // rest is a miss.
      BufferHead *n = new BufferHead(this);
      n->set_start(cur);
      n->set_length(left);
      oc->bh_add(this, n);
      if (complete) {
        oc->mark_zero(n);
        hits[cur] = n;
        ldout(oc->cct, 20) << "map_read miss+complete+zero " << left << " left, " << *n << dendl;
      } else {
        missing[cur] = n;
        ldout(oc->cct, 20) << "map_read miss " << left << " left, " << *n << dendl;
      }
      cur += left;
      assert(cur == (loff_t)ex.offset + (loff_t)ex.length);
      break;  // no more.
    }
    
    if (p->first <= cur) {
      // have it (or part of it)
      BufferHead *e = p->second;

      if (e->is_clean() ||
          e->is_dirty() ||
          e->is_tx() ||
          e->is_zero()) {
        hits[cur] = e;     // readable!
        ldout(oc->cct, 20) << "map_read hit " << *e << dendl;
      } else if (e->is_rx()) {
        rx[cur] = e;       // missing, not readable.
        ldout(oc->cct, 20) << "map_read rx " << *e << dendl;
      } else if (e->is_error()) {
        errors[cur] = e;
        ldout(oc->cct, 20) << "map_read error " << *e << dendl;
      } else {
        assert(0);
      }
      
      loff_t lenfromcur = MIN(e->end() - cur, left);
      cur += lenfromcur;
      left -= lenfromcur;
      ++p;
      continue;  // more?
      
    } else if (p->first > cur) {
      // gap.. miss
      loff_t next = p->first;
      BufferHead *n = new BufferHead(this);
      loff_t len = MIN(next - cur, left);
      n->set_start(cur);
      n->set_length(len);
      oc->bh_add(this,n);
      if (complete) {
        oc->mark_zero(n);
        hits[cur] = n;
        ldout(oc->cct, 20) << "map_read gap+complete+zero " << *n << dendl;
      } else {
        missing[cur] = n;
        ldout(oc->cct, 20) << "map_read gap " << *n << dendl;
      }
      cur += MIN(left, n->length());
      left -= MIN(left, n->length());
      continue;    // more?
    } else {
      assert(0);
    }
  }
  return 0;
}

void ObjectCacher::Object::audit_buffers()
{
  loff_t offset = 0;
  for (map<loff_t, BufferHead*>::const_iterator it = data.begin();
       it != data.end(); ++it) {
    if (it->first != it->second->start()) {
      lderr(oc->cct) << "AUDIT FAILURE: map position " << it->first
		     << " does not match bh start position: "
		     << *it->second << dendl;
      assert(it->first == it->second->start());
    }
    if (it->first < offset) {
      lderr(oc->cct) << "AUDIT FAILURE: " << it->first << " " << *it->second
		     << " overlaps with previous bh " << *((--it)->second)
		     << dendl;
      assert(it->first >= offset);
    }
    BufferHead *bh = it->second;
    map<loff_t, list<Context*> >::const_iterator w_it;
    for (w_it = bh->waitfor_read.begin();
	 w_it != bh->waitfor_read.end(); ++w_it) {
      if (w_it->first < bh->start() ||
	    w_it->first >= bh->start() + bh->length()) {
	lderr(oc->cct) << "AUDIT FAILURE: waiter at " << w_it->first
		       << " is not within bh " << *bh << dendl;
	assert(w_it->first >= bh->start());
	assert(w_it->first < bh->start() + bh->length());
      }
    }
    offset = it->first + it->second->length();
  }
}

/*
 * map a range of extents on an object's buffer cache.
 * - combine any bh's we're writing into one
 * - break up bufferheads that don't fall completely within the range
 * //no! - return a bh that includes the write.  may also include
 * other dirty data to left and/or right.
 */
ObjectCacher::BufferHead *ObjectCacher::Object::map_write(ObjectExtent &ex,
    ceph_tid_t tid)
{
  assert(oc->lock.is_locked());
  BufferHead *final = 0;

  ldout(oc->cct, 10) << "map_write oex " << ex.oid
      	       << " " << ex.offset << "~" << ex.length << dendl;

  loff_t cur = ex.offset;
  loff_t left = ex.length;

  map<loff_t, BufferHead*>::iterator p = data_lower_bound(ex.offset);
  while (left > 0) {
    loff_t max = left;

    // at end ?
    if (p == data.end()) {
      if (final == NULL) {
        final = new BufferHead(this);
        replace_journal_tid(final, tid);
        final->set_start( cur );
        final->set_length( max );
        oc->bh_add(this, final);
        ldout(oc->cct, 10) << "map_write adding trailing bh " << *final << dendl;
      } else {
        oc->bh_stat_sub(final);
        final->set_length(final->length() + max);
        oc->bh_stat_add(final);
      }
      left -= max;
      cur += max;
      continue;
    }

    ldout(oc->cct, 10) << "cur is " << cur << ", p is " << *p->second << dendl;
    //oc->verify_stats();

    if (p->first <= cur) {
      BufferHead *bh = p->second;
      ldout(oc->cct, 10) << "map_write bh " << *bh << " intersected" << dendl;

      if (p->first < cur) {
        assert(final == 0);
        if (cur + max >= bh->end()) {
          // we want right bit (one splice)
          final = split(bh, cur);   // just split it, take right half.
          replace_journal_tid(final, tid);
          ++p;
          assert(p->second == final);
        } else {
          // we want middle bit (two splices)
          final = split(bh, cur);
          ++p;
          assert(p->second == final);
          split(final, cur+max);
          replace_journal_tid(final, tid);
        }
      } else {
        assert(p->first == cur);
        if (bh->length() <= max) {
          // whole bufferhead, piece of cake.
        } else {
          // we want left bit (one splice)
          split(bh, cur + max);        // just split
        }
        if (final) {
          oc->mark_dirty(bh);
          oc->mark_dirty(final);
          --p;  // move iterator back to final
          assert(p->second == final);
          replace_journal_tid(bh, tid);
          merge_left(final, bh);
        } else {
          final = bh;
          replace_journal_tid(final, tid);
        }
      }

      // keep going.
      loff_t lenfromcur = final->end() - cur;
      cur += lenfromcur;
      left -= lenfromcur;
      ++p;
      continue;
    } else {
      // gap!
      loff_t next = p->first;
      loff_t glen = MIN(next - cur, max);
      ldout(oc->cct, 10) << "map_write gap " << cur << "~" << glen << dendl;
      if (final) {
        oc->bh_stat_sub(final);
        final->set_length(final->length() + glen);
        oc->bh_stat_add(final);
      } else {
        final = new BufferHead(this);
	replace_journal_tid(final, tid);
        final->set_start( cur );
        final->set_length( glen );
        oc->bh_add(this, final);
      }

      cur += glen;
      left -= glen;
      continue;    // more?
    }
  }

  // set version
  assert(final);
  assert(final->get_journal_tid() == tid);
  ldout(oc->cct, 10) << "map_write final is " << *final << dendl;

  return final;
}

void ObjectCacher::Object::replace_journal_tid(BufferHead *bh,
					       ceph_tid_t tid) {
  ceph_tid_t bh_tid = bh->get_journal_tid();

  assert(tid == 0 || bh_tid <= tid);
  if (bh_tid != 0 && bh_tid != tid) {
    // inform journal that it should not expect a writeback from this extent
    oc->writeback_handler.overwrite_extent(get_oid(), bh->start(),
					   bh->length(), bh_tid, tid);
  }
  bh->set_journal_tid(tid);
}

void ObjectCacher::Object::truncate(loff_t s)
{
  assert(oc->lock.is_locked());
  ldout(oc->cct, 10) << "truncate " << *this << " to " << s << dendl;

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
    assert(bh->waitfor_read.empty());
    replace_journal_tid(bh, 0);
    oc->bh_remove(this, bh);
    delete bh;
  }
}

void ObjectCacher::Object::discard(loff_t off, loff_t len)
{
  assert(oc->lock.is_locked());
  ldout(oc->cct, 10) << "discard " << *this << " " << off << "~" << len
		     << dendl;

  if (!exists) {
    ldout(oc->cct, 10) << " setting exists on " << *this << dendl;
    exists = true;
  }
  if (complete) {
    ldout(oc->cct, 10) << " clearing complete on " << *this << dendl;
    complete = false;
  }

  map<loff_t, BufferHead*>::iterator p = data_lower_bound(off);
  while (p != data.end()) {
    BufferHead *bh = p->second;
    if (bh->start() >= off + len)
      break;

    // split bh at truncation point?
    if (bh->start() < off) {
      split(bh, off);
      ++p;
      continue;
    }

    assert(bh->start() >= off);
    if (bh->end() > off + len) {
      split(bh, off + len);
    }

    ++p;
    ldout(oc->cct, 10) << "discard " << *this << " bh " << *bh << dendl;
    assert(bh->waitfor_read.empty());
    replace_journal_tid(bh, 0);
    oc->bh_remove(this, bh);
    delete bh;
  }
}



/*** ObjectCacher ***/

#undef dout_prefix
#define dout_prefix *_dout << "objectcacher "


ObjectCacher::ObjectCacher(CephContext *cct_, string name,
			   WritebackHandler& wb, Mutex& l,
			   flush_set_callback_t flush_callback,
			   void *flush_callback_arg, uint64_t max_bytes,
			   uint64_t max_objects, uint64_t max_dirty,
			   uint64_t target_dirty, double max_dirty_age,
			   bool block_writes_upfront)
  : perfcounter(NULL),
    cct(cct_), writeback_handler(wb), name(name), lock(l),
    max_dirty(max_dirty), target_dirty(target_dirty),
    max_size(max_bytes), max_objects(max_objects),
    max_dirty_age(ceph::make_timespan(max_dirty_age)),
    block_writes_upfront(block_writes_upfront),
    flush_set_callback(flush_callback),
    flush_set_callback_arg(flush_callback_arg),
    last_read_tid(0), flusher_stop(false), flusher_thread(this),finisher(cct),
    stat_clean(0), stat_zero(0), stat_dirty(0), stat_rx(0), stat_tx(0),
    stat_missing(0), stat_error(0), stat_dirty_waiting(0), reads_outstanding(0)
{
  perf_start();
  finisher.start();
  scattered_write = writeback_handler.can_scattered_write();
}

ObjectCacher::~ObjectCacher()
{
  finisher.stop();
  perf_stop();
  // we should be empty.
  for (vector<ceph::unordered_map<sobject_t, Object *> >::iterator i
	 = objects.begin();
       i != objects.end();
       ++i)
    assert(i->empty());
  assert(bh_lru_rest.lru_get_size() == 0);
  assert(bh_lru_dirty.lru_get_size() == 0);
  assert(ob_lru.lru_get_size() == 0);
  assert(dirty_or_tx_bh.empty());
}

void ObjectCacher::perf_start()
{
  string n = "objectcacher-" + name;
  PerfCountersBuilder plb(cct, n, l_objectcacher_first, l_objectcacher_last);

  plb.add_u64_counter(l_objectcacher_cache_ops_hit,
		      "cache_ops_hit", "Hit operations");
  plb.add_u64_counter(l_objectcacher_cache_ops_miss,
		      "cache_ops_miss", "Miss operations");
  plb.add_u64_counter(l_objectcacher_cache_bytes_hit,
		      "cache_bytes_hit", "Hit data");
  plb.add_u64_counter(l_objectcacher_cache_bytes_miss,
		      "cache_bytes_miss", "Miss data");
  plb.add_u64_counter(l_objectcacher_data_read,
		      "data_read", "Read data");
  plb.add_u64_counter(l_objectcacher_data_written,
		      "data_written", "Data written to cache");
  plb.add_u64_counter(l_objectcacher_data_flushed,
		      "data_flushed", "Data flushed");
  plb.add_u64_counter(l_objectcacher_overwritten_in_flush,
		      "data_overwritten_while_flushing",
		      "Data overwritten while flushing");
  plb.add_u64_counter(l_objectcacher_write_ops_blocked, "write_ops_blocked",
		      "Write operations, delayed due to dirty limits");
  plb.add_u64_counter(l_objectcacher_write_bytes_blocked,
		      "write_bytes_blocked",
		      "Write data blocked on dirty limit");
  plb.add_time(l_objectcacher_write_time_blocked, "write_time_blocked",
	       "Time spent blocking a write due to dirty limits");

  perfcounter = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perfcounter);
}

void ObjectCacher::perf_stop()
{
  assert(perfcounter);
  cct->get_perfcounters_collection()->remove(perfcounter);
  delete perfcounter;
}

/* private */
ObjectCacher::Object *ObjectCacher::get_object(sobject_t oid,
					       uint64_t object_no,
					       ObjectSet *oset,
					       object_locator_t &l,
					       uint64_t truncate_size,
					       uint64_t truncate_seq)
{
  // XXX: Add handling of nspace in object_locator_t in cache
  assert(lock.is_locked());
  // have it?
  if ((uint32_t)l.pool < objects.size()) {
    if (objects[l.pool].count(oid)) {
      Object *o = objects[l.pool][oid];
      o->object_no = object_no;
      o->truncate_size = truncate_size;
      o->truncate_seq = truncate_seq;
      return o;
    }
  } else {
    objects.resize(l.pool+1);
  }

  // create it.
  Object *o = new Object(this, oid, object_no, oset, l, truncate_size,
			 truncate_seq);
  objects[l.pool][oid] = o;
  ob_lru.lru_insert_top(o);
  return o;
}

void ObjectCacher::close_object(Object *ob)
{
  assert(lock.is_locked());
  ldout(cct, 10) << "close_object " << *ob << dendl;
  assert(ob->can_close());

  // ok!
  ob_lru.lru_remove(ob);
  objects[ob->oloc.pool].erase(ob->get_soid());
  ob->set_item.remove_myself();
  delete ob;
}

void ObjectCacher::bh_read(BufferHead *bh, int op_flags)
{
  assert(lock.is_locked());
  ldout(cct, 7) << "bh_read on " << *bh << " outstanding reads "
		<< reads_outstanding << dendl;

  mark_rx(bh);
  bh->last_read_tid = ++last_read_tid;

  // finisher
  C_ReadFinish *onfinish = new C_ReadFinish(this, bh->ob, bh->last_read_tid,
					    bh->start(), bh->length());
  // go
  writeback_handler.read(bh->ob->get_oid(), bh->ob->get_object_number(),
			 bh->ob->get_oloc(), bh->start(), bh->length(),
			 bh->ob->get_snap(), &onfinish->bl,
			 bh->ob->truncate_size, bh->ob->truncate_seq,
			 op_flags, onfinish);

  ++reads_outstanding;
}

void ObjectCacher::bh_read_finish(int64_t poolid, sobject_t oid,
				  ceph_tid_t tid, loff_t start,
				  uint64_t length, bufferlist &bl, int r,
				  bool trust_enoent)
{
  assert(lock.is_locked());
  ldout(cct, 7) << "bh_read_finish "
		<< oid
		<< " tid " << tid
		<< " " << start << "~" << length
		<< " (bl is " << bl.length() << ")"
		<< " returned " << r
		<< " outstanding reads " << reads_outstanding
		<< dendl;

  if (r >= 0 && bl.length() < length) {
    ldout(cct, 7) << "bh_read_finish " << oid << " padding " << start << "~"
		  << length << " with " << length - bl.length() << " bytes of zeroes"
		  << dendl;
    bl.append_zero(length - bl.length());
  }

  list<Context*> ls;
  int err = 0;

  if (objects[poolid].count(oid) == 0) {
    ldout(cct, 7) << "bh_read_finish no object cache" << dendl;
  } else {
    Object *ob = objects[poolid][oid];

    if (r == -ENOENT && !ob->complete) {
      // wake up *all* rx waiters, or else we risk reordering
      // identical reads. e.g.
      //   read 1~1
      //   reply to unrelated 3~1 -> !exists
      //   read 1~1 -> immediate ENOENT
      //   reply to first 1~1 -> ooo ENOENT
      bool allzero = true;
      for (map<loff_t, BufferHead*>::iterator p = ob->data.begin();
	   p != ob->data.end(); ++p) {
	BufferHead *bh = p->second;
	for (map<loff_t, list<Context*> >::iterator p
	       = bh->waitfor_read.begin();
	     p != bh->waitfor_read.end();
	     ++p)
	  ls.splice(ls.end(), p->second);
	bh->waitfor_read.clear();
	if (!bh->is_zero() && !bh->is_rx())
	  allzero = false;
      }

      // just pass through and retry all waiters if we don't trust
      // -ENOENT for this read
      if (trust_enoent) {
	ldout(cct, 7)
	  << "bh_read_finish ENOENT, marking complete and !exists on " << *ob
	  << dendl;
	ob->complete = true;
	ob->exists = false;

	/* If all the bhs are effectively zero, get rid of them.  All
	 * the waiters will be retried and get -ENOENT immediately, so
	 * it's safe to clean up the unneeded bh's now. Since we know
	 * it's safe to remove them now, do so, so they aren't hanging
	 *around waiting for more -ENOENTs from rados while the cache
	 * is being shut down.
	 *
	 * Only do this when all the bhs are rx or clean, to match the
	 * condition in _readx(). If there are any non-rx or non-clean
	 * bhs, _readx() will wait for the final result instead of
	 * returning -ENOENT immediately.
	 */
	if (allzero) {
	  ldout(cct, 10)
	    << "bh_read_finish ENOENT and allzero, getting rid of "
	    << "bhs for " << *ob << dendl;
	  map<loff_t, BufferHead*>::iterator p = ob->data.begin();
	  while (p != ob->data.end()) {
	    BufferHead *bh = p->second;
	    // current iterator will be invalidated by bh_remove()
	    ++p;
	    bh_remove(ob, bh);
	    delete bh;
	  }
	}
      }
    }

    // apply to bh's!
    loff_t opos = start;
    while (true) {
      map<loff_t, BufferHead*>::iterator p = ob->data_lower_bound(opos);
      if (p == ob->data.end())
	break;
      if (opos >= start+(loff_t)length) {
	ldout(cct, 20) << "break due to opos " << opos << " >= start+length "
		       << start << "+" << length << "=" << start+(loff_t)length
		       << dendl;
	break;
      }

      BufferHead *bh = p->second;
      ldout(cct, 20) << "checking bh " << *bh << dendl;

      // finishers?
      for (map<loff_t, list<Context*> >::iterator it
	     = bh->waitfor_read.begin();
	   it != bh->waitfor_read.end();
	   ++it)
	ls.splice(ls.end(), it->second);
      bh->waitfor_read.clear();

      if (bh->start() > opos) {
	ldout(cct, 1) << "bh_read_finish skipping gap "
		      << opos << "~" << bh->start() - opos
		      << dendl;
	opos = bh->start();
	continue;
      }

      if (!bh->is_rx()) {
	ldout(cct, 10) << "bh_read_finish skipping non-rx " << *bh << dendl;
	opos = bh->end();
	continue;
      }

      if (bh->last_read_tid != tid) {
	ldout(cct, 10) << "bh_read_finish bh->last_read_tid "
		       << bh->last_read_tid << " != tid " << tid
		       << ", skipping" << dendl;
	opos = bh->end();
	continue;
      }

      assert(opos >= bh->start());
      assert(bh->start() == opos);   // we don't merge rx bh's... yet!
      assert(bh->length() <= start+(loff_t)length-opos);

      if (bh->error < 0)
	err = bh->error;

      opos = bh->end();

      if (r == -ENOENT) {
	if (trust_enoent) {
	  ldout(cct, 10) << "bh_read_finish removing " << *bh << dendl;
	  bh_remove(ob, bh);
	  delete bh;
	} else {
	  ldout(cct, 10) << "skipping unstrusted -ENOENT and will retry for "
			 << *bh << dendl;
	}
	continue;
      }

      if (r < 0) {
	bh->error = r;
	mark_error(bh);
      } else {
	bh->bl.substr_of(bl,
			 bh->start() - start,
			 bh->length());
	mark_clean(bh);
      }

      ldout(cct, 10) << "bh_read_finish read " << *bh << dendl;

      ob->try_merge_bh(bh);
    }
  }

  // called with lock held.
  ldout(cct, 20) << "finishing waiters " << ls << dendl;

  finish_contexts(cct, ls, err);
  retry_waiting_reads();

  --reads_outstanding;
  read_cond.Signal();
}

void ObjectCacher::bh_write_adjacencies(BufferHead *bh, ceph::real_time cutoff,
					int64_t *max_amount, int *max_count)
{
  list<BufferHead*> blist;

  int count = 0;
  int64_t total_len = 0;
  set<BufferHead*, BufferHead::ptr_lt>::iterator it = dirty_or_tx_bh.find(bh);
  assert(it != dirty_or_tx_bh.end());
  for (set<BufferHead*, BufferHead::ptr_lt>::iterator p = it;
       p != dirty_or_tx_bh.end();
       ++p) {
    BufferHead *obh = *p;
    if (obh->ob != bh->ob)
      break;
    if (obh->is_dirty() && obh->last_write <= cutoff) {
      blist.push_back(obh);
      ++count;
      total_len += obh->length();
      if ((max_count && count > *max_count) ||
	  (max_amount && total_len > *max_amount))
	break;
    }
  }

  while (it != dirty_or_tx_bh.begin()) {
    --it;
    BufferHead *obh = *it;
    if (obh->ob != bh->ob)
      break;
    if (obh->is_dirty() && obh->last_write <= cutoff) {
      blist.push_front(obh);
      ++count;
      total_len += obh->length();
      if ((max_count && count > *max_count) ||
	  (max_amount && total_len > *max_amount))
	break;
    }
  }
  if (max_count)
    *max_count -= count;
  if (max_amount)
    *max_amount -= total_len;

  bh_write_scattered(blist);
}

void ObjectCacher::bh_write_scattered(list<BufferHead*>& blist)
{
  assert(lock.is_locked());

  Object *ob = blist.front()->ob;
  ob->get();

  ceph::real_time last_write;
  SnapContext snapc;
  vector<pair<loff_t, uint64_t> > ranges;
  vector<pair<uint64_t, bufferlist> > io_vec;

  ranges.reserve(blist.size());
  io_vec.reserve(blist.size());

  uint64_t total_len = 0;
  for (list<BufferHead*>::iterator p = blist.begin(); p != blist.end(); ++p) {
    BufferHead *bh = *p;
    ldout(cct, 7) << "bh_write_scattered " << *bh << dendl;
    assert(bh->ob == ob);
    assert(bh->bl.length() == bh->length());
    ranges.push_back(pair<loff_t, uint64_t>(bh->start(), bh->length()));

    int n = io_vec.size();
    io_vec.resize(n + 1);
    io_vec[n].first = bh->start();
    io_vec[n].second = bh->bl;

    total_len += bh->length();
    if (bh->snapc.seq > snapc.seq)
      snapc = bh->snapc;
    if (bh->last_write > last_write)
      bh->last_write = bh->last_write;
  }

  C_WriteCommit *oncommit = new C_WriteCommit(this, ob->oloc.pool, ob->get_soid(), ranges);

  ceph_tid_t tid = writeback_handler.write(ob->get_oid(), ob->get_oloc(),
					   io_vec, snapc, last_write,
					   ob->truncate_size, ob->truncate_seq,
					   oncommit);
  oncommit->tid = tid;
  ob->last_write_tid = tid;
  for (list<BufferHead*>::iterator p = blist.begin(); p != blist.end(); ++p) {
    BufferHead *bh = *p;
    bh->last_write_tid = tid;
    mark_tx(bh);
  }

  if (perfcounter)
    perfcounter->inc(l_objectcacher_data_flushed, total_len);
}

void ObjectCacher::bh_write(BufferHead *bh)
{
  assert(lock.is_locked());
  ldout(cct, 7) << "bh_write " << *bh << dendl;

  bh->ob->get();

  // finishers
  C_WriteCommit *oncommit = new C_WriteCommit(this, bh->ob->oloc.pool,
					      bh->ob->get_soid(), bh->start(),
					      bh->length());
  // go
  ceph_tid_t tid = writeback_handler.write(bh->ob->get_oid(),
					   bh->ob->get_oloc(),
					   bh->start(), bh->length(),
					   bh->snapc, bh->bl, bh->last_write,
					   bh->ob->truncate_size,
					   bh->ob->truncate_seq,
					   bh->journal_tid, oncommit);
  ldout(cct, 20) << " tid " << tid << " on " << bh->ob->get_oid() << dendl;

  // set bh last_write_tid
  oncommit->tid = tid;
  bh->ob->last_write_tid = tid;
  bh->last_write_tid = tid;

  if (perfcounter) {
    perfcounter->inc(l_objectcacher_data_flushed, bh->length());
  }

  mark_tx(bh);
}

void ObjectCacher::bh_write_commit(int64_t poolid, sobject_t oid,
				   vector<pair<loff_t, uint64_t> >& ranges,
				   ceph_tid_t tid, int r)
{
  assert(lock.is_locked());
  ldout(cct, 7) << "bh_write_commit " << oid << " tid " << tid
		<< " ranges " << ranges << " returned " << r << dendl;

  if (objects[poolid].count(oid) == 0) {
    ldout(cct, 7) << "bh_write_commit no object cache" << dendl;
    return;
  }

  Object *ob = objects[poolid][oid];
  int was_dirty_or_tx = ob->oset->dirty_or_tx;

  for (vector<pair<loff_t, uint64_t> >::iterator p = ranges.begin();
       p != ranges.end();
       ++p) {
    loff_t start = p->first;
    uint64_t length = p->second;
    if (!ob->exists) {
      ldout(cct, 10) << "bh_write_commit marking exists on " << *ob << dendl;
      ob->exists = true;

      if (writeback_handler.may_copy_on_write(ob->get_oid(), start, length,
					      ob->get_snap())) {
	ldout(cct, 10) << "bh_write_commit may copy on write, clearing "
	  "complete on " << *ob << dendl;
	ob->complete = false;
      }
    }

    // apply to bh's!
    for (map<loff_t, BufferHead*>::iterator p = ob->data_lower_bound(start);
	 p != ob->data.end();
	 ++p) {
      BufferHead *bh = p->second;

      if (bh->start() > start+(loff_t)length)
	break;

      if (bh->start() < start &&
	  bh->end() > start+(loff_t)length) {
	ldout(cct, 20) << "bh_write_commit skipping " << *bh << dendl;
	continue;
      }

      // make sure bh is tx
      if (!bh->is_tx()) {
	ldout(cct, 10) << "bh_write_commit skipping non-tx " << *bh << dendl;
	continue;
      }

      // make sure bh tid matches
      if (bh->last_write_tid != tid) {
	assert(bh->last_write_tid > tid);
	ldout(cct, 10) << "bh_write_commit newer tid on " << *bh << dendl;
	continue;
      }

      if (r >= 0) {
	// ok!  mark bh clean and error-free
	mark_clean(bh);
	bh->set_journal_tid(0);
	if (bh->get_nocache())
	  bh_lru_rest.lru_bottouch(bh);
	ldout(cct, 10) << "bh_write_commit clean " << *bh << dendl;
      } else {
	mark_dirty(bh);
	ldout(cct, 10) << "bh_write_commit marking dirty again due to error "
		       << *bh << " r = " << r << " " << cpp_strerror(-r)
		       << dendl;
      }
    }
  }

  // update last_commit.
  assert(ob->last_commit_tid < tid);
  ob->last_commit_tid = tid;

  // waiters?
  list<Context*> ls;
  if (ob->waitfor_commit.count(tid)) {
    ls.splice(ls.begin(), ob->waitfor_commit[tid]);
    ob->waitfor_commit.erase(tid);
  }

  // is the entire object set now clean and fully committed?
  ObjectSet *oset = ob->oset;
  ob->put();

  if (flush_set_callback &&
      was_dirty_or_tx > 0 &&
      oset->dirty_or_tx == 0) {        // nothing dirty/tx
    flush_set_callback(flush_set_callback_arg, oset);
  }

  if (!ls.empty())
    finish_contexts(cct, ls, r);
}

void ObjectCacher::flush(loff_t amount)
{
  assert(lock.is_locked());
  ceph::real_time cutoff = ceph::real_clock::now();

  ldout(cct, 10) << "flush " << amount << dendl;

  /*
   * NOTE: we aren't actually pulling things off the LRU here, just
   * looking at the tail item.  Then we call bh_write, which moves it
   * to the other LRU, so that we can call
   * lru_dirty.lru_get_next_expire() again.
   */
  int64_t left = amount;
  while (amount == 0 || left > 0) {
    BufferHead *bh = static_cast<BufferHead*>(
      bh_lru_dirty.lru_get_next_expire());
    if (!bh) break;
    if (bh->last_write > cutoff) break;

    if (scattered_write) {
      bh_write_adjacencies(bh, cutoff, amount > 0 ? &left : NULL, NULL);
    } else {
      left -= bh->length();
      bh_write(bh);
    }
  }    
}


void ObjectCacher::trim()
{
  assert(lock.is_locked());
  ldout(cct, 10) << "trim  start: bytes: max " << max_size << "  clean "
		 << get_stat_clean() << ", objects: max " << max_objects
		 << " current " << ob_lru.lru_get_size() << dendl;

  while (get_stat_clean() > 0 && (uint64_t) get_stat_clean() > max_size) {
    BufferHead *bh = static_cast<BufferHead*>(bh_lru_rest.lru_expire());
    if (!bh)
      break;

    ldout(cct, 10) << "trim trimming " << *bh << dendl;
    assert(bh->is_clean() || bh->is_zero() || bh->is_error());

    Object *ob = bh->ob;
    bh_remove(ob, bh);
    delete bh;

    if (ob->complete) {
      ldout(cct, 10) << "trim clearing complete on " << *ob << dendl;
      ob->complete = false;
    }
  }

  while (ob_lru.lru_get_size() > max_objects) {
    Object *ob = static_cast<Object*>(ob_lru.lru_expire());
    if (!ob)
      break;

    ldout(cct, 10) << "trim trimming " << *ob << dendl;
    close_object(ob);
  }

  ldout(cct, 10) << "trim finish:  max " << max_size << "  clean "
		 << get_stat_clean() << ", objects: max " << max_objects
		 << " current " << ob_lru.lru_get_size() << dendl;
}



/* public */

bool ObjectCacher::is_cached(ObjectSet *oset, vector<ObjectExtent>& extents,
			     snapid_t snapid)
{
  assert(lock.is_locked());
  for (vector<ObjectExtent>::iterator ex_it = extents.begin();
       ex_it != extents.end();
       ++ex_it) {
    ldout(cct, 10) << "is_cached " << *ex_it << dendl;

    // get Object cache
    sobject_t soid(ex_it->oid, snapid);
    Object *o = get_object_maybe(soid, ex_it->oloc);
    if (!o)
      return false;
    if (!o->is_cached(ex_it->offset, ex_it->length))
      return false;
  }
  return true;
}


/*
 * returns # bytes read (if in cache).  onfinish is untouched (caller
 *           must delete it)
 * returns 0 if doing async read
 */
int ObjectCacher::readx(OSDRead *rd, ObjectSet *oset, Context *onfinish)
{
  return _readx(rd, oset, onfinish, true);
}

int ObjectCacher::_readx(OSDRead *rd, ObjectSet *oset, Context *onfinish,
			 bool external_call)
{
  assert(lock.is_locked());
  bool success = true;
  int error = 0;
  uint64_t bytes_in_cache = 0;
  uint64_t bytes_not_in_cache = 0;
  uint64_t total_bytes_read = 0;
  map<uint64_t, bufferlist> stripe_map;  // final buffer offset -> substring
  bool dontneed = rd->fadvise_flags & LIBRADOS_OP_FLAG_FADVISE_DONTNEED;
  bool nocache = rd->fadvise_flags & LIBRADOS_OP_FLAG_FADVISE_NOCACHE;

  /*
   * WARNING: we can only meaningfully return ENOENT if the read request
   * passed in a single ObjectExtent.  Any caller who wants ENOENT instead of
   * zeroed buffers needs to feed single extents into readx().
   */
  assert(!oset->return_enoent || rd->extents.size() == 1);

  for (vector<ObjectExtent>::iterator ex_it = rd->extents.begin();
       ex_it != rd->extents.end();
       ++ex_it) {
    ldout(cct, 10) << "readx " << *ex_it << dendl;

    total_bytes_read += ex_it->length;

    // get Object cache
    sobject_t soid(ex_it->oid, rd->snap);
    Object *o = get_object(soid, ex_it->objectno, oset, ex_it->oloc,
			   ex_it->truncate_size, oset->truncate_seq);
    if (external_call)
      touch_ob(o);

    // does not exist and no hits?
    if (oset->return_enoent && !o->exists) {
      ldout(cct, 10) << "readx  object !exists, 1 extent..." << dendl;

      // should we worry about COW underneath us?
      if (writeback_handler.may_copy_on_write(soid.oid, ex_it->offset,
					      ex_it->length, soid.snap)) {
	ldout(cct, 20) << "readx  may copy on write" << dendl;
	bool wait = false;
	list<BufferHead*> blist;
	for (map<loff_t, BufferHead*>::iterator bh_it = o->data.begin();
	     bh_it != o->data.end();
	     ++bh_it) {
	  BufferHead *bh = bh_it->second;
	  if (bh->is_dirty() || bh->is_tx()) {
	    ldout(cct, 10) << "readx  flushing " << *bh << dendl;
	    wait = true;
	    if (bh->is_dirty()) {
	      if (scattered_write)
		blist.push_back(bh);
	      else
		bh_write(bh);
	    }
	  }
	}
	if (scattered_write && !blist.empty())
	  bh_write_scattered(blist);
	if (wait) {
	  ldout(cct, 10) << "readx  waiting on tid " << o->last_write_tid
			 << " on " << *o << dendl;
	  o->waitfor_commit[o->last_write_tid].push_back(
	    new C_RetryRead(this,rd, oset, onfinish));
	  // FIXME: perfcounter!
	  return 0;
	}
      }

      // can we return ENOENT?
      bool allzero = true;
      for (map<loff_t, BufferHead*>::iterator bh_it = o->data.begin();
	   bh_it != o->data.end();
	   ++bh_it) {
	ldout(cct, 20) << "readx  ob has bh " << *bh_it->second << dendl;
	if (!bh_it->second->is_zero() && !bh_it->second->is_rx()) {
	  allzero = false;
	  break;
	}
      }
      if (allzero) {
	ldout(cct, 10) << "readx  ob has all zero|rx, returning ENOENT"
		       << dendl;
	delete rd;
	if (dontneed)
	  bottouch_ob(o);
	return -ENOENT;
      }
    }

    // map extent into bufferheads
    map<loff_t, BufferHead*> hits, missing, rx, errors;
    o->map_read(*ex_it, hits, missing, rx, errors);
    if (external_call) {
      // retry reading error buffers
      missing.insert(errors.begin(), errors.end());
    } else {
      // some reads had errors, fail later so completions
      // are cleaned up properly
      // TODO: make read path not call _readx for every completion
      hits.insert(errors.begin(), errors.end());
    }

    if (!missing.empty() || !rx.empty()) {
      // read missing
      map<loff_t, BufferHead*>::iterator last = missing.end();
      for (map<loff_t, BufferHead*>::iterator bh_it = missing.begin();
	   bh_it != missing.end();
	   ++bh_it) {
	uint64_t rx_bytes = static_cast<uint64_t>(
	  stat_rx + bh_it->second->length());
	bytes_not_in_cache += bh_it->second->length();
	if (!waitfor_read.empty() || (stat_rx > 0 && rx_bytes > max_size)) {
	  // cache is full with concurrent reads -- wait for rx's to complete
	  // to constrain memory growth (especially during copy-ups)
	  if (success) {
	    ldout(cct, 10) << "readx missed, waiting on cache to complete "
			   << waitfor_read.size() << " blocked reads, "
			   << (MAX(rx_bytes, max_size) - max_size)
			   << " read bytes" << dendl;
	    waitfor_read.push_back(new C_RetryRead(this, rd, oset, onfinish));
	  }

	  bh_remove(o, bh_it->second);
	  delete bh_it->second;
	} else {
	  bh_it->second->set_nocache(nocache);
	  bh_read(bh_it->second, rd->fadvise_flags);
	  if ((success && onfinish) || last != missing.end())
	    last = bh_it;
	}
	success = false;
      }

      //add wait in last bh avoid wakeup early. Because read is order
      if (last != missing.end()) {
	ldout(cct, 10) << "readx missed, waiting on " << *last->second
	  << " off " << last->first << dendl;
	last->second->waitfor_read[last->first].push_back(
	  new C_RetryRead(this, rd, oset, onfinish) );

      }

      // bump rx
      for (map<loff_t, BufferHead*>::iterator bh_it = rx.begin();
	   bh_it != rx.end();
	   ++bh_it) {
	touch_bh(bh_it->second); // bump in lru, so we don't lose it.
	if (success && onfinish) {
	  ldout(cct, 10) << "readx missed, waiting on " << *bh_it->second
			 << " off " << bh_it->first << dendl;
	  bh_it->second->waitfor_read[bh_it->first].push_back(
	    new C_RetryRead(this, rd, oset, onfinish) );
	}
	bytes_not_in_cache += bh_it->second->length();
	success = false;
      }

      for (map<loff_t, BufferHead*>::iterator bh_it = hits.begin();
	   bh_it != hits.end();  ++bh_it)
	//bump in lru, so we don't lose it when later read
	touch_bh(bh_it->second);

    } else {
      assert(!hits.empty());

      // make a plain list
      for (map<loff_t, BufferHead*>::iterator bh_it = hits.begin();
	   bh_it != hits.end();
	   ++bh_it) {
	BufferHead *bh = bh_it->second;
	ldout(cct, 10) << "readx hit bh " << *bh << dendl;
	if (bh->is_error() && bh->error)
	  error = bh->error;
	bytes_in_cache += bh->length();

	if (bh->get_nocache() && bh->is_clean())
	  bh_lru_rest.lru_bottouch(bh);
	else
	  touch_bh(bh);
	//must be after touch_bh because touch_bh set dontneed false
	if (dontneed &&
	    ((loff_t)ex_it->offset <= bh->start() &&
	     (bh->end() <=(loff_t)(ex_it->offset + ex_it->length)))) {
	  bh->set_dontneed(true); //if dirty
	  if (bh->is_clean())
	    bh_lru_rest.lru_bottouch(bh);
	}
      }

      if (!error) {
	// create reverse map of buffer offset -> object for the
	// eventual result.  this is over a single ObjectExtent, so we
	// know that
	//  - the bh's are contiguous
	//  - the buffer frags need not be (and almost certainly aren't)
	loff_t opos = ex_it->offset;
	map<loff_t, BufferHead*>::iterator bh_it = hits.begin();
	assert(bh_it->second->start() <= opos);
	uint64_t bhoff = opos - bh_it->second->start();
	vector<pair<uint64_t,uint64_t> >::iterator f_it
	  = ex_it->buffer_extents.begin();
	uint64_t foff = 0;
	while (1) {
	  BufferHead *bh = bh_it->second;
	  assert(opos == (loff_t)(bh->start() + bhoff));

	  uint64_t len = MIN(f_it->second - foff, bh->length() - bhoff);
	  ldout(cct, 10) << "readx rmap opos " << opos << ": " << *bh << " +"
			 << bhoff << " frag " << f_it->first << "~"
			 << f_it->second << " +" << foff << "~" << len
			 << dendl;

	  bufferlist bit;
	  // put substr here first, since substr_of clobbers, and we
	  // may get multiple bh's at this stripe_map position
	  if (bh->is_zero()) {
	    stripe_map[f_it->first].append_zero(len);
	  } else {
	    bit.substr_of(bh->bl,
		opos - bh->start(),
		len);
	    stripe_map[f_it->first].claim_append(bit);
	  }

	  opos += len;
	  bhoff += len;
	  foff += len;
	  if (opos == bh->end()) {
	    ++bh_it;
	    bhoff = 0;
	  }
	  if (foff == f_it->second) {
	    ++f_it;
	    foff = 0;
	  }
	  if (bh_it == hits.end()) break;
	  if (f_it == ex_it->buffer_extents.end())
	    break;
	}
	assert(f_it == ex_it->buffer_extents.end());
	assert(opos == (loff_t)ex_it->offset + (loff_t)ex_it->length);
      }

      if (dontneed && o->include_all_cached_data(ex_it->offset, ex_it->length))
	  bottouch_ob(o);
    }
  }

  if (!success) {
    if (perfcounter && external_call) {
      perfcounter->inc(l_objectcacher_data_read, total_bytes_read);
      perfcounter->inc(l_objectcacher_cache_bytes_miss, bytes_not_in_cache);
      perfcounter->inc(l_objectcacher_cache_ops_miss);
    }
    if (onfinish) {
      ldout(cct, 20) << "readx defer " << rd << dendl;
    } else {
      ldout(cct, 20) << "readx drop " << rd << " (no complete, but no waiter)"
		     << dendl;
      delete rd;
    }
    return 0;  // wait!
  }
  if (perfcounter && external_call) {
    perfcounter->inc(l_objectcacher_data_read, total_bytes_read);
    perfcounter->inc(l_objectcacher_cache_bytes_hit, bytes_in_cache);
    perfcounter->inc(l_objectcacher_cache_ops_hit);
  }

  // no misses... success!  do the read.
  ldout(cct, 10) << "readx has all buffers" << dendl;

  // ok, assemble into result buffer.
  uint64_t pos = 0;
  if (rd->bl && !error) {
    rd->bl->clear();
    for (map<uint64_t,bufferlist>::iterator i = stripe_map.begin();
	 i != stripe_map.end();
	 ++i) {
      assert(pos == i->first);
      ldout(cct, 10) << "readx  adding buffer len " << i->second.length()
		     << " at " << pos << dendl;
      pos += i->second.length();
      rd->bl->claim_append(i->second);
      assert(rd->bl->length() == pos);
    }
    ldout(cct, 10) << "readx  result is " << rd->bl->length() << dendl;
  } else if (!error) {
    ldout(cct, 10) << "readx  no bufferlist ptr (readahead?), done." << dendl;
    map<uint64_t,bufferlist>::reverse_iterator i = stripe_map.rbegin();
    pos = i->first + i->second.length();
  }

  // done with read.
  int ret = error ? error : pos;
  ldout(cct, 20) << "readx done " << rd << " " << ret << dendl;
  assert(pos <= (uint64_t) INT_MAX);

  delete rd;

  trim();

  return ret;
}

void ObjectCacher::retry_waiting_reads()
{
  list<Context *> ls;
  ls.swap(waitfor_read);

  while (!ls.empty() && waitfor_read.empty()) {
    Context *ctx = ls.front();
    ls.pop_front();
    ctx->complete(0);
  }
  waitfor_read.splice(waitfor_read.end(), ls);
}

int ObjectCacher::writex(OSDWrite *wr, ObjectSet *oset, Context *onfreespace)
{
  assert(lock.is_locked());
  ceph::real_time now = ceph::real_clock::now();
  uint64_t bytes_written = 0;
  uint64_t bytes_written_in_flush = 0;
  bool dontneed = wr->fadvise_flags & LIBRADOS_OP_FLAG_FADVISE_DONTNEED;
  bool nocache = wr->fadvise_flags & LIBRADOS_OP_FLAG_FADVISE_NOCACHE;

  for (vector<ObjectExtent>::iterator ex_it = wr->extents.begin();
       ex_it != wr->extents.end();
       ++ex_it) {
    // get object cache
    sobject_t soid(ex_it->oid, CEPH_NOSNAP);
    Object *o = get_object(soid, ex_it->objectno, oset, ex_it->oloc,
			   ex_it->truncate_size, oset->truncate_seq);

    // map it all into a single bufferhead.
    BufferHead *bh = o->map_write(*ex_it, wr->journal_tid);
    bool missing = bh->is_missing();
    bh->snapc = wr->snapc;
    
    bytes_written += ex_it->length;
    if (bh->is_tx()) {
      bytes_written_in_flush += ex_it->length;
    }

    // adjust buffer pointers (ie "copy" data into my cache)
    // this is over a single ObjectExtent, so we know that
    //  - there is one contiguous bh
    //  - the buffer frags need not be (and almost certainly aren't)
    // note: i assume striping is monotonic... no jumps backwards, ever!
    loff_t opos = ex_it->offset;
    for (vector<pair<uint64_t, uint64_t> >::iterator f_it
	   = ex_it->buffer_extents.begin();
	 f_it != ex_it->buffer_extents.end();
	 ++f_it) {
      ldout(cct, 10) << "writex writing " << f_it->first << "~"
		     << f_it->second << " into " << *bh << " at " << opos
		     << dendl;
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
    if (dontneed)
      bh->set_dontneed(true);
    else if (nocache && missing)
      bh->set_nocache(true);
    else
      touch_bh(bh);

    bh->last_write = now;

    o->try_merge_bh(bh);
  }

  if (perfcounter) {
    perfcounter->inc(l_objectcacher_data_written, bytes_written);
    if (bytes_written_in_flush) {
      perfcounter->inc(l_objectcacher_overwritten_in_flush,
		       bytes_written_in_flush);
    }
  }

  int r = _wait_for_write(wr, bytes_written, oset, onfreespace);
  delete wr;

  //verify_stats();
  trim();
  return r;
}

void ObjectCacher::C_WaitForWrite::finish(int r)
{
  Mutex::Locker l(m_oc->lock);
  m_oc->maybe_wait_for_writeback(m_len);
  m_onfinish->complete(r);
}

void ObjectCacher::maybe_wait_for_writeback(uint64_t len)
{
  assert(lock.is_locked());
  ceph::mono_time start = ceph::mono_clock::now();
  int blocked = 0;
  // wait for writeback?
  //  - wait for dirty and tx bytes (relative to the max_dirty threshold)
  //  - do not wait for bytes other waiters are waiting on.  this means that
  //    threads do not wait for each other.  this effectively allows the cache
  //    size to balloon proportional to the data that is in flight.
  while (get_stat_dirty() + get_stat_tx() > 0 &&
	 (uint64_t) (get_stat_dirty() + get_stat_tx()) >=
	 max_dirty + get_stat_dirty_waiting()) {
    ldout(cct, 10) << __func__ << " waiting for dirty|tx "
		   << (get_stat_dirty() + get_stat_tx()) << " >= max "
		   << max_dirty << " + dirty_waiting "
		   << get_stat_dirty_waiting() << dendl;
    flusher_cond.Signal();
    stat_dirty_waiting += len;
    stat_cond.Wait(lock);
    stat_dirty_waiting -= len;
    ++blocked;
    ldout(cct, 10) << __func__ << " woke up" << dendl;
  }
  if (blocked && perfcounter) {
    perfcounter->inc(l_objectcacher_write_ops_blocked);
    perfcounter->inc(l_objectcacher_write_bytes_blocked, len);
    ceph::timespan blocked = ceph::mono_clock::now() - start;
    perfcounter->tinc(l_objectcacher_write_time_blocked, blocked);
  }
}

// blocking wait for write.
int ObjectCacher::_wait_for_write(OSDWrite *wr, uint64_t len, ObjectSet *oset,
				  Context *onfreespace)
{
  assert(lock.is_locked());
  int ret = 0;

  if (max_dirty > 0) {
    if (block_writes_upfront) {
      maybe_wait_for_writeback(len);
      if (onfreespace)
	onfreespace->complete(0);
    } else {
      assert(onfreespace);
      finisher.queue(new C_WaitForWrite(this, len, onfreespace));
    }
  } else {
    // write-thru!  flush what we just wrote.
    Cond cond;
    bool done = false;
    Context *fin = block_writes_upfront ?
      new C_Cond(&cond, &done, &ret) : onfreespace;
    assert(fin);
    bool flushed = flush_set(oset, wr->extents, fin);
    assert(!flushed);   // we just dirtied it, and didn't drop our lock!
    ldout(cct, 10) << "wait_for_write waiting on write-thru of " << len
		   << " bytes" << dendl;
    if (block_writes_upfront) {
      while (!done)
	cond.Wait(lock);
      ldout(cct, 10) << "wait_for_write woke up, ret " << ret << dendl;
      if (onfreespace)
	onfreespace->complete(ret);
    }
  }

  // start writeback anyway?
  if (get_stat_dirty() > 0 && (uint64_t) get_stat_dirty() > target_dirty) {
    ldout(cct, 10) << "wait_for_write " << get_stat_dirty() << " > target "
		   << target_dirty << ", nudging flusher" << dendl;
    flusher_cond.Signal();
  }
  return ret;
}

void ObjectCacher::flusher_entry()
{
  ldout(cct, 10) << "flusher start" << dendl;
  writeback_handler.get_client_lock();
  lock.Lock();
  while (!flusher_stop) {
    loff_t all = get_stat_tx() + get_stat_rx() + get_stat_clean() +
      get_stat_dirty();
    ldout(cct, 11) << "flusher "
		   << all << " / " << max_size << ":  "
		   << get_stat_tx() << " tx, "
		   << get_stat_rx() << " rx, "
		   << get_stat_clean() << " clean, "
		   << get_stat_dirty() << " dirty ("
		   << target_dirty << " target, "
		   << max_dirty << " max)"
		   << dendl;
    loff_t actual = get_stat_dirty() + get_stat_dirty_waiting();
    if (actual > 0 && (uint64_t) actual > target_dirty) {
      // flush some dirty pages
      ldout(cct, 10) << "flusher " << get_stat_dirty() << " dirty + "
		     << get_stat_dirty_waiting() << " dirty_waiting > target "
		     << target_dirty << ", flushing some dirty bhs" << dendl;
      flush(actual - target_dirty);
    } else {
      // check tail of lru for old dirty items
      ceph::real_time cutoff = ceph::real_clock::now();
      cutoff -= max_dirty_age;
      BufferHead *bh = 0;
      int max = MAX_FLUSH_UNDER_LOCK;
      while ((bh = static_cast<BufferHead*>(bh_lru_dirty.
					    lru_get_next_expire())) != 0 &&
	     bh->last_write <= cutoff &&
	     max > 0) {
	ldout(cct, 10) << "flusher flushing aged dirty bh " << *bh << dendl;
	if (scattered_write) {
	  bh_write_adjacencies(bh, cutoff, NULL, &max);
        } else {
	  bh_write(bh);
	  --max;
	}
      }
      if (!max) {
	// back off the lock to avoid starving other threads
	lock.Unlock();
	writeback_handler.put_client_lock();
	writeback_handler.get_client_lock();
	lock.Lock();
	continue;
      }
    }
    if (flusher_stop)
      break;

    writeback_handler.put_client_lock();
    flusher_cond.WaitInterval(cct, lock, seconds(1));
    lock.Unlock();

    writeback_handler.get_client_lock();
    lock.Lock();
  }

  /* Wait for reads to finish. This is only possible if handling
   * -ENOENT made some read completions finish before their rados read
   * came back. If we don't wait for them, and destroy the cache, when
   * the rados reads do come back their callback will try to access the
   * no-longer-valid ObjectCacher.
   */
  while (reads_outstanding > 0) {
    ldout(cct, 10) << "Waiting for all reads to complete. Number left: "
		   << reads_outstanding << dendl;
    read_cond.Wait(lock);
  }

  lock.Unlock();
  writeback_handler.put_client_lock();
  ldout(cct, 10) << "flusher finish" << dendl;
}


// -------------------------------------------------

bool ObjectCacher::set_is_empty(ObjectSet *oset)
{
  assert(lock.is_locked());
  if (oset->objects.empty())
    return true;

  for (xlist<Object*>::iterator p = oset->objects.begin(); !p.end(); ++p)
    if (!(*p)->is_empty())
      return false;

  return true;
}

bool ObjectCacher::set_is_cached(ObjectSet *oset)
{
  assert(lock.is_locked());
  if (oset->objects.empty())
    return false;

  for (xlist<Object*>::iterator p = oset->objects.begin();
       !p.end(); ++p) {
    Object *ob = *p;
    for (map<loff_t,BufferHead*>::iterator q = ob->data.begin();
	 q != ob->data.end();
	 ++q) {
      BufferHead *bh = q->second;
      if (!bh->is_dirty() && !bh->is_tx())
	return true;
    }
  }

  return false;
}

bool ObjectCacher::set_is_dirty_or_committing(ObjectSet *oset)
{
  assert(lock.is_locked());
  if (oset->objects.empty())
    return false;

  for (xlist<Object*>::iterator i = oset->objects.begin();
       !i.end(); ++i) {
    Object *ob = *i;

    for (map<loff_t,BufferHead*>::iterator p = ob->data.begin();
	 p != ob->data.end();
	 ++p) {
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
  assert(lock.is_locked());
  ldout(cct, 10) << "purge " << *ob << dendl;

  ob->truncate(0);
}


// flush.  non-blocking.  no callback.
// true if clean, already flushed.
// false if we wrote something.
// be sloppy about the ranges and flush any buffer it touches
bool ObjectCacher::flush(Object *ob, loff_t offset, loff_t length)
{
  assert(lock.is_locked());
  list<BufferHead*> blist;
  bool clean = true;
  ldout(cct, 10) << "flush " << *ob << " " << offset << "~" << length << dendl;
  for (map<loff_t,BufferHead*>::iterator p = ob->data_lower_bound(offset);
       p != ob->data.end();
       ++p) {
    BufferHead *bh = p->second;
    ldout(cct, 20) << "flush  " << *bh << dendl;
    if (length && bh->start() > offset+length) {
      break;
    }
    if (bh->is_tx()) {
      clean = false;
      continue;
    }
    if (!bh->is_dirty()) {
      continue;
    }

    if (scattered_write)
      blist.push_back(bh);
    else
      bh_write(bh);
    clean = false;
  }
  if (scattered_write && !blist.empty())
    bh_write_scattered(blist);

  return clean;
}

bool ObjectCacher::_flush_set_finish(C_GatherBuilder *gather,
				     Context *onfinish)
{
  assert(lock.is_locked());
  if (gather->has_subs()) {
    gather->set_finisher(onfinish);
    gather->activate();
    return false;
  }

  ldout(cct, 10) << "flush_set has no dirty|tx bhs" << dendl;
  onfinish->complete(0);
  return true;
}

// flush.  non-blocking, takes callback.
// returns true if already flushed
bool ObjectCacher::flush_set(ObjectSet *oset, Context *onfinish)
{
  assert(lock.is_locked());
  assert(onfinish != NULL);
  if (oset->objects.empty()) {
    ldout(cct, 10) << "flush_set on " << oset << " dne" << dendl;
    onfinish->complete(0);
    return true;
  }

  ldout(cct, 10) << "flush_set " << oset << dendl;

  // we'll need to wait for all objects to flush!
  C_GatherBuilder gather(cct);
  set<Object*> waitfor_commit;

  list<BufferHead*> blist;
  Object *last_ob = NULL;
  set<BufferHead*, BufferHead::ptr_lt>::iterator it, p, q;

  // Buffer heads in dirty_or_tx_bh are sorted in ObjectSet/Object/offset
  // order. But items in oset->objects are not sorted. So the iterator can
  // point to any buffer head in the ObjectSet
  BufferHead key(*oset->objects.begin());
  it = dirty_or_tx_bh.lower_bound(&key);
  p = q = it;

  bool backwards = true;
  if (it != dirty_or_tx_bh.begin())
    --it;
  else
    backwards = false;

  for (; p != dirty_or_tx_bh.end(); p = q) {
    ++q;
    BufferHead *bh = *p;
    if (bh->ob->oset != oset)
      break;
    waitfor_commit.insert(bh->ob);
    if (bh->is_dirty()) {
      if (scattered_write) {
	if (last_ob != bh->ob) {
	  if (!blist.empty()) {
	    bh_write_scattered(blist);
	    blist.clear();
	  }
	  last_ob = bh->ob;
	}
	blist.push_back(bh);
      } else {
	bh_write(bh);
      }
    }
  }

  if (backwards) {
    for(p = q = it; true; p = q) {
      if (q != dirty_or_tx_bh.begin())
	--q;
      else
	backwards = false;
      BufferHead *bh = *p;
      if (bh->ob->oset != oset)
	break;
      waitfor_commit.insert(bh->ob);
      if (bh->is_dirty()) {
	if (scattered_write) {
	  if (last_ob != bh->ob) {
	    if (!blist.empty()) {
	      bh_write_scattered(blist);
	      blist.clear();
	    }
	    last_ob = bh->ob;
	  }
	  blist.push_front(bh);
	} else {
	  bh_write(bh);
	}
      }
      if (!backwards)
	break;
    }
  }

  if (scattered_write && !blist.empty())
    bh_write_scattered(blist);

  for (set<Object*>::iterator i = waitfor_commit.begin();
       i != waitfor_commit.end(); ++i) {
    Object *ob = *i;

    // we'll need to gather...
    ldout(cct, 10) << "flush_set " << oset << " will wait for ack tid "
		   << ob->last_write_tid << " on " << *ob << dendl;
    ob->waitfor_commit[ob->last_write_tid].push_back(gather.new_sub());
  }

  return _flush_set_finish(&gather, onfinish);
}

// flush.  non-blocking, takes callback.
// returns true if already flushed
bool ObjectCacher::flush_set(ObjectSet *oset, vector<ObjectExtent>& exv,
			     Context *onfinish)
{
  assert(lock.is_locked());
  assert(onfinish != NULL);
  if (oset->objects.empty()) {
    ldout(cct, 10) << "flush_set on " << oset << " dne" << dendl;
    onfinish->complete(0);
    return true;
  }

  ldout(cct, 10) << "flush_set " << oset << " on " << exv.size()
		 << " ObjectExtents" << dendl;

  // we'll need to wait for all objects to flush!
  C_GatherBuilder gather(cct);

  for (vector<ObjectExtent>::iterator p = exv.begin();
       p != exv.end();
       ++p) {
    ObjectExtent &ex = *p;
    sobject_t soid(ex.oid, CEPH_NOSNAP);
    if (objects[oset->poolid].count(soid) == 0)
      continue;
    Object *ob = objects[oset->poolid][soid];

    ldout(cct, 20) << "flush_set " << oset << " ex " << ex << " ob " << soid
		   << " " << ob << dendl;

    if (!flush(ob, ex.offset, ex.length)) {
      // we'll need to gather...
      ldout(cct, 10) << "flush_set " << oset << " will wait for ack tid "
		     << ob->last_write_tid << " on " << *ob << dendl;
      ob->waitfor_commit[ob->last_write_tid].push_back(gather.new_sub());
    }
  }

  return _flush_set_finish(&gather, onfinish);
}

// flush all dirty data.  non-blocking, takes callback.
// returns true if already flushed
bool ObjectCacher::flush_all(Context *onfinish)
{
  assert(lock.is_locked());
  assert(onfinish != NULL);

  ldout(cct, 10) << "flush_all " << dendl;

  // we'll need to wait for all objects to flush!
  C_GatherBuilder gather(cct);
  set<Object*> waitfor_commit;

  list<BufferHead*> blist;
  Object *last_ob = NULL;
  set<BufferHead*, BufferHead::ptr_lt>::iterator next, it;
  next = it = dirty_or_tx_bh.begin();
  while (it != dirty_or_tx_bh.end()) {
    ++next;
    BufferHead *bh = *it;
    waitfor_commit.insert(bh->ob);

    if (bh->is_dirty()) {
      if (scattered_write) {
	if (last_ob != bh->ob) {
	  if (!blist.empty()) {
	    bh_write_scattered(blist);
	    blist.clear();
	  }
	  last_ob = bh->ob;
	}
	blist.push_back(bh);
      } else {
	bh_write(bh);
      }
    }

    it = next;
  }

  if (scattered_write && !blist.empty())
    bh_write_scattered(blist);

  for (set<Object*>::iterator i = waitfor_commit.begin();
       i != waitfor_commit.end();
       ++i) {
    Object *ob = *i;

    // we'll need to gather...
    ldout(cct, 10) << "flush_all will wait for ack tid "
		   << ob->last_write_tid << " on " << *ob << dendl;
    ob->waitfor_commit[ob->last_write_tid].push_back(gather.new_sub());
  }

  return _flush_set_finish(&gather, onfinish);
}

void ObjectCacher::purge_set(ObjectSet *oset)
{
  assert(lock.is_locked());
  if (oset->objects.empty()) {
    ldout(cct, 10) << "purge_set on " << oset << " dne" << dendl;
    return;
  }

  ldout(cct, 10) << "purge_set " << oset << dendl;
  const bool were_dirty = oset->dirty_or_tx > 0;

  for (xlist<Object*>::iterator i = oset->objects.begin();
       !i.end(); ++i) {
    Object *ob = *i;
	purge(ob);
  }

  // Although we have purged rather than flushed, caller should still
  // drop any resources associate with dirty data.
  assert(oset->dirty_or_tx == 0);
  if (flush_set_callback && were_dirty) {
    flush_set_callback(flush_set_callback_arg, oset);
  }
}


loff_t ObjectCacher::release(Object *ob)
{
  assert(lock.is_locked());
  list<BufferHead*> clean;
  loff_t o_unclean = 0;

  for (map<loff_t,BufferHead*>::iterator p = ob->data.begin();
       p != ob->data.end();
       ++p) {
    BufferHead *bh = p->second;
    if (bh->is_clean() || bh->is_zero() || bh->is_error())
      clean.push_back(bh);
    else
      o_unclean += bh->length();
  }

  for (list<BufferHead*>::iterator p = clean.begin();
       p != clean.end();
       ++p) {
    bh_remove(ob, *p);
    delete *p;
  }

  if (ob->can_close()) {
    ldout(cct, 10) << "release trimming " << *ob << dendl;
    close_object(ob);
    assert(o_unclean == 0);
    return 0;
  }

  if (ob->complete) {
    ldout(cct, 10) << "release clearing complete on " << *ob << dendl;
    ob->complete = false;
  }
  if (!ob->exists) {
    ldout(cct, 10) << "release setting exists on " << *ob << dendl;
    ob->exists = true;
  }

  return o_unclean;
}

loff_t ObjectCacher::release_set(ObjectSet *oset)
{
  assert(lock.is_locked());
  // return # bytes not clean (and thus not released).
  loff_t unclean = 0;

  if (oset->objects.empty()) {
    ldout(cct, 10) << "release_set on " << oset << " dne" << dendl;
    return 0;
  }

  ldout(cct, 10) << "release_set " << oset << dendl;

  xlist<Object*>::iterator q;
  for (xlist<Object*>::iterator p = oset->objects.begin();
       !p.end(); ) {
    q = p;
    ++q;
    Object *ob = *p;

    loff_t o_unclean = release(ob);
    unclean += o_unclean;

    if (o_unclean)
      ldout(cct, 10) << "release_set " << oset << " " << *ob
		     << " has " << o_unclean << " bytes left"
		     << dendl;
    p = q;
  }

  if (unclean) {
    ldout(cct, 10) << "release_set " << oset
		   << ", " << unclean << " bytes left" << dendl;
  }

  return unclean;
}


uint64_t ObjectCacher::release_all()
{
  assert(lock.is_locked());
  ldout(cct, 10) << "release_all" << dendl;
  uint64_t unclean = 0;

  vector<ceph::unordered_map<sobject_t, Object*> >::iterator i
    = objects.begin();
  while (i != objects.end()) {
    ceph::unordered_map<sobject_t, Object*>::iterator p = i->begin();
    while (p != i->end()) {
      ceph::unordered_map<sobject_t, Object*>::iterator n = p;
      ++n;

      Object *ob = p->second;

      loff_t o_unclean = release(ob);
      unclean += o_unclean;

      if (o_unclean)
	ldout(cct, 10) << "release_all " << *ob
		       << " has " << o_unclean << " bytes left"
		       << dendl;
    p = n;
    }
    ++i;
  }

  if (unclean) {
    ldout(cct, 10) << "release_all unclean " << unclean << " bytes left"
		   << dendl;
  }

  return unclean;
}

void ObjectCacher::clear_nonexistence(ObjectSet *oset)
{
  assert(lock.is_locked());
  ldout(cct, 10) << "clear_nonexistence() " << oset << dendl;

  for (xlist<Object*>::iterator p = oset->objects.begin();
       !p.end(); ++p) {
    Object *ob = *p;
    if (!ob->exists) {
      ldout(cct, 10) << " setting exists and complete on " << *ob << dendl;
      ob->exists = true;
      ob->complete = false;
    }
    for (xlist<C_ReadFinish*>::iterator q = ob->reads.begin();
	 !q.end(); ++q) {
      C_ReadFinish *comp = *q;
      comp->distrust_enoent();
    }
  }
}

/**
 * discard object extents from an ObjectSet by removing the objects in
 * exls from the in-memory oset.
 */
void ObjectCacher::discard_set(ObjectSet *oset, const vector<ObjectExtent>& exls)
{
  assert(lock.is_locked());
  if (oset->objects.empty()) {
    ldout(cct, 10) << "discard_set on " << oset << " dne" << dendl;
    return;
  }

  ldout(cct, 10) << "discard_set " << oset << dendl;

  bool were_dirty = oset->dirty_or_tx > 0;

  for (vector<ObjectExtent>::const_iterator p = exls.begin();
       p != exls.end();
       ++p) {
    ldout(cct, 10) << "discard_set " << oset << " ex " << *p << dendl;
    const ObjectExtent &ex = *p;
    sobject_t soid(ex.oid, CEPH_NOSNAP);
    if (objects[oset->poolid].count(soid) == 0)
      continue;
    Object *ob = objects[oset->poolid][soid];

    ob->discard(ex.offset, ex.length);
  }

  // did we truncate off dirty data?
  if (flush_set_callback &&
      were_dirty && oset->dirty_or_tx == 0)
    flush_set_callback(flush_set_callback_arg, oset);
}

void ObjectCacher::verify_stats() const
{
  assert(lock.is_locked());
  ldout(cct, 10) << "verify_stats" << dendl;

  loff_t clean = 0, zero = 0, dirty = 0, rx = 0, tx = 0, missing = 0,
    error = 0;
  for (vector<ceph::unordered_map<sobject_t, Object*> >::const_iterator i
	 = objects.begin();
       i != objects.end();
       ++i) {
    for (ceph::unordered_map<sobject_t, Object*>::const_iterator p
	   = i->begin();
	 p != i->end();
	 ++p) {
      Object *ob = p->second;
      for (map<loff_t, BufferHead*>::const_iterator q = ob->data.begin();
	   q != ob->data.end();
	  ++q) {
	BufferHead *bh = q->second;
	switch (bh->get_state()) {
	case BufferHead::STATE_MISSING:
	  missing += bh->length();
	  break;
	case BufferHead::STATE_CLEAN:
	  clean += bh->length();
	  break;
	case BufferHead::STATE_ZERO:
	  zero += bh->length();
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
	case BufferHead::STATE_ERROR:
	  error += bh->length();
	  break;
	default:
	  assert(0);
	}
      }
    }
  }

  ldout(cct, 10) << " clean " << clean << " rx " << rx << " tx " << tx
		 << " dirty " << dirty << " missing " << missing
		 << " error " << error << dendl;
  assert(clean == stat_clean);
  assert(rx == stat_rx);
  assert(tx == stat_tx);
  assert(dirty == stat_dirty);
  assert(missing == stat_missing);
  assert(zero == stat_zero);
  assert(error == stat_error);
}

void ObjectCacher::bh_stat_add(BufferHead *bh)
{
  assert(lock.is_locked());
  switch (bh->get_state()) {
  case BufferHead::STATE_MISSING:
    stat_missing += bh->length();
    break;
  case BufferHead::STATE_CLEAN:
    stat_clean += bh->length();
    break;
  case BufferHead::STATE_ZERO:
    stat_zero += bh->length();
    break;
  case BufferHead::STATE_DIRTY:
    stat_dirty += bh->length();
    bh->ob->dirty_or_tx += bh->length();
    bh->ob->oset->dirty_or_tx += bh->length();
    break;
  case BufferHead::STATE_TX:
    stat_tx += bh->length();
    bh->ob->dirty_or_tx += bh->length();
    bh->ob->oset->dirty_or_tx += bh->length();
    break;
  case BufferHead::STATE_RX:
    stat_rx += bh->length();
    break;
  case BufferHead::STATE_ERROR:
    stat_error += bh->length();
    break;
  default:
    assert(0 == "bh_stat_add: invalid bufferhead state");
  }
  if (get_stat_dirty_waiting() > 0)
    stat_cond.Signal();
}

void ObjectCacher::bh_stat_sub(BufferHead *bh)
{
  assert(lock.is_locked());
  switch (bh->get_state()) {
  case BufferHead::STATE_MISSING:
    stat_missing -= bh->length();
    break;
  case BufferHead::STATE_CLEAN:
    stat_clean -= bh->length();
    break;
  case BufferHead::STATE_ZERO:
    stat_zero -= bh->length();
    break;
  case BufferHead::STATE_DIRTY:
    stat_dirty -= bh->length();
    bh->ob->dirty_or_tx -= bh->length();
    bh->ob->oset->dirty_or_tx -= bh->length();
    break;
  case BufferHead::STATE_TX:
    stat_tx -= bh->length();
    bh->ob->dirty_or_tx -= bh->length();
    bh->ob->oset->dirty_or_tx -= bh->length();
    break;
  case BufferHead::STATE_RX:
    stat_rx -= bh->length();
    break;
  case BufferHead::STATE_ERROR:
    stat_error -= bh->length();
    break;
  default:
    assert(0 == "bh_stat_sub: invalid bufferhead state");
  }
}

void ObjectCacher::bh_set_state(BufferHead *bh, int s)
{
  assert(lock.is_locked());
  int state = bh->get_state();
  // move between lru lists?
  if (s == BufferHead::STATE_DIRTY && state != BufferHead::STATE_DIRTY) {
    bh_lru_rest.lru_remove(bh);
    bh_lru_dirty.lru_insert_top(bh);
  } else if (s != BufferHead::STATE_DIRTY &&state == BufferHead::STATE_DIRTY) {
    bh_lru_dirty.lru_remove(bh);
    if (bh->get_dontneed())
      bh_lru_rest.lru_insert_bot(bh);
    else
      bh_lru_rest.lru_insert_top(bh);
  }

  if ((s == BufferHead::STATE_TX ||
       s == BufferHead::STATE_DIRTY) &&
      state != BufferHead::STATE_TX &&
      state != BufferHead::STATE_DIRTY) {
    dirty_or_tx_bh.insert(bh);
  } else if ((state == BufferHead::STATE_TX ||
	      state == BufferHead::STATE_DIRTY) &&
	     s != BufferHead::STATE_TX &&
	     s != BufferHead::STATE_DIRTY) {
    dirty_or_tx_bh.erase(bh);
  }

  if (s != BufferHead::STATE_ERROR &&
      bh->get_state() == BufferHead::STATE_ERROR) {
    bh->error = 0;
  }

  // set state
  bh_stat_sub(bh);
  bh->set_state(s);
  bh_stat_add(bh);
}

void ObjectCacher::bh_add(Object *ob, BufferHead *bh)
{
  assert(lock.is_locked());
  ldout(cct, 30) << "bh_add " << *ob << " " << *bh << dendl;
  ob->add_bh(bh);
  if (bh->is_dirty()) {
    bh_lru_dirty.lru_insert_top(bh);
    dirty_or_tx_bh.insert(bh);
  } else {
    if (bh->get_dontneed())
      bh_lru_rest.lru_insert_bot(bh);
    else
      bh_lru_rest.lru_insert_top(bh);
  }

  if (bh->is_tx()) {
    dirty_or_tx_bh.insert(bh);
  }
  bh_stat_add(bh);
}

void ObjectCacher::bh_remove(Object *ob, BufferHead *bh)
{
  assert(lock.is_locked());
  assert(bh->get_journal_tid() == 0);
  ldout(cct, 30) << "bh_remove " << *ob << " " << *bh << dendl;
  ob->remove_bh(bh);
  if (bh->is_dirty()) {
    bh_lru_dirty.lru_remove(bh);
    dirty_or_tx_bh.erase(bh);
  } else {
    bh_lru_rest.lru_remove(bh);
  }

  if (bh->is_tx()) {
    dirty_or_tx_bh.erase(bh);
  }
  bh_stat_sub(bh);
  if (get_stat_dirty_waiting() > 0)
    stat_cond.Signal();
}

