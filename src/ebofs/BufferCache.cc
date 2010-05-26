// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "BufferCache.h"
#include "Onode.h"

#define DOUT_SUBSYS ebofs

void do_apply_partial(bufferlist& bl, map<uint64_t, bufferlist>& pm) 
{
  assert(bl.length() == (unsigned)EBOFS_BLOCK_SIZE);
  //assert(partial_is_complete());
  //cout << "apply_partial" << std::endl;
  for (map<uint64_t, bufferlist>::iterator i = pm.begin();
       i != pm.end();
       i++) {
    //cout << "do_apply_partial at " << i->first << "~" << i->second.length() << std::endl;
    bl.copy_in(i->first, i->second.length(), i->second);
  }
  pm.clear();
}



/*********** BufferHead **************/

#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "ebofs." << *this << "."


void BufferHead::add_partial(uint64_t off, bufferlist& p) 
{
  unsigned len = p.length();
  assert(len <= (unsigned)EBOFS_BLOCK_SIZE);
  assert(off >= 0);
  assert(off + len <= EBOFS_BLOCK_SIZE);
  
  // trim any existing that overlaps
  map<uint64_t, bufferlist>::iterator i = partial.begin();
  while (i != partial.end()) {
    // is [off,off+len)...
    // past i?
    if (off >= i->first + i->second.length()) {  
      i++; 
      continue; 
    }
      // before i?
    if (i->first >= off+len) break;   
    
    // does [off,off+len)...
    // overlap all of i?
    if (off <= i->first && off+len >= i->first + i->second.length()) {
      // erase it and move on.
      partial.erase(i++);
      continue;
    }
    // overlap tail of i?
    if (off > i->first && off+len >= i->first + i->second.length()) {
      // shorten i.
      unsigned taillen = off - i->first;
      bufferlist o;
      o.claim( i->second );
      i->second.substr_of(o, 0, taillen);
      i++;
      continue;
    }
    // overlap head of i?
    if (off <= i->first && off+len < i->first + i->second.length()) {
      // move i (make new tail).
      uint64_t tailoff = off+len;
      unsigned trim = tailoff - i->first;
      partial[tailoff].substr_of(i->second, trim, i->second.length()-trim);
      partial.erase(i++);   // should now be at tailoff
      i++;
      continue;
    } 
    // split i?
    if (off > i->first && off+len < i->first + i->second.length()) {
      bufferlist o;
      o.claim( i->second );
      // shorten head
      unsigned headlen = off - i->first;
      i->second.substr_of(o, 0, headlen);
      // new tail
      unsigned tailoff = off+len - i->first;
      unsigned taillen = o.length() - len - headlen;
      partial[off+len].substr_of(o, tailoff, taillen);
      break;
    }
    assert(0);
  }
  
  // insert and adjust csum
  partial[off] = p;

  dout(10) << "add_partial off " << off << "~" << p.length() << dendl;
}

void BufferHead::apply_partial() 
{
  dout(10) << "apply_partial on " << partial.size() << " substrings" << dendl;
  assert(!partial.empty());
  csum_t *p = oc->on->get_extent_csum_ptr(start(), 1);
  do_apply_partial(data, partial);
  csum_t newc = calc_csum(data.c_str(), EBOFS_BLOCK_SIZE);
  oc->on->data_csum += newc - *p;
  *p = newc;
}


/************ ObjectCache **************/

#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "ebofs.oc."

void ObjectCache::rx_finish(ioh_t ioh, block_t start, block_t length, bufferlist& bl)
{
  list<Context*> waiters;

  dout(10) << "rx_finish " << start << "~" << length << dendl;
  map<block_t, BufferHead*>::iterator p, next;
  for (p = data.lower_bound(start); p != data.end(); p = next) {
    next = p;
    next++;

    BufferHead *bh = p->second;
    dout(10) << "rx_finish ?" << *bh << dendl;
    assert(p->first == bh->start());

    // past?
    if (p->first >= start+length) break;
    if (bh->end() > start+length) break;  // past
    
    assert(p->first >= start);
    assert(bh->end() <= start+length);

    dout(10) << "rx_finish !" << *bh << dendl;

    if (bh->rx_ioh == ioh)
      bh->rx_ioh = 0;

    // trigger waiters
    bh->take_read_waiters(waiters);

    if (bh->is_rx()) {
      assert(bh->get_version() == 0);
      assert(bh->end() <= start+length);
      assert(bh->start() >= start);

      bh->data.substr_of(bl, (bh->start()-start)*EBOFS_BLOCK_SIZE, bh->length()*EBOFS_BLOCK_SIZE);

      // verify checksum
      int bad = 0;
      if (g_conf.ebofs_verify_csum_on_read) {
	csum_t *want = bh->oc->on->get_extent_csum_ptr(bh->start(), bh->length());
	csum_t got[bh->length()];
	for (unsigned i=0; i<bh->length(); i++) {
	  got[i] = calc_csum(&bh->data[i*EBOFS_BLOCK_SIZE], EBOFS_BLOCK_SIZE);
	  if (false && rand() % 10 == 0) {
	    dout(0) << "rx_finish HACK INJECTING bad csum" << dendl;
	    derr(0) << "rx_finish HACK INJECTING bad csum" << dendl;
	    got[i] = 0;
	  }
	  if (got[i] != want[i]) {
	    dout(0) << "rx_finish bad csum wanted " << hex << want[i] << " got " << got[i] << dec 
		    << " for object block " << (i+bh->start()) 
		    << dendl;
	    bad++;
	  }
	}      
	if (bad) {
	  block_t ostart = bh->start();
	  block_t olen = bh->length();
	  for (unsigned s=0; s<olen; s++) {
	    if (got[s] != want[s]) {
	      unsigned e;
	      for (e=s; e<olen; e++)
		if (got[e] == want[e]) break;
	      dout(0) << "rx_finish  bad csum in " << bh->oc->on->object_id << " over " << s << "~" << (e-s) << dendl;
	      derr(0) << "rx_finish  bad csum in " << bh->oc->on->object_id << " over " << s << "~" << (e-s) << dendl;
	      
	      if (s) {
		BufferHead *middle = bc->split(bh, ostart+s);
		dout(0) << "rx_finish  rx -> clean on " << *bh << dendl;	      
		bc->mark_clean(bh);
		bh = middle;
	      }
	      BufferHead *right = bh;
	      if (e < olen) 
		right = bc->split(bh, ostart+e);
	      dout(0) << "rx_finish  rx -> corrupt on " << *bh <<dendl;	      
	      bc->mark_corrupt(bh);
	      bh = right;
	      s = e;
	    }
	  }
	}
      }
      if (bh) {
	dout(10) << "rx_finish  rx -> clean on " << *bh << dendl;
	bc->mark_clean(bh);
      }
    }
    else if (bh->is_partial()) {
      dout(10) << "rx_finish  partial -> tx on " << *bh << dendl;      

      // see what block i am
      vector<extent_t> exv;
      on->map_extents(bh->start(), 1, exv, 0);
      assert(exv.size() == 1);
      assert(exv[0].start != 0);
      block_t cur_block = exv[0].start;
      
      uint64_t off_in_bl = (bh->start() - start) * EBOFS_BLOCK_SIZE;
      assert(off_in_bl >= 0);
      uint64_t len_in_bl = bh->length() * EBOFS_BLOCK_SIZE;

      // verify csum
      csum_t want = *bh->oc->on->get_extent_csum_ptr(bh->start(), 1);
      csum_t got = calc_csum(bl.c_str() + off_in_bl, len_in_bl);
      if (want != got) {
	derr(0) << "rx_finish  bad csum on partial readback, want " << hex << want
		<< " got " << got << dec << dendl;
	dout(0) << "rx_finish  bad csum on partial readback, want " << hex << want
		<< " got " << got << dec << dendl;
	*bh->oc->on->get_extent_csum_ptr(bh->start(), 1) = got;
	bh->oc->on->data_csum += got - want;
	
	interval_set<uint64_t> bad;
	bad.insert(bh->start()*EBOFS_BLOCK_SIZE, EBOFS_BLOCK_SIZE);
	bh->oc->on->bad_byte_extents.union_of(bad);

	interval_set<uint64_t> over;
	for (map<uint64_t,bufferlist>::iterator q = bh->partial.begin();
	     q != bh->partial.end();
	     q++)
	  over.insert(bh->start()*EBOFS_BLOCK_SIZE+q->first, q->second.length());
	interval_set<uint64_t> new_over;
	new_over.intersection_of(over, bh->oc->on->bad_byte_extents);
	bh->oc->on->bad_byte_extents.subtract(new_over);
      }

      // apply partial to myself
      assert(bh->data.length() == 0);
      bufferptr bp = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
      bh->data.push_back( bp );
      bufferlist sub;
      sub.substr_of(bl, off_in_bl, len_in_bl);
      bh->data.copy_in(0, EBOFS_BLOCK_SIZE, sub);
      bh->apply_partial();
      
      // write "normally"
      bc->mark_dirty(bh);
      bc->bh_write(on, bh, cur_block);

      assert(bh->oc->on->is_dirty()); 

      // clean up a bit
      bh->partial.clear();
    }
    else {
      dout(10) << "rx_finish  ignoring status on (dirty|tx|clean) " << *bh << dendl;
      assert(bh->is_dirty() ||  // was overwritten
             bh->is_tx() ||     // was overwritten and queued
             bh->is_clean());   // was overwritten, queued, _and_ flushed to disk
    }

  }    

  finish_contexts(waiters);
}


void ObjectCache::tx_finish(ioh_t ioh, block_t start, block_t length, 
                            version_t version, version_t epoch)
{
  dout(10) << "tx_finish " << start << "~" << length << " v" << version << dendl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
       p != data.end(); 
       p++) {
    BufferHead *bh = p->second;
    dout(30) << "tx_finish ?bh " << *bh << dendl;
    assert(p->first == bh->start());

    // past?
    if (p->first >= start+length) {
      bh->oc->try_merge_bh_right(p);
      break;
    }

    if (bh->tx_ioh == ioh)
      bh->tx_ioh = 0;

    if (!bh->is_tx()) {
      dout(10) << "tx_finish  bh not marked tx, skipping" << dendl;
      continue;
    }
    assert(bh->is_tx());
    
    if (version == bh->version) {
      dout(10) << "tx_finish  tx -> clean on " << *bh << dendl;
      assert(bh->end() <= start+length);
      bh->set_last_flushed(version);
      bc->mark_clean(bh);
      bh->oc->try_merge_bh_left(p);
    } else {
      dout(10) << "tx_finish  leaving tx, " << bh->version << " > " << version 
               << " on " << *bh << dendl;
      assert(bh->version > version);
    }
  }    
}



/*
 * return any bh's that are (partially) in this range that are TX.
 */
int ObjectCache::find_tx(block_t start, block_t len,
                         list<BufferHead*>& tx)
{
  map<block_t, BufferHead*>::iterator p = data.lower_bound(start);

  block_t cur = start;
  block_t left = len;
  
  /* don't care about overlap, we want things _fully_ in start~len.
  if (p != data.begin() && 
      (p == data.end() || p->first > cur)) {
    p--;     // might overlap!
    if (p->first + p->second->length() <= cur) 
      p++;   // doesn't overlap.
  }
  */

  while (left > 0) {
    assert(cur+left == start+len);

    // at end?
    if (p == data.end()) 
      break;

    if (p->first <= cur) {
      // have it (or part of it)
      BufferHead *e = p->second;

      if (e->end() <= start+len &&
          e->is_tx()) 
        tx.push_back(e);
      
      block_t lenfromcur = MIN(e->end() - cur, left);
      cur += lenfromcur;
      left -= lenfromcur;
      p++;
      continue;  // more?
    } else if (p->first > cur) {
      // gap.. miss
      block_t next = p->first;
      left -= (next-cur);
      cur = next;
      continue;
    }
    else 
      assert(0);
  }

  return 0;  
}


int ObjectCache::try_map_read(block_t start, block_t len)
{
  map<block_t, BufferHead*>::iterator p = find_bh(start, len);
  block_t cur = start;
  block_t left = len;

  int num_missing = 0;

  while (left > 0) {
    // at end?
    if (p == data.end()) {
      // rest is a miss.
      vector<extent_t> exv;
      on->map_extents(cur, left,   // no prefetch here!
                      exv, 0);
      for (unsigned i=0; i<exv.size(); i++)
	if (exv[i].start)
	  num_missing++;
      left = 0;
      cur = start+len;
      break;
    }
    
    if (p->first <= cur) {
      // have it (or part of it)
      BufferHead *e = p->second;
      
      if (e->is_clean() ||
          e->is_dirty() ||
          e->is_tx()) {
        dout(20) << "try_map_read hit " << *e << dendl;
      } 
      else if (e->is_corrupt()) {
        dout(20) << "try_map_read corrupt " << *e << dendl;	
      } 
      else if (e->is_rx()) {
        dout(20) << "try_map_read rx " << *e << dendl;
	num_missing++;
      }
      else if (e->is_partial()) {
        dout(-20) << "try_map_read partial " << *e << dendl;
	num_missing++;
      }
      else {
	dout(0) << "try_map_read got unexpected " << *e << dendl;
	assert(0);
      }
      
      block_t lenfromcur = MIN(e->end() - cur, left);
      cur += lenfromcur;
      left -= lenfromcur;
      p++;
      continue;  // more?
    } else if (p->first > cur) {
      // gap.. miss
      block_t next = p->first;
      vector<extent_t> exv;
      on->map_extents(cur, 
                      MIN(next-cur, left),   // no prefetch
                      exv, 0);
      for (unsigned i=0; i<exv.size(); i++)
	if (exv[i].start) {
	  dout(20) << "try_map_read gap " << exv[i] << dendl;
	  num_missing++;
	}
      left -= (p->first - cur);
      cur = p->first;
      continue;    // more?
    }
    else 
      assert(0);
  }

  assert(left == 0);
  assert(cur == start+len);
  return num_missing;
}





/*
 * map a range of blocks into buffer_heads.
 * - create missing buffer_heads as necessary.
 *  - fragment along disk extent boundaries
 */
int ObjectCache::map_read(block_t start, block_t len, 
                          map<block_t, BufferHead*>& hits,
                          map<block_t, BufferHead*>& missing,
                          map<block_t, BufferHead*>& rx,
                          map<block_t, BufferHead*>& partial) {
  
  map<block_t, BufferHead*>::iterator p = find_bh(start, len);
  block_t cur = start;
  block_t left = len;

  while (left > 0) {
    // at end?
    if (p == data.end()) {
      // rest is a miss.
      vector<extent_t> exv;
      on->map_extents(cur, 
                      //MIN(left + g_conf.ebofs_max_prefetch,   // prefetch
                      //on->object_blocks-cur),  
                      left,   // no prefetch
                      exv, 0);
      for (unsigned i=0; i<exv.size() && left > 0; i++) {
        BufferHead *n = new BufferHead(this, cur, exv[i].length);
	if (exv[i].start) {
	  missing[cur] = n;
	  dout(20) << "map_read miss " << left << " left, " << *n << dendl;
	} else {
	  hits[cur] = n;
	  n->set_state(BufferHead::STATE_CLEAN);
	  dout(20) << "map_read hole " << left << " left, " << *n << dendl;
	}
        bc->add_bh(n);
        cur += MIN(left,exv[i].length);
        left -= MIN(left,exv[i].length);
      }
      assert(left == 0);
      assert(cur == start+len);
      break;
    }
    
    if (p->first <= cur) {
      // have it (or part of it)
      BufferHead *e = p->second;
      
      if (e->is_clean() ||
          e->is_dirty() ||
          e->is_tx() ||
	  e->is_corrupt()) {
        hits[cur] = e;     // readable!
        dout(20) << "map_read hit " << *e << dendl;
        bc->touch(e);
      } 
      else if (e->is_rx()) {
        rx[cur] = e;       // missing, not readable.
        dout(20) << "map_read rx " << *e << dendl;
      }
      else if (e->is_partial()) {
        partial[cur] = e;
        dout(20) << "map_read partial " << *e << dendl;
      }
      else {
	dout(0) << "map_read ??? got unexpected " << *e << dendl;
	assert(0);
      }
      
      block_t lenfromcur = MIN(e->end() - cur, left);
      cur += lenfromcur;
      left -= lenfromcur;
      p++;
      continue;  // more?
    } else if (p->first > cur) {
      // gap.. miss
      block_t next = p->first;
      vector<extent_t> exv;
      on->map_extents(cur, 
                      //MIN(next-cur, MIN(left + g_conf.ebofs_max_prefetch,   // prefetch
                      //                on->object_blocks-cur)),  
                      MIN(next-cur, left),   // no prefetch
                      exv, 0);
      
      for (unsigned i=0; i<exv.size() && left>0; i++) {
        BufferHead *n = new BufferHead(this, cur, exv[i].length);
	if (exv[i].start) {
	  missing[cur] = n;
	  dout(20) << "map_read gap " << *n << dendl;
	} else {
	  n->set_state(BufferHead::STATE_CLEAN);
	  hits[cur] = n;
	  dout(20) << "map_read hole " << *n << dendl;
	}
        bc->add_bh(n);
        cur += MIN(left, n->length());
        left -= MIN(left, n->length());
      }
      continue;    // more?
    }
    else 
      assert(0);
  }

  assert(left == 0);
  assert(cur == start+len);
  return 0;  
}


/*
 * map a range of pages on an object's buffer cache.
 *
 * - break up bufferheads that don't fall completely within the range
 * - cancel rx ops we contain
 *   - resubmit non-contained rx ops if we split bufferheads
 * - cancel obsoleted tx ops
 * - break contained bh's over disk extent boundaries
 */
int ObjectCache::map_write(block_t start, block_t len,
                           map<block_t, BufferHead*>& hits,
                           version_t super_epoch)
{
  dout(10) << "map_write " << *on << " " << start << "~" << len << dendl;

  map<block_t, BufferHead*>::iterator p = find_bh(start, len);  // p->first >= start
  block_t cur = start;
  block_t left = len;

  //dump();

  while (left > 0) {
    // max for this bh (bc of (re)alloc on disk)
    block_t max = left;

    // based on disk extent boundary ...
    vector<extent_t> exv;
    on->map_extents(cur, max, exv, 0);
    if (exv.size() > 1) 
      max = exv[0].length;
    bool hole = false;
    if (exv.size() > 0 && exv[0].start == 0)
      hole = true;

    dout(10) << "map_write " << cur << "~" << max << dendl;
    
    // at end?
    if (p == data.end()) {
      BufferHead *n = new BufferHead(this, cur, max);
      if (hole)
	n->set_state(BufferHead::STATE_CLEAN); // hole
      bc->add_bh(n);
      hits[cur] = n;
      left -= max;
      cur += max;
      continue;
    }
    
    dout(10) << "p is " << *p->second << dendl;


    if (p->first <= cur) {
      BufferHead *bh = p->second;
      dout(10) << "map_write bh " << *bh << " intersected" << dendl;

      if (p->first < cur) {
        if (cur+max >= p->first+p->second->length()) {
          // we want right bit (one splice)
          if (bh->is_rx() && bc->bh_cancel_read(bh)) {
            BufferHead *right = bc->split(bh, cur);
            bc->bh_read(on, bh);          // reread left bit
            bh = right;
          } else if (bh->is_tx() && bh->epoch_modified == super_epoch && bc->bh_cancel_write(bh, super_epoch)) {
            BufferHead *right = bc->split(bh, cur);
            bc->bh_write(on, bh);          // rewrite left bit
            bh = right;
          } else {
            bh = bc->split(bh, cur);   // just split it
          }
          p++;
          assert(p->second == bh);
        } else {
          // we want middle bit (two splices)
          if (bh->is_rx() && bc->bh_cancel_read(bh)) {
            BufferHead *middle = bc->split(bh, cur);
            bc->bh_read(on, bh);                       // reread left
            p++;
            assert(p->second == middle);
            BufferHead *right = bc->split(middle, cur+max);
            bc->bh_read(on, right);                    // reread right
            bh = middle;
          } else if (bh->is_tx() && bh->epoch_modified == super_epoch && bc->bh_cancel_write(bh, super_epoch)) {
            BufferHead *middle = bc->split(bh, cur);
            bc->bh_write(on, bh);                       // redo left
            p++;
            assert(p->second == middle);
            BufferHead *right = bc->split(middle, cur+max);
            bc->bh_write(on, right);                    // redo right
            bh = middle;
          } else {
            BufferHead *middle = bc->split(bh, cur);
            p++;
            assert(p->second == middle);
            bc->split(middle, cur+max);
            bh = middle;
          }
        }
      } else if (p->first == cur) {
        if (p->second->length() <= max) {
          // whole bufferhead, piece of cake.
        } else {
          // we want left bit (one splice)
          if (bh->is_rx() && bc->bh_cancel_read(bh)) {
            BufferHead *right = bc->split(bh, cur+max);
            bc->bh_read(on, right);              // re-rx the right bit
          } else if (bh->is_tx() && bh->epoch_modified == super_epoch && bc->bh_cancel_write(bh, super_epoch)) {
            BufferHead *right = bc->split(bh, cur+max);
            bc->bh_write(on, right);              // re-tx the right bit
          } else {
            bc->split(bh, cur+max);        // just split
          }
        }
      }
      
      // try to cancel tx?
      if (bh->is_tx() && bh->epoch_modified == super_epoch)
	bc->bh_cancel_write(bh, super_epoch);
            
      // put in our map
      hits[cur] = bh;

      // keep going.
      block_t lenfromcur = bh->end() - cur;
      cur += lenfromcur;
      left -= lenfromcur;
      p++;
      continue; 
    } else {
      // gap!
      block_t next = p->first;
      block_t glen = MIN(next-cur, max);
      dout(10) << "map_write gap " << cur << "~" << glen << dendl;
      BufferHead *n = new BufferHead(this, cur, glen);
      if (hole)
	n->set_state(BufferHead::STATE_CLEAN); // hole
      bc->add_bh(n);
      hits[cur] = n;
      
      cur += glen;
      left -= glen;
      continue;    // more?
    }
  }

  assert(left == 0);
  assert(cur == start+len);
  return 0;
}

/* don't need this.
int ObjectCache::scan_versions(block_t start, block_t len,
                               version_t& low, version_t& high)
{
  map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
  // p->first >= start
  
  if (p != data.begin() && p->first > start) {
    p--;     // might overlap?
    if (p->first + p->second->length() <= start) 
      p++;   // doesn't overlap.
  }
  if (p->first >= start+len) 
    return -1;  // to the right.  no hits.
  
  // start
  low = high = p->second->get_version();

  for (p++; p != data.end(); p++) {
    // past?
    if (p->first >= start+len) break;
    
    const version_t v = p->second->get_version();
    if (low > v) low = v;
    if (high < v) high = v;
  }    

  return 0;
}
*/

void ObjectCache::touch_bottom(block_t bstart, block_t blast)
{
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(bstart);
       p != data.end();
       ++p) {
    BufferHead *bh = p->second;
    
    // don't trim unless it's entirely in our range
    if (bh->start() < bstart) continue;
    if (bh->end() > blast) break;     
    
    dout(12) << "moving " << *bh << " to bottom of lru" << dendl;
    bc->touch_bottom(bh);  // move to bottom of lru list
  }
}  

void ObjectCache::discard_bh(BufferHead *bh, version_t super_epoch)
{
  bool uncom = on->uncommitted.contains(bh->start(), bh->length());
  dout(10) << "discard_bh " << *bh << " uncom " << uncom 
	   << " of " << on->uncommitted
	   << dendl;
  
  // whole thing
  // cancel any pending/queued io, if possible.
  if (bh->is_rx())
    bc->bh_cancel_read(bh);
  if (bh->is_tx() && uncom && bh->epoch_modified == super_epoch) 
    bc->bh_cancel_write(bh, super_epoch);
  if (bh->shadow_of) {
    dout(10) << "truncate " << *bh << " unshadowing " << *bh->shadow_of << dendl;
    // shadow
    bh->shadow_of->remove_shadow(bh);
  }
  
  // kick read waiters
  list<Context*> finished;
  bh->take_read_waiters(finished);
  finish_contexts(finished, -1);

  bc->remove_bh(bh);
}

void ObjectCache::truncate(block_t blocks, version_t super_epoch)
{
  dout(7) << "truncate " << object_id 
           << " " << blocks << " blocks"
           << dendl;

  while (!data.empty()) {
    BufferHead *bh = data.rbegin()->second;

    if (bh->end() <= blocks) break;

    if (bh->start() < blocks) {
      // we want right bit (one splice)
      if (bh->is_rx() && bc->bh_cancel_read(bh)) {
	BufferHead *right = bc->split(bh, blocks);
	bc->bh_read(on, bh);          // reread left bit
	bh = right;
      } else if (bh->is_tx() && bh->epoch_modified == super_epoch && bc->bh_cancel_write(bh, super_epoch)) {
	BufferHead *right = bc->split(bh, blocks);
	bc->bh_write(on, bh);          // rewrite left bit
	bh = right;
      } else {
	bh = bc->split(bh, blocks);   // just split it
      }
      // no worries about partials up here, they're always 1 block (and thus never split)
    } 

    discard_bh(bh, super_epoch);
  }
}


void ObjectCache::clone_to(Onode *other)
{
  ObjectCache *ton = 0;

  for (map<block_t, BufferHead*>::iterator p = data.begin();
       p != data.end();
       p++) {
    BufferHead *bh = p->second;
    dout(10) << "clone_to ? " << *bh << dendl;
    if (bh->is_dirty() || bh->is_tx() || bh->is_partial()) {
      // dup dirty or tx bh's
      if (!ton)
	ton = other->get_oc(bc);
      BufferHead *nbh = new BufferHead(ton, bh->start(), bh->length());
      nbh->data = bh->data;      // just copy refs to underlying buffers. 
      bc->add_bh(nbh);

      if (bh->is_partial()) {
	dout(0) << "clone_to PARTIAL FIXME NOT FULLY IMPLEMENTED ******" << dendl;
	nbh->partial = bh->partial;
	bc->mark_partial(nbh);
      } else {
	// clean buffer will shadow
	bh->add_shadow(nbh);
	bc->mark_clean(nbh);
      }

      dout(10) << "clone_to dup " << *bh << " -> " << *nbh << dendl;
    } 
  }
}



BufferHead *ObjectCache::merge_bh_left(BufferHead *left, BufferHead *right)
{
  dout(10) << "merge_bh_left " << *left << " " << *right << dendl;
  assert(left->end() == right->start());
  assert(left->is_clean());
  assert(!left->is_hole());
  assert(right->is_clean());
  assert(!right->is_hole());
  assert(right->get_num_ref() == 0);

  // hrm, is this right?
  if (right->version > left->version) left->version = right->version;
  if (right->last_flushed > left->last_flushed) left->last_flushed = right->last_flushed;

  bc->stat_sub(left);
  left->reset_length(left->length() + right->length());
  bc->stat_add(left);
  left->data.claim_append(right->data);

  // remove right
  bc->remove_bh(right);
  dout(10) << "merge_bh_left result " << *left << dendl;
  return left;
}

/* wait until this has a user
void ObjectCache::try_merge_bh(BufferHead *bh)
{
  dout(-10) << "try_merge_bh " << *bh << dendl;

  map<block_t, BufferHead*>::iterator p = data.lower_bound(bh->start());
  assert(p->second == bh);
  
  try_merge_bh_left(p);
  try_merge_bh_right(p);
}
*/


void ObjectCache::try_merge_bh_left(map<block_t, BufferHead*>::iterator& p)
{
  BufferHead *bh = p->second;
  dout(10) << "try_merge_bh_left " << *bh << dendl;

  // left?
  if (p != data.begin()) {
    p--;
    if (p->second->end() == bh->start() &&
	p->second->is_clean() && 
	!p->second->is_hole() &&
	bh->is_clean() &&
	!bh->is_hole() &&
	bh->get_num_ref() == 0 &&
	bh->data.buffers().size() < 8 &&
	p->second->data.buffers().size() < 8)
      bh = merge_bh_left(p->second, bh);      // yay!
    else 
      p++;      // nope.
  }
}

void ObjectCache::try_merge_bh_right(map<block_t, BufferHead*>::iterator& p)
{
  BufferHead *bh = p->second;
  dout(10) << "try_merge_bh_right " << *bh << dendl;

  // right?
  map<block_t, BufferHead*>::iterator o = p;
  p++;
  if (p != data.end() &&
      bh->end() == p->second->start() && 
      p->second->is_clean() && 
      !p->second->is_hole() &&
      bh->is_clean() &&
      !bh->is_hole() &&
      p->second->get_num_ref() == 0 &&
      bh->data.buffers().size() < 8 &&
      p->second->data.buffers().size() < 8) {
    BufferHead *right = p->second;
    p--;
    merge_bh_left(bh, right);
  } else
    p = o;
}


void ObjectCache::scrub_csums()
{
  dout(10) << "scrub_csums on " << *this->on << dendl;
  int bad = 0;
  for (map<block_t, BufferHead*>::iterator p = data.begin();
       p != data.end();
       p++) {
    BufferHead *bh = p->second;
    if (bh->is_rx() || bh->is_missing()) continue;  // nothing to scrub
    if (bh->is_clean() && bh->data.length() == 0) continue;  // hole.
    if (bh->is_clean() || bh->is_tx()) {
      for (unsigned i=0; i<bh->length(); i++) {
	vector<extent_t> exv;
	on->map_extents(bh->start()+i, 1, exv, 0);
	assert(exv.size() == 1);
	if (exv[0].start == 0) continue;  // hole.
	csum_t want = *on->get_extent_csum_ptr(bh->start()+i, 1);
	csum_t b = calc_csum(&bh->data[i*EBOFS_BLOCK_SIZE], EBOFS_BLOCK_SIZE);
	if (b != want) {
	  dout(0) << "scrub_csums bad data at " << (bh->start()+i) << " have " 
		  << hex << b << " should be " << want << dec
		  << " in bh " << *bh
		  << dendl;
	  bad++;
	}
      }
    }    
  }
  assert(bad == 0);
}


/************** BufferCache ***************/

#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "ebofs.bc."


BufferHead *BufferCache::split(BufferHead *orig, block_t after) 
{
  dout(20) << "split " << *orig << " at " << after << dendl;

  // split off right
  block_t newleftlen = after - orig->start();
  BufferHead *right = new BufferHead(orig->get_oc(), after, orig->length() - newleftlen);
  right->set_version(orig->get_version());
  right->epoch_modified = orig->epoch_modified;
  right->last_flushed = orig->last_flushed;
  right->set_state(orig->get_state());
  add_bh(right);
  
  // shorten left
  stat_sub(orig);
  orig->reset_length( newleftlen );
  stat_add(orig);

  // adjust rx_from
  if (orig->is_rx()) {
    right->rx_from = orig->rx_from;
    orig->rx_from.length = newleftlen;
    right->rx_from.length -= newleftlen;
    right->rx_from.start += newleftlen;
  }

  // dup shadows
  for (set<BufferHead*>::iterator p = orig->shadows.begin();
       p != orig->shadows.end();
       ++p)
    right->add_shadow(*p);

  // split buffers too
  bufferlist bl;
  bl.claim(orig->data);
  if (bl.length()) {
    assert(bl.length() == (orig->length()+right->length())*EBOFS_BLOCK_SIZE);
    right->data.substr_of(bl, orig->length()*EBOFS_BLOCK_SIZE, right->length()*EBOFS_BLOCK_SIZE);
    orig->data.substr_of(bl, 0, orig->length()*EBOFS_BLOCK_SIZE);
  }

  // move read waiters
  if (!orig->waitfor_read.empty()) {
    map<block_t, list<Context*> >::iterator o, p = orig->waitfor_read.end();
    p--;
    while (p != orig->waitfor_read.begin()) {
      if (p->first < right->start()) break;      
      dout(0) << "split  moving waiters at block " << p->first << " to right bh" << dendl;
      right->waitfor_read[p->first].swap( p->second );
      o = p;
      p--;
      orig->waitfor_read.erase(o);
    }
  }
  
  dout(20) << "split    left is " << *orig << dendl;
  dout(20) << "split   right is " << *right << dendl;
  return right;
}




void BufferCache::bh_read(Onode *on, BufferHead *bh, block_t from)
{
  dout(10) << "bh_read " << *on << " on " << *bh << dendl;

  if (bh->is_missing()) {
    mark_rx(bh);
  } else {
    assert(bh->is_partial());
  }
  
  // get extent.  there should be only one!
  vector<extent_t> exv;
  on->map_extents(bh->start(), bh->length(), exv, 0);
  assert(exv.size() == 1);
  assert(exv[0].start != 0); // not a hole.
  extent_t ex = exv[0];

  if (from) {  // force behavior, used for reading partials
    dout(10) << "bh_read  forcing read from block " << from << " (for a partial)" << dendl;
    ex.start = from;
    ex.length = 1;
  }
    
  // this should be empty!!
  assert(bh->rx_ioh == 0);
  
  dout(20) << "bh_read  " << *on << " " << *bh << " from " << ex << dendl;
  
  C_OC_RxFinish *fin = new C_OC_RxFinish(ebofs_lock, on->oc, 
                                         bh->start(), bh->length(),
                                         ex.start);

  //bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), fin->bl);  // new buffers!
  fin->bl.push_back( buffer::create_page_aligned(EBOFS_BLOCK_SIZE*bh->length()) );

  bh->rx_ioh = dev.read(ex.start, ex.length, fin->bl,
                        fin);
  bh->rx_from = ex;
  on->oc->get();

}

bool BufferCache::bh_cancel_read(BufferHead *bh)
{
  if (bh->rx_ioh && dev.cancel_io(bh->rx_ioh) >= 0) {
    dout(10) << "bh_cancel_read on " << *bh << dendl;
    bh->rx_ioh = 0;
    mark_missing(bh);
    int l = bh->oc->put();
    assert(l);
    return true;
  }
  return false;
}

void BufferCache::bh_write(Onode *on, BufferHead *bh, block_t shouldbe)
{
  dout(10) << "bh_write " << *on << " on " << *bh << " in epoch " << bh->epoch_modified << dendl;
  assert(bh->get_version() > 0);

  assert(bh->is_dirty());
  mark_tx(bh);
  
  // get extents
  vector<extent_t> exv;
  on->map_extents(bh->start(), bh->length(), exv, 0);
  assert(exv.size() == 1);
  assert(exv[0].start != 0);
  extent_t ex = exv[0];

  if (shouldbe)
    assert(ex.length == 1 && ex.start == shouldbe);

  dout(20) << "bh_write  " << *on << " " << *bh << " to " << ex << dendl;

  //assert(bh->tx_ioh == 0);

  assert(bh->get_last_flushed() < bh->get_version());

  bh->tx_block = ex.start;
  bh->tx_ioh = dev.write(ex.start, ex.length, bh->data,
                         new C_OC_TxFinish(ebofs_lock, on->oc, 
                                           bh->start(), bh->length(),
                                           bh->get_version(),
                                           bh->epoch_modified),
                         "bh_write");

  on->oc->get();
  inc_unflushed( EBOFS_BC_FLUSH_BHWRITE, bh->epoch_modified );
}


bool BufferCache::bh_cancel_write(BufferHead *bh, version_t cur_epoch)
{
  assert(bh->is_tx());
  assert(bh->epoch_modified == cur_epoch);
  assert(bh->epoch_modified > 0);
  if (bh->tx_ioh && dev.cancel_io(bh->tx_ioh) >= 0) {
    dout(10) << "bh_cancel_write on " << *bh << dendl;
    bh->tx_ioh = 0;
    mark_dirty(bh);

    dec_unflushed( EBOFS_BC_FLUSH_BHWRITE, bh->epoch_modified );   // assert.. this should be the same epoch!

    int l = bh->oc->put();
    assert(l);
    return true;
  }
  return false;
}

void BufferCache::tx_finish(ObjectCache *oc, 
                            ioh_t ioh, block_t start, block_t length, 
                            version_t version, version_t epoch)
{
  ebofs_lock.Lock();

  // finish oc
  if (oc->put() == 0) {
    delete oc;
  } else
    oc->tx_finish(ioh, start, length, version, epoch);
  
  // update unflushed counter
  assert(get_unflushed(EBOFS_BC_FLUSH_BHWRITE, epoch) > 0);
  dec_unflushed(EBOFS_BC_FLUSH_BHWRITE, epoch);

  ebofs_lock.Unlock();
}

void BufferCache::rx_finish(ObjectCache *oc,
                            ioh_t ioh, block_t start, block_t length,
                            block_t diskstart, 
                            bufferlist& bl)
{
  ebofs_lock.Lock();
  dout(10) << "rx_finish ioh " << ioh << " on " << start << "~" << length
            << ", at device block " << diskstart << dendl;

  // oc
  if (oc->put() == 0) 
    delete oc;
  else
    oc->rx_finish(ioh, start, length, bl);

  // done.
  ebofs_lock.Unlock();
}

