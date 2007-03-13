// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


/*********** BufferHead **************/


#undef dout
#define dout(x)  if (x <= g_conf.debug_ebofs) cout << "ebofs.bh."






/************ ObjectCache **************/


#undef dout
#define dout(x)  if (x <= g_conf.debug_ebofs) cout << "ebofs.oc."



void ObjectCache::rx_finish(ioh_t ioh, block_t start, block_t length, bufferlist& bl)
{
  list<Context*> waiters;

  dout(10) << "rx_finish " << start << "~" << length << endl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
       p != data.end(); 
       p++) {
    BufferHead *bh = p->second;
    dout(10) << "rx_finish ?" << *bh << endl;
    assert(p->first == bh->start());

    // past?
    if (p->first >= start+length) break;
    if (bh->end() > start+length) break;  // past
    
    assert(p->first >= start);
    assert(bh->end() <= start+length);

    dout(10) << "rx_finish !" << *bh << endl;

    if (bh->rx_ioh == ioh)
      bh->rx_ioh = 0;

    if (bh->is_rx()) {
      assert(bh->get_version() == 0);
      assert(bh->end() <= start+length);
      assert(bh->start() >= start);
      dout(10) << "rx_finish  rx -> clean on " << *bh << endl;
      bh->data.substr_of(bl, (bh->start()-start)*EBOFS_BLOCK_SIZE, bh->length()*EBOFS_BLOCK_SIZE);
      bc->mark_clean(bh);
    }
    else if (bh->is_partial()) {
      dout(10) << "rx_finish  partial -> tx on " << *bh << endl;      

      if (1) {
        // double-check what block i am
        vector<Extent> exv;
        on->map_extents(bh->start(), 1, exv);
        assert(exv.size() == 1);
        block_t cur_block = exv[0].start;
        assert(cur_block == bh->partial_tx_to);
      }
      
      // ok, cancel my low-level partial (since we're still here, and can bh_write ourselves)
      bc->cancel_partial( bh->rx_from.start, bh->partial_tx_to, bh->partial_tx_epoch );
      
      // apply partial to myself
      assert(bh->data.length() == 0);
      bufferptr bp = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
      bh->data.push_back( bp );
      bh->data.copy_in(0, EBOFS_BLOCK_SIZE, bl);
      bh->apply_partial();
      
      // write "normally"
      bc->mark_dirty(bh);
      bc->bh_write(on, bh, bh->partial_tx_to);//cur_block);

      // clean up a bit
      bh->partial_tx_to = 0;
      bh->partial_tx_epoch = 0;
      bh->partial.clear();
    }
    else {
      dout(10) << "rx_finish  ignoring status on (dirty|tx|clean) " << *bh << endl;
      assert(bh->is_dirty() ||  // was overwritten
             bh->is_tx() ||     // was overwritten and queued
             bh->is_clean());   // was overwritten, queued, _and_ flushed to disk
    }

    // trigger waiters
    for (map<block_t,list<Context*> >::iterator p = bh->waitfor_read.begin();
         p != bh->waitfor_read.end();
         p++) {
      assert(p->first >= bh->start() && p->first < bh->end());
      waiters.splice(waiters.begin(), p->second);
    }
    bh->waitfor_read.clear();
  }    

  finish_contexts(waiters);
}


void ObjectCache::tx_finish(ioh_t ioh, block_t start, block_t length, 
                            version_t version, version_t epoch)
{
  dout(10) << "tx_finish " << start << "~" << length << " v" << version << endl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
       p != data.end(); 
       p++) {
    BufferHead *bh = p->second;
    dout(30) << "tx_finish ?bh " << *bh << endl;
    assert(p->first == bh->start());

    // past?
    if (p->first >= start+length) break;

    if (bh->tx_ioh == ioh)
      bh->tx_ioh = 0;

    if (!bh->is_tx()) {
      dout(10) << "tx_finish  bh not marked tx, skipping" << endl;
      continue;
    }
    assert(bh->is_tx());
    
    if (version == bh->version) {
      dout(10) << "tx_finish  tx -> clean on " << *bh << endl;
      assert(bh->end() <= start+length);
      bh->set_last_flushed(version);
      bc->mark_clean(bh);
    } else {
      dout(10) << "tx_finish  leaving tx, " << bh->version << " > " << version 
               << " on " << *bh << endl;
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
  map<block_t, BufferHead*>::iterator p = data.lower_bound(start);

  block_t cur = start;
  block_t left = len;
  
  if (p != data.begin() && 
      (p == data.end() || p->first > cur)) {
    p--;     // might overlap!
    if (p->first + p->second->length() <= cur) 
      p++;   // doesn't overlap.
  }

  int num_missing = 0;

  while (left > 0) {
    // at end?
    if (p == data.end()) {
      // rest is a miss.
      vector<Extent> exv;
      on->map_extents(cur, 
                      left,   // no prefetch here!
                      exv);

      num_missing += exv.size();
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
        dout(20) << "try_map_read hit " << *e << endl;
      } 
      else if (e->is_rx()) {
        dout(20) << "try_map_read rx " << *e << endl;
	num_missing++;
      }
      else if (e->is_partial()) {
        dout(-20) << "try_map_read partial " << *e << endl;
	num_missing++;
      }
      else {
	dout(0) << "try_map_read got unexpected " << *e << endl;
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
      vector<Extent> exv;
      on->map_extents(cur, 
                      MIN(next-cur, left),   // no prefetch
                      exv);

      dout(-20) << "try_map_read gap of " << p->first-cur << " blocks, " 
		<< exv.size() << " extents" << endl;
      num_missing += exv.size();
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
  
  map<block_t, BufferHead*>::iterator p = data.lower_bound(start);

  block_t cur = start;
  block_t left = len;
  
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
      vector<Extent> exv;
      //on->map_extents(cur, left, exv);          // we might consider some prefetch here.
      on->map_extents(cur, 
                      //MIN(left + g_conf.ebofs_max_prefetch,   // prefetch
                      //on->object_blocks-cur),  
                      left,   // no prefetch
                      exv);
      for (unsigned i=0; i<exv.size() && left > 0; i++) {
        BufferHead *n = new BufferHead(this);
        n->set_start( cur );
        n->set_length( exv[i].length );
        bc->add_bh(n);
        missing[cur] = n;
        dout(20) << "map_read miss " << left << " left, " << *n << endl;
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
          e->is_tx()) {
        hits[cur] = e;     // readable!
        dout(20) << "map_read hit " << *e << endl;
        bc->touch(e);
      } 
      else if (e->is_rx()) {
        rx[cur] = e;       // missing, not readable.
        dout(20) << "map_read rx " << *e << endl;
      }
      else if (e->is_partial()) {
        partial[cur] = e;
        dout(20) << "map_read partial " << *e << endl;
      }
      else {
	dout(0) << "map_read ??? got unexpected " << *e << endl;
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
      vector<Extent> exv;
      on->map_extents(cur, 
                      //MIN(next-cur, MIN(left + g_conf.ebofs_max_prefetch,   // prefetch
                      //                on->object_blocks-cur)),  
                      MIN(next-cur, left),   // no prefetch
                      exv);
      
      for (unsigned i=0; i<exv.size() && left>0; i++) {
        BufferHead *n = new BufferHead(this);
        n->set_start( cur );
        n->set_length( exv[i].length );
        bc->add_bh(n);
        missing[cur] = n;
        cur += MIN(left, n->length());
        left -= MIN(left, n->length());
        dout(20) << "map_read gap " << *n << endl;
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
 * - cancel rx ops we obsolete.
 *   - resubmit rx ops if we split bufferheads
 *
 * - leave potentially obsoleted tx ops alone (for now)
 * - don't worry about disk extent boundaries (yet)
 */
int ObjectCache::map_write(block_t start, block_t len,
                           interval_set<block_t>& alloc,
                           map<block_t, BufferHead*>& hits,
                           version_t super_epoch)
{
  map<block_t, BufferHead*>::iterator p = data.lower_bound(start);

  dout(10) << "map_write " << *on << " " << start << "~" << len << " ... alloc " << alloc << endl;
  // p->first >= start
  
  block_t cur = start;
  block_t left = len;
  
  if (p != data.begin() && 
      (p == data.end() || p->first > cur)) {
    p--;     // might overlap!
    if (p->first + p->second->length() <= cur) 
      p++;   // doesn't overlap.
  }

  //dump();

  while (left > 0) {
    // max for this bh (bc of (re)alloc on disk)
    block_t max = left;
    bool newalloc = false;

    // based on alloc/no-alloc boundary ...
    if (alloc.contains(cur, left)) {
      if (alloc.contains(cur)) {
        block_t ends = alloc.end_after(cur);
        max = MIN(left, ends-cur);
        newalloc = true;
      } else {
        if (alloc.starts_after(cur)) {
          block_t st = alloc.start_after(cur);
          max = MIN(left, st-cur);
        } 
      }
    } 

    // based on disk extent boundary ...
    vector<Extent> exv;
    on->map_extents(cur, max, exv);
    if (exv.size() > 1) 
      max = exv[0].length;

    if (newalloc) {
      dout(10) << "map_write " << cur << "~" << max << " is new alloc on disk" << endl;
    } else {
      dout(10) << "map_write " << cur << "~" << max << " keeps old alloc on disk" << endl;
    }
    
    // at end?
    if (p == data.end()) {
      BufferHead *n = new BufferHead(this);
      n->set_start( cur );
      n->set_length( max );
      bc->add_bh(n);
      hits[cur] = n;
      left -= max;
      cur += max;
      continue;
    }
    
    dout(10) << "p is " << *p->second << endl;


    if (p->first <= cur) {
      BufferHead *bh = p->second;
      dout(10) << "map_write bh " << *bh << " intersected" << endl;

      if (p->first < cur) {
        if (cur+max >= p->first+p->second->length()) {
          // we want right bit (one splice)
          if (bh->is_rx() && bc->bh_cancel_read(bh)) {
            BufferHead *right = bc->split(bh, cur);
            bc->bh_read(on, bh);          // reread left bit
            bh = right;
          } else if (bh->is_tx() && !newalloc && bc->bh_cancel_write(bh, super_epoch)) {
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
          } else if (bh->is_tx() && !newalloc && bc->bh_cancel_write(bh, super_epoch)) {
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
          } else if (bh->is_tx() && !newalloc && bc->bh_cancel_write(bh, super_epoch)) {
            BufferHead *right = bc->split(bh, cur+max);
            bc->bh_write(on, right);              // re-tx the right bit
          } else {
            bc->split(bh, cur+max);        // just split
          }
        }
      }
      
      // try to cancel tx?
      if (bh->is_tx() && !newalloc) bc->bh_cancel_write(bh, super_epoch);
            
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
      dout(10) << "map_write gap " << cur << "~" << glen << endl;
      BufferHead *n = new BufferHead(this);
      n->set_start( cur );
      n->set_length( glen );
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
    
    dout(12) << "moving " << *bh << " to bottom of lru" << endl;
    bc->touch_bottom(bh);  // move to bottom of lru list
  }
}  


void ObjectCache::truncate(block_t blocks, version_t super_epoch)
{
  dout(7) << "truncate " << object_id 
           << " " << blocks << " blocks"
           <<  endl;

  while (!data.empty()) {
    block_t bhoff = data.rbegin()->first;
    BufferHead *bh = data.rbegin()->second;

    if (bh->end() <= blocks) break;

    bool uncom = on->uncommitted.contains(bh->start(), bh->length());
    dout(10) << "truncate " << *bh << " uncom " << uncom 
             << " of " << on->uncommitted
             << endl;
    
    if (bhoff < blocks) {
      // we want right bit (one splice)
      if (bh->is_rx() && bc->bh_cancel_read(bh)) {
        BufferHead *right = bc->split(bh, blocks);
        bc->bh_read(on, bh);          // reread left bit
        bh = right;
      } else if (bh->is_tx() && uncom && bc->bh_cancel_write(bh, super_epoch)) {
        BufferHead *right = bc->split(bh, blocks);
        bc->bh_write(on, bh);          // rewrite left bit
        bh = right;
      } else {
        bh = bc->split(bh, blocks);   // just split it
      }
      // no worries about partials up here, they're always 1 block (and thus never split)
    } else {
      // whole thing
      // cancel any pending/queued io, if possible.
      if (bh->is_rx())
        bc->bh_cancel_read(bh);
      if (bh->is_tx() && uncom) 
        bc->bh_cancel_write(bh, super_epoch);
      if (bh->shadow_of) {
	dout(10) << "truncate " << *bh << " unshadowing " << *bh->shadow_of << endl;
	// shadow
	bh->shadow_of->remove_shadow(bh);
	if (bh->is_partial()) 
	  bc->cancel_shadow_partial(bh->rx_from.start, bh);
      } else {
	// normal
	if (bh->is_partial() && uncom)
	  bc->bh_cancel_partial_write(bh);
      }
    }
    
    for (map<block_t,list<Context*> >::iterator p = bh->waitfor_read.begin();
         p != bh->waitfor_read.end();
         p++) {
      finish_contexts(p->second, -1);
    }

    bc->remove_bh(bh);
    delete bh;
  }
}


void ObjectCache::clone_to(Onode *other)
{
  ObjectCache *ton = 0;

  for (map<block_t, BufferHead*>::iterator p = data.begin();
       p != data.end();
       p++) {
    BufferHead *bh = p->second;
    dout(10) << "clone_to ? " << *bh << endl;
    if (bh->is_dirty() || bh->is_tx() || bh->is_partial()) {
      // dup dirty or tx bh's
      if (!ton)
	ton = other->get_oc(bc);
      BufferHead *nbh = new BufferHead(ton);
      nbh->set_start( bh->start() );
      nbh->set_length( bh->length() );
      nbh->data = bh->data;      // just copy refs to underlying buffers. 
      bc->add_bh(nbh);

      if (bh->is_partial()) {
	dout(0) << "clone_to PARTIAL FIXME NOT FULLY IMPLEMENTED ******" << endl;
	nbh->partial = bh->partial;
	bc->mark_partial(nbh);
	// register as shadow_partial
	bc->add_shadow_partial(bh->rx_from.start, nbh);
      } else {
	// clean buffer will shadow
	bh->add_shadow(nbh);
	bc->mark_clean(nbh);
      }

      dout(10) << "clone_to dup " << *bh << " -> " << *nbh << endl;
    } 
  }
}



/************** BufferCache ***************/

#undef dout
#define dout(x)  if (x <= g_conf.debug_ebofs) cout << "ebofs.bc."



BufferHead *BufferCache::split(BufferHead *orig, block_t after) 
{
  dout(20) << "split " << *orig << " at " << after << endl;

  // split off right
  BufferHead *right = new BufferHead(orig->get_oc());
  right->set_version(orig->get_version());
  right->epoch_modified = orig->epoch_modified;
  right->last_flushed = orig->last_flushed;
  right->set_state(orig->get_state());

  block_t newleftlen = after - orig->start();
  right->set_start( after );
  right->set_length( orig->length() - newleftlen );
  
  // shorten left
  stat_sub(orig);
  orig->set_length( newleftlen );
  stat_add(orig);

  // add right
  add_bh(right);

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
      dout(0) << "split  moving waiters at block " << p->first << " to right bh" << endl;
      right->waitfor_read[p->first].swap( p->second );
      o = p;
      p--;
      orig->waitfor_read.erase(o);
    }
  }
  
  dout(20) << "split    left is " << *orig << endl;
  dout(20) << "split   right is " << *right << endl;
  return right;
}


void BufferCache::bh_read(Onode *on, BufferHead *bh, block_t from)
{
  dout(10) << "bh_read " << *on << " on " << *bh << endl;

  if (bh->is_missing()) {
    mark_rx(bh);
  } else {
    assert(bh->is_partial());
  }
  
  // get extent.  there should be only one!
  vector<Extent> exv;
  on->map_extents(bh->start(), bh->length(), exv);
  assert(exv.size() == 1);
  Extent ex = exv[0];

  if (from) {  // force behavior, used for reading partials
    dout(10) << "bh_read  forcing read from block " << from << " (for a partial)" << endl;
    ex.start = from;
    ex.length = 1;
  }
    
  // this should be empty!!
  assert(bh->rx_ioh == 0);
  
  dout(20) << "bh_read  " << *on << " " << *bh << " from " << ex << endl;
  
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
    dout(10) << "bh_cancel_read on " << *bh << endl;
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
  dout(10) << "bh_write " << *on << " on " << *bh << " in epoch " << bh->epoch_modified << endl;
  assert(bh->get_version() > 0);

  assert(bh->is_dirty());
  mark_tx(bh);
  
  // get extents
  vector<Extent> exv;
  on->map_extents(bh->start(), bh->length(), exv);
  assert(exv.size() == 1);
  Extent ex = exv[0];

  if (shouldbe)
    assert(ex.length == 1 && ex.start == shouldbe);

  dout(20) << "bh_write  " << *on << " " << *bh << " to " << ex << endl;

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

  /*
  // assert: no partials on the same block
  // hose any partial on the same block
  if (bh->partial_write.count(ex.start)) {
    dout(10) << "bh_write hosing parital write on same block " << ex.start << " " << *bh << endl;
    dec_unflushed( bh->partial_write[ex.start].epoch );
    bh->partial_write.erase(ex.start);
  }
  */
}


bool BufferCache::bh_cancel_write(BufferHead *bh, version_t cur_epoch)
{
  if (bh->tx_ioh && dev.cancel_io(bh->tx_ioh) >= 0) {
    dout(10) << "bh_cancel_write on " << *bh << endl;
    bh->tx_ioh = 0;
    mark_dirty(bh);

    assert(bh->epoch_modified == cur_epoch);
    assert(bh->epoch_modified > 0);
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
            << ", at device block " << diskstart << endl;

  // oc
  if (oc->put() == 0) 
    delete oc;
  else
    oc->rx_finish(ioh, start, length, bl);

  // finish any partials?
  //  note: these are partials that were re-written after a commit,
  //        or for whom the OC was destroyed (eg truncated after a commit)
  map<block_t, map<block_t, PartialWrite> >::iterator sp = partial_write.lower_bound(diskstart);
  while (sp != partial_write.end()) {
    if (sp->first >= diskstart+length) break;
    assert(sp->first >= diskstart);

    block_t pblock = sp->first;
    map<block_t, PartialWrite> writes;
    writes.swap( sp->second );

    map<block_t, map<block_t, PartialWrite> >::iterator t = sp;
    sp++;
    partial_write.erase(t);

    for (map<block_t, PartialWrite>::iterator p = writes.begin();
         p != writes.end();
         p++) {
      dout(10) << "rx_finish partial from " << pblock << " -> " << p->first
                << " for epoch " << p->second.epoch
        //<< " (bh.epoch_modified is now " << bh->epoch_modified << ")"
                << endl;
      // this had better be a past epoch
      //assert(p->epoch == epoch_modified - 1);  // ??
      
      // make the combined block
      bufferlist combined;
      bufferptr bp = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
      combined.push_back( bp );
      combined.copy_in((pblock-diskstart)*EBOFS_BLOCK_SIZE, (pblock-diskstart+1)*EBOFS_BLOCK_SIZE, bl);
      BufferHead::apply_partial( combined, p->second.partial );

      // write it!
      dev.write( pblock, 1, combined,
                 new C_OC_PartialTxFinish( this, p->second.epoch ),
                 "finish_partials");
    }
  }

  // shadow partials?
  {
    list<Context*> waiters;
    map<block_t, set<BufferHead*> >::iterator sp = shadow_partials.lower_bound(diskstart);
    while (sp != shadow_partials.end()) {
      if (sp->first >= diskstart+length) break;
      assert(sp->first >= diskstart);
      
      block_t pblock = sp->first;
      set<BufferHead*> ls;
      ls.swap( sp->second );
      
      map<block_t, set<BufferHead*> >::iterator t = sp;
      sp++;
      shadow_partials.erase(t);
      
      for (set<BufferHead*>::iterator p = ls.begin();
	   p != ls.end();
	   ++p) {
	BufferHead *bh = *p;
	dout(10) << "rx_finish applying shadow_partial for " << pblock
		 << " to " << *bh << endl;
	bufferptr bp = buffer::create_page_aligned(EBOFS_BLOCK_SIZE);
	bh->data.clear();
	bh->data.push_back( bp );
	bh->data.copy_in((pblock-diskstart)*EBOFS_BLOCK_SIZE, 
			 (pblock-diskstart+1)*EBOFS_BLOCK_SIZE, 
			 bl);
	bh->apply_partial();
	bh->set_state(BufferHead::STATE_CLEAN);
	
	// trigger waiters
	for (map<block_t,list<Context*> >::iterator p = bh->waitfor_read.begin();
	     p != bh->waitfor_read.end();
	     p++) {
	  assert(p->first >= bh->start() && p->first < bh->end());
	  waiters.splice(waiters.begin(), p->second);
	}
	bh->waitfor_read.clear();
      }  
    }

    // kick waiters
    finish_contexts(waiters);
  }

  // done.
  ebofs_lock.Unlock();
}

void BufferCache::partial_tx_finish(version_t epoch)
{
  ebofs_lock.Lock();

  dout(10) << "partial_tx_finish in epoch " << epoch << endl;

  // update unflushed counter
  assert(get_unflushed(EBOFS_BC_FLUSH_PARTIAL, epoch) > 0);
  dec_unflushed(EBOFS_BC_FLUSH_PARTIAL, epoch);

  ebofs_lock.Unlock();
}




void BufferCache::bh_queue_partial_write(Onode *on, BufferHead *bh)
{
  assert(bh->get_version() > 0);

  assert(bh->is_partial());
  assert(bh->length() == 1);
  
  // get the block no
  vector<Extent> exv;
  on->map_extents(bh->start(), bh->length(), exv);
  assert(exv.size() == 1);
  block_t b = exv[0].start;
  assert(exv[0].length == 1);
  bh->partial_tx_to = exv[0].start;
  bh->partial_tx_epoch = bh->epoch_modified;

  dout(10) << "bh_queue_partial_write " << *on << " on " << *bh << " block " << b << " epoch " << bh->epoch_modified << endl;


  // copy map state, queue for this block
  assert(bh->rx_from.length == 1);
  queue_partial( bh->rx_from.start, bh->partial_tx_to, bh->partial, bh->partial_tx_epoch );
}

void BufferCache::bh_cancel_partial_write(BufferHead *bh)
{
  assert(bh->is_partial());
  assert(bh->length() == 1);

  cancel_partial( bh->rx_from.start, bh->partial_tx_to, bh->partial_tx_epoch );
}


void BufferCache::queue_partial(block_t from, block_t to, 
                                map<off_t, bufferlist>& partial, version_t epoch)
{
  dout(10) << "queue_partial " << from << " -> " << to
           << " in epoch " << epoch 
           << endl;
  
  if (partial_write[from].count(to)) {
    // this should be in the same epoch.
    assert( partial_write[from][to].epoch == epoch);
    assert(0); // actually.. no!
  } else {
    inc_unflushed( EBOFS_BC_FLUSH_PARTIAL, epoch );
  }
  
  partial_write[from][to].partial = partial;
  partial_write[from][to].epoch = epoch;
}

void BufferCache::cancel_partial(block_t from, block_t to, version_t epoch)
{
  assert(partial_write.count(from));
  assert(partial_write[from].count(to));
  assert(partial_write[from][to].epoch == epoch);

  dout(10) << "cancel_partial " << from << " -> " << to 
           << "  (was epoch " << partial_write[from][to].epoch << ")"
           << endl;

  partial_write[from].erase(to);
  if (partial_write[from].empty())
    partial_write.erase(from);

  dec_unflushed( EBOFS_BC_FLUSH_PARTIAL, epoch );
}


void BufferCache::add_shadow_partial(block_t from, BufferHead *bh)
{
  dout(10) << "add_shadow_partial from " << from << " " << *bh << endl;
  shadow_partials[from].insert(bh);
}

void BufferCache::cancel_shadow_partial(block_t from, BufferHead *bh)
{
  dout(10) << "cancel_shadow_partial from " << from << " " << *bh << endl;
  shadow_partials[from].erase(bh);
}
