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



#include "common/config.h"
#include "Allocator.h"
#include "Ebofs.h"


#define DOUT_SUBSYS ebofs
#undef dout_prefix
#define dout_prefix *_dout << "ebofs(" << fs->dev.get_device_name() << ").allocator."


void Allocator::dump_freelist()
{
  if (1) {
    interval_set<block_t> free;     // validate too
    
    block_t n = 0;
    for (int b=0; b<=EBOFS_NUM_FREE_BUCKETS; b++) {
      Table<block_t,block_t> *tab;
      if (b < EBOFS_NUM_FREE_BUCKETS) {
        tab = fs->free_tab[b];
        dout(0) << "dump bucket " << b << "  " << tab->get_num_keys() << dendl;
      } else {
        tab = fs->limbo_tab;
        dout(0) << "dump limbo  " << tab->get_num_keys() << dendl;;
      }

      if (tab->get_num_keys() > 0) {
        Table<block_t,block_t>::Cursor cursor(tab);
        assert(tab->find(0, cursor) >= 0);
        while (1) {
          dout(0) << "dump  ex " << cursor.current().key << "~" << cursor.current().value << dendl;
          assert(cursor.current().value > 0);

          if (b < EBOFS_NUM_FREE_BUCKETS)
            n += cursor.current().value;

          if (free.contains( cursor.current().key, cursor.current().value )) 
            dout(0) << "dump   bad " << cursor.current().key << "~" << cursor.current().value << dendl;
          assert(!free.contains( cursor.current().key, cursor.current().value ));
          free.insert( cursor.current().key, cursor.current().value );
          if (cursor.move_right() <= 0) break;
        }
      } else {
        //dout(0) << "  empty" << dendl;
      }
    }
    
    assert(n == fs->free_blocks);
    dout(0) << "dump combined freelist is " << free << dendl;

    
    // alloc_tab
    if (fs->alloc_tab->get_num_keys() > 0) {
      Table<block_t,pair<block_t,int> >::Cursor cursor(fs->alloc_tab);
      assert(fs->alloc_tab->find(0, cursor) >= 0);
      while (1) {
	dout(0) << "alloc  ex " << cursor.current().key << "~" << cursor.current().value.first << " ref "
		<< cursor.current().value.second
		<< dendl;
	assert(cursor.current().value.first > 0);
	
	if (cursor.move_right() <= 0) break;
      }
    }
  }
}


int Allocator::find(extent_t& ex, int bucket, block_t num, block_t near, int dir)
{
  Table<block_t,block_t>::Cursor cursor(fs->free_tab[bucket]);
  bool found = false;

  if ((dir == DIR_ANY || dir == DIR_FWD) && 
      fs->free_tab[bucket]->find( near, cursor ) >= 0) {
    // look to the right
    do {
      if (cursor.current().value >= num)
        found = true;
    } while (!found && cursor.move_right() > 0);
  }

  if ((dir == DIR_ANY || dir == DIR_BACK) && 
      !found) {
    // look to the left
    fs->free_tab[bucket]->find( near, cursor );

    while (!found && cursor.move_left() >= 0) 
      if (cursor.current().value >= num)
        found = true;
  }

  if (found) {
    ex.start = cursor.current().key;
    ex.length = cursor.current().value;
    return 0;
  }
  
  return -1;
}

int Allocator::allocate(extent_t& ex, block_t num, block_t near)
{
  //dump_freelist();

  int dir = DIR_ANY; // no dir
  if (near == NEAR_LAST_FWD) {
    near = last_pos;
    dir = DIR_FWD;  // fwd
  }
  else if (near == NEAR_LAST)
    near = last_pos;

  int bucket;

  while (1) {  // try twice, if fwd = true

    // look for contiguous extent
    for (bucket = pick_bucket(num); bucket < EBOFS_NUM_FREE_BUCKETS; bucket++) {
      if (find(ex, bucket, num, near, dir) >= 0) {
        // yay!
        
        // remove original
        fs->free_tab[bucket]->remove( ex.start );
        fs->free_blocks -= ex.length;
        
        if (ex.length > num) {
          if (ex.start < near) {
            // to the left
            if (ex.start + ex.length - num <= near) {
              // by a lot.  take right-most portion.
              extent_t left;
              left.start = ex.start;
              left.length = ex.length - num;
              ex.start += left.length;
              ex.length -= left.length;
              assert(ex.length == num);
              _release_loner(left);
            } else {
              // take middle part.
              extent_t left,right;
              left.start = ex.start;
              left.length = near - ex.start;
              ex.start = near;
              right.start = ex.start + num;
              right.length = ex.length - left.length - num;
              ex.length = num;
              _release_loner(left);
              _release_loner(right);
            }
          }
          else {
            // to the right.  take left-most part.
            extent_t right;
            right.start = ex.start + num;
            right.length = ex.length - num;
            ex.length = num;
            _release_loner(right);
          }
        }
        
        dout(20) << "allocate " << ex << " near " << near << dendl;
        last_pos = ex.end();
        //dump_freelist();
	if (g_conf.ebofs_cloneable)
	  alloc_inc(ex);
        return num;
      }
    }

    if (dir == DIR_BACK || dir == DIR_ANY) break;
    dir = DIR_BACK;
  }

  // ok, find partial extent instead.
  for (block_t trysize = num/2; trysize >= 1; trysize /= 2) {
    int bucket = pick_bucket(trysize);
    if (find(ex, bucket, trysize, near) >= 0) {
      // yay!
      assert(ex.length < num);
      
      fs->free_tab[bucket]->remove(ex.start);
      fs->free_blocks -= ex.length;
      last_pos = ex.end();
      dout(20) << "allocate partial " << ex << " (wanted " << num << ") near " << near << dendl;
      //dump_freelist();
      if (g_conf.ebofs_cloneable)
	alloc_inc(ex);
      return ex.length;
    }    
  }

  dout(1) << "allocate failed, fs completely full!  " << fs->free_blocks << dendl;
  assert(0);
  //dump_freelist();
  return -1;
}

int Allocator::_release_into_limbo(extent_t& ex)
{
  dout(10) << "_release_into_limbo " << ex << dendl;
  dout(10) << "limbo is " << limbo << dendl;
  assert(ex.length > 0);
  limbo.insert(ex.start, ex.length);
  fs->limbo_blocks += ex.length;
  return 0;
}

int Allocator::release(extent_t& ex)
{
  if (g_conf.ebofs_cloneable)
    return alloc_dec(ex);

  _release_into_limbo(ex);
  return 0;
}

int Allocator::commit_limbo()
{
  dout(20) << "commit_limbo" << dendl;
  for (map<block_t,block_t>::iterator i = limbo.m.begin();
       i != limbo.m.end();
       i++) {
    fs->limbo_tab->insert(i->first, i->second);
    //fs->free_blocks += i->second;
  }
  limbo.clear();
  //fs->limbo_blocks = 0;
  //dump_freelist();
  return 0;
}

int Allocator::release_limbo()
{
  //dump_freelist();
  if (fs->limbo_tab->get_num_keys() > 0) {
    Table<block_t,block_t>::Cursor cursor(fs->limbo_tab);
    fs->limbo_tab->find(0, cursor);
    while (1) {
      extent_t ex = {cursor.current().key, cursor.current().value};
      dout(20) << "release_limbo  ex " << ex << dendl;

      fs->limbo_blocks -= ex.length;
      _release_merge(ex);

      if (cursor.move_right() <= 0) break;
    }
  }
  fs->limbo_tab->clear();
  //dump_freelist();
  return 0;
}



/*
int Allocator::_alloc_loner_inc(extent_t& ex)
{
  Table<block_t,pair<block_t,int> >::Cursor cursor(fs->alloc_tab);
  
  if (fs->alloc_tab->find( ex.start, cursor ) 
      == Table<block_t,pair<block_t,int> >::Cursor::MATCH) {
    assert(cursor.current().value.first == ex.length);
    pair<block_t,int>& v = cursor.dirty_current_value();
    v.second++;
    dout(10) << "_alloc_loner_inc " << ex << " "
             << (v.second-1) << " -> " << v.second 
             << dendl;
  } else {
    // insert it, @1
    fs->alloc_tab->insert(ex.start, pair<block_t,int>(ex.length,1));
    dout(10) << "_alloc_loner_inc " << ex << " 0 -> 1" << dendl;
  }
  return 0;
}

int Allocator::_alloc_loner_dec(extent_t& ex)
{
  Table<block_t,pair<block_t,int> >::Cursor cursor(fs->alloc_tab);
  
  if (fs->alloc_tab->find( ex.start, cursor ) 
      == Table<block_t,pair<block_t,int> >::Cursor::MATCH) {
    assert(cursor.current().value.first == ex.length);
    if (cursor.current().value.second == 1) {
      dout(10) << "_alloc_loner_dec " << ex << " 1 -> 0" << dendl;
      fs->alloc_tab->remove( cursor.current().key );
    } else {
      pair<block_t,int>& v = cursor.dirty_current_value();
      --v.second;
      dout(10) << "_alloc_loner_dec " << ex << " "
               << (v.second+1) << " -> " << v.second 
               << dendl;
    }
  } else {
    assert(0);
  }
  return 0;
}
*/


int Allocator::alloc_inc(extent_t ex)
{
  dout(10) << "alloc_inc " << ex << dendl;

  // empty table?
  if (fs->alloc_tab->get_num_keys() == 0) {
    // easy.
    fs->alloc_tab->insert(ex.start, pair<block_t,int>(ex.length,1));
    dout(10) << "alloc_inc + " << ex << " 0 -> 1 (first entry)" << dendl;
    return 0;
  }

  Table<block_t,pair<block_t,int> >::Cursor cursor(fs->alloc_tab);

  // try to move to left (to check for overlap)
  int r = fs->alloc_tab->find( ex.start, cursor );
  if (r == Table<block_t,pair<block_t,int> >::Cursor::OOB ||
      cursor.current().key > ex.start) {
    r = cursor.move_left();
    dout(10) << "alloc_inc move_left r = " << r << dendl;
  }
  
  while (1) {
    dout(10) << "alloc_inc loop at " << cursor.current().key 
	     << "~" << cursor.current().value.first
	     << " ref " << cursor.current().value.second
	     << dendl;

    // too far left?
    if (cursor.current().key < ex.start &&
	cursor.current().key + cursor.current().value.first <= ex.start) {
      // adjacent?
      bool adjacent = false;
      if (cursor.current().key + cursor.current().value.first == ex.start &&
	  cursor.current().value.second == 1) 
	adjacent = true;

      // no overlap.
      r = cursor.move_right();
      dout(10) << "alloc_inc move_right r = " << r << dendl;
      
      // at end?
      if (r <= 0) {
	// hmm!
	if (adjacent) {
	  // adjust previous entry
	  cursor.move_left();
	  pair<block_t,int> &v = cursor.dirty_current_value();
	  v.first += ex.length; // yay!
	  dout(10) << "alloc_inc + " << ex << " 0 -> 1 (adjust at end)" << dendl;
	} else {
	  // insert at end, finish.
	  int r = fs->alloc_tab->insert(ex.start, pair<block_t,int>(ex.length,1));
	  dout(10) << "alloc_inc + " << ex << " 0 -> 1 (at end) .. r = " << r << dendl;
	  //dump_freelist();
	}
	return 0;
      }
    }
    dout(10) << "alloc_inc at " << cursor.current().key
	     << "~" << cursor.current().value.first
	     << " ref " << cursor.current().value.second << dendl;
    if (cursor.current().key > ex.start) {
      // gap.
      //    oooooo
      //  nnnnn.....
      block_t l = MIN(ex.length, cursor.current().key - ex.start);
      
      fs->alloc_tab->insert(ex.start, pair<block_t,int>(l,1));
      dout(10) << "alloc_inc + " << ex.start << "~" << l << " 0 -> 1" << dendl;
      ex.start += l;
      ex.length -= l;
      if (ex.length == 0) break;
      fs->alloc_tab->find( ex.start, cursor );
    } 
    else if (cursor.current().key < ex.start) {
      block_t end = cursor.current().value.first + cursor.current().key;

      if (end <= ex.end()) {
	// single split
	// oooooo
	//    nnnnn
	pair<block_t,int>& v = cursor.dirty_current_value();
	v.first = ex.start - cursor.current().key;
	int ref = v.second;

	block_t l = end - ex.start;
	fs->alloc_tab->insert(ex.start, pair<block_t,int>(l, 1+ref));
	
	dout(10) << "alloc_inc   " << ex.start << "~" << l 
		 << " " << ref << " -> " << ref+1
		 << " (right split)" << dendl;
	
	ex.start += l;
	ex.length -= l;
	if (ex.length == 0) break;
	fs->alloc_tab->find( ex.start, cursor );

      } else {
	// double split, finish.
	// -------------
	//    ------
	pair<block_t,int>& v = cursor.dirty_current_value();
	v.first = ex.start - cursor.current().key;
	int ref = v.second;
	
	fs->alloc_tab->insert(ex.start, pair<block_t,int>(ex.length, 1+ref));

	int rl = end - ex.end();
	fs->alloc_tab->insert(ex.end(), pair<block_t,int>(rl, ref));

	dout(10) << "alloc_inc   " << ex
		 << " " << ref << " -> " << ref+1
		 << " (double split finish)"
		 << dendl;

	break;
      }
    } 
    else {
      assert(cursor.current().key == ex.start);
      
      if (cursor.current().value.first <= ex.length) {
	// inc.
	// oooooo
	// nnnnnnnn
	pair<block_t,int>& v = cursor.dirty_current_value();
	v.second++;
	dout(10) << "alloc_inc   " << ex.start << "~" << cursor.current().value.first 
		 << " " << cursor.current().value.second-1 << " -> "
		 << cursor.current().value.second 
		 << " (left split)" << dendl;
	ex.start += v.first;
	ex.length -= v.first;
	if (ex.length == 0) break;
	cursor.move_right();
      } else {
	// single split, finish.
	// oooooo
	// nnn
	block_t l = cursor.current().value.first - ex.length;
	int ref = cursor.current().value.second;

	pair<block_t,int>& v = cursor.dirty_current_value();
	v.first = ex.length;
	v.second++;
	
	fs->alloc_tab->insert(ex.end(), pair<block_t,int>(l, ref));

	dout(10) << "alloc_inc   " << ex
		 << " " << ref << " -> " << ref+1
		 << " (left split finish)"
		 << dendl;
	
	break;
      }
    }
  }

  return 0;
}


int Allocator::alloc_dec(extent_t ex)
{
  dout(10) << "alloc_dec " << ex << dendl;

  assert(fs->alloc_tab->get_num_keys() >= 0);
  
  Table<block_t,pair<block_t,int> >::Cursor cursor(fs->alloc_tab);

  // try to move to left (to check for overlap)
  int r = fs->alloc_tab->find( ex.start, cursor );
  dout(10) << "alloc_dec find r = " << r << dendl;

  if (r == Table<block_t,pair<block_t,int> >::Cursor::OOB ||
      cursor.current().key > ex.start) {
    r = cursor.move_left();
    dout(10) << "alloc_dec move_left r = " << r << dendl;

    // too far left?
    if (cursor.current().key < ex.start &&
	cursor.current().key + cursor.current().value.first <= ex.start) {
      // no overlap.
      dout(10) << "alloc_dec no overlap " << cursor.current().key
	       << "~" << cursor.current().value.first
	       << " " << cursor.current().value.second
	       << " with " << ex << dendl;
      dump_freelist();
      assert(0);
    }
  }

  while (1) {
    dout(10) << "alloc_dec ? " << cursor.current().key 
	     << "~" << cursor.current().value.first
	     << " " << cursor.current().value.second
	     << ", ex is " << ex
	     << dendl;
    
    assert(cursor.current().key <= ex.start);  // no gap allowed.
    
    if (cursor.current().key < ex.start) {
      block_t end = cursor.current().value.first + cursor.current().key;
      
      if (end <= ex.end()) {
	// single split
	// oooooo
	//    -----
	pair<block_t,int>& v = cursor.dirty_current_value();
	v.first = ex.start - cursor.current().key;
	int ref = v.second;
	dout(10) << "alloc_dec s " << cursor.current().key << "~" << cursor.current().value.first 
		 << " " << ref
		 << " shortened left bit of single" << dendl;

	block_t l = end - ex.start;
	if (ref > 1) {
	  fs->alloc_tab->insert(ex.start, pair<block_t,int>(l, ref-1));
	  dout(10) << "alloc_dec . " << ex.start << "~" << l 
		   << " " << ref << " -> " << ref-1
		   << dendl;
	} else {
	  extent_t r = {ex.start, l};
	  _release_into_limbo(r);
	}
		
	ex.start += l;
	ex.length -= l;
	if (ex.length == 0) break;
	fs->alloc_tab->find( ex.start, cursor );

      } else {
	// double split, finish.
	// ooooooooooooo
	//    ------
	pair<block_t,int>& v = cursor.dirty_current_value();
	v.first = ex.start - cursor.current().key;
	int ref = v.second;
	dout(10) << "alloc_dec s " << cursor.current().key << "~" << cursor.current().value.first
		 << " " << ref 
		 << " shorted left bit of double split" << dendl;

	if (ref > 1) {
	  fs->alloc_tab->insert(ex.start, pair<block_t,int>(ex.length, ref-1));
	  dout(10) << "alloc_inc s " << ex
		   << " " << ref << " -> " << ref-1
		   << " reinserted middle bit of double split"
		   << dendl;
	} else {
	  _release_into_limbo(ex);
	}

	int rl = end - ex.end();
	fs->alloc_tab->insert(ex.end(), pair<block_t,int>(rl, ref));
	dout(10) << "alloc_dec s " << ex.end() << "~" << rl
		 << " " << ref 
		 << " reinserted right bit of double split" << dendl;
	break;
      }
    } 
    else {
      assert(cursor.current().key == ex.start);
      
      if (cursor.current().value.first <= ex.length) {
	// inc.
	// oooooo
	// nnnnnnnn
	if (cursor.current().value.second > 1) {
	  pair<block_t,int>& v = cursor.dirty_current_value();
	  v.second--;
	  dout(10) << "alloc_dec s " << ex.start << "~" << cursor.current().value.first 
		   << " " << cursor.current().value.second+1 << " -> " << cursor.current().value.second 
		   << dendl;
	  ex.start += v.first;
	  ex.length -= v.first;
	  if (ex.length == 0) break;
	  cursor.move_right();
	} else {
	  extent_t r = {cursor.current().key, cursor.current().value.first};
	  _release_into_limbo(r);

	  ex.start += cursor.current().value.first;
	  ex.length -= cursor.current().value.first;
	  fs->alloc_tab->remove(cursor.current().key);

	  if (ex.length == 0) break;
	  fs->alloc_tab->find( ex.start, cursor );
	}
      } else {
	// single split, finish.
	// oooooo
	// nnn
	block_t l = cursor.current().value.first - ex.length;
	int ref = cursor.current().value.second;

	if (ref > 1) {
	  pair<block_t,int>& v = cursor.dirty_current_value();
	  v.first = ex.length;
	  v.second--;
	  dout(10) << "alloc_inc . " << ex
		   << " " << ref << " -> " << ref-1
		   << dendl;
	} else {
	  fs->alloc_tab->remove(cursor.current().key);
	  _release_into_limbo(ex);
	}
	
	dout(10) << "alloc_dec s " << ex.end() << "~" << l
		 << " " << ref 
		 << " reinserted right bit of single split" << dendl;
	fs->alloc_tab->insert(ex.end(), pair<block_t,int>(l, ref));
	break;
      }
    }


  }

  return 0;
}


/*
 * release extent into freelist
 * WARNING: *ONLY* use this if you _know_ there are no adjacent free extents
 */
int Allocator::_release_loner(extent_t& ex) 
{
  assert(ex.length > 0);
  int b = pick_bucket(ex.length);
  fs->free_tab[b]->insert(ex.start, ex.length);
  fs->free_blocks += ex.length;
  return 0;
}

/*
 * release extent into freelist
 * look for any adjacent extents and merge with them!
 */
int Allocator::_release_merge(extent_t& orig) 
{
  dout(15) << "_release_merge " << orig << dendl;
  assert(orig.length > 0);

  extent_t newex = orig;
  
  // one after us?
  for (int b=0; b<EBOFS_NUM_FREE_BUCKETS; b++) {
    Table<block_t,block_t>::Cursor cursor(fs->free_tab[b]);
    
    if (fs->free_tab[b]->find( newex.start+newex.length, cursor ) 
        == Table<block_t,block_t>::Cursor::MATCH) {
      // add following extent to ours
      newex.length += cursor.current().value;
      
      // remove it
      fs->free_blocks -= cursor.current().value;
      fs->free_tab[b]->remove( cursor.current().key );
      break;
    }
  }
  
  // one before us?
  for (int b=0; b<EBOFS_NUM_FREE_BUCKETS; b++) {
    Table<block_t,block_t>::Cursor cursor(fs->free_tab[b]);
    fs->free_tab[b]->find( newex.start+newex.length, cursor );
    if (cursor.move_left() >= 0 &&
        (cursor.current().key + cursor.current().value == newex.start)) {
      // merge
      newex.start = cursor.current().key;
      newex.length += cursor.current().value;

      // remove it
      fs->free_blocks -= cursor.current().value;
      fs->free_tab[b]->remove( cursor.current().key );
      break;
    }
  }
  
  // ok, insert newex
  _release_loner(newex);
  return 0;
}
