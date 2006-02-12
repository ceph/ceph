// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
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


#include "Allocator.h"
#include "Ebofs.h"


#undef dout
#define dout(x) if (x <= g_conf.debug_ebofs) cout << "ebofs.allocator." 


void Allocator::dump_freelist()
{
  if (0) {
	interval_set<block_t> free;     // validate too
	
	block_t n = 0;
	for (int b=0; b<=EBOFS_NUM_FREE_BUCKETS; b++) {
	  Table<block_t,block_t> *tab;
	  if (b < EBOFS_NUM_FREE_BUCKETS) {
		tab = fs->free_tab[b];
		dout(30) << "dump bucket " << b << "  " << tab->get_num_keys() << endl;
	  } else {
		tab = fs->limbo_tab;
		dout(30) << "dump limbo  " << tab->get_num_keys() << endl;;
	  }

	  if (tab->get_num_keys() > 0) {
		Table<block_t,block_t>::Cursor cursor(tab);
		assert(tab->find(0, cursor) >= 0);
		while (1) {
		  dout(30) << "dump  ex " << cursor.current().key << "~" << cursor.current().value << endl;
		  assert(cursor.current().value > 0);

		  if (b < EBOFS_NUM_FREE_BUCKETS)
			n += cursor.current().value;

		  if (free.contains( cursor.current().key, cursor.current().value )) 
			dout(0) << "dump   bad " << cursor.current().key << "~" << cursor.current().value << endl;
		  assert(!free.contains( cursor.current().key, cursor.current().value ));
		  free.insert( cursor.current().key, cursor.current().value );
		  if (cursor.move_right() <= 0) break;
		}
	  } else {
		//cout << "  empty" << endl;
	  }
	}
	
	assert(n == fs->free_blocks);
	dout(31) << "dump combined freelist is " << free << endl;
  }
}


int Allocator::find(Extent& ex, int bucket, block_t num, block_t near)
{
  Table<block_t,block_t>::Cursor cursor(fs->free_tab[bucket]);
  bool found = false;


  if (fs->free_tab[bucket]->find( near, cursor ) >= 0) {
	// look to the right
	do {
	  if (cursor.current().value >= num)
		found = true;
	} while (!found && cursor.move_right() > 0);
  }

  if (!found) {
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

int Allocator::allocate(Extent& ex, block_t num, block_t near)
{
  dump_freelist();

  /*
  if (!near) {
	near = num/2;  // this is totally wrong and stupid.
  }
  */

  int bucket;

  // look for contiguous extent
  for (bucket = pick_bucket(num); bucket < EBOFS_NUM_FREE_BUCKETS; bucket++) {
	if (find(ex, bucket, num, near) >= 0) {
	  // yay!

	  // remove original
	  fs->free_tab[bucket]->remove( ex.start );
	  fs->free_blocks -= ex.length;

	  if (ex.length > num) {
		if (ex.start < near) {
		  // to the left
		  if (ex.start + ex.length - num <= near) {
			// by a lot.  take right-most portion.
			Extent left;
			left.start = ex.start;
			left.length = ex.length - num;
			ex.start += left.length;
			ex.length -= left.length;
			assert(ex.length == num);
			_release_loner(left);
		  } else {
			// take middle part.
			Extent left,right;
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
		  Extent right;
		  right.start = ex.start + num;
		  right.length = ex.length - num;
		  ex.length = num;
		  _release_loner(right);
		}
	  }

	  dout(20) << "allocate " << ex << " near " << near << endl;
	  dump_freelist();
	  return num;
	}
  }

  // ok, find partial extent instead.
  for (block_t trysize = num/2; trysize >= 1; trysize /= 2) {
	int bucket = pick_bucket(trysize);
	if (find(ex, bucket, trysize, near) >= 0) {
	  // yay!
	  assert(ex.length < num);
	  
	  fs->free_tab[bucket]->remove(ex.start);
	  fs->free_blocks -= ex.length;
	  dout(20) << "allocate partial " << ex << " near " << near << endl;
	  dump_freelist();
	  return ex.length;
	}	
  }

  dout(1) << "allocate failed, fs completely full!  " << fs->free_blocks << endl;
  assert(0);
  dump_freelist();
  return -1;
}

int Allocator::release(Extent& ex)
{
  dout(20) << "release " << ex << " (into limbo)" << endl;
  assert(ex.length > 0);
  limbo.insert(ex.start, ex.length);
  fs->limbo_blocks += ex.length;
  return 0;
}

int Allocator::commit_limbo()
{
  dout(20) << "commit_limbo" << endl;
  for (map<block_t,block_t>::iterator i = limbo.m.begin();
	   i != limbo.m.end();
	   i++) {
	fs->limbo_tab->insert(i->first, i->second);
	//fs->free_blocks += i->second;
  }
  limbo.clear();
  //fs->limbo_blocks = 0;
  dump_freelist();
  return 0;
}

int Allocator::release_limbo()
{
  dump_freelist();
  if (fs->limbo_tab->get_num_keys() > 0) {
	Table<block_t,block_t>::Cursor cursor(fs->limbo_tab);
	fs->limbo_tab->find(0, cursor);
	while (1) {
	  Extent ex(cursor.current().key, cursor.current().value);
	  dout(20) << "release_limbo  ex " << ex << endl;

	  fs->limbo_blocks -= ex.length;
	  _release_merge(ex);

	  if (cursor.move_right() <= 0) break;
	}
  }
  fs->limbo_tab->clear();
  dump_freelist();
  return 0;
}



/*
 * release extent into freelist
 * WARNING: *ONLY* use this if you _know_ there are no adjacent free extents
 */
int Allocator::_release_loner(Extent& ex) 
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
int Allocator::_release_merge(Extent& orig) 
{
  dout(15) << "_release_merge " << orig << endl;
  assert(orig.length > 0);

  Extent newex = orig;
  
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
