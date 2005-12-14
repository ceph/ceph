
#include "Allocator.h"
#include "Ebofs.h"


#undef dout
#define dout(x) if (x <= g_conf.debug) cout << "ebofs.allocator." 


void Allocator::dump_freelist()
{
  if (0)
  for (int b=0; b<EBOFS_NUM_FREE_BUCKETS; b++) {
	dout(20) << "dump bucket " << b << endl;
	if (fs->free_tab[b]->get_num_keys() > 0) {
	  Table<block_t,block_t>::Cursor cursor(fs->free_tab[b]);
	  fs->free_tab[b]->find(0, cursor);
	  while (1) {
		dout(20) << "dump  ex " << cursor.current().key << "~" << cursor.current().value << endl;
		if (cursor.move_right() <= 0) break;
	  }
	} else {
	  //cout << "  empty" << endl;
	}
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
	} while (!found && cursor.move_right() >= 0);
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
	  fs->free_blocks += ex.length;

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
			release_now(left);
		  } else {
			// take middle part.
			Extent left,right;
			left.start = ex.start;
			left.length = near - ex.start;
			ex.start = near;
			right.start = ex.start + num;
			right.length = ex.length - left.length - num;
			ex.length = num;
			release_now(left);
			release_now(right);
		  }
		}
		else {
		  // to the right.  take left-most part.
		  Extent right;
		  right.start = ex.start + num;
		  right.length = ex.length - num;
		  ex.length = num;
		  release_now(right);
		}
	  }

	  dout(10) << "allocate " << ex << " near " << near << endl;
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
	  dout(10) << "allocate partial " << ex << " near " << near << endl;
	  dump_freelist();
	  return ex.length;
	}	
  }

  dout(1) << "allocate failed, fs full!  " << fs->free_blocks << endl;
  dump_freelist();
  return -1;
}

int Allocator::release(Extent& ex)
{
  dout(10) << "release " << ex << " (into limbo)" << endl;
  limbo.insert(ex.start, ex.length);
  return 0;
}

int Allocator::release_limbo()
{
  for (map<block_t,block_t>::iterator i = limbo.m.begin();
	   i != limbo.m.end();
	   i++) {
	Extent ex(i->first, i->second);
	release_now(ex);
  }
  return 0;
}

int Allocator::release_now(Extent& ex) 
{
  Extent newex = ex;
  
  dout(10) << "release " << ex << endl;

  // one after us?
  for (int b=0; b<EBOFS_NUM_FREE_BUCKETS; b++) {
	Table<block_t,block_t>::Cursor cursor(fs->free_tab[b]);
	
	if (fs->free_tab[b]->find( newex.start+newex.length, cursor ) 
		== Table<block_t,block_t>::Cursor::MATCH) {
	  // add following extent to ours
	  newex.length += cursor.current().value;
	  
	  // remove it
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
	  fs->free_tab[b]->remove( cursor.current().key );
	  break;
	}
  }
  
  fs->free_blocks += ex.length;
  
  // ok, insert newex
  int b = pick_bucket(ex.length);
  fs->free_tab[b]->insert(ex.start, ex.length);

  dump_freelist();
  return 0;
}


