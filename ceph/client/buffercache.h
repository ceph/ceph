#ifndef __BUFFERCACHE_H
#define __BUFFERCACHE_H

#include "include/buffer.h"
#include "include/bufferlist.h"
#include "include/lru.h"

/** map<off_t, bufferptr> buffer_map;

for writes:

void map_or_alloc(size_t len, off_t off, bufferlist &blist);
- build a bufferlist that includes bufferptr's for existing buffers 
  in the map and (if necessary) newly allocated ones.  caller can then use

  blist.copy_from(int off, int len, char *buf);

  to copy data into the bufferlist.  (copy() copies data _out_, and append appends, 
  but copy_from or copy_into or whatever doesn't exist yet.)


  for reads:
  void map_existing(size_t len, off_t off, map<off_t, bufferlist>& hits, map<off_t, size_t>& holes)
  
- gives caller map of hits and holes.


both of those could be wrapped up in a tidy class, aside from 2 things:

- buffers need to be put in an lru of some sort with last_accessed etc. 
  so we can flush them intelligently

- maybe the buffers should be allocated w/ fixed size?  for writes at least, 
  the new buffers can always be created as 256k (or whatever) and the bufferptrs 
  that reference them (in buffer_map) will point to the relevant substring.  
  the map_or_alloc() bit just needs to be smart then to check the underlying 
  buffer's alloc'd size and adjust those subsets as necessary.

  for reads, we'll be getting buffers of weird sizes coming from osds, 
  so the cache should work with weird sized buffers regardless.  
  (even if we always prefetch large blocks, files tails will come back small.)


  so somewhere in there there should be a buffer_head struct or something. maybe it shoudl be

  map<off_t, bufferhead> buffer_map;

  we can reuse the LRU (include/lru.h) thinger to handle lru stuff pretty easily. 
  maybe

  class bufferhead : public LRUObject {
  bufferptr bptr;
  time_t    last_written;
  int       state;  // clean, dirty, writing, reading
  };

gets weird too if say multiple reads are issued on the same (or overlapping) ranges of bytes.. 
only want to submit a single read request.

s
*/

// bufferhead states
#define BUFHD_STATE_CLEAN	1
#define BUFHD_STATE_DIRTY	2
#define BUFHD_STATE_WRITING	3
#define BUFHD_STATE_READING	4

class bufferhead : public LRUObject {
  bufferptr	bptr;
  time_t	last_written;
  int		state;
};

class buffercache {
 private:
  map<off_t, bufferhead> buffer_map;
  
  map<off_t, bufferhead>::iterator overlap(size_t len, off_t off)
  {
    // returns iterator to buffer overlapping specified extent or end() if no overlap exists
    map<off_t, bufferhead>::iterator it = buffer_map.lower_bound(off);
    if (it == buffer_map.end() || (*it)->first < off + len) {
      return it;
    } else if (it == buffer_map.begin()) {
      return buffer_map.end();
    } else {
      --it;
      if ((*it)->first + (*it)->second.bptr.length() > off) {
        return it;
      } else {
        return buffer_map.end();
      }
    }
  }

 public:
  /* for writes
     build a bufferlist that includes bufferptr's for existing buffers 
     in the map and (if necessary) newly allocated ones.  caller can then use
	 
	 blist.copy_from(int off, int len, char *buf);
	 
	 to copy data into the bufferlist. (copy() copies data _out_, and append appends, 
	 but copy_from or copy_into or whatever doesn't exist yet.)
  */
  void map_or_alloc(size_t len, off_t off, bufferlist &blist)
  {
  }

  /* for reads
     gives caller maps of hits and holes
  */
  void map_existing(size_t len, off_t off, map<off_t, bufferhead>& hits, map<off_t, size_t>& holes)
  {
    off_t next_off = off;
    for (map<off_t, bufferhead>::iterator it = overlap(len, off);
         it != buffer_map.end() && (*it)->first < off + len;
         it++) {
      if ((*it)->first > next_off) {
        holes[next_off] = (size_t) ((*it)->first - next_off);
      }
      hits[(*it)->first] = (*it)->second;
      next_off = (*it)->first + (*it)->second.bptr.length();
    }
    if (next_off < off + len) {
      holes[next_off] = (size_t) (off + len - next_off);
    }
  }
};

#endif

