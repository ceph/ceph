#ifndef __EBOFS_BUFFERPOOL_H
#define __EBOFS_BUFFERPOOL_H


#include <iostream>
#include <list>
using namespace std;

#include "include/buffer.h"
#include "include/bufferlist.h"

//#undef dout
//#define dout(x) if (x <= g_conf.debug) cout << "bufferpool "



class AlignedBufferPool {
  int buffer_size;
  int num_buffers;
  int num_free;

  static const int MIN_ALLOC = 1024*1024;

  list<char*>     allocated; // my pools
  list<char*>     freelist;  // my freelist



 public:
  AlignedBufferPool(int size) : buffer_size(size), num_buffers(0), num_free(0) {}
  ~AlignedBufferPool() {
	if (num_free != num_buffers) {
	  dout(1) << "WARNING: " << num_buffers-num_free << "/" << num_buffers << " buffers still allocated" << endl;
	}
	for (list<char*>::iterator i = allocated.begin();
		 i != allocated.end();
		 i++)
	  delete[] *i;
  }


  // individual buffers
  void free(bufferptr& bp) {
	free(bp.c_str());
  }
  void free(char *p) {
	dout(10) << "bufferpool.free " << (void*)p << endl;
	freelist.push_back(p);
	num_free++;
  }

  static void aligned_buffer_free_func(void *arg, char *ptr) {
	AlignedBufferPool *pool = (AlignedBufferPool*)arg;
	pool->free(ptr);
  }


  buffer* alloc() {
	// get more memory?
	if (freelist.empty()) {
	  int get = (num_buffers ? num_buffers:1) * buffer_size;
	  if (get < MIN_ALLOC) get = MIN_ALLOC;

	  char *n = new char[get];
	  assert(n);
	  allocated.push_back(n);
	  
	  // add items to freelist
	  int num = get / buffer_size;
	  char *p = n;
	  int misalign = (unsigned)p % buffer_size;
	  if (misalign) {
		p += buffer_size - misalign;
		num--;
	  }
	  dout(2) << "bufferpool.alloc allocated " << get << " bytes, got " << num << " aligned buffers" << endl;
	  while (num--) {
		freelist.push_back( p );
		num_free++;
		num_buffers++;
		p += buffer_size;
	  }
	}

	// allocate one.
	assert(!freelist.empty());
	char *p = freelist.front();
	dout(10) << "bufferpool.alloc " << (void*)p << endl;
	freelist.pop_front();
	num_free--;
	return new buffer(p, buffer_size, BUFFER_MODE_NOCOPY|BUFFER_MODE_NOFREE|BUFFER_MODE_CUSTOMFREE,
					  buffer_size,
					  aligned_buffer_free_func, this);
  }



  // bufferlists
  void alloc_list(int n, bufferlist& bl) {
	while (n--) {
	  bl.push_back(alloc());
	}
  }
  void free_list(bufferlist& bl) {
	for (list<bufferptr>::iterator i = bl.buffers().begin();
		 i != bl.buffers().end();
		 i++)
	  free(*i);
	bl.clear();
  }


};


#endif
