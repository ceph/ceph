#ifndef __EBOFS_BUFFERPOOL_H
#define __EBOFS_BUFFERPOOL_H


#include <iostream>
#include <list>
using namespace std;

#include <sys/mman.h>

#include "include/buffer.h"
#include "include/bufferlist.h"



class AlignedBufferPool {
  int alignment;              // err, this isn't actually enforced!  we just use mmap.

 public:
  AlignedBufferPool(int a) : alignment(a) {}
  ~AlignedBufferPool() {
  }

  void free(char *p, unsigned len) {
	dout(10) << "bufferpool.free " << (void*)p << " len " << len << endl;
	munmap(p, len);
  }

  static void aligned_buffer_free_func(void *arg, char *ptr, unsigned len) {
	AlignedBufferPool *pool = (AlignedBufferPool*)arg;
	pool->free(ptr, len);
  }

  buffer *alloc(int bytes) {
	assert(bytes % alignment == 0);
	char *p = (char*)mmap(NULL, bytes, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	assert(p);

	dout(10) << "bufferpool.alloc " << (void*)p << endl;

	return new buffer(p, bytes, BUFFER_MODE_NOCOPY|BUFFER_MODE_NOFREE|BUFFER_MODE_CUSTOMFREE,
					  bytes,
					  aligned_buffer_free_func, this);
  }

  // allocate a single buffer
  buffer* alloc_page() {
	return alloc(alignment);
  }


  // bufferlists
  void alloc(int bytes, bufferlist& bl) {
	bl.clear();
	bl.push_back( alloc(bytes) );
  }


};


#endif
