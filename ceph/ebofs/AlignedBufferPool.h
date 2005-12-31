#ifndef __EBOFS_BUFFERPOOL_H
#define __EBOFS_BUFFERPOOL_H


#include <iostream>
#include <list>
using namespace std;

// for posix_memalign
#define _XOPEN_SOURCE 600
#include <stdlib.h>
#include <malloc.h>

// for mmap
#include <sys/mman.h>

#include "include/buffer.h"
#include "include/bufferlist.h"

#include "config.h"


class AlignedBufferPool {
  int alignment;              // err, this isn't actually enforced!  we just use mmap.

  bool dommap;

  off_t talloc;

 public:
  AlignedBufferPool(int a) : alignment(a), dommap(true), talloc(0) {}
  ~AlignedBufferPool() {
  }

  void free(char *p, unsigned len) {
	dout(20) << "bufferpool(" << (void*)this << ").free " << (void*)p << " len " << len << " ... total " << talloc << endl;
	talloc -= len;
	if (dommap)
	  ::munmap(p, len);
	else 
	  ::free((void*)p);
  }

  static void aligned_buffer_free_func(void *arg, char *ptr, unsigned len) {
	AlignedBufferPool *pool = (AlignedBufferPool*)arg;
	pool->free(ptr, len);
  }

  buffer *alloc(size_t bytes) {
	assert(bytes % alignment == 0);
	char *p = 0;
	if (dommap)
	  p = (char*)::mmap(NULL, bytes, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	else 
	  ::posix_memalign((void**)&p, alignment, bytes);
	assert(p);
	
	talloc += bytes;

	if (g_conf.ebofs_abp_zero)
	  ::bzero(p, bytes);  // only to shut up valgrind

	dout(20) << "bufferpool(" << (void*)this << ").alloc " << (void*)p << " len " << bytes << " ... total " << talloc << endl;

	return new buffer(p, bytes, BUFFER_MODE_NOCOPY|BUFFER_MODE_NOFREE|BUFFER_MODE_CUSTOMFREE,
					  bytes,
					  aligned_buffer_free_func, this);
  }

  // allocate a single buffer
  buffer* alloc_page() {
	return alloc(alignment);
  }


  // bufferlists
  void alloc(size_t bytes, bufferlist& bl) {
	bl.clear();

	// keep allocations reasonably small to avoid fragmenting memory
	while (bytes > 0) {
	  size_t max = MIN(bytes, g_conf.ebofs_abp_max_alloc);
	  bl.push_back( alloc(max) );
	  bytes -= max;
	}
  }


};


#endif
