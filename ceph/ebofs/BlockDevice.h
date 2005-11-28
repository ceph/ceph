#ifndef __EBOFS_BLOCKDEVICE_H
#define __EBOFS_BLOCKDEVICE_H

#include "include/bufferlist.h"
#include "common/Mutex.h"
#include "common/Cond.h"

#include "types.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <cassert>
#include <errno.h>

#include <sys/ioctl.h>
#include <linux/fs.h>

class BlockDevice {
  char   *dev;
  int     fd;
  block_t num_blocks;

  Mutex lock;
  
 public:
  BlockDevice(char *d) : dev(d), fd(0), num_blocks(0) {
  };

  // get size in blocks
  block_t get_num_blocks() {
	if (!num_blocks) {
	  // stat
	  struct stat st;
	  assert(fd > 0);
	  int r = ::fstat(fd, &st);
	  assert(r == 0);
	  num_blocks = st.st_size / (block_t)EBOFS_BLOCK_SIZE;
	}
	return num_blocks;
  }

  int open() {
	fd = ::open(dev, O_CREAT|O_RDWR|O_SYNC|O_DIRECT);
	if (fd < 0) return fd;
	//dout(1) << "blockdevice.open " << dev << endl;

	// figure size
	__uint64_t bsize = 0;
	//int r = 
	ioctl(fd, BLKGETSIZE64, &bsize);
	
	if (bsize == 0) {
	  // try stat!
	  struct stat st;
	  fstat(fd, &st);
	  bsize = st.st_size;
	}	
	num_blocks = bsize / (__uint64_t)EBOFS_BLOCK_SIZE;

	dout(1) << "blockdevice.open " << dev << "  " << bsize << " bytes, " << num_blocks << " blocks" << endl;
	return fd;
  }
  int close() {
	return ::close(fd);
  }


  // single blocks
  int read(block_t bno, unsigned num,
		   bufferptr& bptr) {
	lock.Lock();
	assert(fd > 0);

	off_t offset = bno * EBOFS_BLOCK_SIZE;
	off_t actual = lseek(fd, offset, SEEK_SET);
	assert(actual == offset);
	
	int len = num*EBOFS_BLOCK_SIZE;
	assert((int)bptr.length() >= len);
	int got = ::read(fd, bptr.c_str(), len);
	assert(got <= len);

	lock.Unlock();
	return 0;
  }

  int write(unsigned bno, unsigned num,
			bufferptr& bptr) {
	lock.Lock();
	assert(fd > 0);

	off_t offset = bno * EBOFS_BLOCK_SIZE;
	off_t actual = lseek(fd, offset, SEEK_SET);
	assert(actual == offset);

	// write buffers
	off_t len = num*EBOFS_BLOCK_SIZE;
	off_t left = bptr.length();
	if (left > len) left = len;
	int r = ::write(fd, bptr.c_str(), left);
	//dout(1) << "write " << fd << " " << (void*)bptr.c_str() << " " << left << endl;
	if (r < 0) {
	  dout(1) << "couldn't write bno " << bno << " num " << num << " (" << left << " bytes) r=" << r << " errno " << errno << " " << strerror(errno) << endl;
	}
	
	lock.Unlock();
	return 0;
  }


  // lists
  int read(block_t bno, unsigned num,
		   bufferlist& bl) {
	lock.Lock();
	assert(fd > 0);

	off_t offset = bno * EBOFS_BLOCK_SIZE;
	off_t actual = lseek(fd, offset, SEEK_SET);
	assert(actual == offset);
	
	off_t len = num*EBOFS_BLOCK_SIZE;
	assert((int)bl.length() >= len);
	
	for (list<bufferptr>::iterator i = bl.buffers().begin();
		 i != bl.buffers().end();
		 i++) {
	  assert(i->length() % EBOFS_BLOCK_SIZE == 0);
	  
	  int blen = i->length();
	  if (blen > len) blen = len;
	  
	  int got = ::read(fd, i->c_str(), blen);
	  assert(got <= blen);
	  
	  len -= blen;
	  if (len == 0) break;
	}

	lock.Unlock();
	return 0;
  }

  int write(unsigned bno, unsigned num,
			bufferlist& bl) {
	lock.Lock();
	assert(fd > 0);
	
	off_t offset = bno * EBOFS_BLOCK_SIZE;
	off_t actual = lseek(fd, offset, SEEK_SET);
	assert(actual == offset);
	
	// write buffers
	off_t len = num*EBOFS_BLOCK_SIZE;

	for (list<bufferptr>::iterator i = bl.buffers().begin();
		 i != bl.buffers().end();
		 i++) {
	  assert(i->length() % EBOFS_BLOCK_SIZE == 0);
	  
	  off_t left = i->length();
	  if (left > len) left = len;
	  int r = ::write(fd, i->c_str(), left);
	  //dout(1) << "write " << fd << " " << (void*)bptr.c_str() << " " << left << endl;
	  if (r < 0) {
		dout(1) << "couldn't write bno " << bno << " num " << num << " (" << left << " bytes) r=" << r << " errno " << errno << " " << strerror(errno) << endl;
	  } else {
		assert(r == left);
	  }
	  
	  len -= left;
	  if (len == 0) break;
	}

	lock.Unlock();
	return 0;
  }



};

#endif
