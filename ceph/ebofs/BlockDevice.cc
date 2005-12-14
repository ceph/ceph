
#include "BlockDevice.h"

#include "config.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <cassert>
#include <errno.h>

#include <sys/uio.h>

#include <sys/ioctl.h>
#include <linux/fs.h>

#undef dout
#define dout(x) if (x <= g_conf.debug) cout << "dev."


block_t BlockDevice::get_num_blocks() 
{
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


int BlockDevice::io_thread_entry()
{
  dout(10) << "io_thread start" << endl;
  
  // elevator nonsense!
  bool dir_forward = true;
  block_t pos = 0;
  
  lock.Lock();
  while (!io_stop) {
	// queue?
	if (!io_queue.empty()) {
	  
	  if (dir_forward) {
		// forward sweep
		dout(20) << "io_thread forward sweep" << endl;
		pos = 0;

		while (1) {
		  // find i >= pos
		  multimap<block_t,biovec*>::iterator i = io_queue.lower_bound(pos);
		  if (i == io_queue.end()) break;
		  
		  // merge contiguous ops
		  list<biovec*> biols;
		  char type = i->second->type;
		  pos = i->first;
		  while (pos == i->first && 
				 type == i->second->type) {
			dout(20) << "io_thread dequeue io at " << pos << " " << (void*)i->second << endl;
			biovec *bio = i->second;
			biols.push_back(bio);
			pos += bio->length;

			multimap<block_t,biovec*>::iterator prev = i;
			i++;
			io_queue_map.erase(bio);
			io_queue.erase(prev);

			if (i == io_queue.end()) break;
		  }

		  lock.Unlock();
		  do_io(biols);
		  lock.Lock();
		}
	  } else {
		// reverse sweep
		dout(20) << "io_thread reverse sweep" << endl;
		pos = get_num_blocks();

		while (1) {
		  // find i > pos
		  multimap<block_t,biovec*>::iterator i = io_queue.upper_bound(pos);
		  if (i == io_queue.begin()) break;
		  i--;  // and back down one (to get i <= pos)
		  
		  // merge continguous ops
		  list<biovec*> biols;   
		  char type = i->second->type;
		  pos = i->first;
		  while (pos == i->first && type == i->second->type) {
			dout(20) << "io_thread dequeue io at " << pos << " " << (void*)i->second << endl;
			biovec *bio = i->second;
			biols.push_back(bio);
			pos += bio->length;

			multimap<block_t,biovec*>::iterator prev = i;
			bool begin = (i == io_queue.begin());
			if (!begin) i--;
			io_queue_map.erase(bio);
			io_queue.erase(prev);

			if (begin) break;
		  }
			
		  lock.Unlock();
		  do_io(biols);
		  lock.Lock();
		}
	  }
	  dir_forward = !dir_forward;

	} else {
	  // sleep
	  dout(20) << "io_thread sleeping" << endl;
	  io_wakeup.Wait(lock);
	  dout(20) << "io_thread woke up" << endl;
	}
  }
  lock.Unlock();

  dout(10) << "io_thread finish" << endl;
  return 0;
}

void BlockDevice::do_io(list<biovec*>& biols)
{
  int r;
  assert(!biols.empty());

  // get full range, type, bl
  bufferlist bl;
  bl.claim(biols.front()->bl);
  block_t start = biols.front()->start;
  block_t length = biols.front()->length;
  char type = biols.front()->type;

  list<biovec*>::iterator p = biols.begin();
  for (p++; p != biols.end(); p++) {
	length += (*p)->length;
	bl.claim_append((*p)->bl);
  }

  // do it
  dout(20) << "do_io start " << (type==biovec::IO_WRITE?"write":"read") 
		   << " " << start << "~" << length << endl;
  if (type == biovec::IO_WRITE) {
	r = _write(start, length, bl);
  } else if (type == biovec::IO_READ) {
	r = _read(start, length, bl);
  } else assert(0);
  dout(20) << "do_io finish " << (type==biovec::IO_WRITE?"write":"read") 
		   << " " << start << "~" << length << endl;
  
  // finish
  for (p = biols.begin(); p != biols.end(); p++) {
	biovec *bio = *p;
	if (bio->cond) {
	  //lock.Lock();
	  bio->rval = r;
	  bio->cond->Signal();
	  //lock.Unlock();
	}
	else if (bio->context) {
	  bio->context->finish((int)bio);
	  delete bio->context;
	  delete bio;
	}
  }
}


// io queue

void BlockDevice::_submit_io(biovec *b) 
{
  // NOTE: lock must be held
  dout(15) << "_submit_io " << (void*)b << endl;
  
  // wake up thread?
  if (io_queue.empty()) io_wakeup.Signal();

  // queue anew
  io_queue.insert(pair<block_t,biovec*>(b->start, b));
  io_queue_map[b] = b->start;
}

int BlockDevice::_cancel_io(biovec *bio) 
{
  // NOTE: lock must be held
  if (io_queue_map.count(bio) == 0) {
	dout(15) << "_cancel_io " << (void*)bio << " FAILED" << endl;
	return -1;
  }
  
  dout(15) << "_cancel_io " << (void*)bio << endl;

  block_t b = io_queue_map[bio];
  multimap<block_t,biovec*>::iterator p = io_queue.lower_bound(b);
  while (p->second != bio) p++;
  assert(p->second == bio);
  io_queue_map.erase(bio);
  io_queue.erase(p);
  return 0;
}



// low level io

int BlockDevice::_read(block_t bno, unsigned num, bufferlist& bl) 
{
  dout(10) << "_read " << bno << "~" << num << endl;

  assert(fd > 0);
  
  off_t offset = bno * EBOFS_BLOCK_SIZE;
  off_t actual = lseek(fd, offset, SEEK_SET);
  assert(actual == offset);
  
  size_t len = num*EBOFS_BLOCK_SIZE;
  assert(bl.length() >= len);

  struct iovec iov[ bl.buffers().size() ];
  int n = 0;
  size_t left = len;
  for (list<bufferptr>::iterator i = bl.buffers().begin();
	   i != bl.buffers().end();
	   i++) {
	assert(i->length() % EBOFS_BLOCK_SIZE == 0);
	
	iov[n].iov_base = i->c_str();
	iov[n].iov_len = MIN(left, i->length());

	left -= iov[n].iov_len;
	n++;
	if (left == 0) break;
  }

  int got = ::readv(fd, iov, n);
  assert(got <= (int)len);
  
  return 0;
}

int BlockDevice::_write(unsigned bno, unsigned num, bufferlist& bl) 
{
  dout(10) << "_write " << bno << "~" << num << endl;

  assert(fd > 0);
  
  off_t offset = bno * EBOFS_BLOCK_SIZE;
  off_t actual = lseek(fd, offset, SEEK_SET);
  assert(actual == offset);
  
  // write buffers
  size_t len = num*EBOFS_BLOCK_SIZE;

  struct iovec iov[ bl.buffers().size() ];

  int n = 0;
  size_t left = len;
  for (list<bufferptr>::iterator i = bl.buffers().begin();
	   i != bl.buffers().end();
	   i++) {
	assert(i->length() % EBOFS_BLOCK_SIZE == 0);

	iov[n].iov_base = i->c_str();
	iov[n].iov_len = MIN(left, i->length());
	
	left -= iov[n].iov_len;
	n++;
	if (left == 0) break;
  }

  int r = ::writev(fd, iov, n);
  
  if (r < 0) {
	dout(1) << "couldn't write bno " << bno << " num " << num 
			<< " (" << num << " bytes) r=" << r 
			<< " errno " << errno << " " << strerror(errno) << endl;
  } else {
	assert(r == (int)len);
  }
  
  return 0;
}



// open/close

int BlockDevice::open() 
{
  dout(1) << "open " << dev << endl;
  assert(fd == 0);

  fd = ::open(dev, O_CREAT|O_RDWR|O_SYNC|O_DIRECT);
  if (fd < 0) {
	dout(1) << "open failed, r = " << fd << " " << strerror(errno) << endl;
	fd = 0;
	return -1;
  }
  
  // figure size
  __uint64_t bsize = 0;
  ioctl(fd, BLKGETSIZE64, &bsize);
  if (bsize == 0) {
	// hmm, try stat!
	struct stat st;
	fstat(fd, &st);
	bsize = st.st_size;
  }	
  num_blocks = bsize / (__uint64_t)EBOFS_BLOCK_SIZE;
  
  dout(1) << "open " << dev << " is " << bsize << " bytes, " << num_blocks << " blocks" << endl;
  
  // start thread
  io_thread.create();
 
  return fd;
}


int BlockDevice::close() 
{
  assert(fd>0);

  // shut down io thread
  dout(10) << "close stopping io thread" << endl;
  lock.Lock();
  io_stop = true;
  io_wakeup.Signal();
  lock.Unlock();	
  io_thread.join();

  dout(1) << "close" << endl;
  ::close(fd);
  fd = 0;

  return 0;
}

int BlockDevice::cancel_io(ioh_t ioh) 
{
  biovec *pbio = (biovec*)ioh;
  
  lock.Lock();
  int r = _cancel_io(pbio);
  lock.Unlock();
  
  // FIXME?
  if (r == 0 && pbio->context) {
	//pbio->context->finish(0);
	delete pbio->context;
	delete pbio;
  }
  
  return r;
}

