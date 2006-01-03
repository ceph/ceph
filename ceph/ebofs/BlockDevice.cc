
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
#define dout(x) if (x <= g_conf.debug_bdev) cout << "dev."


inline ostream& operator<<(ostream& out, BlockDevice::biovec &bio)
{
  out << "bio(";
  if (bio.type == BlockDevice::biovec::IO_READ) out << "rd ";
  if (bio.type == BlockDevice::biovec::IO_WRITE) out << "wr ";
  out << bio.start << "~" << bio.length;
  if (bio.note) out << " " << bio.note;
  out << " " << &bio;
  out << ")";
  return out;
}

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
	  
	  utime_t stop = g_clock.now();

	  if (dir_forward) {
		// forward sweep
		dout(20) << "io_thread forward sweep" << endl;
		pos = 0;

		utime_t max(0, 1000*g_conf.bdev_el_fw_max_ms);  // (s,us), convert ms -> us!
		stop += max;

		while (1) {
		  // find i >= pos
		  multimap<block_t,biovec*>::iterator i = io_queue.lower_bound(pos);
		  if (i == io_queue.end()) break;
		  
		  // merge contiguous ops
		  list<biovec*> biols;
		  int n = 0;
		  char type = i->second->type;
		  pos = i->first;
		  while (pos == i->first && 
				 type == i->second->type &&
				 ++n <= g_conf.bdev_iov_max) {
			dout(20) << "io_thread dequeue io at " << pos << " " << *i->second << endl;
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

		  utime_t now = g_clock.now();
		  if (now > stop) break;
		}
	  } else {
		// reverse sweep
		dout(20) << "io_thread reverse sweep" << endl;
		pos = get_num_blocks();

		utime_t max(0, 1000*g_conf.bdev_el_bw_max_ms);  // (s,us), convert ms -> us!
		stop += max;

		while (1) {
		  // find i > pos
		  multimap<block_t,biovec*>::iterator i = io_queue.upper_bound(pos);
		  if (i == io_queue.begin()) break;
		  i--;  // and back down one (to get i <= pos)
		  
		  // merge continguous ops
		  list<biovec*> biols;   
		  char type = i->second->type;
		  int n = 0;
		  pos = i->first;
		  while (pos == i->first && type == i->second->type &&
				 ++n <= g_conf.bdev_iov_max) {
			dout(20) << "io_thread dequeue io at " << pos << " " << *i->second << endl;
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

		  utime_t now = g_clock.now();
		  if (now > stop) break;
		}
	  }

	  if (g_conf.bdev_el_bidir)
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
  
  // set rval
  for (p = biols.begin(); p != biols.end(); p++)
	(*p)->rval = r;

  if (1) {
	// put in completion queue
	complete_lock.Lock();
	complete_queue.splice( complete_queue.end(), biols );
	complete_wakeup.Signal();
	complete_lock.Unlock();
  } else {
	// be slow and finish synchronously
	for (p = biols.begin(); p != biols.end(); p++)
	  finish_io(*p);
  }
}


void BlockDevice::finish_io(biovec *bio)
{
  if (bio->cond) {
	bio->cond->Signal();
  }
  else if (bio->cb) {
	bio->cb->finish((ioh_t)bio, bio->rval);
	delete bio->cb;
	delete bio;
  }
}

int BlockDevice::complete_thread_entry()
{
  complete_lock.Lock();
  dout(10) << "complete_thread start" << endl;

  while (!io_stop) {

	while (!complete_queue.empty()) {
	  list<biovec*> ls;
	  ls.swap(complete_queue);
	  
	  complete_lock.Unlock();
	  
	  // finish
	  for (list<biovec*>::iterator p = ls.begin(); 
		   p != ls.end(); 
		   p++) {
		biovec *bio = *p;
		dout(20) << "complete_thread finishing " << (void*)bio << endl;
		finish_io(bio);
	  }
	  
	  complete_lock.Lock();
	}
	if (io_stop) break;
	
	dout(25) << "complete_thread sleeping" << endl;
	complete_wakeup.Wait(complete_lock);
  }

  dout(10) << "complete_thread finish" << endl;
  complete_lock.Unlock();
  return 0;
}

  


// io queue

void BlockDevice::_submit_io(biovec *b) 
{
  // NOTE: lock must be held
  dout(15) << "_submit_io " << *b << endl;
  
  // wake up thread?
  if (io_queue.empty()) io_wakeup.Signal();

  // check for overlapping ios
  {
	// BUG: this doesn't catch everything!  eg 1~10000000 will be missed....
	multimap<block_t, biovec*>::iterator p = io_queue.lower_bound(b->start);
	if ((p != io_queue.end() &&
		 p->first < b->start+b->length) ||
		(p != io_queue.begin() && 
		 (p--, p->second->start + p->second->length > b->start))) {
	  dout(1) << "_submit_io new io " << *b 
			  << " overlaps with existing " << *p->second << endl;
	  cerr << "_submit_io new io " << *b 
			  << " overlaps with existing " << *p->second << endl;
	}
  }

  // queue anew
  io_queue.insert(pair<block_t,biovec*>(b->start, b));
  io_queue_map[b] = b->start;
}

int BlockDevice::_cancel_io(biovec *bio) 
{
  // NOTE: lock must be held
  if (io_queue_map.count(bio) == 0) {
	dout(15) << "_cancel_io " << *bio << " FAILED" << endl;
	return -1;
  }
  
  dout(15) << "_cancel_io " << *bio << endl;

  block_t b = io_queue_map[bio];
  multimap<block_t,biovec*>::iterator p = io_queue.lower_bound(b);
  while (p->second != bio) p++;
  assert(p->second == bio);
  io_queue_map.erase(bio);
  io_queue.erase(p);
  return 0;
}



int BlockDevice::count_io(block_t start, block_t len)
{
  int n = 0;
  multimap<block_t,biovec*>::iterator p = io_queue.lower_bound(start);
  while (p != io_queue.end() && p->first < start+len) {
	dout(1) << "count_io in " << start << "~" << len << " : " << *p->second << endl;
	n++;
	p++;
  }
  return n;
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
  
  off_t offset = (off_t)bno << EBOFS_BLOCK_BITS;
  assert((off_t)bno * (off_t)EBOFS_BLOCK_SIZE == offset);
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

	assert((((unsigned)iov[n].iov_base) & 4095) == 0);
	assert((iov[n].iov_len & 4095) == 0);
	
	left -= iov[n].iov_len;
	n++;
	if (left == 0) break;
  }

  int r = ::writev(fd, iov, n);

  if (r < 0) {
	dout(1) << "couldn't write bno " << bno << " num " << num 
			<< " (" << len << " bytes) in " << n << " iovs,  r=" << r 
			<< " errno " << errno << " " << strerror(errno) << endl;
	dout(1) << "bl is " << bl << endl;
	assert(0);
  } else {
	assert(r == (int)len);
  }
  
  return 0;
}



// open/close

int BlockDevice::open() 
{
  assert(fd == 0);

  fd = ::open(dev, O_CREAT|O_RDWR|O_SYNC|O_DIRECT, 0);
  if (fd < 0) {
	dout(1) << "open " << dev << " failed, r = " << fd << " " << strerror(errno) << endl;
	fd = 0;
	return -1;
  }

  // lock
  int r = ::flock(fd, LOCK_EX);
  if (r < 0) {
	dout(1) << "open " << dev << " failed to get LOCK_EX" << endl;
	assert(0);
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
  complete_thread.create();
 
  return fd;
}


int BlockDevice::close() 
{
  assert(fd>0);

  // shut down io thread
  dout(10) << "close stopping io+complete threads" << endl;
  lock.Lock();
  complete_lock.Lock();
  io_stop = true;
  io_wakeup.Signal();
  complete_wakeup.Signal();
  complete_lock.Unlock();
  lock.Unlock();	
  
  io_thread.join();
  complete_thread.join();

  io_stop = false;   // in case we start again

  dout(1) << "close " << dev << endl;

  ::flock(fd, LOCK_UN);
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
  if (r == 0 && pbio->cb) {
	//pbio->cb->finish(ioh, 0);
	delete pbio->cb;
	delete pbio;
  }
  
  return r;
}

