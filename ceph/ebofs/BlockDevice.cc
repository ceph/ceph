
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
  lock.Lock();

  int whoami = io_threads_started++;
  dout(10) << "io_thread" << whoami << " start" << endl;
  
  if (whoami == 0) {  // i'm the first one starting...
	el_dir_forward = true;
	el_pos = 0;
	el_stop = g_clock.now();
	utime_t max(0, 1000*g_conf.bdev_el_fw_max_ms);  // (s,us), convert ms -> us!
	el_stop += max;
  }

  int fd = open_fd();
  assert(fd > 0);

  while (!io_stop) {
	// queue?
	if (!io_queue.empty()) {

	  // go until (we) reverse
	  dout(20) << "io_thread" << whoami << " going" << endl;

	  while (1) {
		// find i >= pos
		multimap<block_t,biovec*>::iterator i;
		if (el_dir_forward) {
		  i = io_queue.lower_bound(el_pos);
		  if (i == io_queue.end()) break;
		} else {
		  i = io_queue.upper_bound(el_pos);
		  if (i == io_queue.begin()) break;
		  i--; // and back down one (to get i <= pos)
		}
		
		// merge contiguous ops
		list<biovec*> biols;
		char type = i->second->type;
		int n = 0;

		el_pos = i->first;
		while (el_pos == i->first && type == i->second->type &&  // while (contiguous)
			   ++n <= g_conf.bdev_iov_max) {                     //  and not too big
		  biovec *bio = i->second;

		  if (el_dir_forward) {
			dout(20) << "io_thread" << whoami << " fw dequeue io at " << el_pos << " " << *i->second << endl;
			biols.push_back(bio);      // at back
		  } else {
			dout(20) << "io_thread" << whoami << " bw dequeue io at " << el_pos << " " << *i->second << endl;
			biols.push_front(bio);     // at front
		  }

		  multimap<block_t,biovec*>::iterator prev;
		  bool stop = false;
		  if (el_dir_forward) {
			el_pos += bio->length;                 // cont. next would start right after us
			prev = i;
			i++;
			if (i == io_queue.end()) stop = true;
		  } else {
			prev = i;
			if (i == io_queue.begin()) {
			  stop = true;
			} else {
			  i--;                                 // cont. would start before us...
			  if (i->first + i->second->length == el_pos)
				el_pos = i->first;                 // yep, it does!
			}
		  }
		  
		  // dequeue
		  io_queue_map.erase(bio);
		  io_queue.erase(prev);
		  
		  if (stop) break;
		}
		
		// drop lock to do the io
		lock.Unlock();
		do_io(fd, biols);
		lock.Lock();
		
		utime_t now = g_clock.now();
		if (now > el_stop) break;
	  }
	  
	  // reverse?
	  if (g_conf.bdev_el_bidir) {
		dout(20) << "io_thread" << whoami << " reversing" << endl;
		el_dir_forward = !el_dir_forward;
	  }

	  // reset disk pointers, timers
	  el_stop = g_clock.now();
	  if (el_dir_forward) {
		el_pos = 0;
		utime_t max(0, 1000*g_conf.bdev_el_fw_max_ms);  // (s,us), convert ms -> us!
		el_stop += max;
		dout(20) << "io_thread" << whoami << " forward sweep for " << max << endl;
	  } else {
		el_pos = num_blocks;
		utime_t max(0, 1000*g_conf.bdev_el_bw_max_ms);  // (s,us), convert ms -> us!
		el_stop += max;
		dout(20) << "io_thread" << whoami << " reverse sweep for " << max << endl;
	  }
	  
	} else {
	  // sleep
	  dout(20) << "io_thread" << whoami << " sleeping" << endl;
	  io_wakeup.Wait(lock);
	  dout(20) << "io_thread" << whoami << " woke up" << endl;
	}
  }

  ::close(fd);

  lock.Unlock();

  dout(10) << "io_thread" << whoami << " finish" << endl;
  return 0;
}

void BlockDevice::do_io(int fd, list<biovec*>& biols)
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
  int n = 1;
  for (p++; p != biols.end(); p++) {
	length += (*p)->length;
	bl.claim_append((*p)->bl);
	n++;
  }

  // do it
  dout(20) << "do_io start " << (type==biovec::IO_WRITE?"write":"read") 
		   << " " << start << "~" << length 
		   << " " << n << " bits" << endl;
  if (type == biovec::IO_WRITE) {
	r = _write(fd, start, length, bl);
  } else if (type == biovec::IO_READ) {
	r = _read(fd, start, length, bl);
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
  
  // wake up io_thread(s)?
  if (io_queue.empty()) 
	io_wakeup.SignalOne();
  else 
	io_wakeup.SignalAll();

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

int BlockDevice::_read(int fd, block_t bno, unsigned num, bufferlist& bl) 
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

int BlockDevice::_write(int fd, unsigned bno, unsigned num, bufferlist& bl) 
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

int BlockDevice::open_fd()
{
  return ::open(dev, O_CREAT|O_RDWR|O_SYNC|O_DIRECT, 0);
}

int BlockDevice::open() 
{
  assert(fd == 0);

  fd = open_fd();
  if (fd < 0) {
	dout(1) << "open " << dev << " failed, r = " << fd << " " << strerror(errno) << endl;
	fd = 0;
	return -1;
  }

  // lock
  /*int r = ::flock(fd, LOCK_EX);
  if (r < 0) {
	dout(1) << "open " << dev << " failed to get LOCK_EX" << endl;
	assert(0);
	return -1;
	}*/
  
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
  io_threads_started = 0;
  for (int i=0; i<g_conf.bdev_iothreads; i++) {
	io_threads.push_back(IOThread(this));
	io_threads.back().create();
  }
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
  io_wakeup.SignalAll();
  complete_wakeup.SignalAll();
  complete_lock.Unlock();
  lock.Unlock();	
  
  
  for (int i=0; i<g_conf.bdev_iothreads; i++) 
	io_threads[i].join();

  complete_thread.join();

  io_stop = false;   // in case we start again

  dout(1) << "close " << dev << endl;

  //::flock(fd, LOCK_UN);
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

