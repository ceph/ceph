// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



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
#define dout(x) if (x <= g_conf.debug_bdev) cout << "bdev(" << dev << ")."


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
	assert(fd > 0);
	
	// ioctl block device?
	ioctl(fd, BLKGETSIZE64, &num_blocks);
	
	if (!num_blocks) {
	  // hmm, try stat!
	  struct stat st;
	  fstat(fd, &st);
	  num_blocks = st.st_size;
	}
	
	num_blocks /= (__uint64_t)EBOFS_BLOCK_SIZE;

	if (g_conf.bdev_fake_max_mb &&
		num_blocks > (block_t)g_conf.bdev_fake_max_mb * 256ULL) {
	  dout(0) << "faking dev size " << g_conf.bdev_fake_max_mb << " mb" << endl;
	  num_blocks = g_conf.bdev_fake_max_mb * 256;
	}
  }
  return num_blocks;
}


int BlockDevice::io_thread_entry()
{
  lock.Lock();

  int whoami = io_threads_started++;
  io_threads_running++;
  assert(io_threads_running <= g_conf.bdev_iothreads);
  dout(10) << "io_thread" << whoami << " start, " << io_threads_running << " now running" << endl;
  
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
	
	bool at_end = false;
	bool do_sleep = false;
	
	// queue empty?
	if (io_queue.empty()) {
	  // sleep
	  do_sleep = true;
	} else {
	  // go until (we) reverse
	  dout(20) << "io_thread" << whoami << " going" << endl;

	  while (1) {
		// find i >= pos
		multimap<block_t,biovec*>::iterator i;
		if (el_dir_forward) {
		  i = io_queue.lower_bound(el_pos);
		  if (i == io_queue.end()) {
			at_end = true;
			break;
		  }
		} else {
		  i = io_queue.upper_bound(el_pos);
		  if (i == io_queue.begin()) {
			at_end = true;
			break;
		  }
		  i--; // and back down one (to get i <= pos)
		}
		
		// merge contiguous ops
		block_t start, length;
		list<biovec*> biols;
		char type = i->second->type;
		int n = 0;  // count eventual iov's for readv/writev

		start = el_pos = i->first;
		length = 0;
		while (el_pos == i->first && type == i->second->type) {  // while (contiguous)
		  biovec *bio = i->second;

		  // ok?
		  if (io_block_lock.intersects(bio->start, bio->length)) {
			dout(20) << "io_thread" << whoami << " dequeue " << bio->start << "~" << bio->length 
					 << " intersects block_lock " << io_block_lock << endl;
			break;  // go to sleep, or go with what we've got so far
		  }

		  // add to biols
		  int nv = bio->bl.buffers().size();     // how many iov's in this bio's bufferlist?
		  if (n + nv >= g_conf.bdev_iov_max) break;
		  n += nv;

		  start = MIN(start, bio->start);
		  length += bio->length;

		  if (el_dir_forward) {
			dout(20) << "io_thread" << whoami << " fw dequeue io at " << el_pos << " " << *i->second << endl;
			biols.push_back(bio);      // at back
		  } else {
			dout(20) << "io_thread" << whoami << " bw dequeue io at " << el_pos << " " << *i->second << endl;
			biols.push_front(bio);     // at front
		  }

		  // next bio?
		  multimap<block_t,biovec*>::iterator prev;
		  if (el_dir_forward) {
			el_pos += bio->length;                 // cont. next would start right after us
			prev = i;
			i++;
			if (i == io_queue.end()) {
			  at_end = true;
			}
		  } else {
			prev = i;
			if (i == io_queue.begin()) {
			  at_end = true;
			} else {
			  i--;                                 // cont. would start before us...
			  if (i->first + i->second->length == el_pos)
				el_pos = i->first;                 // yep, it does!
			}
		  }

		  // dequeue
		  io_queue_map.erase(bio);
		  io_queue.erase(prev);
		  
		  if (at_end) break;
		}
		
		if (biols.empty()) {
		  // failed to dequeue a do-able op, sleep for now
		  assert(io_threads_running > 1);
		  do_sleep = true;
		  break;
		}

		{ // lock blocks
		  assert(start == biols.front()->start);
		  io_block_lock.insert(start, length);
		  
		  // drop lock to do the io
		  lock.Unlock();
		  do_io(fd, biols);
		  lock.Lock();
		  
		  // unlock blocks
		  io_block_lock.erase(start, length);
		}
		
		if ((int)io_queue.size() > io_threads_running)   // someone might have blocked on our block_lock
		  io_wakeup.SignalAll();
			
		utime_t now = g_clock.now();
		if (now > el_stop) break;
	  }
	  
	  if (at_end) {
		at_end = false;
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
	  }
	}

	if (do_sleep) {
	  do_sleep = false;
	  
	  // sleep
	  io_threads_running--;
	  dout(20) << "io_thread" << whoami << " sleeping, " << io_threads_running << " threads now running" << endl;

	  if (g_conf.bdev_idle_kick_after_ms > 0 &&
		  io_threads_running == 0 && 
		  idle_kicker) {
		// first wait for signal | timeout
		io_wakeup.WaitInterval(lock, utime_t(0, g_conf.bdev_idle_kick_after_ms*1000));   

		// should we still be sleeping?  (did we get woken up, or did timer expire?
		if (io_queue.empty() && io_threads_running == 0) {
		  idle_kicker->kick();		  // kick
		  io_wakeup.Wait(lock);		  // and wait
		}
	  } else {
		// normal, just wait.
		io_wakeup.Wait(lock);
	  }

	  io_threads_running++;
	  assert(io_threads_running <= g_conf.bdev_iothreads);
	  dout(20) << "io_thread" << whoami << " woke up, " << io_threads_running << " threads now running" << endl;
	}
  }

  ::close(fd);
  io_threads_running--;

  lock.Unlock();

  dout(10) << "io_thread" << whoami << " finish" << endl;
  return 0;
}

bool BlockDevice::is_idle()
{
  
  lock.Lock();
  bool idle = (io_threads_running == 0) && io_queue.empty();
  lock.Unlock();
  return idle;
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
  bio->done = true;
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
	
	/*
	if (io_threads_running == 0 && idle_kicker) {
	  complete_lock.Unlock();
	  idle_kicker->kick();
	  complete_lock.Lock();
	  if (!complete_queue.empty() || io_stop) 
		continue;
	}
	*/

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
  if ((int)io_queue.size() == io_threads_running) 
	io_wakeup.SignalOne();
  else if ((int)io_queue.size() > io_threads_running) 
	io_wakeup.SignalAll();

  // [DEBUG] check for overlapping ios
  if (g_conf.bdev_debug_check_io_overlap) {
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
  return ::open(dev, O_RDWR|O_SYNC|O_DIRECT, 0);
}

int BlockDevice::open(kicker *idle) 
{
  assert(fd == 0);

  // open?
  fd = open_fd();
  if (fd < 0) {
	dout(1) << "open failed, r = " << fd << " " << strerror(errno) << endl;
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
  __uint64_t bsize = get_num_blocks();
  
  dout(2) << "open " << bsize << " bytes, " << num_blocks << " blocks" << endl;
  
  // start thread
  io_threads_started = 0;
  io_threads.clear();
  for (int i=0; i<g_conf.bdev_iothreads; i++) {
	io_threads.push_back(new IOThread(this));
	io_threads.back()->create();
  }
  complete_thread.create();
 
  // idle kicker?
  idle_kicker = idle;

  return fd;
}


int BlockDevice::close() 
{
  assert(fd>0);
  
  idle_kicker = 0;

  // shut down io thread
  dout(10) << "close stopping io+complete threads" << endl;
  lock.Lock();
  complete_lock.Lock();
  io_stop = true;
  io_wakeup.SignalAll();
  complete_wakeup.SignalAll();
  complete_lock.Unlock();
  lock.Unlock();	
  
  
  for (int i=0; i<g_conf.bdev_iothreads; i++) {
	io_threads[i]->join();
	delete io_threads[i];
  }
  io_threads.clear();

  complete_thread.join();

  io_stop = false;   // in case we start again

  dout(2) << "close " << endl;

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

