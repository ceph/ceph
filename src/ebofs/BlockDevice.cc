// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "common/config.h"
#include "BlockDevice.h"


#include <unistd.h>
#include <stdlib.h>
//#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <errno.h>

#include <sys/uio.h>
#include <limits.h>

#include <sys/ioctl.h>

#ifndef __CYGWIN__
#ifndef DARWIN
#include <linux/fs.h>
#else
/* lseek works on 64-bit offsets on OS/X */
#define lseek64 lseek
#endif
#endif

#define DOUT_SUBSYS bdev


/*******************************************
 * biovec
 */

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



/*******************************************
 * ElevatorQueue
 */

#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << dev << ").elevatorq."


int BlockDevice::ElevatorQueue::dequeue_io(list<biovec*>& biols, 
                                           block_t& start, block_t& length,
                                           interval_set<block_t>& block_lock)
{
  // queue empty?
  assert(!io_map.empty());

  dout(20) << "dequeue_io el_pos " << el_pos << " dir " << el_dir_forward << dendl;

  // find our position: i >= pos
  map<block_t,biovec*>::iterator i;
  
  int tries = 2;
  while (tries > 0) {
    if (el_dir_forward) {
      i = io_map.lower_bound(el_pos);
      if (i != io_map.end()) {
        break;  // not at end.  good.
      }
    } else {
      i = io_map.upper_bound(el_pos);
      if (i != io_map.begin()) {
        i--;   // and back down one (to get i <= pos).  good.
        break;
      }
    }

    // reverse (or initial startup)?
    if (g_conf.bdev_el_bidir || !el_dir_forward) {
      //      dout(20) << "restart reversing" << dendl;
      el_dir_forward = !el_dir_forward;
    }
    
    if (el_dir_forward) {
      // forward
      el_pos = 0;
      
      if (g_conf.bdev_el_fw_max_ms) {
        el_stop = g_clock.now();
        utime_t max(0, 1000*g_conf.bdev_el_fw_max_ms);  // (s,us), convert ms -> us!
        el_stop += max;
        //    dout(20) << "restart forward sweep for " << max << dendl;
      } else {
        //    dout(20) << "restart fowrard sweep" << dendl;
      }
    } else {
      // reverse
      el_pos = bdev->get_num_blocks();
      
      if (g_conf.bdev_el_bw_max_ms) {
        el_stop = g_clock.now();
        utime_t max(0, 1000*g_conf.bdev_el_bw_max_ms);  // (s,us), convert ms -> us!
        el_stop += max;
        //    dout(20) << "restart reverse sweep for " << max << dendl;
      } else {
        //    dout(20) << "restart reverse sweep" << dendl;
      }
    }

    tries--;
  }
  
  assert(tries > 0);  // this shouldn't happen if the queue is non-empty.

  // get some biovecs
  int num_bio = 0;
  
  dout(20) << "dequeue_io  starting with " << i->first << " " << *i->second << dendl;

  // merge contiguous ops
  char type = i->second->type;  // read or write
  int num_iovs = 0;  // count eventual iov's for readv/writev
  
  start = i->first;
  length = 0;

  if (el_dir_forward)
    el_pos = start;
  else
    el_pos = i->first + i->second->length;
  
  // while (contiguous)
  while ((( el_dir_forward && el_pos == i->first) ||
          (!el_dir_forward && el_pos == i->first + i->second->length)) && 
         type == i->second->type) { 
    biovec *bio = i->second;
    
    // allowed?  (not already submitted to kernel?)
    if (block_lock.intersects(bio->start, bio->length)) {
      dout(20) << "dequeue_io " << bio->start << "~" << bio->length 
	       << " intersects block_lock " << block_lock << dendl;
      break;  // stop, or go with what we've got so far
    }
    
    // add to biols
    int nv = bio->bl.buffers().size();     // how many iov's in this bio's bufferlist?
    if (num_bio &&
	num_iovs + nv >= IOV_MAX) break; // to many   //g_conf.bdev_iov_max) break;  // too many!
    num_iovs += nv;
    
    start = MIN(start, bio->start);
    length += bio->length;
    
    if (el_dir_forward) {
      dout(20) << "dequeue_io fw dequeue io at " << el_pos << " " << *i->second << dendl;
      biols.push_back(bio);      // add at back
    } else {
      dout(20) << "dequeue_io bw dequeue io at " << el_pos << " " << *i->second << dendl;
      biols.push_front(bio);     // add at front
    }
    num_bio++;
    
    // move elevator pointer
    bool at_end = false; 
    map<block_t,biovec*>::iterator prev = i;
    if (el_dir_forward) {
      el_pos += bio->length;                 // cont. next would start right after us
      i++;
      if (i == io_map.end()) {
        at_end = true;
      }
    } else {
      el_pos -= bio->length;
      if (i == io_map.begin()) {
        at_end = true;
      } else {
        i--;
      }
    }
    
    // dequeue
    io_map.erase(prev);
    bio->in_queue = 0;
    
    if (at_end) break;
  }
  
  return num_bio;
}



/*******************************************
 * BarrierQueue
 */
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << dev << ").barrierq."

void BlockDevice::BarrierQueue::barrier()
{
  if (!qls.empty() && qls.front()->empty()) {
    assert(qls.size() == 1);
    dout(10) << "barrier not adding new queue, front is empty" << dendl;
  } else {
    qls.push_back(new ElevatorQueue(bdev, dev));
    dout(10) << "barrier adding new elevator queue (now " << qls.size() << "), front queue has "
             << qls.front()->size() << " ios left" << dendl;
  }
}

bool BlockDevice::BarrierQueue::bump()
{
  assert(!qls.empty());
  
  // is the front queue(s) empty?
  bool did = false;
  while (!qls.empty() &&
	 qls.front()->empty() &&
	 qls.front() != qls.back()) {
    delete qls.front();
    qls.pop_front();
    dout(10) << "dequeue_io front empty, moving to next queue (" << qls.front()->size() << ")" << dendl;
    did = true;
  }
  return did;
}

int BlockDevice::BarrierQueue::dequeue_io(list<biovec*>& biols, 
                                          block_t& start, block_t& length,
                                          interval_set<block_t>& locked) 
{
  assert(!qls.empty());
  int n = qls.front()->dequeue_io(biols, start, length, locked);
  bump();  // in case we emptied the front queue
  return n;
}




/*******************************************
 * BlockDevice
 */

#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << dev << ")."

block_t BlockDevice::get_num_blocks() 
{
  if (!num_blocks) {
    assert(fd > 0);

    int r;
    uint64_t bytes = 0;
#ifdef BLKGETSIZE64
    // ioctl block device
    r = ioctl(fd, BLKGETSIZE64, &bytes);
    num_blocks = bytes / (uint64_t)EBOFS_BLOCK_SIZE;
    if (r == 0) {
      dout(10) << "get_num_blocks ioctl BLKGETSIZE64 reports "
	       << num_blocks << " 4k blocks, " 
	       << bytes << " bytes" 
	       << dendl;
#else
# ifdef BLKGETSIZE
    // hrm, try the 32 bit ioctl?
    unsigned long sectors = 0;
    r = ioctl(fd, BLKGETSIZE, &sectors);
    num_blocks = sectors/8ULL;
    bytes = sectors*512ULL;
    if (r == 0) {
      dout(10) << "get_num_blocks ioctl BLKGETSIZE reports " << sectors << " sectors, "
	       << num_blocks << " 4k blocks, " << bytes << " bytes" << dendl;
# else
    // probably CYGWIN or similar lame plaform...
    unsigned long sectors = 0;
    r = sectors;  // shut up compiler
    if (0) {
# endif
#endif
    } else {
      // hmm, try stat!
      char buf[80];
      dout(10) << "get_num_blocks ioctl(2) failed with " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << ", using stat(2)" << dendl;
      struct stat st;
      fstat(fd, &st);
      uint64_t bytes = st.st_size;
      num_blocks = bytes / EBOFS_BLOCK_SIZE;
      dout(10) << "get_num_blocks stat reports " << num_blocks << " 4k blocks, " << bytes << " bytes" << dendl;
    }
    
    if (g_conf.bdev_fake_mb) {
      num_blocks = g_conf.bdev_fake_mb * 256;
      dout(0) << "faking dev size " << g_conf.bdev_fake_mb << " mb" << dendl;
    }
    if (g_conf.bdev_fake_max_mb &&
        num_blocks > (block_t)g_conf.bdev_fake_max_mb * 256ULL) {
      dout(0) << "faking dev size " << g_conf.bdev_fake_max_mb << " mb" << dendl;
      num_blocks = g_conf.bdev_fake_max_mb * 256;
    }
    
  }
  return num_blocks;
}



/** io thread
 * each worker thread dequeues ios from the root_queue and submits them to the kernel.
 */
void* BlockDevice::io_thread_entry()
{
  lock.Lock();

  int whoami = io_threads_started++;
  io_threads_running++;
  assert(io_threads_running <= g_conf.bdev_iothreads);
  dout(10) << "io_thread" << whoami << " start, " << io_threads_running << " now running" << dendl;

  // get my own fd (and file position pointer)
  int fd = open_fd();
  assert(fd > 0);

  while (!io_stop) {
    if (!root_queue.empty()) {
      dout(20) << "io_thread" << whoami << "/" << io_threads_running << " going" << dendl;

      block_t start, length;
      list<biovec*> biols;
      int n = root_queue.dequeue_io(biols, start, length, io_block_lock);

      if (n == 0) {
        // failed to dequeue a do-able op, sleep for now
        dout(20) << "io_thread" << whoami << "/" << io_threads_running << " couldn't dequeue doable op, sleeping" << dendl;
        assert(io_threads_running > 1);   // there must be someone else, if we couldn't dequeue something doable.
      } 
      else {
        // lock blocks
        assert(start == biols.front()->start);
        io_block_lock.insert(start, length);
          
        // drop lock to do the io
        lock.Unlock();
        do_io(fd, biols);
        lock.Lock();
          
        // unlock blocks
        io_block_lock.erase(start, length);
        
        // someone might have blocked on our block_lock?
        if (io_threads_running < g_conf.bdev_iothreads &&
            (int)root_queue.size() > io_threads_running)   
          io_wakeup.SignalAll();
	
	// loop again (don't sleep)
	continue; 
      }
    }

    // sleep
    io_threads_running--;
    dout(20) << "io_thread" << whoami << " sleeping, "
	     << io_threads_running << " threads now running," 
	     << " queue has " << root_queue.size() 
	     << dendl;
    
    // first wait for signal | timeout?
    if (g_conf.bdev_idle_kick_after_ms > 0 &&
	idle_kicker &&
	io_threads_running == 0 && !is_idle_waiting) {  // only the last thread asleep needs to kick.
      // sleep, but just briefly.
      dout(20) << "io_thread" << whoami << " doing short wait, to see if i stay idle" << dendl;
      is_idle_waiting = true;
      int r = io_wakeup.WaitInterval(lock, utime_t(0, g_conf.bdev_idle_kick_after_ms*1000));
      is_idle_waiting = false;

      if (io_stop)
	break;

      if (r == ETIMEDOUT) {
	dout(20) << "io_thread" << whoami << " timeout expired, kicking ebofs" << dendl;
	kicker_cond.Signal(); // signal kicker thread
      } else {
 	dout(20) << "io_thread" << whoami << " signaled during short sleep, waking up" << dendl;
	goto wake_up;
      }
    }

    // sleeeep
    io_wakeup.Wait(lock);          // and wait (if condition still holds)
    
  wake_up:
    io_threads_running++;
    assert(io_threads_running <= g_conf.bdev_iothreads);
    dout(20) << "io_thread" << whoami << "/" << io_threads_running << " woke up, " << io_threads_running << " threads now running" << dendl;
  }

  // clean up
  ::close(fd);
  io_threads_running--;
  
  lock.Unlock();

  dout(10) << "io_thread" << whoami << " finish" << dendl;
  return 0;
}



/** do_io
 * do a single io operation
 * (lock is NOT held, but we own the *biovec)
 */
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
  int numbio = 1;
  for (p++; p != biols.end(); p++) {
    length += (*p)->length;
    bl.claim_append((*p)->bl);
    numbio++;
  }

  // do it
  dout(20) << "do_io start " << (type==biovec::IO_WRITE?"write":"read") 
           << " " << start << "~" << length 
           << " " << numbio << " bits" << dendl;
  if (type == biovec::IO_WRITE) {
    r = _write(fd, start, length, bl);
  } else if (type == biovec::IO_READ) {
    r = _read(fd, start, length, bl);
  } else assert(0);
  dout(20) << "do_io finish " << (type==biovec::IO_WRITE?"write":"read") 
           << " " << start << "~" << length << dendl;
  
  // set rval
  for (p = biols.begin(); p != biols.end(); p++)
    (*p)->rval = r;

  if (1) {
    // put in completion queue
    complete_lock.Lock();
    complete_queue.splice( complete_queue.end(), biols );
    complete_queue_len += numbio;
    complete_wakeup.Signal();
    complete_lock.Unlock();
    dout(20) << "do_io kicked completer on " << (type==biovec::IO_WRITE?"write":"read") 
	     << " " << start << "~" << length << dendl;

  } else {
    // be slow and finish synchronously
    for (p = biols.begin(); p != biols.end(); p++)
      finish_io(*p);
  }
}


/** finish_io
 *
 * finish an io by signaling the cond or performing a callback.
 * called by completion thread, unless that's disabled above.
 */
void BlockDevice::finish_io(biovec *bio)
{
  bio->done = true;
  if (bio->cond) {
    lock.Lock();   // hmm?
    bio->cond->Signal();
    lock.Unlock();
  }
  else if (bio->cb) {
    bio->cb->finish((ioh_t)bio, bio->rval);
    delete bio->cb;
    delete bio;
  }
}

/*** completion_thread
 * handle Cond signals or callbacks for completed ios
 */
void* BlockDevice::complete_thread_entry()
{
  complete_lock.Lock();
  dout(10) << "complete_thread start" << dendl;

  while (!io_stop) {

    while (!complete_queue.empty()) {
      list<biovec*> ls;
      ls.swap(complete_queue);
      dout(10) << "complete_thread grabbed " << complete_queue_len << " biovecs" << dendl;
      complete_queue_len = 0;
      
      complete_lock.Unlock();
      
      // finish
      for (list<biovec*>::iterator p = ls.begin(); 
           p != ls.end(); 
           p++) {
        biovec *bio = *p;
        dout(20) << "complete_thread finishing " << *bio << dendl;
        finish_io(bio);
      }
      
      complete_lock.Lock();
    }
    if (io_stop) break;
    
    dout(25) << "complete_thread sleeping" << dendl;
    complete_wakeup.Wait(complete_lock);
  }

  dout(10) << "complete_thread finish" << dendl;
  complete_lock.Unlock();
  return 0;
}


/*** idle kicker thread
 * kick ebofs when we're idle.  we're a separate thread (yuck)
 * because ebofs may be holding it's lock _and_ waiting for us 
 * to do useful work.  that rules out io_thread and complete_thread!
 */
void* BlockDevice::kicker_thread_entry()
{
  lock.Lock();
  dout(10) << "kicker_thread start" << dendl;

  while (!io_stop) {
    
    if (io_threads_running == 0 && idle_kicker) {
      dout(25) << "kicker_thread kicking ebofs" << dendl;
      lock.Unlock();
      idle_kicker->kick();
      lock.Lock();
      dout(25) << "kicker_thread done kicking ebofs" << dendl;
    }
    if (io_stop) break;

    dout(25) << "kicker_thread sleeping" << dendl;
    kicker_cond.Wait(lock);
  }

  dout(10) << "kicker_thread finish" << dendl;
  lock.Unlock();
  return 0;
}

  


// io queue

void BlockDevice::_submit_io(biovec *b) 
{
  // NOTE: lock must be held
  dout(15) << "_submit_io " << *b << dendl;
  
  // wake up io_thread(s)?
  if ((int)root_queue.size() == io_threads_running) 
    io_wakeup.SignalOne();
  else if ((int)root_queue.size() > io_threads_running) 
    io_wakeup.SignalAll();
    
  // queue 
  root_queue.submit_io(b);

  /*
  // [DEBUG] check for overlapping ios
  // BUG: this doesn't detect all overlaps w/ the next queue thing.
  if (g_conf.bdev_debug_check_io_overlap) {
      // BUG: this doesn't catch everything!  eg 1~10000000 will be missed....
      multimap<block_t, biovec*>::iterator p = io_queue.lower_bound(b->start);
      if ((p != io_queue.end() &&
           p->first < b->start+b->length) ||
          (p != io_queue.begin() && 
           (p--, p->second->start + p->second->length > b->start))) {
        dout(1) << "_submit_io new io " << *b 
                << " overlaps with existing " << *p->second << dendl;
        cerr << "_submit_io new io " << *b 
             << " overlaps with existing " << *p->second << dendl;
      }
    }
  */

}

int BlockDevice::_cancel_io(biovec *bio) 
{
  // NOTE: lock must be held

  if (bio->in_queue == 0) {
    dout(15) << "_cancel_io " << *bio << " FAILED" << dendl;
    return -1;
  } else {
    dout(15) << "_cancel_io " << *bio << dendl;
    bio->in_queue->cancel_io(bio);
    if (root_queue.bump()) 
      io_wakeup.SignalAll();  // something happened!
    return 0;
  }
}



// low level io

int BlockDevice::_read(int fd, block_t bno, unsigned num, bufferlist& bl) 
{
  dout(10) << "_read " << bno << "~" << num << dendl;

  assert(fd > 0);
  
  uint64_t offset = bno * EBOFS_BLOCK_SIZE;
  uint64_t actual = ::lseek64(fd, offset, SEEK_SET);
  assert(actual == offset);
  
  size_t len = num*EBOFS_BLOCK_SIZE;
  assert(bl.length() >= len);

  struct iovec iov[ bl.buffers().size() ];
  int n = 0;
  size_t left = len;
  for (list<bufferptr>::const_iterator i = bl.buffers().begin();
       i != bl.buffers().end();
       i++) {
    assert(i->length() % EBOFS_BLOCK_SIZE == 0);
    
    iov[n].iov_base = (void*)i->c_str();
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
  dout(10) << "_write " << bno << "~" << num << dendl;

  assert(fd > 0);
  
  while (1) {
    uint64_t offset = (uint64_t)bno << EBOFS_BLOCK_BITS;
    assert((uint64_t)bno * (uint64_t)EBOFS_BLOCK_SIZE == offset);
    uint64_t actual = ::lseek64(fd, offset, SEEK_SET);
    assert(actual == offset);
    
    // write buffers
    size_t len = num*EBOFS_BLOCK_SIZE;
    
    struct iovec iov[ bl.buffers().size() ];
    
    int n = 0;
    size_t left = len;
    for (list<bufferptr>::const_iterator i = bl.buffers().begin();
	 i != bl.buffers().end();
	 i++) {
      assert(i->length() % EBOFS_BLOCK_SIZE == 0);
      
      iov[n].iov_base = (void*)i->c_str();
      iov[n].iov_len = MIN(left, i->length());

      /*
      dout(10) << "_write " << (bno+(len-left))
	       << "~" << (iov[n].iov_len / 4096)
	       << " " << *i << dendl;
      */
      
      assert((((intptr_t)iov[n].iov_base) & ((intptr_t)4095ULL)) == 0);
      assert((iov[n].iov_len & 4095) == 0);
      
      left -= iov[n].iov_len;
      n++;
      if (left == 0 ||
	  n == IOV_MAX) break;
    }
    
    int r = ::writev(fd, iov, n);
    
    if (r < 0) {
      char buf[80];
      dout(1) << "couldn't write bno " << bno << " num " << num 
	      << " (" << len << " bytes) in " << n << " iovs,  r=" << r 
	      << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      dout(1) << "bl is " << bl << dendl;
      assert(0);
    } else if (r < (int)len) {
      // hrm, we didn't write _all_ of our data.  WTF kind of FS is this?
      dout(0) << "bloody hell, writev only wrote " << r << " of " << len << " bytes, looping" << dendl;
      assert(r % 4096 == 0);
      int wrote = r / 4096;
      bno += wrote;
      num -= wrote;
      bufferlist tail;
      tail.substr_of(bl, r, len-r);
      bl.claim(tail);
      continue;
    } else {
      // yay
      assert(r == (int)len);
      break;
    }
  }
  return 0;
}



// open/close

int BlockDevice::open_fd()
{
#ifdef DARWIN
  int fd = ::open(dev.c_str(), O_RDWR|O_SYNC, 0);
  ::fcntl(fd, F_NOCACHE);
  return fd;
#else
  return ::open(dev.c_str(), O_RDWR|O_SYNC|O_DIRECT, 0);
#endif
}

int BlockDevice::open(kicker *idle) 
{
  assert(fd == 0);

  // open?
  fd = open_fd();
  if (fd < 0) {
    char buf[80];
    dout(1) << "open failed, r = " << fd << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    fd = 0;
    return -1;
  }

  // lock
  if (g_conf.bdev_lock) {
    int r = ::flock(fd, LOCK_EX|LOCK_NB);
    if (r < 0) {
      derr(1) << "open " << dev << " failed to get LOCK_EX" << dendl;
      return -1;
    }
  }
               
  // figure size
  block_t b = get_num_blocks();
  if (!b) {
    dout(0) << "open can't determine size of device" << dendl;
    assert(0);
  }
  dout(2) << "open " << b << " blocks, " << b*4096 << " bytes" << dendl;
  
  // start thread
  io_threads_started = 0;
  io_threads.clear();
  for (int i=0; i<g_conf.bdev_iothreads; i++) {
    io_threads.push_back(new IOThread(this));
    io_threads.back()->create();
  }
  complete_thread.create();
  kicker_thread.create();

  // idle kicker?
  idle_kicker = idle;

  return fd;
}


/*
 * warning: ebofs shoudl drop it's lock before calling close(),
 * or else deadlock against the idle kicker
 */
int BlockDevice::close() 
{
  assert(fd>0);
  
  idle_kicker = 0;

  // shut down io thread
  dout(10) << "close stopping io+complete threads" << dendl;
  lock.Lock();
  complete_lock.Lock();
  io_stop = true;
  io_wakeup.SignalAll();
  complete_wakeup.SignalAll();
  kicker_cond.Signal();
  complete_lock.Unlock();
  lock.Unlock();    
    
  for (int i=0; i<g_conf.bdev_iothreads; i++) {
    io_threads[i]->join();
    delete io_threads[i];
  }
  io_threads.clear();

  complete_thread.join();
  kicker_thread.join();

  io_stop = false;   // in case we start again

  dout(2) << "close " << dendl;

  if (g_conf.bdev_lock)
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

