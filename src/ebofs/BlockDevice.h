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


#ifndef CEPH_EBOFS_BLOCKDEVICE_H
#define CEPH_EBOFS_BLOCKDEVICE_H

using namespace std;
#include "include/buffer.h"
#include "include/interval_set.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

#include "types.h"


typedef void *ioh_t;    // opaque handle to an io request.  (in actuality, a biovec*)


class BlockDevice {
 public:
  // callback type for io completion notification
  class callback {
  public:
    virtual ~callback() {}
    virtual void finish(ioh_t ioh, int rval) = 0;
  };

  // kicker for idle notification
  class kicker {
  public:
    virtual ~kicker() {}
    virtual void kick() = 0;
  };
  
  
  /********************************************************/

  class Queue;

  // io item
  // two variants: one with Cond*, one with callback*.
  class biovec {
  public:
    static const char IO_WRITE = 1;
    static const char IO_READ = 2;

    char type;
    block_t start, length;
    bufferlist bl;
    callback *cb;
    Cond *cond;
    int rval;
    const char *note;
    bool done;

    Queue *in_queue;

    biovec(char t, block_t s, block_t l, bufferlist& b, callback *c, const char *n=0) :
      type(t), start(s), length(l), bl(b), cb(c), cond(0), rval(0), note(n), done(false), in_queue(0) {}
    biovec(char t, block_t s, block_t l, bufferlist& b, Cond *c, const char *n=0) :
      type(t), start(s), length(l), bl(b), cb(0), cond(c), rval(0), note(n), done(false), in_queue(0) {}
  };
  friend ostream& operator<<(ostream& out, biovec &bio);


  /********************************************************/

  /*
   * Queue -- abstract IO queue interface
   */
  class Queue {
  public:
    virtual ~Queue() {}
    virtual void submit_io(biovec *b) = 0;
    virtual void cancel_io(biovec *b) = 0;
    virtual int dequeue_io(list<biovec*>& biols, 
                           block_t& start, block_t& length,
                           interval_set<block_t>& locked) = 0;
    virtual int size() = 0;
    virtual bool empty() { return size() == 0; }
  };
  
  /*
   * ElevatorQueue - simple elevator scheduler queue
   */
  class ElevatorQueue : public Queue {
    BlockDevice *bdev;
    const char *dev;
    map<block_t, biovec*> io_map;
    bool    el_dir_forward;
    block_t el_pos;
    utime_t el_stop;
    
  public:
    ElevatorQueue(BlockDevice *bd, const char *d) :
      bdev(bd), dev(d), 
      el_dir_forward(false),
      el_pos(0) {}
    void submit_io(biovec *b) {
      b->in_queue = this;
      assert(io_map.count(b->start) == 0);
      io_map[b->start] = b;
    }
    void cancel_io(biovec *b) {
      assert(b->in_queue == this);
      assert(io_map.count(b->start) &&
             io_map[b->start] == b);
      io_map.erase(b->start);
      b->in_queue = 0;
    }
    int dequeue_io(list<biovec*>& biols, 
                   block_t& start, block_t& length,
                   interval_set<block_t>& locked);
    int size() {
      return io_map.size();
    }
  };

  /*
   * BarrierQueue - lets you specify io "barriers"
   *  barrier() - force completion of all prior IOs before
   *    future ios are started.
   *  bump()    - must be called after cancel_io to properly
   *    detect empty subqueue.
   */
  class BarrierQueue : public Queue {
    BlockDevice *bdev;    
    const char *dev;
    list<Queue*> qls; 
  public:
    BarrierQueue(BlockDevice *bd, const char *d) : bdev(bd), dev(d) {
      barrier();
    }
    ~BarrierQueue() {
      for (list<Queue*>::iterator p = qls.begin();
	   p != qls.end();
	   ++p) 
	delete *p;
      qls.clear();
    }
    int size() {
      // this isn't perfectly accurate.
      if (!qls.empty())
        return qls.front()->size();
      return 0;
    }
    void submit_io(biovec *b) {
      assert(!qls.empty());
      qls.back()->submit_io(b);
    }
    void cancel_io(biovec *b) {
      assert(0);  // shouldn't happen.
    }
    int dequeue_io(list<biovec*>& biols, 
                   block_t& start, block_t& length,
                   interval_set<block_t>& locked);
    void barrier();
    bool bump();
  };

  
 private:
  string  dev;           // my device file
  int     fd;
  block_t num_blocks;

  Mutex lock;

  /** the root io queue. 
   * i current assumeit's a barrier queue,but this can be changed
   * with some minor rearchitecting.
   */
  BarrierQueue root_queue;

  /* io_block_lock - block ranges current dispatched to kernel
   *  once a bio is dispatched, it cannot be canceled, so an overlapping
   *  io and be submitted.  the overlapping io cannot be dispatched 
   *  to the kernel, however, until the original io finishes, or else
   *  there will be a race condition.
   */
  interval_set<block_t>      io_block_lock;    // blocks currently dispatched to kernel

  // io threads
  Cond io_wakeup;
  bool io_stop;
  int  io_threads_started, io_threads_running;
  bool is_idle_waiting;

  void *io_thread_entry();

  class IOThread : public Thread {
    BlockDevice *dev;
  public:
    IOThread(BlockDevice *d) : dev(d) {}
    void *entry() { return (void*)dev->io_thread_entry(); }
  } ;

  vector<IOThread*> io_threads;

  // private io interface
  int open_fd();  // get an fd (for a thread)

  void _submit_io(biovec *b);
  int _cancel_io(biovec *bio);
  void do_io(int fd, list<biovec*>& biols);   // called by an io thread

  // low level io
  int _read(int fd, block_t bno, unsigned num, bufferlist& bl);
  int _write(int fd, unsigned bno, unsigned num, bufferlist& bl);


  // completion callback queue
  Mutex          complete_lock;
  Cond           complete_wakeup;
  list<biovec*>  complete_queue;
  int            complete_queue_len;
  
  void finish_io(biovec *bio);

  // complete thread
  void *complete_thread_entry();
  class CompleteThread : public Thread {
    BlockDevice *dev;
  public:
    CompleteThread(BlockDevice *d) : dev(d) {}
    void *entry() { return (void*)dev->complete_thread_entry(); }
  } complete_thread;

  // kicker
  kicker *idle_kicker;  // not used..
  Mutex kicker_lock;
  Cond kicker_cond;
  void *kicker_thread_entry();
  class KickerThread : public Thread {
    BlockDevice *dev;
  public:
    KickerThread(BlockDevice *d) : dev(d) {}
    void *entry() { return (void*)dev->complete_thread_entry(); }
  } kicker_thread;



 public:
  BlockDevice(const char *d) : 
    dev(d), fd(0), num_blocks(0),
    lock("BlockDevice::lock"),
    root_queue(this, dev.c_str()),
    io_stop(false), io_threads_started(0), io_threads_running(0), is_idle_waiting(false),
    complete_lock("BlockDevice::complete_lock"),
    complete_queue_len(0),
    complete_thread(this),
    idle_kicker(0), kicker_lock("BlockDevice::kicker_lock"), kicker_thread(this) { }
  ~BlockDevice() {
    if (fd > 0) close();
  }

  // get size in blocks
  block_t get_num_blocks();
  const char *get_device_name() const { return dev.c_str(); }

  // open/close
  int open(kicker *idle = 0);
  int close();

  // state stuff
  bool is_idle() {
    lock.Lock();
    bool idle = (io_threads_running == 0) && root_queue.empty();
    lock.Unlock();
    return idle;
  }
  void barrier() {
    lock.Lock();
    root_queue.barrier();
    lock.Unlock();
  }
  void _barrier() {
    root_queue.barrier();
  }

  // ** blocking interface **

  // read
  int read(block_t bno, unsigned num, bufferptr& bptr, const char *n=0) {
    bufferlist bl;
    bl.push_back(bptr);
    return read(bno, num, bl, n);
  }
  int read(block_t bno, unsigned num, bufferlist& bl, const char *n=0) {
    Cond c;
    biovec bio(biovec::IO_READ, bno, num, bl, &c, n);
    
    lock.Lock();
    _submit_io(&bio);
    _barrier();         // need this, to prevent starvation!
    while (!bio.done) 
      c.Wait(lock);
    lock.Unlock();
    return bio.rval;
  }

  // write
  int write(unsigned bno, unsigned num, bufferptr& bptr, const char *n=0) {
    bufferlist bl;
    bl.push_back(bptr);
    return write(bno, num, bl, n);
  }
  int write(unsigned bno, unsigned num, bufferlist& bl, const char *n=0) {
    Cond c;
    biovec bio(biovec::IO_WRITE, bno, num, bl, &c, n);

    lock.Lock();
    _submit_io(&bio);
    _barrier();         // need this, to prevent starvation!
    while (!bio.done) 
      c.Wait(lock);
    lock.Unlock();
    return bio.rval;
  }

  // ** non-blocking interface **
  ioh_t read(block_t bno, unsigned num, bufferlist& bl, callback *fin, const char *n=0) {
    biovec *pbio = new biovec(biovec::IO_READ, bno, num, bl, fin, n);
    lock.Lock();
    _submit_io(pbio);
    lock.Unlock();
    return (ioh_t)pbio;
  }
  ioh_t write(block_t bno, unsigned num, bufferlist& bl, callback *fin, const char *n=0) {
    biovec *pbio = new biovec(biovec::IO_WRITE, bno, num, bl, fin, n);
    lock.Lock();
    _submit_io(pbio);
    lock.Unlock();
    return (ioh_t)pbio;
  }
  int cancel_io(ioh_t ioh);

};




#endif
