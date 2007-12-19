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


#ifndef __EBOFS_FILEJOURNAL_H
#define __EBOFS_FILEJOURNAL_H


#include "Journal.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Thread.h"

class FileJournal : public Journal {
public:
  /** log header
   * we allow 4 pointers:
   *  top/initial,
   *  one for an epoch boundary (if any),
   *  one for a wrap in the ring buffer/journal file,
   *  one for a second epoch boundary (if any).
   * the epoch boundary one is useful only for speedier recovery in certain cases
   * (i.e. when ebofs committed, but the journal didn't rollover ... very small window!)
   */
  struct header_t {
    uint64_t fsid;
    int num;
    off_t wrap;
    off_t max_size;
    epoch_t epoch[4];
    off_t offset[4];

    header_t() : fsid(0), num(0), wrap(0), max_size(0) {}

    void clear() {
      num = 0;
      wrap = 0;
    }
    void pop() {
      if (num >= 2 && offset[0] > offset[1]) 
	wrap = 0;  // we're eliminating a wrap
      num--;
      for (int i=0; i<num; i++) {
	epoch[i] = epoch[i+1];
	offset[i] = offset[i+1];
      }
    }
    void push(epoch_t e, off_t o) {
      assert(num < 4);
      if (num > 2 && 
	  epoch[num-1] == e &&
	  epoch[num-2] == (e-1)) 
	num--;  // tail was an epoch boundary; replace it.
      epoch[num] = e;
      offset[num] = o;
      num++;
    }
  } header;

  struct entry_header_t {
    uint64_t epoch;
    uint64_t len;
    uint64_t magic1;
    uint64_t magic2;
    
    void make_magic(off_t pos, uint64_t fsid) {
      magic1 = pos;
      magic2 = fsid ^ epoch ^ len;
    }
    bool check_magic(off_t pos, uint64_t fsid) {
      return
	magic1 == (uint64_t)pos &&
	magic2 == (fsid ^ epoch ^ len);
    }
  };

private:
  string fn;

  bool full;
  off_t write_pos;      // byte where next entry written goes
  off_t queue_pos;      // byte where next entry queued for write goes

  off_t read_pos;       // 

  int fd;

  list<pair<epoch_t,bufferlist> > writeq;  // currently journaling
  list<Context*> commitq; // currently journaling
  
  // write thread
  Mutex write_lock;
  Cond write_cond;
  bool write_stop;

  void print_header();
  void read_header();
  void write_header();
  void start_writer();
  void stop_writer();
  void write_thread_entry();

  class Writer : public Thread {
    FileJournal *journal;
  public:
    Writer(FileJournal *fj) : journal(fj) {}
    void *entry() {
      journal->write_thread_entry();
      return 0;
    }
  } write_thread;

 public:
  FileJournal(Ebofs *e, char *f) : 
    Journal(e), fn(f),
    full(false),
    write_pos(0), queue_pos(0), read_pos(0),
    fd(0),
    write_stop(false), write_thread(this) { }
  ~FileJournal() {}

  int create();
  int open();
  void close();

  void make_writeable();

  // writes
  bool submit_entry(bufferlist& e, Context *oncommit);  // submit an item
  void commit_epoch_start();   // mark epoch boundary
  void commit_epoch_finish();  // mark prior epoch as committed (we can expire)

  bool read_entry(bufferlist& bl, epoch_t& e);

  // reads
};

#endif
