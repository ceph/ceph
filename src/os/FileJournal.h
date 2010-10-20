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


#ifndef CEPH_EBOFS_FILEJOURNAL_H
#define CEPH_EBOFS_FILEJOURNAL_H

#include <deque>
using std::deque;

#include "Journal.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Thread.h"
#include "common/Throttle.h"

class FileJournal : public Journal {
public:
  /*
   * journal header
   */
  struct header_t {
    __u32 version;
    __u32 flags;
    uint64_t fsid;
    __u32 block_size;
    __u32 alignment;
    int64_t max_size;   // max size of journal ring buffer
    int64_t start;      // offset of first entry

    header_t() : version(1), flags(0), fsid(0), block_size(0), alignment(0), max_size(0), start(0) {}

    void clear() {
      start = block_size;
    }
  } header __attribute__((__packed__, aligned(4)));

  struct entry_header_t {
    uint64_t seq;  // fs op seq #
    uint32_t flags;
    uint32_t len;
    uint32_t pre_pad, post_pad;
    uint64_t magic1;
    uint64_t magic2;
    
    void make_magic(off64_t pos, uint64_t fsid) {
      magic1 = pos;
      magic2 = fsid ^ seq ^ len;
    }
    bool check_magic(off64_t pos, uint64_t fsid) {
      return
	magic1 == (uint64_t)pos &&
	magic2 == (fsid ^ seq ^ len);
    }
  } __attribute__((__packed__, aligned(4)));

private:
  string fn;

  char *zero_buf;

  off64_t max_size;
  size_t block_size;
  bool is_bdev;
  bool directio;
  bool writing, must_write_header;
  off64_t write_pos;      // byte where the next entry to be written will go
  off64_t read_pos;       // 

  uint64_t last_committed_seq;

  uint64_t full_commit_seq;  // don't write, wait for this seq to commit
  uint64_t full_restart_seq; // start writing again with this seq

  int fd;

  // in journal
  deque<pair<uint64_t, off64_t> > journalq;  // track seq offsets, so we can trim later.

  // currently being journaled and awaiting callback.
  //  or, awaiting callback bc journal was full.
  deque<uint64_t> writing_seq;
  deque<Context*> writing_fin;

  // waiting to be journaled
  struct write_item {
    uint64_t seq;
    bufferlist bl;
    int alignment;
    Context *fin;
    write_item(uint64_t s, bufferlist& b, int al, Context *f) :
      seq(s), alignment(al), fin(f) { 
      bl.claim(b);
    }
  };
  deque<write_item> writeq;
  
  // throttle
  Throttle throttle_ops, throttle_bytes;

  // write thread
  Mutex write_lock;
  Cond write_cond, write_empty_cond;
  bool write_stop;

  Cond commit_cond;

  int _open(bool wr, bool create=false);
  int _open_block_device();
  void _check_disk_write_cache() const;
  int _open_file(int64_t oldsize, blksize_t blksize, bool create);
  void print_header();
  int read_header();
  bufferptr prepare_header();
  void start_writer();
  void stop_writer();
  void write_thread_entry();

  int check_for_full(uint64_t seq, off64_t pos, off64_t size);
  int prepare_multi_write(bufferlist& bl, uint64_t& orig_ops, uint64_t& orig_bytee);
  int prepare_single_write(bufferlist& bl, off64_t& queue_pos, uint64_t& orig_ops, uint64_t& orig_bytes);
  void do_write(bufferlist& bl);

  void write_bl(off64_t& pos, bufferlist& bl);
  void wrap_read_bl(off64_t& pos, int64_t len, bufferlist& bl);

  class Writer : public Thread {
    FileJournal *journal;
  public:
    Writer(FileJournal *fj) : journal(fj) {}
    void *entry() {
      journal->write_thread_entry();
      return 0;
    }
  } write_thread;

  off64_t get_top() {
    return ROUND_UP_TO(sizeof(header), block_size);
  }

 public:
  FileJournal(uint64_t fsid, Finisher *fin, Cond *sync_cond, const char *f, bool dio=false) : 
    Journal(fsid, fin, sync_cond), fn(f),
    zero_buf(NULL),
    max_size(0), block_size(0),
    is_bdev(false),directio(dio),
    writing(false), must_write_header(false),
    write_pos(0), read_pos(0),
    last_committed_seq(0), 
    full_commit_seq(0), full_restart_seq(0),
    fd(-1),
    write_lock("FileJournal::write_lock"),
    write_stop(false),
    write_thread(this) { }
  ~FileJournal() {
    delete[] zero_buf;
  }

  int create();
  int open(uint64_t last_seq);
  void close();

  void flush();

  void throttle();

  bool is_writeable() {
    return read_pos == 0;
  }
  void make_writeable();

  // writes
  void submit_entry(uint64_t seq, bufferlist& bl, int alignment, Context *oncommit);  // submit an item
  void committed_thru(uint64_t seq);
  bool is_full();

  void set_wait_on_full(bool b) { wait_on_full = b; }

  // reads
  bool read_entry(bufferlist& bl, uint64_t& seq);
};

#endif
