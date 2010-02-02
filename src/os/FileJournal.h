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

#include <deque>
using std::deque;

#include "Journal.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Thread.h"

class FileJournal : public Journal {
public:
  /*
   * journal header
   */
  struct header_t {
    __u32 version;
    __u32 flags;
    __u64 fsid;
    __u32 block_size;
    __u32 alignment;
    __s64 max_size;   // max size of journal ring buffer
    __s64 start;      // offset of first entry

    header_t() : version(1), flags(0), fsid(0), block_size(0), alignment(0), max_size(0), start(0) {}

    void clear() {
      start = block_size;
    }
  } header;

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
  };

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

  __u64 last_committed_seq;

  __u64 full_commit_seq;  // don't write, wait for this seq to commit
  __u64 full_restart_seq; // start writing again with this seq

  int fd;

  // in journal
  deque<pair<__u64, off64_t> > journalq;  // track seq offsets, so we can trim later.

  // currently being journaled and awaiting callback.
  //  or, awaiting callback bc journal was full.
  deque<__u64> writing_seq;
  deque<Context*> writing_fin;

  // waiting to be journaled
  struct write_item {
    __u64 seq;
    bufferlist bl;
    Context *fin;
    write_item(__u64 s, bufferlist& b, Context *f) : seq(s), fin(f) { bl.claim(b); }
  };
  deque<write_item> writeq;
  
  // write thread
  Mutex write_lock;
  Cond write_cond, write_empty_cond;
  bool write_stop;

  Cond commit_cond;

  int _open(bool wr, bool create=false);
  void print_header();
  void read_header();
  bufferptr prepare_header();
  void start_writer();
  void stop_writer();
  void write_thread_entry();

  bool check_for_full(__u64 seq, off64_t pos, off64_t size);
  void prepare_multi_write(bufferlist& bl);
  bool prepare_single_write(bufferlist& bl, off64_t& queue_pos);
  bool prepare_single_dio_write(bufferlist& bl, off64_t& queue_pos);
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
  FileJournal(__u64 fsid, Finisher *fin, Cond *sync_cond, const char *f, bool dio=false) : 
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
  int open(__u64 last_seq);
  void close();

  void flush();

  bool is_writeable() {
    return read_pos == 0;
  }
  void make_writeable();

  // writes
  void submit_entry(__u64 seq, bufferlist& bl, Context *oncommit);  // submit an item
  void committed_thru(__u64 seq);
  bool is_full();

  void set_wait_on_full(bool b) { wait_on_full = b; }

  // reads
  bool read_entry(bufferlist& bl, __u64& seq);
};

#endif
