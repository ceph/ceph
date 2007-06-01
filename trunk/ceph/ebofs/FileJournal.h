// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


class FileJournal : public Journal {
  string fn;

  off_t max_size;
  off_t top;            // byte of first entry chronologically
  off_t bottom;         // byte where next entry goes
  off_t committing_to;  // offset of epoch boundary, if we are committing

  int fd;

  list<pair<epoch_t,bufferlist> > writeq;  // currently journaling
  map<off_t,Context*> commitq; // currently journaling
  
  // write thread
  bool writer_stop;

  void write_header();
  void start_writer();
  void stop_writer();
  void write_thread_entry();

  class Writer : public Thread {
	FileJournal *journal;
  public:
	Writer(FileJournal *fj) : journal(fj) {}
	void *entry() {
	  journal->write_thread();
	  return 0;
	}
  } writer_thread;

 public:
  FileJournal(Ebofs *e, char *f, off_t sz) : 
	Journal(e),
	fn(f), max_size(sz),
	top(0), bottom(0), committing_to(0),
	fd(0),
	writer_stop(false), writer_thread(this)
	{ }
  ~FileJournal() {}

  void create();
  void open();
  void close();

  // writes
  void submit_entry(bufferlist& e, Context *oncommit);  // submit an item
  void commit_epoch_start();  // mark epoch boundary
  void commit_epoch_finish(); // mark prior epoch as committed (we can expire)

  // reads
};

#endif
