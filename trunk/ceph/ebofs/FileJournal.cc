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

#include "FileJournal.h"
#include "Ebofs.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include "config.h"
#undef dout
#define dout(x) if (true || x <= g_conf.debug_ebofs) cout << "ebofs(" << ebofs->dev.get_device_name() << ").journal "
#define derr(x) if (x <= g_conf.debug_ebofs) cerr << "ebofs(" << ebofs->dev.get_device_name() << ").journal "


int FileJournal::create()
{
  dout(1) << "create " << fn << endl;

  // open/create
  fd = ::open(fn.c_str(), O_RDWR|O_SYNC);
  if (fd < 0) {
    dout(1) << "create failed " << errno << " " << strerror(errno) << endl;
    return -errno;
  }
  assert(fd > 0);

  //::ftruncate(fd, 0);
  //::fchmod(fd, 0644);

  // get size
  struct stat st;
  ::fstat(fd, &st);
  dout(1) << "open " << fn << " " << st.st_size << " bytes" << endl;

  // write empty header
  header.clear();
  header.fsid = ebofs->get_fsid();
  header.max_size = st.st_size;
  write_header();
  
  read_pos = write_pos = queue_pos = sizeof(header);

  ::close(fd);

  return 0;
}

int FileJournal::open()
{
  //dout(1) << "open " << fn << endl;

  // open and file
  assert(fd == 0);
  fd = ::open(fn.c_str(), O_RDWR|O_SYNC);
  if (fd < 0) {
    dout(1) << "open failed " << errno << " " << strerror(errno) << endl;
    return -errno;
  }
  assert(fd > 0);

  // read header?
  read_header();
  if (header.num == 0 ||
      header.fsid != ebofs->get_fsid()) {
    // empty.
    read_pos = 0;
    write_pos = queue_pos = sizeof(header);
  } else {
    // pick an offset
    read_pos = write_pos = queue_pos = 0;
    for (int i=0; i<header.num; i++) {
      if (header.epoch[i] == ebofs->get_super_epoch()) {
	dout(2) << "using read_pos header pointer "
		<< header.epoch[i] << " at " << header.offset[i]
		<< endl;
	read_pos = header.offset[i];
	break;
      }      
      if (header.epoch[i] < ebofs->get_super_epoch()) {
	dout(2) << "super_epoch is " << ebofs->get_super_epoch() 
		<< ", skipping " << header.epoch[i] << " at " << header.offset[i]
		<< endl;
	continue;
      }
      if (header.epoch[i] > ebofs->get_super_epoch()) {
	dout(2) << "super_epoch is " << ebofs->get_super_epoch() 
		<< ", but wtf, journal is later " << header.epoch[i] << " at " << header.offset[i]
		<< endl;
	break;
      }
      assert(0);
    }
  }

  start_writer();

  return 0;
}

void FileJournal::close()
{
  dout(1) << "close " << fn << endl;

  // stop writer thread
  stop_writer();

  // close
  assert(writeq.empty());
  assert(commitq.empty());
  assert(fd > 0);
  ::close(fd);
  fd = 0;
}

void FileJournal::start_writer()
{
  write_stop = false;
  write_thread.create();
}

void FileJournal::stop_writer()
{
  write_lock.Lock();
  {
    write_stop = true;
    write_cond.Signal();
  } 
  write_lock.Unlock();
  write_thread.join();
}


void FileJournal::print_header()
{
  for (int i=0; i<header.num; i++) {
    if (i && header.offset[i] < header.offset[i-1]) {
      assert(header.wrap);
      dout(10) << "header: wrap at " << header.wrap << endl;
    }
    dout(10) << "header: epoch " << header.epoch[i] << " at " << header.offset[i] << endl;
  }
  //if (header.wrap) dout(10) << "header: wrap at " << header.wrap << endl;
}
void FileJournal::read_header()
{
  dout(10) << "read_header" << endl;
  memset(&header, 0, sizeof(header));  // zero out (read may fail)
  ::lseek(fd, 0, SEEK_SET);
  int r = ::read(fd, &header, sizeof(header));
  if (r < 0) 
    dout(0) << "read_header error " << errno << " " << strerror(errno) << endl;
  print_header();
}
void FileJournal::write_header()
{
  dout(10) << "write_header " << endl;
  print_header();

  ::lseek(fd, 0, SEEK_SET);
  int r = ::write(fd, &header, sizeof(header));
  if (r < 0) 
    dout(0) << "write_header error " << errno << " " << strerror(errno) << endl;
}


void FileJournal::write_thread_entry()
{
  dout(10) << "write_thread_entry start" << endl;
  write_lock.Lock();
  
  while (!write_stop) {
    if (writeq.empty()) {
      // sleep
      dout(20) << "write_thread_entry going to sleep" << endl;
      assert(write_pos == queue_pos);
      write_cond.Wait(write_lock);
      dout(20) << "write_thread_entry woke up" << endl;
      continue;
    }
    
    // do queued writes
    while (!writeq.empty()) {
      // grab next item
      epoch_t epoch = writeq.front().first;
      bufferlist bl;
      bl.claim(writeq.front().second);
      writeq.pop_front();
      Context *oncommit = commitq.front();
      commitq.pop_front();
      
      // wrap?
      if (write_pos == header.wrap) {
	dout(15) << "write_thread_entry wrapped write_pos at " << write_pos << " to " << sizeof(header_t) << endl;
	assert(header.wrap == write_pos);
	write_header();
	write_pos = sizeof(header_t);
      }

      // write!
      dout(15) << "write_thread_entry writing " << write_pos << " : " 
	       << bl.length() 
	       << " epoch " << epoch
	       << endl;
      
      // write entry header
      entry_header_t h;
      h.epoch = epoch;
      h.len = bl.length();
      h.make_magic(write_pos, header.fsid);

      ::lseek(fd, write_pos, SEEK_SET);
      ::write(fd, &h, sizeof(h));
      
      for (list<bufferptr>::const_iterator it = bl.buffers().begin();
	   it != bl.buffers().end();
	   it++) {
	if ((*it).length() == 0) continue;  // blank buffer.
	::write(fd, (char*)(*it).c_str(), (*it).length() );
      }

      ::write(fd, &h, sizeof(h));
      
      // move position pointer
      write_pos += 2*sizeof(entry_header_t) + bl.length();
      
      if (oncommit) {
	if (1) {
	  // queue callback
	  ebofs->queue_finisher(oncommit);
	} else {
	  // callback now
	  oncommit->finish(0);
	  delete oncommit;
	}
      }
    }
  }
  
  write_lock.Unlock();
  dout(10) << "write_thread_entry finish" << endl;
}

bool FileJournal::submit_entry(bufferlist& e, Context *oncommit)
{
  assert(queue_pos != 0); // bad create(), or journal didn't replay to completion.

  // ** lock **
  Mutex::Locker locker(write_lock);

  // wrap? full?
  off_t size = 2*sizeof(entry_header_t) + e.length();

  if (full) return false;  // already marked full.

  if (header.wrap) {
    // we're wrapped.  don't overwrite ourselves.
    if (queue_pos + size >= header.offset[0]) {
      dout(10) << "submit_entry JOURNAL FULL (and wrapped), " << queue_pos << "+" << size
	       << " >= " << header.offset[0]
	       << endl;
      full = true;
      print_header();
      return false;      
    }
  } else {
    // we haven't wrapped.  
    if (queue_pos + size >= header.max_size) {
      // is there room if we wrap?
      if ((off_t)sizeof(header_t) + size < header.offset[0]) {
	// yes!
	dout(10) << "submit_entry wrapped from " << queue_pos << " to " << sizeof(header_t) << endl;
	header.wrap = queue_pos;
	queue_pos = sizeof(header_t);
	header.push(ebofs->get_super_epoch(), queue_pos);
      } else {
	// no room.
	dout(10) << "submit_entry JOURNAL FULL (and can't wrap), " << queue_pos << "+" << size
		 << " >= " << header.max_size
		 << endl;
	full = true;
	return false;
      }
    }
  }
  
  dout(10) << "submit_entry " << queue_pos << " : " << e.length()
	   << " epoch " << ebofs->get_super_epoch()
	   << " " << oncommit << endl;
  
  // dump on queue
  writeq.push_back(pair<epoch_t,bufferlist>(ebofs->get_super_epoch(), e));
  commitq.push_back(oncommit);
  
  queue_pos += size;
  
  // kick writer thread
  write_cond.Signal();

  return true;
}


void FileJournal::commit_epoch_start()
{
  dout(10) << "commit_epoch_start on " << ebofs->get_super_epoch()-1 
	   << " -- new epoch " << ebofs->get_super_epoch()
	   << endl;

  Mutex::Locker locker(write_lock);

  // was full -> empty -> now usable?
  if (full) {
    if (header.num != 0) {
      dout(1) << " journal FULL, ignoring this epoch" << endl;
      return;
    }
    
    dout(1) << " clearing FULL flag, journal now usable" << endl;
    full = false;
  } 

  // note epoch boundary
  header.push(ebofs->get_super_epoch(), queue_pos);  // note: these entries may not yet be written.
  //write_header();  // no need to write it now, though...
}

void FileJournal::commit_epoch_finish()
{
  dout(10) << "commit_epoch_finish committed " << ebofs->get_super_epoch()-1 << endl;

  write_lock.Lock();
  {
    if (full) {
      // full journal damage control.
      dout(15) << " journal was FULL, contents now committed, clearing header.  journal still not usable until next epoch." << endl;
      header.clear();
      write_pos = queue_pos = sizeof(header_t);
    } else {
      // update header -- trim/discard old (committed) epochs
      while (header.epoch[0] < ebofs->get_super_epoch())
	header.pop();
    }
    write_header();

    // discard any unwritten items in previous epoch, and do callbacks
    epoch_t epoch = ebofs->get_super_epoch();
    list<Context*> callbacks;
    while (!writeq.empty() && writeq.front().first < epoch) {
      dout(15) << " dropping unwritten and committed " 
	       << write_pos << " : " << writeq.front().second.length()
	       << " epoch " << writeq.front().first 
	       << endl;
      // finisher?
      Context *oncommit = commitq.front();
      if (oncommit) callbacks.push_back(oncommit);

      write_pos += 2*sizeof(entry_header_t) + writeq.front().second.length();

      // discard.
      writeq.pop_front();  
      commitq.pop_front();
    }
    
    // queue the finishers
    ebofs->queue_finishers(callbacks);
  }
  write_lock.Unlock();
  
}


void FileJournal::make_writeable()
{
  if (read_pos)
    write_pos = queue_pos = read_pos;
  else
    write_pos = queue_pos = sizeof(header_t);
  read_pos = 0;
}


bool FileJournal::read_entry(bufferlist& bl, epoch_t& epoch)
{
  if (!read_pos) {
    dout(1) << "read_entry -- not readable" << endl;
    make_writeable();
    return false;
  }

  if (read_pos == header.wrap) {
    // find wrap point
    for (int i=1; i<header.num; i++) {
      if (header.offset[i] < read_pos) {
	assert(header.offset[i-1] < read_pos);
	read_pos = header.offset[i];
	break;
      }
    }
    assert(read_pos != header.wrap);
    dout(10) << "read_entry wrapped from " << header.wrap << " to " << read_pos << endl;
  }

  // header
  entry_header_t h;
  ::lseek(fd, read_pos, SEEK_SET);
  ::read(fd, &h, sizeof(h));
  if (!h.check_magic(read_pos, header.fsid)) {
    dout(1) << "read_entry " << read_pos << " : bad header magic, end of journal" << endl;
    make_writeable();
    return false;
  }

  // body
  bufferptr bp(h.len);
  ::read(fd, bp.c_str(), h.len);

  // footer
  entry_header_t f;
  ::read(fd, &f, sizeof(h));
  if (!f.check_magic(read_pos, header.fsid) ||
      h.epoch != f.epoch ||
      h.len != f.len) {
    dout(1) << "read_entry " << read_pos << " : bad footer magic, partially entry, end of journal" << endl;
    make_writeable();
    return false;
  }


  // yay!
  dout(1) << "read_entry " << read_pos << " : " 
	  << " " << h.len << " bytes"
	  << " epoch " << h.epoch 
	  << endl;
  
  bl.push_back(bp);
  epoch = h.epoch;

  read_pos += 2*sizeof(entry_header_t) + h.len;

  return true;
}
