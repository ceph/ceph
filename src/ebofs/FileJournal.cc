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

#define dout(x) if (x <= g_conf.debug_ebofs) *_dout << dbeginl << g_clock.now() << " ebofs(" << ebofs->dev.get_device_name() << ").journal "
#define derr(x) if (x <= g_conf.debug_ebofs) *_derr << dbeginl << g_clock.now() << " ebofs(" << ebofs->dev.get_device_name() << ").journal "


int FileJournal::create()
{
  dout(2) << "create " << fn << dendl;

  // open/create
  fd = ::open(fn.c_str(), O_RDWR|O_SYNC);
  if (fd < 0) {
    dout(2) << "create failed " << errno << " " << strerror(errno) << dendl;
    return -errno;
  }
  assert(fd > 0);

  //::ftruncate(fd, 0);
  //::fchmod(fd, 0644);

  // get size
  struct stat st;
  ::fstat(fd, &st);
  dout(2) << "create " << fn << " " << st.st_size << " bytes" << dendl;

  // write empty header
  memset(&header, 0, sizeof(header));
  header.clear();
  header.fsid = ebofs->get_fsid();
  header.max_size = st.st_size;
  write_header();
  
  // writeable.
  read_pos = 0;
  write_pos = sizeof(header);

  ::close(fd);

  return 0;
}

int FileJournal::open()
{
  //dout(1) << "open " << fn << dendl;

  // open and file
  assert(fd == 0);
  fd = ::open(fn.c_str(), O_RDWR|O_SYNC);
  if (fd < 0) {
    dout(2) << "open failed " << errno << " " << strerror(errno) << dendl;
    return -errno;
  }
  assert(fd > 0);

  // assume writeable, unless...
  read_pos = 0;
  write_pos = sizeof(header);

  // read header?
  read_header();
  if (header.fsid != ebofs->get_fsid()) {
    dout(2) << "open journal fsid doesn't match, invalid (someone else's?) journal" << dendl;
  } 
  else if (header.num > 0) {
    // valid header, pick an offset
    for (int i=0; i<header.num; i++) {
      if (header.epoch[i] == ebofs->get_super_epoch()) {
	dout(2) << "using read_pos header pointer "
		<< header.epoch[i] << " at " << header.offset[i]
		<< dendl;
	read_pos = header.offset[i];
	write_pos = 0;
	break;
      }      
      else if (header.epoch[i] < ebofs->get_super_epoch()) {
	dout(2) << "super_epoch is " << ebofs->get_super_epoch() 
		<< ", skipping old " << header.epoch[i] << " at " << header.offset[i]
		<< dendl;
      }
      else if (header.epoch[i] > ebofs->get_super_epoch()) {
	dout(2) << "super_epoch is " << ebofs->get_super_epoch() 
		<< ", but wtf, journal is later " << header.epoch[i] << " at " << header.offset[i]
		<< dendl;
	break;
      }
    }
  }

  start_writer();

  return 0;
}

void FileJournal::close()
{
  dout(1) << "close " << fn << dendl;

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
      dout(10) << "header: wrap at " << header.wrap << dendl;
    }
    dout(10) << "header: epoch " << header.epoch[i] << " at " << header.offset[i] << dendl;
  }
  //if (header.wrap) dout(10) << "header: wrap at " << header.wrap << dendl;
}
void FileJournal::read_header()
{
  dout(10) << "read_header" << dendl;
  memset(&header, 0, sizeof(header));  // zero out (read may fail)
  ::lseek(fd, 0, SEEK_SET);
  int r = ::read(fd, &header, sizeof(header));
  if (r < 0) 
    dout(0) << "read_header error " << errno << " " << strerror(errno) << dendl;
  print_header();
}
void FileJournal::write_header()
{
  dout(10) << "write_header " << dendl;
  print_header();

  ::lseek(fd, 0, SEEK_SET);
  int r = ::write(fd, &header, sizeof(header));
  if (r < 0) 
    dout(0) << "write_header error " << errno << " " << strerror(errno) << dendl;
}


void FileJournal::write_thread_entry()
{
  dout(10) << "write_thread_entry start" << dendl;
  write_lock.Lock();
  
  while (!write_stop) {
    if (writeq.empty()) {
      // sleep
      dout(20) << "write_thread_entry going to sleep" << dendl;
      write_cond.Wait(write_lock);
      dout(20) << "write_thread_entry woke up" << dendl;
      continue;
    }
    
    // gather queued writes
    off_t queue_pos = write_pos;
    bufferlist bl;

    while (!writeq.empty()) {
      // grab next item
      epoch_t epoch = writeq.front().first;
      bufferlist &ebl = writeq.front().second;
      off_t size = 2*sizeof(entry_header_t) + ebl.length();
      
      // epoch boundary?
      if (epoch > header.last_epoch()) {
	dout(10) << "saw an epoch boundary " << header.last_epoch() << " -> " << epoch << dendl;
	header.push(epoch, queue_pos);
      }

      // does it fit?
      if (header.wrap) {
	// we're wrapped.  don't overwrite ourselves.
	if (queue_pos + size >= header.offset[0]) {
	  if (queue_pos != write_pos) break;  // do what we have, first
	  dout(10) << "JOURNAL FULL (and wrapped), " << queue_pos << "+" << size
		   << " >= " << header.offset[0]
		   << dendl;
	  full = true;
	  writeq.clear();
	  print_header();
	  break;
	}
      } else {
	// we haven't wrapped.  
	if (queue_pos + size >= header.max_size) {
	  if (queue_pos != write_pos) break;  // do what we have, first
	  // is there room if we wrap?
	  if ((off_t)sizeof(header_t) + size < header.offset[0]) {
	    // yes!
	    dout(10) << "wrapped from " << queue_pos << " to " << sizeof(header_t) << dendl;
	    header.wrap = queue_pos;
	    queue_pos = sizeof(header_t);
	    header.push(ebofs->get_super_epoch(), queue_pos);
	    write_header();
	  } else {
	    // no room.
	    dout(10) << "submit_entry JOURNAL FULL (and can't wrap), " << queue_pos << "+" << size
		     << " >= " << header.max_size
		     << dendl;
	    full = true;
	    writeq.clear();
	    break;
	  }
	}
      }
	
      // add to write buffer
      dout(15) << "write_thread_entry will write " << queue_pos << " : " 
		<< ebl.length() 
		<< " epoch " << epoch
		<< dendl;
      
      // add it this entry
      entry_header_t h;
      h.epoch = epoch;
      h.len = ebl.length();
      h.make_magic(write_pos, header.fsid);
      bl.append((const char*)&h, sizeof(h));
      bl.claim_append(ebl);
      bl.append((const char*)&h, sizeof(h));
      
      Context *oncommit = commitq.front();
      if (oncommit)
	writingq.push_back(oncommit);
      
      // pop from writeq
      writeq.pop_front();
      commitq.pop_front();
      break;
    }

    // write anything?
    if (bl.length() > 0) {
      writing = true;
      //write_lock.Unlock();
      dout(15) << "write_thread_entry writing " << write_pos << "~" << bl.length() << dendl;
      
      ::lseek(fd, write_pos, SEEK_SET);
      for (list<bufferptr>::const_iterator it = bl.buffers().begin();
	   it != bl.buffers().end();
	   it++) {
	if ((*it).length() == 0) continue;  // blank buffer.
	::write(fd, (char*)(*it).c_str(), (*it).length() );
      }
      
      //write_lock.Lock();    
      writing = false;
      write_pos = queue_pos;
      ebofs->queue_finishers(writingq);
    }
  }
  write_lock.Unlock();
  dout(10) << "write_thread_entry finish" << dendl;
}

bool FileJournal::is_full()
{
  Mutex::Locker locker(write_lock);
  return full;
}

void FileJournal::submit_entry(bufferlist& e, Context *oncommit)
{
  Mutex::Locker locker(write_lock);  // ** lock **

  // dump on queue
  dout(10) << "submit_entry " << e.length()
	   << " epoch " << ebofs->get_super_epoch()
	   << " " << oncommit << dendl;
  commitq.push_back(oncommit);
  if (!full) {
    writeq.push_back(pair<epoch_t,bufferlist>(ebofs->get_super_epoch(), e));
    write_cond.Signal(); // kick writer thread
  }
}


void FileJournal::commit_epoch_start()
{
  dout(10) << "commit_epoch_start on " << ebofs->get_super_epoch()-1 
	   << " -- new epoch " << ebofs->get_super_epoch()
	   << dendl;

  Mutex::Locker locker(write_lock);

  // was full -> empty -> now usable?
  if (full) {
    if (header.num != 0) {
      dout(1) << " journal FULL, ignoring this epoch" << dendl;
      return;
    }
    
    dout(1) << " clearing FULL flag, journal now usable" << dendl;
    full = false;
  } 
}

void FileJournal::commit_epoch_finish()
{
  dout(10) << "commit_epoch_finish committed " << ebofs->get_super_epoch()-1 << dendl;

   Mutex::Locker locker(write_lock);

   if (full) {
     // full journal damage control.
     dout(15) << " journal was FULL, contents now committed, clearing header.  journal still not usable until next epoch." << dendl;
     header.clear();
     write_pos = sizeof(header_t);
   } else {
     // update header -- trim/discard old (committed) epochs
     while (header.num && header.epoch[0] < ebofs->get_super_epoch())
       header.pop();
   }
   write_header();
   
   // discard any unwritten items in previous epoch
   epoch_t epoch = ebofs->get_super_epoch();
   while (!writeq.empty() && writeq.front().first < epoch) {
     dout(15) << " dropping unwritten and committed " 
	      << write_pos << " : " << writeq.front().second.length()
	      << " epoch " << writeq.front().first 
	      << dendl;
     // finisher?
     Context *oncommit = commitq.front();
     if (oncommit) writingq.push_back(oncommit);
     
     // discard.
     writeq.pop_front();  
     commitq.pop_front();
   }
   
   // queue the finishers
   ebofs->queue_finishers(writingq);
}


void FileJournal::make_writeable()
{
  if (read_pos)
    write_pos = read_pos;
  else
    write_pos = sizeof(header_t);
  read_pos = 0;
}


bool FileJournal::read_entry(bufferlist& bl, epoch_t& epoch)
{
  if (!read_pos) {
    dout(2) << "read_entry -- not readable" << dendl;
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
    dout(10) << "read_entry wrapped from " << header.wrap << " to " << read_pos << dendl;
  }

  // header
  entry_header_t h;
  ::lseek(fd, read_pos, SEEK_SET);
  ::read(fd, &h, sizeof(h));
  if (!h.check_magic(read_pos, header.fsid)) {
    dout(2) << "read_entry " << read_pos << " : bad header magic, end of journal" << dendl;
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
    dout(2) << "read_entry " << read_pos << " : bad footer magic, partially entry, end of journal" << dendl;
    return false;
  }


  // yay!
  dout(1) << "read_entry " << read_pos << " : " 
	  << " " << h.len << " bytes"
	  << " epoch " << h.epoch 
	  << dendl;
  
  bl.push_back(bp);
  epoch = h.epoch;

  read_pos += 2*sizeof(entry_header_t) + h.len;

  return true;
}
