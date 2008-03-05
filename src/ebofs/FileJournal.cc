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


int FileJournal::_open(bool forwrite)
{
  int flags;

  if (forwrite) {
    flags = O_RDWR;
    if (directio) flags |= O_DIRECT;
  } else {
    flags = O_RDONLY;
  }
  
  if (fd >= 0) 
    ::close(fd);
  fd = ::open(fn.c_str(), flags);
  if (fd < 0) {
    dout(2) << "_open failed " << errno << " " << strerror(errno) << dendl;
    return -errno;
  }

  // get size
  struct stat st;
  int r = ::fstat(fd, &st);
  assert(r == 0);
  max_size = st.st_size;
  block_size = st.st_blksize;
  dout(2) << "_open " << fn << " fd " << fd 
	  << ": " << st.st_size << " bytes, block size " << block_size << dendl;

  return 0;
}

int FileJournal::create()
{
  dout(2) << "create " << fn << dendl;

  int err = _open(true);
  if (err < 0) return err;

  // write empty header
  memset(&header, 0, sizeof(header));
  header.clear();
  header.fsid = ebofs->get_fsid();
  header.max_size = max_size;
  header.block_size = block_size;
  if (directio)
    header.alignment = block_size;
  else
    header.alignment = 16;  // at least stay word aligned on 64bit machines...
  print_header();

  buffer::ptr bp = prepare_header();
  int r = ::pwrite(fd, bp.c_str(), bp.length(), 0);
  if (r < 0) {
    dout(0) << "create write header error " << errno << " " << strerror(errno) << dendl;
    return -errno;
  }

  ::close(fd);
  fd = -1;
  dout(2) << "create done" << dendl;
  return 0;
}

int FileJournal::open()
{
  dout(2) << "open " << fn << dendl;

  int err = _open(false);
  if (err < 0) return err;

  // assume writeable, unless...
  read_pos = 0;
  write_pos = get_top();

  // read header?
  read_header();
  if (header.fsid != ebofs->get_fsid()) {
    dout(2) << "open journal fsid doesn't match, invalid (someone else's?) journal" << dendl;
    err = -EINVAL;
  } 
  if (header.max_size > max_size) {
    dout(2) << "open journal size " << header.max_size << " > current " << max_size << dendl;
    err = -EINVAL;
  }
  if (header.block_size != block_size) {
    dout(2) << "open journal block size " << header.block_size << " != current " << block_size << dendl;
    err = -EINVAL;
  }
  if (header.alignment != block_size && directio) {
    derr(0) << "open journal alignment " << header.alignment << " does not match block size " 
	    << block_size << " (required for direct_io journal mode)" << dendl;
    err = -EINVAL;
  }
  if (err)
    return err;

  // looks like a valid header.
  write_pos = 0;  // not writeable yet
  read_pos = 0;

  if (header.num > 0) {
    // pick an offset
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

    if (read_pos == 0) {
      dout(0) << "no valid journal segments" << dendl;
      return -EINVAL;
    }

  } else {
    dout(0) << "journal was empty" << dendl;
    read_pos = get_top();
  }

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
  fd = -1;
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
  int r;
  dout(10) << "read_header" << dendl;
  if (directio) {
    buffer::ptr bp = buffer::create_page_aligned(block_size);
    bp.zero();
    r = ::pread(fd, bp.c_str(), bp.length(), 0);
    memcpy(&header, bp.c_str(), sizeof(header));
  } else {
    memset(&header, 0, sizeof(header));  // zero out (read may fail)
    r = ::pread(fd, &header, sizeof(header), 0);
  }
  if (r < 0) 
    dout(0) << "read_header error " << errno << " " << strerror(errno) << dendl;
  print_header();
}

bufferptr FileJournal::prepare_header()
{
  bufferptr bp;
  if (directio) {
    bp = buffer::create_page_aligned(block_size);
    bp.zero();
    memcpy(bp.c_str(), &header, sizeof(header));
  } else {
    bp = buffer::create(sizeof(header));
    memcpy(bp.c_str(), &header, sizeof(header));
  }
  return bp;
}




void FileJournal::check_for_wrap(epoch_t epoch, off64_t pos, off64_t size)
{
  // epoch boundary?
  dout(10) << "check_for_wrap epoch " << epoch << " last " << header.last_epoch() << " of " << header.num << dendl;
  if (epoch > header.last_epoch()) {
    dout(10) << "saw an epoch boundary " << header.last_epoch() << " -> " << epoch << dendl;
    header.push(epoch, pos);
    must_write_header = true;
  }

  // does it fit?
  if (header.wrap) {
    // we're wrapped.  don't overwrite ourselves.
    if (pos + size >= header.offset[0]) {
      dout(10) << "JOURNAL FULL (and wrapped), " << pos << "+" << size
	       << " >= " << header.offset[0]
	       << dendl;
      full = true;
      writeq.clear();
      print_header();
    }
  } else {
    // we haven't wrapped.  
    if (pos + size >= header.max_size) {
      // is there room if we wrap?
      if (get_top() + size < header.offset[0]) {
	// yes!
	dout(10) << "wrapped from " << pos << " to " << get_top() << dendl;
	header.wrap = pos;
	pos = get_top();
	header.push(ebofs->get_super_epoch(), pos);
	must_write_header = true;
      } else {
	// no room.
	dout(10) << "submit_entry JOURNAL FULL (and can't wrap), " << pos << "+" << size
		 << " >= " << header.max_size
		 << dendl;
	full = true;
	writeq.clear();
      }
    }
  }
}


void FileJournal::prepare_multi_write(bufferlist& bl)
{
  // gather queued writes
  off64_t queue_pos = write_pos;

  int eleft = g_conf.ebofs_journal_max_write_entries;
  int bleft = g_conf.ebofs_journal_max_write_bytes;

  while (!writeq.empty()) {
    // grab next item
    epoch_t epoch = writeq.front().first;
    bufferlist &ebl = writeq.front().second;
    off64_t size = 2*sizeof(entry_header_t) + ebl.length();

    if (bl.length() > 0 && bleft > 0 && bleft < size) break;
    
    check_for_wrap(epoch, queue_pos, size);
    if (full) break;
    if (bl.length() && must_write_header) 
      break;
    
    // add to write buffer
    dout(15) << "prepare_multi_write will write " << queue_pos << " : " 
	     << ebl.length() << " epoch " << epoch << " -> " << size << dendl;
    
    // add it this entry
    entry_header_t h;
    h.epoch = epoch;
    h.len = ebl.length();
    h.make_magic(queue_pos, header.fsid);
    bl.append((const char*)&h, sizeof(h));
    bl.claim_append(ebl);
    bl.append((const char*)&h, sizeof(h));
    
    Context *oncommit = commitq.front();
    if (oncommit)
      writingq.push_back(oncommit);
    
    // pop from writeq
    writeq.pop_front();
    commitq.pop_front();

    queue_pos += size;
    if (--eleft == 0) break;
    bleft -= size;
    if (bleft == 0) break;
  }
}

bool FileJournal::prepare_single_dio_write(bufferlist& bl)
{
  // grab next item
  epoch_t epoch = writeq.front().first;
  bufferlist &ebl = writeq.front().second;
    
  off64_t size = 2*sizeof(entry_header_t) + ebl.length();
  size = ROUND_UP_TO(size, header.alignment);
  
  check_for_wrap(epoch, write_pos, size);
  if (full) return false;

  // build it
  dout(15) << "prepare_single_dio_write will write " << write_pos << " : " 
	   << ebl.length() << " epoch " << epoch << " -> " << size << dendl;

  bufferptr bp = buffer::create_page_aligned(size);
  entry_header_t *h = (entry_header_t*)bp.c_str();
  h->epoch = epoch;
  h->len = ebl.length();
  h->make_magic(write_pos, header.fsid);
  ebl.copy(0, ebl.length(), bp.c_str()+sizeof(*h));
  memcpy(bp.c_str() + sizeof(*h) + ebl.length(), h, sizeof(*h));
  bl.push_back(bp);
  
  Context *oncommit = commitq.front();
  if (oncommit)
    writingq.push_back(oncommit);
  
  // pop from writeq
  writeq.pop_front();
  commitq.pop_front();
  return true;
}

void FileJournal::do_write(bufferlist& bl)
{
  // nothing to do?
  if (bl.length() == 0 && !must_write_header) 
    return;

  buffer::ptr hbp;
  if (must_write_header) 
    hbp = prepare_header();

  writing = true;

  header_t old_header = header;

  write_lock.Unlock();

  dout(15) << "do_write writing " << write_pos << "~" << bl.length() 
	   << (must_write_header ? " + header":"")
	   << dendl;
  
  // header
  if (hbp.length())
    ::pwrite(fd, hbp.c_str(), hbp.length(), 0);
  
  // entry
#ifdef DARWIN
  off_t pos = write_pos;
  ::lseek(fd, write_pos, SEEK_SET);
#else
  off64_t pos = write_pos;
  ::lseek64(fd, write_pos, SEEK_SET);
#endif
  for (list<bufferptr>::const_iterator it = bl.buffers().begin();
       it != bl.buffers().end();
       it++) {
    if ((*it).length() == 0) continue;  // blank buffer.
    int r = ::write(fd, (char*)(*it).c_str(), (*it).length());
    if (r < 0)
      derr(0) << "do_write failed with " << errno << " " << strerror(errno) 
	      << " with " << (void*)(*it).c_str() << " len " << (*it).length()
	      << dendl;
    pos += (*it).length();
  }
#ifdef DARWIN
  if (!directio)
    ::fsync(fd);
#else
  if (!directio)
    ::fdatasync(fd);
#endif


  write_lock.Lock();    

  writing = false;
  if (memcmp(&old_header, &header, sizeof(header)) == 0) {
    write_pos += bl.length();
    write_pos = ROUND_UP_TO(write_pos, header.alignment);
    ebofs->queue_finishers(writingq);
  } else {
    dout(10) << "do_write finished write but header changed?  not moving write_pos." << dendl;
    derr(0) << "do_write finished write but header changed?  not moving write_pos." << dendl;
    assert(writingq.empty());
  }
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
    
    bufferlist bl;
    must_write_header = false;
    if (directio)
      prepare_single_dio_write(bl);
    else
      prepare_multi_write(bl);
    do_write(bl);
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

void FileJournal::commit_epoch_finish(epoch_t new_epoch)
{
  dout(10) << "commit_epoch_finish committed " << (new_epoch-1) << dendl;

  Mutex::Locker locker(write_lock);
  
  if (full) {
    // full journal damage control.
    dout(15) << " journal was FULL, contents now committed, clearing header.  journal still not usable until next epoch." << dendl;
    header.clear();
    write_pos = get_top();
  } else {
    // update header -- trim/discard old (committed) epochs
    print_header();
    while (header.num && header.epoch[0] < new_epoch) {
      dout(10) << " popping epoch " << header.epoch[0] << " < " << new_epoch << dendl;
      header.pop();
    }
    if (header.num == 0) {
      dout(10) << " starting fresh" << dendl;
      write_pos = get_top();
      header.push(new_epoch, write_pos);
    }
  }
  must_write_header = true;
  
  // discard any unwritten items in previous epoch
  while (!writeq.empty() && writeq.front().first < new_epoch) {
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
  dout(10) << "commit_epoch_finish done" << dendl;
}


void FileJournal::make_writeable()
{
  _open(true);

  if (read_pos)
    write_pos = read_pos;
  else
    write_pos = get_top();
  read_pos = 0;

  must_write_header = true;
  start_writer();
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
#ifdef DARWIN
  ::lseek(fd, read_pos, SEEK_SET);
#else
  ::lseek64(fd, read_pos, SEEK_SET);
#endif
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
    dout(2) << "read_entry " << read_pos << " : bad footer magic, partial entry, end of journal" << dendl;
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
  read_pos = ROUND_UP_TO(read_pos, header.alignment);

  return true;
}
