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

#include "config.h"
#include "FileJournal.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <fcntl.h>


#define DOUT_SUBSYS journal
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << pthread_self() << " journal "


int FileJournal::_open(bool forwrite, bool create)
{
  int flags;

  if (forwrite) {
    flags = O_RDWR;
    if (directio) flags |= O_DIRECT;
  } else {
    flags = O_RDONLY;
  }
  if (create)
    flags |= O_CREAT;
  
  if (fd >= 0) 
    ::close(fd);
  fd = ::open(fn.c_str(), flags, 0644);
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

  if (create && max_size < (g_conf.osd_journal_size << 20)) {
    __u64 newsize = g_conf.osd_journal_size << 20;
    dout(10) << "_open extending to " << newsize << " bytes" << dendl;
    r = ::ftruncate(fd, newsize);
    if (r == 0)
      max_size = newsize;
  }

  if (max_size == 0) {
    // hmm, is this a raw block device?
#ifdef BLKGETSIZE64
    // ioctl block device
    uint64_t bytes;
    r = ::ioctl(fd, BLKGETSIZE64, &bytes);
    assert(r == 0);
    max_size = bytes;
#else
# ifdef BLKGETSIZE
    // hrm, try the 32 bit ioctl?
    unsigned long sectors = 0;
    r = ioctl(fd, BLKGETSIZE, &sectors);
    assert(r == 0);
    max_size = sectors * 512ULL;
# endif
#endif
  }

  dout(2) << "_open " << fn << " fd " << fd 
	  << ": " << max_size << " bytes, block size " << block_size << dendl;

  return 0;
}

int FileJournal::create()
{
  dout(2) << "create " << fn << dendl;

  int err = _open(true, true);
  if (err < 0) return err;

  // write empty header
  memset(&header, 0, sizeof(header));
  header.clear();
  header.fsid = fsid;
  header.max_size = max_size;
  header.block_size = block_size;
  if (directio)
    header.alignment = block_size;
  else
    header.alignment = 16;  // at least stay word aligned on 64bit machines...
  header.start = get_top();
  print_header();

  buffer::ptr bp = prepare_header();
  int r = ::pwrite(fd, bp.c_str(), bp.length(), 0);
  if (r < 0) {
    dout(0) << "create write header error " << errno << " " << strerror(errno) << dendl;
    return -errno;
  }

  // zero first little bit, too.
  char z[block_size];
  memset(z, 0, block_size);
  ::pwrite(fd, z, block_size, get_top());

  ::close(fd);
  fd = -1;
  dout(2) << "create done" << dendl;
  return 0;
}

int FileJournal::open(__u64 next_seq)
{
  dout(2) << "open " << fn << " next_seq " << next_seq << dendl;

  int err = _open(false);
  if (err < 0) return err;

  // assume writeable, unless...
  read_pos = 0;
  write_pos = get_top();

  // read header?
  read_header();
  dout(10) << "open header.fsid = " << header.fsid 
    //<< " vs expected fsid = " << fsid 
	   << dendl;
  if (header.fsid != fsid) {
    dout(2) << "open fsid doesn't match, invalid (someone else's?) journal" << dendl;
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

  // find next entry
  read_pos = header.start;
  __u64 seq = 0;
  while (1) {
    bufferlist bl;
    off64_t old_pos = read_pos;
    if (!read_entry(bl, seq)) {
      dout(10) << "open reached end of journal." << dendl;
      break;
    }
    if (seq > next_seq) {
      dout(10) << "open entry " << seq << " len " << bl.length() << " > next_seq " << next_seq << dendl;
      read_pos = -1;
      return 0;
    }
    if (seq == next_seq) {
      dout(10) << "open reached seq " << seq << dendl;
      read_pos = old_pos;
      break;
    }
    seq++;  // next event should follow.
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
  dout(10) << "header: block_size " << header.block_size
	   << " alignment " << header.alignment
	   << " max_size " << header.max_size
	   << dendl;
  dout(10) << "header: start " << header.start << " wrap " << header.wrap << dendl;
  dout(10) << " write_pos " << write_pos << dendl;
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




bool FileJournal::check_for_wrap(__u64 seq, off64_t *pos, off64_t size, bool can_wrap)
{
  dout(20) << "check_for_wrap seq " << seq
	   << " pos " << *pos << " size " << size
	   << " max_size " << header.max_size
	   << " wrap " << header.wrap
	   << dendl;
  
  // already full?
  if (full_commit_seq || full_restart_seq)
    return false;

  if (do_sync_cond) {
    __s64 approxroom = header.wrap ?
      header.wrap + *pos - header.start :
      header.max_size + header.start - *pos;
    if (approxroom < (header.max_size >> 1) &&
	approxroom + size > (header.max_size >> 1))
      do_sync_cond->Signal();  // initiate a real commit so we can trim
  }

  // does it fit?
  if (header.wrap) {
    // we're wrapped.  don't overwrite ourselves.

    if (*pos + size < header.start)
      return true; // fits

    dout(1) << "JOURNAL FULL (and wrapped), " << *pos << "+" << size
	     << " >= " << header.start
	     << dendl;
  } else {
    // we haven't wrapped.  

    if (*pos + size < header.max_size)
      return true; // fits

    if (!can_wrap)
      return false;  // can't wrap just now..

    // is there room if we wrap?
    if (get_top() + size < header.start) {
      // yes!
      dout(10) << " wrapping from " << *pos << " to " << get_top() << dendl;
      header.wrap = *pos;
      *pos = get_top();
      must_write_header = true;
      return true;
    }

    // no room.
    dout(1) << "submit_entry JOURNAL FULL (and can't wrap), " << *pos << "+" << size
	     << " >= " << header.max_size
	     << dendl;
  }

  full_commit_seq = seq;
  full_restart_seq = seq+1;
  while (!writeq.empty()) {
    if (writeq.front().fin) {
      writing_seq.push_back(writeq.front().seq);
      writing_fin.push_back(writeq.front().fin);
    }
    writeq.pop_front();
  }  
  print_header();
  return false;
}


void FileJournal::prepare_multi_write(bufferlist& bl)
{
  // gather queued writes
  off64_t queue_pos = write_pos;

  int eleft = g_conf.journal_max_write_entries;
  int bleft = g_conf.journal_max_write_bytes;

  if (full_commit_seq || full_restart_seq)
    return;

  while (!writeq.empty()) {
    // grab next item
    __u64 seq = writeq.front().seq;
    bufferlist &ebl = writeq.front().bl;
    off64_t size = 2*sizeof(entry_header_t) + ebl.length();

    bool can_wrap = !bl.length();  // only wrap if this is a new thinger
    if (!check_for_wrap(seq, &queue_pos, size, can_wrap))
      break;

    // set write_pos?  (check_for_wrap may have moved it)
    if (!bl.length())
      write_pos = queue_pos;
    
    // add to write buffer
    dout(15) << "prepare_multi_write will write " << queue_pos << " : seq " << seq
	     << " len " << ebl.length() << " -> " << size
	     << " (left " << eleft << "/" << bleft << ")"
	     << dendl;
    
    // add it this entry
    entry_header_t h;
    h.seq = seq;
    h.len = ebl.length();
    h.make_magic(queue_pos, header.fsid);
    bl.append((const char*)&h, sizeof(h));
    bl.claim_append(ebl);
    bl.append((const char*)&h, sizeof(h));
    
    if (writeq.front().fin) {
      writing_seq.push_back(seq);
      writing_fin.push_back(writeq.front().fin);
    }

    // pop from writeq
    writeq.pop_front();
    journalq.push_back(pair<__u64,off64_t>(seq, queue_pos));

    queue_pos += size;

    // pad...
    if (queue_pos % header.alignment) {
      int pad = header.alignment - (queue_pos % header.alignment);
      bufferptr bp(pad);
      bp.zero();
      bl.push_back(bp);
      queue_pos += pad;
      //dout(20) << "   padding with " << pad << " bytes, queue_pos now " << queue_pos << dendl;
    }

    if (eleft) {
      if (--eleft == 0) {
	dout(20) << "    hit max events per write " << g_conf.journal_max_write_entries << dendl;
	break;
      }
    }
    if (bleft) {
      bleft -= size;
      if (bleft == 0) {
	dout(20) << "    hit max write size " << g_conf.journal_max_write_bytes << dendl;
	break;
      }
    }
  }
}

bool FileJournal::prepare_single_dio_write(bufferlist& bl)
{
  // grab next item
  __u64 seq = writeq.front().seq;
  bufferlist &ebl = writeq.front().bl;
    
  off64_t size = 2*sizeof(entry_header_t) + ebl.length();
  size = ROUND_UP_TO(size, header.alignment);
  
  if (!check_for_wrap(seq, &write_pos, size, true))
    return false;
  if (full_commit_seq || full_restart_seq) return false;

  // build it
  dout(15) << "prepare_single_dio_write will write " << write_pos << " : seq " << seq
	   << " len " << ebl.length() << " -> " << size << dendl;

  bufferptr bp = buffer::create_page_aligned(size);
  entry_header_t *h = (entry_header_t*)bp.c_str();
  h->seq = seq;
  h->len = ebl.length();
  h->make_magic(write_pos, header.fsid);
  ebl.copy(0, ebl.length(), bp.c_str()+sizeof(*h));
  memcpy(bp.c_str() + sizeof(*h) + ebl.length(), h, sizeof(*h));
  bl.push_back(bp);
  
  if (writeq.front().fin) {
    writing_seq.push_back(seq);
    writing_fin.push_back(writeq.front().fin);
  }
  
  // pop from writeq
  writeq.pop_front();
  journalq.push_back(pair<__u64,off64_t>(seq, write_pos));

  return true;
}

void FileJournal::do_write(bufferlist& bl)
{
  // nothing to do?
  if (bl.length() == 0 && !must_write_header) 
    return;

  buffer::ptr hbp;
  if (must_write_header) {
    must_write_header = false;
    hbp = prepare_header();
  }

  writing = true;

  header_t old_header = header;

  write_lock.Unlock();

  dout(15) << "do_write writing " << write_pos << "~" << bl.length() 
	   << (hbp.length() ? " + header":"")
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

  write_pos += bl.length();
  write_pos = ROUND_UP_TO(write_pos, header.alignment);

  // kick finisher?  
  //  only if we haven't filled up recently!
  if (full_commit_seq || full_restart_seq) {
    dout(10) << "do_write NOT queueing finisher seq " << writing_seq.front()
	     << ", full_commit_seq|full_restart_seq" << dendl;
  } else {
    dout(20) << "do_write doing finisher queue " << writing_fin << dendl;
    writing_seq.clear();
    finisher->queue(writing_fin);
  }
}


void FileJournal::write_thread_entry()
{
  dout(10) << "write_thread_entry start" << dendl;
  write_lock.Lock();
  
  while (!write_stop || 
	 !writeq.empty()) {  // ensure we fully flush the writeq before stopping
    if (writeq.empty()) {
      // sleep
      dout(20) << "write_thread_entry going to sleep" << dendl;
      write_cond.Wait(write_lock);
      dout(20) << "write_thread_entry woke up" << dendl;
      continue;
    }
    
    bufferlist bl;
    if (directio)
      prepare_single_dio_write(bl);
    else
      prepare_multi_write(bl);
    do_write(bl);
  }

  write_lock.Unlock();
  dout(10) << "write_thread_entry finish" << dendl;
}


void FileJournal::submit_entry(__u64 seq, bufferlist& e, Context *oncommit)
{
  Mutex::Locker locker(write_lock);  // ** lock **

  // dump on queue
  dout(10) << "submit_entry seq " << seq
	   << " len " << e.length()
	   << " (" << oncommit << ")" << dendl;
  
  if (!full_commit_seq && full_restart_seq && 
      seq >= full_restart_seq) {
    dout(1) << " seq " << seq << " >= full_restart_seq " << full_restart_seq 
	     << ", restarting journal" << dendl;
    full_restart_seq = 0;
  }
  if (!full_commit_seq && !full_restart_seq) {
    writeq.push_back(write_item(seq, e, oncommit));
    write_cond.Signal(); // kick writer thread
  } else {
    // not journaling this.  restart writing no sooner than seq + 1.
    full_restart_seq = seq+1;
    dout(10) << " journal is/was full, will restart no sooner than seq " << full_restart_seq << dendl;
    if (oncommit) {
      writing_seq.push_back(seq);
      writing_fin.push_back(oncommit);
    }
  }
}


void FileJournal::committed_thru(__u64 seq)
{
  Mutex::Locker locker(write_lock);

  if (seq < last_committed_seq) {
    dout(10) << "committed_thru " << seq << " < last_committed_seq " << last_committed_seq << dendl;
    assert(seq >= last_committed_seq);
    return;
  }
  if (seq == last_committed_seq) {
    dout(10) << "committed_thru " << seq << " == last_committed_seq " << last_committed_seq << dendl;
    return;
  }

  dout(10) << "committed_thru " << seq << " (last_committed_seq " << last_committed_seq << ")" << dendl;
  last_committed_seq = seq;

  // was full?
  if (full_commit_seq && seq >= full_commit_seq) {
    dout(1) << " seq " << seq << " >= full_commit_seq " << full_commit_seq 
	     << ", prior journal contents are now fully committed.  resetting journal." << dendl;
    full_commit_seq = 0;
  }

  // adjust start pointer
  while (!journalq.empty() && journalq.front().first <= seq) {
    if (journalq.front().second == get_top()) {
      dout(10) << " committed event at " << journalq.front().second << ", clearing wrap marker" << dendl;
      header.wrap = 0;  // clear wrap marker
    }
    journalq.pop_front();
  }
  if (!journalq.empty()) {
    header.start = journalq.front().second;
  } else {
    header.start = write_pos;
  }
  must_write_header = true;
  print_header();
  
  // committed but writing
  while (!writing_seq.empty() && writing_seq.front() <= seq) {
    dout(15) << " finishing committed but writing|waiting seq " << writing_seq.front() << dendl;
    finisher->queue(writing_fin.front());
    writing_seq.pop_front();
    writing_fin.pop_front();
  }
  
  // committed but unjournaled items
  while (!writeq.empty() && writeq.front().seq <= seq) {
    dout(15) << " dropping committed but unwritten seq " << writeq.front().seq 
	     << " len " << writeq.front().bl.length()
	     << " (" << writeq.front().fin << ")"
	     << dendl;
    if (writeq.front().fin)
      finisher->queue(writeq.front().fin);
    writeq.pop_front();  
  }
  
  dout(10) << "committed_thru done" << dendl;
}


void FileJournal::make_writeable()
{
  _open(true);

  if (read_pos > 0)
    write_pos = read_pos;
  else
    write_pos = get_top();
  read_pos = 0;

  must_write_header = true;
  start_writer();
}


bool FileJournal::read_entry(bufferlist& bl, __u64& seq)
{
  if (!read_pos) {
    dout(2) << "read_entry -- not readable" << dendl;
    return false;
  }
  if (read_pos == header.wrap) {
    read_pos = get_top();
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
      h.seq != f.seq ||
      h.len != f.len) {
    dout(2) << "read_entry " << read_pos << " : bad footer magic, partial entry, end of journal" << dendl;
    return false;
  }


  // yay!
  dout(1) << "read_entry " << read_pos << " : seq " << h.seq
	  << " " << h.len << " bytes"
	  << dendl;

  if (seq && h.seq != seq) {
    dout(2) << "read_entry " << read_pos << " : got seq " << h.seq << ", expected " << seq << ", stopping" << dendl;
    return false;
  }

  if (h.seq < last_committed_seq) {
    dout(0) << "read_entry seq " << seq << " < last_committed_seq " << last_committed_seq << dendl;
    assert(h.seq >= last_committed_seq);
    return false;
  }
  last_committed_seq = h.seq;

  // ok!
  bl.clear();
  bl.push_back(bp);
  seq = h.seq;
  journalq.push_back(pair<__u64,off64_t>(h.seq, read_pos));

  read_pos += 2*sizeof(entry_header_t) + h.len;
  read_pos = ROUND_UP_TO(read_pos, header.alignment);
  
  return true;
}
