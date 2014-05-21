// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Greg Farnum <gregf@hq.newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef _BACKWARD_BACKWARD_WARNING_H
#define _BACKWARD_BACKWARD_WARNING_H   // make gcc 4.3 shut up about hash_*
#endif

#include "include/compat.h"
#include "common/entity_name.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "mds/mdstypes.h"
#include "mds/LogEvent.h"
#include "mds/JournalPointer.h"
#include "osdc/Journaler.h"

#include "Dumper.h"

#define dout_subsys ceph_subsys_mds


int Dumper::init(int rank_)
{
  rank = rank_;

  int r = MDSUtility::init();
  if (r < 0) {
    return r;
  }

  JournalPointer jp(rank, mdsmap->get_metadata_pool());
  int jp_load_result = jp.load(objecter, &lock);
  if (jp_load_result != 0) {
    std::cerr << "Error loading journal: " << cpp_strerror(jp_load_result) << std::endl;
    return jp_load_result;
  } else {
    ino = jp.front;
    return 0;
  }
}


int Dumper::recover_journal(Journaler *journaler)
{
  C_SaferCond cond;
  lock.Lock();
  journaler->recover(&cond);
  lock.Unlock();
  int const r = cond.wait();

  if (r < 0) { // Error
    derr << "error on recovery: " << cpp_strerror(r) << dendl;
    return r;
  } else {
    dout(10) << "completed journal recovery" << dendl;
    return 0;
  }
}


void Dumper::dump(const char *dump_file)
{
  int r = 0;

  Journaler journaler(ino, mdsmap->get_metadata_pool(), CEPH_FS_ONDISK_MAGIC,
                                       objecter, 0, 0, &timer);
  r = recover_journal(&journaler);
  if (r) {
    return;
  }
  uint64_t start = journaler.get_read_pos();
  uint64_t end = journaler.get_write_pos();
  uint64_t len = end-start;

  cout << "journal is " << start << "~" << len << std::endl;

  Filer filer(objecter);
  bufferlist bl;

  C_SaferCond cond;
  lock.Lock();
  filer.read(ino, &journaler.get_layout(), CEPH_NOSNAP,
             start, len, &bl, 0, &cond);
  lock.Unlock();
  r = cond.wait();

  cout << "read " << bl.length() << " bytes at offset " << start << std::endl;

  int fd = ::open(dump_file, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd >= 0) {
    // include an informative header
    char buf[200];
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "Ceph mds%d journal dump\n start offset %llu (0x%llx)\n       length %llu (0x%llx)\n%c",
	    rank, 
	    (unsigned long long)start, (unsigned long long)start,
	    (unsigned long long)bl.length(), (unsigned long long)bl.length(),
	    4);
    int r = safe_write(fd, buf, sizeof(buf));
    if (r)
      ceph_abort();

    // write the data
    ::lseek64(fd, start, SEEK_SET);
    bl.write_fd(fd);
    ::close(fd);

    cout << "wrote " << bl.length() << " bytes at offset " << start << " to " << dump_file << "\n"
	 << "NOTE: this is a _sparse_ file; you can\n"
	 << "\t$ tar cSzf " << dump_file << ".tgz " << dump_file << "\n"
	 << "      to efficiently compress it while preserving sparseness." << std::endl;
  } else {
    int err = errno;
    derr << "unable to open " << dump_file << ": " << cpp_strerror(err) << dendl;
  }
}

void Dumper::undump(const char *dump_file)
{

  cout << "undump " << dump_file << std::endl;
  
  int fd = ::open(dump_file, O_RDONLY);
  if (fd < 0) {
    derr << "couldn't open " << dump_file << ": " << cpp_strerror(errno) << dendl;
    return;
  }

  // Ceph mds0 journal dump
  //  start offset 232401996 (0xdda2c4c)
  //        length 1097504 (0x10bf20)

  char buf[200];
  int r = safe_read(fd, buf, sizeof(buf));
  if (r < 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return;
  }

  long long unsigned start, len;
  sscanf(strstr(buf, "start offset"), "start offset %llu", &start);
  sscanf(strstr(buf, "length"), "length %llu", &len);

  cout << "start " << start << " len " << len << std::endl;
  
  Journaler::Header h;
  h.trimmed_pos = start;
  h.expire_pos = start;
  h.write_pos = start+len;
  h.magic = CEPH_FS_ONDISK_MAGIC;

  h.layout = g_default_file_layout;
  h.layout.fl_pg_pool = mdsmap->get_metadata_pool();
  
  bufferlist hbl;
  ::encode(h, hbl);

  object_t oid = file_object_t(ino, 0);
  object_locator_t oloc(mdsmap->get_metadata_pool());
  SnapContext snapc;

  cout << "writing header " << oid << std::endl;
  C_SaferCond header_cond;
  lock.Lock();
  objecter->write_full(oid, oloc, snapc, hbl, ceph_clock_now(g_ceph_context), 0, 
		       NULL, &header_cond);
  lock.Unlock();

  r = header_cond.wait();
  if (r != 0) {
    derr << "Failed to write header: " << cpp_strerror(r) << dendl;
    return;
  }

  Filer filer(objecter);

  /* Erase any objects at the end of the region to which we shall write
   * the new log data.  This is to avoid leaving trailing junk after
   * the newly written data.  Any junk more than one object ahead
   * will be taken care of during normal operation by Journaler's
   * prezeroing behaviour */
  {
    uint32_t const object_size = h.layout.fl_object_size;
    assert(object_size > 0);
    uint64_t const last_obj = h.write_pos / object_size;
    uint64_t const purge_count = 2;
    C_SaferCond purge_cond;
    cout << "Purging " << purge_count << " objects from " << last_obj << std::endl;
    lock.Lock();
    filer.purge_range(ino, &h.layout, snapc, last_obj, purge_count, ceph_clock_now(g_ceph_context), 0, &purge_cond);
    lock.Unlock();
    purge_cond.wait();
  }
  
  // Stream from `fd` to `filer`
  uint64_t pos = start;
  uint64_t left = len;
  while (left > 0) {
    // Read
    bufferlist j;
    lseek64(fd, pos, SEEK_SET);
    uint64_t l = MIN(left, 1024*1024);
    j.read_fd(fd, l);

    // Write
    cout << " writing " << pos << "~" << l << std::endl;
    C_SaferCond write_cond;
    lock.Lock();
    filer.write(ino, &h.layout, snapc, pos, l, j, ceph_clock_now(g_ceph_context), 0, NULL, &write_cond);
    lock.Unlock();

    r = write_cond.wait();
    if (r != 0) {
      derr << "Failed to write header: " << cpp_strerror(r) << dendl;
      return;
    }
      
    // Advance
    pos += l;
    left -= l;
  }

  VOID_TEMP_FAILURE_RETRY(::close(fd));
  cout << "done." << std::endl;
}

