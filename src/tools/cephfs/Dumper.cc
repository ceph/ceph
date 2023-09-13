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
#include "include/fs_types.h"
#include "common/entity_name.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "mds/mdstypes.h"
#include "mds/LogEvent.h"
#include "mds/JournalPointer.h"
#include "osdc/Journaler.h"
#include "mon/MonClient.h"

#include "Dumper.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

#define HEADER_LEN 4096

using namespace std;

int Dumper::init(mds_role_t role_, const std::string &type)
{
  role = role_;

  int r = MDSUtility::init();
  if (r < 0) {
    return r;
  }

  auto& fs =  fsmap->get_filesystem(role.fscid);

  if (type == "mdlog") {
    JournalPointer jp(role.rank, fs.get_mds_map().get_metadata_pool());
    int jp_load_result = jp.load(objecter);
    if (jp_load_result != 0) {
      std::cerr << "Error loading journal: " << cpp_strerror(jp_load_result) << std::endl;
      return jp_load_result;
    } else {
      ino = jp.front;
    }
  } else if (type == "purge_queue") {
    ino = MDS_INO_PURGE_QUEUE + role.rank;
  } else {
    ceph_abort(); // should not get here 
  }
  return 0;
}


int Dumper::recover_journal(Journaler *journaler)
{
  C_SaferCond cond;
  lock.lock();
  journaler->recover(&cond);
  lock.unlock();
  const int r = cond.wait();

  if (r < 0) { // Error
    derr << "error on recovery: " << cpp_strerror(r) << dendl;
    return r;
  } else {
    dout(10) << "completed journal recovery" << dendl;
    return 0;
  }
}


int Dumper::dump(const char *dump_file)
{
  int r = 0;

  auto& fs = fsmap->get_filesystem(role.fscid);

  Journaler journaler("dumper", ino, fs.get_mds_map().get_metadata_pool(),
                      CEPH_FS_ONDISK_MAGIC, objecter, 0, 0,
                      &finisher);
  r = recover_journal(&journaler);
  if (r) {
    return r;
  }
  uint64_t start = journaler.get_read_pos();
  uint64_t end = journaler.get_write_pos();
  uint64_t len = end-start;

  Filer filer(objecter, &finisher);

  cout << "journal is " << start << "~" << len << std::endl;

  int fd = ::open(dump_file, O_WRONLY|O_CREAT|O_TRUNC|O_BINARY, 0644);
  if (fd >= 0) {
    // include an informative header
    uuid_d fsid = monc->get_fsid();
    char fsid_str[40];
    fsid.print(fsid_str);
    char buf[HEADER_LEN];
    memset(buf, 0, sizeof(buf));
    snprintf(buf, HEADER_LEN, "Ceph mds%d journal dump\n start offset %llu (0x%llx)\n\
       length %llu (0x%llx)\n    write_pos %llu (0x%llx)\n    format %llu\n\
       trimmed_pos %llu (0x%llx)\n    stripe_unit %lu (0x%lx)\n    stripe_count %lu (0x%lx)\n\
       object_size %lu (0x%lx)\n    fsid %s\n%c",
	    role.rank, 
	    (unsigned long long)start, (unsigned long long)start,
	    (unsigned long long)len, (unsigned long long)len,
	    (unsigned long long)journaler.last_committed.write_pos, (unsigned long long)journaler.last_committed.write_pos,
	    (unsigned long long)journaler.last_committed.stream_format,
	    (unsigned long long)journaler.last_committed.trimmed_pos, (unsigned long long)journaler.last_committed.trimmed_pos,
            (unsigned long)journaler.last_committed.layout.stripe_unit, (unsigned long)journaler.last_committed.layout.stripe_unit,
            (unsigned long)journaler.last_committed.layout.stripe_count, (unsigned long)journaler.last_committed.layout.stripe_count,
            (unsigned long)journaler.last_committed.layout.object_size, (unsigned long)journaler.last_committed.layout.object_size,
	    fsid_str,
	    4);
    r = safe_write(fd, buf, sizeof(buf));
    if (r) {
      derr << "Error " << r << " (" << cpp_strerror(r) << ") writing journal file header" << dendl;
      ::close(fd);
      return r;
    }

    // write the data
    off64_t seeked = ::lseek64(fd, start, SEEK_SET);
    if (seeked == (off64_t)-1) {
      r = errno;
      derr << "Error " << r << " (" << cpp_strerror(r) << ") seeking to 0x" << std::hex << start << std::dec << dendl;
      ::close(fd);
      return r;
    }


    // Read and write 32MB chunks.  Slower than it could be because we're not
    // streaming, but that's okay because this is just a debug/disaster tool.
    const uint32_t chunk_size = 32 * 1024 * 1024;

    for (uint64_t pos = start; pos < start + len; pos += chunk_size) {
      bufferlist bl;
      dout(10) << "Reading at pos=0x" << std::hex << pos << std::dec << dendl;

      const uint32_t read_size = std::min<uint64_t>(chunk_size, end - pos);

      C_SaferCond cond;
      lock.lock();
      filer.read(ino, &journaler.get_layout(), CEPH_NOSNAP,
                 pos, read_size, &bl, 0, &cond);
      lock.unlock();
      r = cond.wait();
      if (r < 0) {
        derr << "Error " << r << " (" << cpp_strerror(r) << ") reading "
                "journal at offset 0x" << std::hex << pos << std::dec << dendl;
        ::close(fd);
        return r;
      }
      dout(10) << "Got 0x" << std::hex << bl.length() << std::dec
               << " bytes" << dendl;

      r = bl.write_fd(fd);
      if (r) {
        derr << "Error " << r << " (" << cpp_strerror(r) << ") writing journal file" << dendl;
        ::close(fd);
        return r;
      }
    }

    r = ::close(fd);
    if (r) {
      r = errno;
      derr << "Error " << r << " (" << cpp_strerror(r) << ") closing journal file" << dendl;
      return r;
    }

    cout << "wrote " << len << " bytes at offset " << start << " to " << dump_file << "\n"
	 << "NOTE: this is a _sparse_ file; you can\n"
	 << "\t$ tar cSzf " << dump_file << ".tgz " << dump_file << "\n"
	 << "      to efficiently compress it while preserving sparseness." << std::endl;
    return 0;
  } else {
    int err = errno;
    derr << "unable to open " << dump_file << ": " << cpp_strerror(err) << dendl;
    return err;
  }
}

int Dumper::undump(const char *dump_file, bool force)
{
  cout << "undump " << dump_file << std::endl;
  
  auto& fs = fsmap->get_filesystem(role.fscid);

  int r = 0;
  // try get layout info from cluster
  Journaler journaler("umdumper", ino, fs.get_mds_map().get_metadata_pool(),
                      CEPH_FS_ONDISK_MAGIC, objecter, 0, 0,
                      &finisher);
  int recovered = recover_journal(&journaler);
  if (recovered != 0) {
    derr << "recover_journal failed, try to get header from dump file " << dendl;
  }

  int fd = ::open(dump_file, O_RDONLY|O_BINARY);
  if (fd < 0) {
    r = errno;
    derr << "couldn't open " << dump_file << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  // Ceph mds0 journal dump
  //  start offset 232401996 (0xdda2c4c)
  //        length 1097504 (0x10bf20)

  char buf[HEADER_LEN];
  r = safe_read(fd, buf, sizeof(buf));
  if (r < 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return r;
  }

  long long unsigned start, len, write_pos, format, trimmed_pos;
  long unsigned stripe_unit, stripe_count, object_size;
  sscanf(strstr(buf, "start offset"), "start offset %llu", &start);
  sscanf(strstr(buf, "length"), "length %llu", &len);
  sscanf(strstr(buf, "write_pos"), "write_pos %llu", &write_pos);
  sscanf(strstr(buf, "format"), "format %llu", &format);

  if (!force) {
    // need to check if fsid match onlien cluster fsid
    if (strstr(buf, "fsid")) {
      uuid_d fsid;
      char fsid_str[40];
      sscanf(strstr(buf, "fsid"), "fsid %39s", fsid_str);
      r = fsid.parse(fsid_str);
      if (!r) {
	derr  << "Invalid fsid" << dendl;
	::close(fd);
	return -EINVAL;
      }

      if (fsid != monc->get_fsid()) {
	derr << "Imported journal fsid does not match online cluster fsid" << dendl;
	derr << "Use --force to skip fsid check" << dendl;
	::close(fd);
	return -EINVAL;
      }
    } else {
      derr  << "Invalid header, no fsid embeded" << dendl;
      ::close(fd);
      return -EINVAL;
    }
  }

  if (recovered == 0) {
    stripe_unit = journaler.last_committed.layout.stripe_unit;
    stripe_count = journaler.last_committed.layout.stripe_count;
    object_size = journaler.last_committed.layout.object_size;
  } else {
    // try to get layout from dump file header, if failed set layout to default
    if (strstr(buf, "stripe_unit")) {
      sscanf(strstr(buf, "stripe_unit"), "stripe_unit %lu", &stripe_unit);
    } else {
      stripe_unit = file_layout_t::get_default().stripe_unit;
    }
    if (strstr(buf, "stripe_count")) {
      sscanf(strstr(buf, "stripe_count"), "stripe_count %lu", &stripe_count);
    } else {
      stripe_count = file_layout_t::get_default().stripe_count;
    }
    if (strstr(buf, "object_size")) {
      sscanf(strstr(buf, "object_size"), "object_size %lu", &object_size);
    } else {
      object_size = file_layout_t::get_default().object_size;
    }
  }

  if (strstr(buf, "trimmed_pos")) {
    sscanf(strstr(buf, "trimmed_pos"), "trimmed_pos %llu", &trimmed_pos);
  } else {
    // Old format dump, any untrimmed objects before expire_pos will
    // be discarded as trash.
    trimmed_pos = start - (start % object_size);
  }

  if (trimmed_pos > start) {
    derr << std::hex << "Invalid header (trimmed 0x" << trimmed_pos
      << " > expire 0x" << start << std::dec << dendl;
    ::close(fd);
    return -EINVAL;
  }

  if (start > write_pos) {
    derr << std::hex << "Invalid header (expire 0x" << start
      << " > write 0x" << write_pos << std::dec << dendl;
    ::close(fd);
    return -EINVAL;
  }

  cout << "start " << start <<
    " len " << len <<
    " write_pos " << write_pos <<
    " format " << format <<
    " trimmed_pos " << trimmed_pos <<
    " stripe_unit " << stripe_unit <<
    " stripe_count " << stripe_count <<
    " object_size " << object_size << std::endl;
  
  Journaler::Header h;
  h.trimmed_pos = trimmed_pos;
  h.expire_pos = start;
  h.write_pos = write_pos;
  h.stream_format = format;
  h.magic = CEPH_FS_ONDISK_MAGIC;

  h.layout.stripe_unit = stripe_unit;
  h.layout.stripe_count = stripe_count;
  h.layout.object_size = object_size;
  h.layout.pool_id = fs.get_mds_map().get_metadata_pool();
  
  bufferlist hbl;
  encode(h, hbl);

  object_t oid = file_object_t(ino, 0);
  object_locator_t oloc(fs.get_mds_map().get_metadata_pool());
  SnapContext snapc;

  cout << "writing header " << oid << std::endl;
  C_SaferCond header_cond;
  lock.lock();
  objecter->write_full(oid, oloc, snapc, hbl,
		       ceph::real_clock::now(), 0,
		       &header_cond);
  lock.unlock();

  r = header_cond.wait();
  if (r != 0) {
    derr << "Failed to write header: " << cpp_strerror(r) << dendl;
    ::close(fd);
    return r;
  }

  Filer filer(objecter, &finisher);

  /* Erase any objects at the end of the region to which we shall write
   * the new log data.  This is to avoid leaving trailing junk after
   * the newly written data.  Any junk more than one object ahead
   * will be taken care of during normal operation by Journaler's
   * prezeroing behaviour */
  {
    uint32_t const object_size = h.layout.object_size;
    ceph_assert(object_size > 0);
    uint64_t last_obj = h.write_pos / object_size;
    uint64_t purge_count = 2;
    /* When the length is zero, the last_obj should be zeroed 
     * from the offset determined by the new write_pos instead of being purged.
     */
    if (!len) {
        purge_count = 1;
        ++last_obj;
    }
    C_SaferCond purge_cond;
    cout << "Purging " << purge_count << " objects from " << last_obj << std::endl;
    lock.lock();
    filer.purge_range(ino, &h.layout, snapc, last_obj, purge_count,
		      ceph::real_clock::now(), 0, &purge_cond);
    lock.unlock();
    purge_cond.wait();
  }
  /* When the length is zero, zero the last object 
   * from the offset determined by the new write_pos.
   */
  if (!len) {
    uint64_t offset_in_obj = h.write_pos % h.layout.object_size;
    uint64_t len           = h.layout.object_size - offset_in_obj;
    C_SaferCond zero_cond;
    cout << "Zeroing " << len << " bytes in the last object." << std::endl;
    
    lock.lock();
    filer.zero(ino, &h.layout, snapc, h.write_pos, len, ceph::real_clock::now(), 0, &zero_cond);
    lock.unlock();
    zero_cond.wait();
  }

  // Stream from `fd` to `filer`
  uint64_t pos = start;
  uint64_t left = len;
  while (left > 0) {
    // Read
    bufferlist j;
    lseek64(fd, pos, SEEK_SET);
    uint64_t l = std::min<uint64_t>(left, 1024*1024);
    j.read_fd(fd, l);

    // Write
    cout << " writing " << pos << "~" << l << std::endl;
    C_SaferCond write_cond;
    lock.lock();
    filer.write(ino, &h.layout, snapc, pos, l, j,
		ceph::real_clock::now(), 0, &write_cond);
    lock.unlock();

    r = write_cond.wait();
    if (r != 0) {
      derr << "Failed to write header: " << cpp_strerror(r) << dendl;
      ::close(fd);
      return r;
    }
      
    // Advance
    pos += l;
    left -= l;
  }

  VOID_TEMP_FAILURE_RETRY(::close(fd));
  cout << "done." << std::endl;
  return 0;
}

