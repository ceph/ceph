// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OSDC_WRITEBACKHANDLER_H
#define CEPH_OSDC_WRITEBACKHANDLER_H

#include "include/Context.h"
#include "include/types.h"
#include "osd/osd_types.h"

class WritebackHandler {
 public:
  WritebackHandler() {}
  virtual ~WritebackHandler() {}

  virtual tid_t read(const object_t& oid, const object_locator_t& oloc,
		     uint64_t off, uint64_t len, snapid_t snapid,
		     bufferlist *pbl, uint64_t trunc_size,  __u32 trunc_seq,
		     Context *onfinish) = 0;
  virtual tid_t write(const object_t& oid, const object_locator_t& oloc,
		      uint64_t off, uint64_t len, const SnapContext& snapc,
		      const bufferlist &bl, utime_t mtime, uint64_t trunc_size,
		      __u32 trunc_seq, Context *oncommit) = 0;
  virtual tid_t lock(const object_t& oid, const object_locator_t& oloc, int op,
		     int flags, Context *onack, Context *oncommit) {
    assert(0 == "this WritebackHandler does not support the lock operation");
  }
};

#endif
