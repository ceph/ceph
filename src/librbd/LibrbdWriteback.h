// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OSDC_LIBRBDWRITEBACKHANDLER_H
#define CEPH_OSDC_LIBRBDWRITEBACKHANDLER_H

#include "include/Context.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

class LibrbdWriteback : public WritebackHandler {
 public:
  LibrbdWriteback(const librados::IoCtx& io, Mutex& lock);
  virtual ~LibrbdWriteback() {}

  // Note that oloc, trunc_size, and trunc_seq are ignored
  virtual tid_t read(const object_t& oid, const object_locator_t& oloc,
		     uint64_t off, uint64_t len, snapid_t snapid,
		     bufferlist *pbl, uint64_t trunc_size,  __u32 trunc_seq,
		     Context *onfinish);

  // Note that oloc, trunc_size, and trunc_seq are ignored
  virtual tid_t write(const object_t& oid, const object_locator_t& oloc,
		      uint64_t off, uint64_t len, const SnapContext& snapc,
		      const bufferlist &bl, utime_t mtime, uint64_t trunc_size,
		      __u32 trunc_seq, Context *oncommit);

 private:
  int m_tid;
  Mutex& m_lock;
  librados::IoCtx m_ioctx;
};

#endif
