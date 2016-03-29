// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_TEST_OSDC_FAKEWRITEBACK_H
#define CEPH_TEST_OSDC_FAKEWRITEBACK_H

#include "include/atomic.h"
#include "include/Context.h"
#include "include/types.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

class Finisher;
class Mutex;

class FakeWriteback : public WritebackHandler {
public:
  FakeWriteback(CephContext *cct, Mutex *lock, uint64_t delay_ns);
  virtual ~FakeWriteback();

  virtual void read(const object_t& oid, uint64_t object_no,
		    const object_locator_t& oloc, uint64_t off, uint64_t len,
		    snapid_t snapid, bufferlist *pbl, uint64_t trunc_size,
		    __u32 trunc_seq, int op_flags, Context *onfinish);

  virtual ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
			   uint64_t off, uint64_t len,
			   const SnapContext& snapc, const bufferlist &bl,
			   ceph::real_time mtime, uint64_t trunc_size,
			   __u32 trunc_seq, ceph_tid_t journal_tid,
			   Context *oncommit);

  using WritebackHandler::write;

  virtual bool may_copy_on_write(const object_t&, uint64_t, uint64_t,
				 snapid_t);
private:
  CephContext *m_cct;
  Mutex *m_lock;
  uint64_t m_delay_ns;
  atomic_t m_tid;
  Finisher *m_finisher;
};

#endif
