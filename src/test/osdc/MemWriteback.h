// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_TEST_OSDC_MEMWRITEBACK_H
#define CEPH_TEST_OSDC_MEMWRITEBACK_H

#include "include/Context.h"
#include "include/types.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

#include <atomic>

class Finisher;

class MemWriteback : public WritebackHandler {
public:
  MemWriteback(CephContext *cct, ceph::mutex *lock, uint64_t delay_ns);
  ~MemWriteback() override;

  void read(const object_t& oid, uint64_t object_no,
		    const object_locator_t& oloc, uint64_t off, uint64_t len,
		    snapid_t snapid, bufferlist *pbl, uint64_t trunc_size,
		    __u32 trunc_seq, int op_flags,
                    const ZTracer::Trace &parent_trace,
                    Context *onfinish) override;

  ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
			   uint64_t off, uint64_t len,
			   const SnapContext& snapc, const bufferlist &bl,
			   ceph::real_time mtime, uint64_t trunc_size,
			   __u32 trunc_seq, ceph_tid_t journal_tid,
                           const ZTracer::Trace &parent_trace,
			   Context *oncommit) override;

  using WritebackHandler::write;

  bool may_copy_on_write(const object_t&, uint64_t, uint64_t,
				 snapid_t) override;
  void write_object_data(const object_t& oid, uint64_t off, uint64_t len,
			 const bufferlist& data_bl);
  int read_object_data(const object_t& oid, uint64_t off, uint64_t len,
		       bufferlist *data_bl);
private:
  std::map<object_t, bufferlist> object_data;
  CephContext *m_cct;
  ceph::mutex *m_lock;
  uint64_t m_delay_ns;
  std::atomic<unsigned> m_tid = { 0 };
  Finisher *m_finisher;
};

#endif
