// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <time.h>

#include <thread>
#include "common/debug.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/ceph_mutex.h"
#include "include/ceph_assert.h"
#include "common/ceph_time.h"

#include "FakeWriteback.h"

#define dout_subsys ceph_subsys_objectcacher
#undef dout_prefix
#define dout_prefix *_dout << "FakeWriteback(" << this << ") "

class C_Delay : public Context {
  CephContext *m_cct;
  Context *m_con;
  ceph::timespan m_delay;
  ceph::mutex *m_lock;
  bufferlist *m_bl;
  uint64_t m_off;

public:
  C_Delay(CephContext *cct, Context *c, ceph::mutex *lock, uint64_t off,
	  bufferlist *pbl, uint64_t delay_ns=0)
    : m_cct(cct), m_con(c), m_delay(delay_ns * std::chrono::nanoseconds(1)),
      m_lock(lock), m_bl(pbl), m_off(off) {}
  void finish(int r) override {
    std::this_thread::sleep_for(m_delay);
    if (m_bl) {
      buffer::ptr bp(r);
      bp.zero();
      m_bl->append(bp);
      ldout(m_cct, 20) << "finished read " << m_off << "~" << r << dendl;
    }
    std::lock_guard locker{*m_lock};
    m_con->complete(r);
  }
};

FakeWriteback::FakeWriteback(CephContext *cct, ceph::mutex *lock, uint64_t delay_ns)
  : m_cct(cct), m_lock(lock), m_delay_ns(delay_ns)
{
  m_finisher = new Finisher(cct);
  m_finisher->start();
}

FakeWriteback::~FakeWriteback()
{
  m_finisher->stop();
  delete m_finisher;
}

void FakeWriteback::read(const object_t& oid, uint64_t object_no,
			 const object_locator_t& oloc,
			 uint64_t off, uint64_t len, snapid_t snapid,
			 bufferlist *pbl, uint64_t trunc_size,
			 __u32 trunc_seq, int op_flags,
                         const ZTracer::Trace &parent_trace,
                         Context *onfinish)
{
  C_Delay *wrapper = new C_Delay(m_cct, onfinish, m_lock, off, pbl,
				 m_delay_ns);
  m_finisher->queue(wrapper, len);
}

ceph_tid_t FakeWriteback::write(const object_t& oid,
				const object_locator_t& oloc,
				uint64_t off, uint64_t len,
				const SnapContext& snapc,
				const bufferlist &bl, ceph::real_time mtime,
				uint64_t trunc_size, __u32 trunc_seq,
				ceph_tid_t journal_tid,
                                const ZTracer::Trace &parent_trace,
                                Context *oncommit)
{
  C_Delay *wrapper = new C_Delay(m_cct, oncommit, m_lock, off, NULL,
				 m_delay_ns);
  m_finisher->queue(wrapper, 0);
  return ++m_tid;
}

bool FakeWriteback::may_copy_on_write(const object_t&, uint64_t, uint64_t,
				      snapid_t)
{
  return false;
}
