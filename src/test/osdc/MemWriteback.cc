// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <time.h>

#include <thread>
#include "common/debug.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Mutex.h"
#include "include/assert.h"
#include "common/ceph_time.h"

#include "MemWriteback.h"

#define dout_subsys ceph_subsys_objectcacher
#undef dout_prefix
#define dout_prefix *_dout << "MemWriteback(" << this << ") "

class C_DelayRead : public Context {
  MemWriteback *wb;
  CephContext *m_cct;
  Context *m_con;
  ceph::timespan m_delay;
  Mutex *m_lock;
  object_t m_oid;
  uint64_t m_off;
  uint64_t m_len;
  bufferlist *m_bl;

public:
  C_DelayRead(MemWriteback *mwb, CephContext *cct, Context *c, Mutex *lock,
	      const object_t& oid, uint64_t off, uint64_t len, bufferlist *pbl,
	      uint64_t delay_ns=0)
    : wb(mwb), m_cct(cct), m_con(c),
      m_delay(delay_ns * std::chrono::nanoseconds(1)),
      m_lock(lock), m_oid(oid), m_off(off), m_len(len), m_bl(pbl) {}
  void finish(int r) {
    std::this_thread::sleep_for(m_delay);
    m_lock->Lock();
    r = wb->read_object_data(m_oid, m_off, m_len, m_bl);
    if (m_con)
      m_con->complete(r);
    m_lock->Unlock();
  }
};

class C_DelayWrite : public Context {
  MemWriteback *wb;
  CephContext *m_cct;
  Context *m_con;
  ceph::timespan m_delay;
  Mutex *m_lock;
  object_t m_oid;
  uint64_t m_off;
  uint64_t m_len;
  const bufferlist& m_bl;

public:
  C_DelayWrite(MemWriteback *mwb, CephContext *cct, Context *c, Mutex *lock,
	       const object_t& oid, uint64_t off, uint64_t len,
	       const bufferlist& bl, uint64_t delay_ns=0)
    : wb(mwb), m_cct(cct), m_con(c),
      m_delay(delay_ns * std::chrono::nanoseconds(1)),
      m_lock(lock), m_oid(oid), m_off(off), m_len(len), m_bl(bl) {}
  void finish(int r) {
    std::this_thread::sleep_for(m_delay);
    m_lock->Lock();
    wb->write_object_data(m_oid, m_off, m_len, m_bl);
    if (m_con)
      m_con->complete(r);
    m_lock->Unlock();
  }
};

MemWriteback::MemWriteback(CephContext *cct, Mutex *lock, uint64_t delay_ns)
  : m_cct(cct), m_lock(lock), m_delay_ns(delay_ns)
{
  m_finisher = new Finisher(cct);
  m_finisher->start();
}

MemWriteback::~MemWriteback()
{
  m_finisher->stop();
  delete m_finisher;
}

void MemWriteback::read(const object_t& oid, uint64_t object_no,
			 const object_locator_t& oloc,
			 uint64_t off, uint64_t len, snapid_t snapid,
			 bufferlist *pbl, uint64_t trunc_size,
			 __u32 trunc_seq, int op_flags, Context *onfinish)
{
  assert(snapid == CEPH_NOSNAP);
  C_DelayRead *wrapper = new C_DelayRead(this, m_cct, onfinish, m_lock, oid,
					 off, len, pbl, m_delay_ns);
  m_finisher->queue(wrapper, len);
}

ceph_tid_t MemWriteback::write(const object_t& oid,
				const object_locator_t& oloc,
				uint64_t off, uint64_t len,
				const SnapContext& snapc,
				const bufferlist &bl, ceph::real_time mtime,
				uint64_t trunc_size, __u32 trunc_seq,
				ceph_tid_t journal_tid, Context *oncommit)
{
  assert(snapc.seq == 0);
  C_DelayWrite *wrapper = new C_DelayWrite(this, m_cct, oncommit, m_lock, oid,
					   off, len, bl, m_delay_ns);
  m_finisher->queue(wrapper, 0);
  return m_tid.inc();
}

void MemWriteback::write_object_data(const object_t& oid, uint64_t off, uint64_t len,
				     const bufferlist& data_bl)
{
  dout(1) << "writing " << oid << " " << off << "~" << len  << dendl;
  assert(len == data_bl.length());
  bufferlist& obj_bl = object_data[oid];
  bufferlist new_obj_bl;
  // ensure size, or set it if new object
  if (off + len > obj_bl.length()) {
    obj_bl.append_zero(off + len - obj_bl.length());
  }

  // beginning
  new_obj_bl.substr_of(obj_bl, 0, off);
  // overwritten bit
  new_obj_bl.append(data_bl);
  // tail bit
  bufferlist tmp;
  tmp.substr_of(obj_bl, off+len, obj_bl.length()-(off+len));
  new_obj_bl.append(tmp);
  obj_bl.swap(new_obj_bl);
  dout(1) << oid << " final size " << obj_bl.length() << dendl;
}

int MemWriteback::read_object_data(const object_t& oid, uint64_t off, uint64_t len,
				   bufferlist *data_bl)
{
  dout(1) << "reading " << oid << " " << off << "~" << len << dendl;
  auto obj_i = object_data.find(oid);
  if (obj_i == object_data.end()) {
    dout(1) << oid << "DNE!" << dendl;
    return -ENOENT;
  }

  const bufferlist& obj_bl = obj_i->second;
  dout(1) << "reading " << oid << " from total size " << obj_bl.length() << dendl;

  uint64_t read_len = MIN(len, obj_bl.length()-off);
  data_bl->substr_of(obj_bl, off, read_len);
  return 0;
}

bool MemWriteback::may_copy_on_write(const object_t&, uint64_t, uint64_t,
				      snapid_t)
{
  return false;
}
