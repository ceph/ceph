// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OSDC_OBJECTERWRITEBACKHANDLER_H
#define CEPH_OSDC_OBJECTERWRITEBACKHANDLER_H

#include "osdc/Objecter.h"
#include "osdc/WritebackHandler.h"

class ObjecterWriteback : public WritebackHandler {
 public:
  ObjecterWriteback(Objecter *o, Finisher *fin, Mutex *lock)
    : m_objecter(o),
      m_finisher(fin),
      m_lock(lock) { }
  virtual ~ObjecterWriteback() {}

  virtual void read(const object_t& oid, uint64_t object_no,
		    const object_locator_t& oloc, uint64_t off, uint64_t len,
		    snapid_t snapid, bufferlist *pbl, uint64_t trunc_size,
		    __u32 trunc_seq, int op_flags, Context *onfinish) {
    m_objecter->read_trunc(oid, oloc, off, len, snapid, pbl, 0,
			   trunc_size, trunc_seq,
			   new C_OnFinisher(new C_Lock(m_lock, onfinish),
					    m_finisher));
  }

  virtual bool may_copy_on_write(const object_t& oid, uint64_t read_off,
				 uint64_t read_len, snapid_t snapid) {
    return false;
  }

  virtual ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
			   uint64_t off, uint64_t len,
			   const SnapContext& snapc, const bufferlist &bl,
			   ceph::real_time mtime, uint64_t trunc_size,
			   __u32 trunc_seq, ceph_tid_t journal_tid,
			   Context *oncommit) {
    return m_objecter->write_trunc(oid, oloc, off, len, snapc, bl, mtime, 0,
				   trunc_size, trunc_seq, NULL,
				   new C_OnFinisher(new C_Lock(m_lock,
							       oncommit),
						    m_finisher));
  }

 private:
  Objecter *m_objecter;
  Finisher *m_finisher;
  Mutex *m_lock;
};

#endif
