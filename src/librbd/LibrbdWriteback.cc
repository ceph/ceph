// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/Mutex.h"
#include "include/rados/librados.h"

#include "LibrbdWriteback.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbdwriteback: "

// If we change the librados api to use an overrideable class for callbacks
// (like it does with watch/notify) this will be much nicer
struct CallbackArgs {
  CephContext *cct;
  Context *ctx;
  Mutex *lock;
  CallbackArgs(CephContext *cct, Context *c, Mutex *l) :
    cct(cct), ctx(c), lock(l) {}
};

static void librbd_writeback_librados_aio_cb(rados_completion_t c, void *arg)
{
  CallbackArgs *args = reinterpret_cast<CallbackArgs *>(arg);
  ldout(args->cct, 20) << "aio_cb completing " << dendl;
  {
    Mutex::Locker l(*args->lock);
    args->ctx->complete(rados_aio_get_return_value(c));
  }
  rados_aio_release(c);
  ldout(args->cct, 20) << "aio_cb finished" << dendl;
  delete args;
}

LibrbdWriteback::LibrbdWriteback(const librados::IoCtx& io, Mutex& lock)
  : m_tid(0), m_lock(lock)
{
  m_ioctx.dup(io);
}

tid_t LibrbdWriteback::read(const object_t& oid,
			    const object_locator_t& oloc,
			    uint64_t off, uint64_t len, snapid_t snapid,
			    bufferlist *pbl, uint64_t trunc_size,
			    __u32 trunc_seq, Context *onfinish)
{
  CallbackArgs *args = new CallbackArgs((CephContext *)m_ioctx.cct(),
					onfinish, &m_lock);
  librados::AioCompletion *rados_cb =
    librados::Rados::aio_create_completion(args, librbd_writeback_librados_aio_cb, NULL);

  m_ioctx.snap_set_read(snapid.val);
  m_ioctx.aio_read(oid.name, rados_cb, pbl, len, off);
  return ++m_tid;
}

tid_t LibrbdWriteback::write(const object_t& oid,
			     const object_locator_t& oloc,
			     uint64_t off, uint64_t len,
			     const SnapContext& snapc,
			     const bufferlist &bl, utime_t mtime,
			     uint64_t trunc_size, __u32 trunc_seq,
			     Context *oncommit)
{
  CallbackArgs *args = new CallbackArgs((CephContext *)m_ioctx.cct(),
					oncommit, &m_lock);
  librados::AioCompletion *rados_cb =
    librados::Rados::aio_create_completion(args, NULL, librbd_writeback_librados_aio_cb);
  // TODO: find a way to make this less stupid
  vector<librados::snap_t> snaps;
  for (vector<snapid_t>::const_iterator it = snapc.snaps.begin();
       it != snapc.snaps.end(); ++it) {
    snaps.push_back(it->val);
  }

  m_ioctx.snap_set_read(CEPH_NOSNAP);
  m_ioctx.selfmanaged_snap_set_write_ctx(snapc.seq.val, snaps);
  m_ioctx.aio_write(oid.name, rados_cb, bl, len, off);
  return ++m_tid;
}
