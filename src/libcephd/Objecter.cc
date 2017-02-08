// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <condition_variable>
#include <mutex>
#include "ceph_osd.h"

#include "msg/Connection.h"
#include "osd/OSDMap.h"
#include "Objecter.h"
#include "Dispatcher.h"
#include "osd/OpRequest.h"
#include "messages/MOSDOpReply.h"

#define dout_subsys ceph_subsys_osd

// tried using FreeList for OnReadReply/OnWriteReply, but saw no benefit
#define COMPLETION_FREELIST 0

namespace ceph
{
namespace osd
{

// helper class to wait on a libosd_io_completion_fn
class SyncCompletion {
private:
  std::mutex mutex;
  std::condition_variable cond;
  bool done;
  bool waiting;
  int result;
  int length;

  void signal(int r, int len) {
    std::lock_guard<std::mutex> lock(mutex);
    done = true;
    result = r;
    length = len;
    if (waiting) // only signal if there's a waiter
      cond.notify_one();
  }

public:
  SyncCompletion() : done(false), waiting(false) {}

  int wait() {
    if (!done) { // read dirty to avoid lock
      std::unique_lock<std::mutex> lock(mutex);
      waiting = true;
      while (!done)
        cond.wait(lock);
    }
    return result != 0 ? result : length;
  }

  // libosd_io_completion_fn to signal the condition variable
  static void callback(int result, uint64_t length, int flags,
		       void *user) {
    SyncCompletion *sync = static_cast<SyncCompletion*>(user);
    sync->signal(result, length);
  }
};

// Dispatcher callback to fire the read completion
class OnReadReply : public Dispatcher::OnReply {
#if COMPLETION_FREELIST
  typedef cohort::CharArrayAlloc<OnReadReply> Alloc;
  typedef cohort::FreeList<OnReadReply, Alloc> FreeList;
  static Alloc alloc;
  static FreeList freelist;
#endif
  char *data;
  uint64_t length;
  libosd_io_completion_fn cb;
  void *user;

 public:
#if COMPLETION_FREELIST
  static void *operator new(size_t num_bytes) {
    return freelist.alloc();
  }
  void operator delete(void *p) {
    return freelist.free(static_cast<OnReadReply*>(p));
  }
#endif
  OnReadReply(char *data, uint64_t length,
	      libosd_io_completion_fn cb, void *user)
    : data(data), length(length), cb(cb), user(user) {}

  void on_reply(Message *reply) {
    assert(reply->get_type() == CEPH_MSG_OSD_OPREPLY);
    MOSDOpReply *m = static_cast<MOSDOpReply*>(reply);

    vector<OSDOp> ops;
    m->claim_ops(ops);
    m->put();

    assert(ops.size() == 1);
    OSDOp &op = *ops.begin();
    assert(op.op.op == CEPH_OSD_OP_READ);

    if (op.rval) {
      length = 0;
    } else {
      assert(length >= op.outdata.length());
      length = op.outdata.length();
      op.outdata.copy(0, length, data);
    }

    cb(-op.rval, length, 0, user);

    // delete unless we're synchronous (on the stack)
    if (cb != SyncCompletion::callback)
      delete this;
  }
};
#if COMPLETION_FREELIST
OnReadReply::Alloc OnReadReply::alloc;
OnReadReply::FreeList OnReadReply::freelist(COMPLETION_FREELIST,
                                            OnReadReply::alloc);
#endif

int Objecter::read_sync(const char *object, const uint8_t volume[16],
                        uint64_t offset, uint64_t length, char *data,
			int flags)
{
  const int client = 0;
  const long tid = 0;
  object_t oid(object);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  SyncCompletion completion;
  int64_t pool_id(0);
  object_locator_t oloc(pool_id);
  pg_t pgid;

  memcpy(&vol, volume, sizeof(vol));

  if (!wait_for_active(&epoch))
    return -ENODEV;

  // set up osd read op
  MOSDOp *m =
    new MOSDOp(client, tid, oid, oloc, pgid, epoch, flags,0);

  m->set_snapid(CEPH_NOSNAP);

  vector<OSDOp> ops;
  ops.resize(1);
  ops[0].op.op = CEPH_OSD_OP_READ;
  ops[0].op.extent.offset = offset;
  ops[0].op.extent.length = length;
  ops[0].op.extent.truncate_size = 0;
  ops[0].op.extent.truncate_seq = 0;
  ops[0].op.flags = flags;
  m->ops=ops;

  // create reply callback
  OnReadReply onreply(data, length, SyncCompletion::callback,
		      &completion);

  // send request over direct messenger
  dispatcher->send_request(m, &onreply);

  return completion.wait();
}

int Objecter::read(const char *object, const uint8_t volume[16],
                   uint64_t offset, uint64_t length, char *data,
                   int flags, libosd_io_completion_fn cb, void *user)
{
  if (cb == nullptr)
    return read_sync(object, volume, offset, length, data, flags);

  const int client = 0;
  const long tid = 0;
  object_t oid(object);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  memcpy(&vol, volume, sizeof(vol));

  if (!wait_for_active(&epoch))
    return -ENODEV;

  // set up osd read op
  int64_t pool_id(0);
  object_locator_t oloc(pool_id);
  pg_t pgid;
  MOSDOp *m =
    new MOSDOp(client, tid, oid, oloc, pgid, epoch, flags,0);

  m->set_snapid(CEPH_NOSNAP);

  vector<OSDOp> ops;
  ops.resize(1);
  ops[0].op.op = CEPH_OSD_OP_READ;
  ops[0].op.extent.offset = offset;
  ops[0].op.extent.length = length;
  ops[0].op.extent.truncate_size = 0;
  ops[0].op.extent.truncate_seq = 0;
  ops[0].op.flags = flags;
  m->ops=ops;

  // create reply callback
  OnReadReply *onreply = new OnReadReply(data, length, cb, user);

  // send request over direct messenger
  dispatcher->send_request(m, onreply);
  return 0;
}

// Dispatcher callback to fire the write completion
class OnWriteReply : public Dispatcher::OnReply {
#if COMPLETION_FREELIST
  typedef cohort::CharArrayAlloc<OnWriteReply> Alloc;
  typedef cohort::FreeList<OnWriteReply, Alloc> FreeList;
  static Alloc alloc;
  static FreeList freelist;
#endif
  libosd_io_completion_fn cb;
  int flags;
  void *user;

public:
#if COMPLETION_FREELIST
  static void *operator new(size_t num_bytes) {
    return freelist.alloc();
  }
  void operator delete(void *p) {
    return freelist.free(static_cast<OnWriteReply*>(p));
  }
#endif
  OnWriteReply(libosd_io_completion_fn cb, int flags, void *user)
    : cb(cb), flags(flags), user(user) {}

  void on_reply(Message *reply) {
    assert(reply->get_type() == CEPH_MSG_OSD_OPREPLY);
    MOSDOpReply *m = static_cast<MOSDOpReply*>(reply);

    const int flag = m->is_ondisk() ? LIBOSD_WRITE_CB_STABLE :
      LIBOSD_WRITE_CB_UNSTABLE;

    vector<OSDOp> ops;
    m->claim_ops(ops);
    m->put();

    assert(ops.size() == 1);
    OSDOp &op = *ops.begin();
    assert(op.op.op == CEPH_OSD_OP_WRITE ||
	   op.op.op == CEPH_OSD_OP_TRUNCATE);

    uint64_t length = op.rval ? 0 : op.op.extent.length;
    cb(-op.rval, length, flag, user);

    // expecting another message for ondisk
    if ((flags & LIBOSD_WRITE_CB_STABLE) && !m->is_ondisk())
      return;
    // delete unless we're synchronous (on the stack)
    if (cb != SyncCompletion::callback)
      delete this;
  }
};
#if COMPLETION_FREELIST
OnWriteReply::Alloc OnWriteReply::alloc;
OnWriteReply::FreeList OnWriteReply::freelist(COMPLETION_FREELIST,
                                              OnWriteReply::alloc);
#endif

#define WRITE_CB_FLAGS \
  (LIBOSD_WRITE_CB_UNSTABLE | \
   LIBOSD_WRITE_CB_STABLE)

int Objecter::write_sync(const char *object, const uint8_t volume[16],
                         uint64_t offset, uint64_t length, char *data,
                         int flags)
{
  const int client = 0;
  const long tid = 0;
  object_t oid(object);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  SyncCompletion completion;
  mempcpy(&vol, volume, sizeof(vol));

  /* when synchronous, flags must specify exactly one of UNSTABLE
   * or STABLE */
  if ((flags & WRITE_CB_FLAGS) == 0 ||
      (flags & WRITE_CB_FLAGS) == WRITE_CB_FLAGS)
    return -EINVAL;

  if (!wait_for_active(&epoch))
    return -ENODEV;

  bufferlist bl;
  bl.append(ceph::buffer::create_static(length, data));

  int64_t pool_id(0);
  object_locator_t oloc(pool_id);
  pg_t pgid;

  // set up osd write op
  MOSDOp *m =
    new MOSDOp(client, tid, oid, oloc, pgid, epoch, flags,0);
  m->set_snapid(CEPH_NOSNAP);

  vector<OSDOp> ops;
  ops.resize(1);
  ops[0].op.op = CEPH_OSD_OP_WRITE;
  ops[0].op.extent.offset = offset;
  ops[0].op.extent.length = length;
  ops[0].op.extent.truncate_size = 0;
  ops[0].op.extent.truncate_seq = 0;
  ops[0].indata = bl;
  ops[0].op.flags = flags;
  m->ops=ops;
  
  if (flags & LIBOSD_WRITE_CB_UNSTABLE)
    m->set_want_ack(true);
  if (flags & LIBOSD_WRITE_CB_STABLE)
    m->set_want_ondisk(true);

  // create reply callback on the stack
  OnWriteReply onreply(SyncCompletion::callback, flags, &completion);

  // send request over direct messenger
  dispatcher->send_request(m, &onreply);

  return completion.wait();

}

int Objecter::write(const char *object, const uint8_t volume[16],
		    uint64_t offset, uint64_t length, char *data,
		    int flags, libosd_io_completion_fn cb, void *user)
{
  if (cb == nullptr)
    return write_sync(object, volume, offset, length, data, flags);

  const int client = 0;
  const long tid = 0;
  object_t oid(object);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  mempcpy(&vol, volume, sizeof(vol));
  int64_t pool_id(0);
  object_locator_t oloc(pool_id);
  pg_t pgid;

  // when asynchronous, flags must specify one or more of UNSTABLE or STABLE
  if ((flags & WRITE_CB_FLAGS) == 0)
    return -EINVAL;

  if (!wait_for_active(&epoch))
    return -ENODEV;

  bufferlist bl;
  bl.append(ceph::buffer::create_static(length, data));

  // set up osd write op
  MOSDOp *m =
    new MOSDOp(client, tid, oid, oloc, pgid, epoch, flags,0);
  m->set_snapid(CEPH_NOSNAP);
  vector<OSDOp> ops;
  ops.resize(1);
  ops[0].op.op = CEPH_OSD_OP_WRITE;
  ops[0].op.extent.offset = offset;
  ops[0].op.extent.length = length;
  ops[0].op.extent.truncate_size = 0;
  ops[0].op.extent.truncate_seq = 0;
  ops[0].indata = bl;
  ops[0].op.flags = flags;
  m->ops=ops;
  
  if (flags & LIBOSD_WRITE_CB_UNSTABLE)
    m->set_want_ack(true);
  if (flags & LIBOSD_WRITE_CB_STABLE)
    m->set_want_ondisk(true);

  // create reply callback
  OnWriteReply *onreply = new OnWriteReply(cb, flags, user);

  // send request over direct messenger
  dispatcher->send_request(m, onreply);
  return 0;
}

  int Objecter::truncate_sync(const char *object, const int64_t pool_id,
			      OSDMapRef &omap,uint64_t offset, int flags)
{
  const int client = 0;
  const long tid = 0;
  object_t oid(object);
  epoch_t epoch = 0;
  SyncCompletion completion;
  object_locator_t oloc(pool_id);
  pg_t pgid;

  omap->object_locator_to_pg(oid, oloc, pgid);

  // when synchronous, flags must specify exactly one of UNSTABLE or STABLE
  if ((flags & WRITE_CB_FLAGS) == 0 ||
      (flags & WRITE_CB_FLAGS) == WRITE_CB_FLAGS)
    return -EINVAL;

  if (!wait_for_active(&epoch))
    return -ENODEV;

  // set up osd truncate op
  MOSDOp *m =
    new MOSDOp(client, tid, oid, oloc, pgid, epoch, flags,0);
  vector<OSDOp> ops;
  ops.resize(1);
  ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
  ops[0].op.extent.offset = offset;
  ops[0].op.extent.truncate_size = 0;
  ops[0].op.extent.truncate_seq = 0;
  ops[0].op.flags = flags;
  m->ops=ops;

  if (flags & LIBOSD_WRITE_CB_UNSTABLE)
    m->set_want_ack(true);
  if (flags & LIBOSD_WRITE_CB_STABLE)
    m->set_want_ondisk(true);

  // create reply callback on the stack
  OnWriteReply onreply(SyncCompletion::callback, flags, &completion);

  // send request over direct messenger
  dispatcher->send_request(m, &onreply);

  return completion.wait();

}

  int Objecter::truncate(const char *object, const int64_t pool_id,
			 OSDMapRef& omap, uint64_t offset, int flags,
		       libosd_io_completion_fn cb, void *user)
{
  if (cb == nullptr)
    return truncate_sync(object, pool_id, omap, offset, flags);

  const int client = 0;
  const long tid = 0;
  object_t oid(object);
  epoch_t epoch = 0;
  object_locator_t oloc(pool_id);
  pg_t pgid;

  omap->object_locator_to_pg(oid, oloc, pgid);
  
  if ((flags & WRITE_CB_FLAGS) == 0) {
    // when asynchronous, flags must specify one or more of UNSTABLE or STABLE
    return -EINVAL;
  }

  if (!wait_for_active(&epoch))
    return -ENODEV;

  // set up osd truncate op

  MOSDOp *m =
    new MOSDOp(client, tid, oid, oloc, pgid, epoch, flags,0);
  vector<OSDOp> ops;
  ops.resize(1);
  ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
  ops[0].op.extent.offset = offset;
  ops[0].op.extent.truncate_size = 0;
  ops[0].op.extent.truncate_seq = 0;
  ops[0].op.flags = flags;
  m->ops=ops;
    
  if (flags & LIBOSD_WRITE_CB_UNSTABLE)
    m->set_want_ack(true);
  if (flags & LIBOSD_WRITE_CB_STABLE)
    m->set_want_ondisk(true);

  // create reply callback
  OnWriteReply *onreply = new OnWriteReply(cb, flags, user);

  // send request over direct messenger
  dispatcher->send_request(m, onreply);
  return 0;
}

} // namespace osd
} // namespace ceph
