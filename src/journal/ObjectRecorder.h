// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_OBJECT_RECORDER_H
#define CEPH_JOURNAL_OBJECT_RECORDER_H

#include "include/utime.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "journal/FutureImpl.h"
#include <list>
#include <map>
#include <set>
#include <boost/noncopyable.hpp>
#include "include/ceph_assert.h"

namespace journal {

class ObjectRecorder;

typedef std::pair<ceph::ref_t<FutureImpl>, bufferlist> AppendBuffer;
typedef std::list<AppendBuffer> AppendBuffers;

class ObjectRecorder : public RefCountedObject, boost::noncopyable {
public:
  struct Handler {
    virtual ~Handler() {
    }
    virtual void closed(ObjectRecorder *object_recorder) = 0;
    virtual void overflow(ObjectRecorder *object_recorder) = 0;
  };

  void set_append_batch_options(int flush_interval, uint64_t flush_bytes,
                                double flush_age);

  inline uint64_t get_object_number() const {
    return m_object_number;
  }
  inline const std::string &get_oid() const {
    return m_oid;
  }

  bool append(AppendBuffers &&append_buffers);
  void flush(Context *on_safe);
  void flush(const ceph::ref_t<FutureImpl> &future);

  void claim_append_buffers(AppendBuffers *append_buffers);

  bool is_closed() const {
    ceph_assert(ceph_mutex_is_locked(*m_lock));
    return (m_object_closed && m_in_flight_appends.empty());
  }
  bool close();

  inline CephContext *cct() const {
    return m_cct;
  }

  inline size_t get_pending_appends() const {
    std::lock_guard locker{*m_lock};
    return m_pending_buffers.size();
  }

private:
  FRIEND_MAKE_REF(ObjectRecorder);
  ObjectRecorder(librados::IoCtx &ioctx, std::string_view oid,
                 uint64_t object_number, ceph::mutex* lock,
                 ContextWQ *work_queue, Handler *handler, uint8_t order,
                 int32_t max_in_flight_appends);
  ~ObjectRecorder() override;

  typedef std::set<uint64_t> InFlightTids;
  typedef std::map<uint64_t, AppendBuffers> InFlightAppends;

  struct FlushHandler : public FutureImpl::FlushHandler {
    ceph::ref_t<ObjectRecorder> object_recorder;
    virtual void flush(const ceph::ref_t<FutureImpl> &future) override {
      object_recorder->flush(future);
    }
    FlushHandler(ceph::ref_t<ObjectRecorder> o) : object_recorder(std::move(o)) {}
  };
  struct C_AppendFlush : public Context {
    ceph::ref_t<ObjectRecorder> object_recorder;
    uint64_t tid;
    C_AppendFlush(ceph::ref_t<ObjectRecorder> o, uint64_t _tid)
        : object_recorder(std::move(o)), tid(_tid) {
    }
    void finish(int r) override {
      object_recorder->handle_append_flushed(tid, r);
    }
  };

  librados::IoCtx m_ioctx;
  std::string m_oid;
  uint64_t m_object_number;
  CephContext *m_cct = nullptr;

  ContextWQ *m_op_work_queue;

  Handler *m_handler;

  uint8_t m_order;
  uint64_t m_soft_max_size;

  uint32_t m_flush_interval = 0;
  uint64_t m_flush_bytes = 0;
  double m_flush_age = 0;
  int32_t m_max_in_flight_appends;

  bool m_compat_mode;

  /* So that ObjectRecorder::FlushHandler doesn't create a circular reference: */
  std::weak_ptr<FlushHandler> m_flush_handler;
  auto get_flush_handler() {
    auto h = m_flush_handler.lock();
    if (!h) {
      h = std::make_shared<FlushHandler>(this);
      m_flush_handler = h;
    }
    return h;
  }

  mutable ceph::mutex* m_lock;
  AppendBuffers m_pending_buffers;
  uint64_t m_pending_bytes = 0;
  utime_t m_last_flush_time;

  uint64_t m_append_tid = 0;

  InFlightTids m_in_flight_tids;
  InFlightAppends m_in_flight_appends;
  uint64_t m_object_bytes = 0;

  bool m_overflowed = false;

  bool m_object_closed = false;
  bool m_object_closed_notify = false;

  bufferlist m_prefetch_bl;

  uint32_t m_in_flight_callbacks = 0;
  ceph::condition_variable m_in_flight_callbacks_cond;
  uint64_t m_in_flight_bytes = 0;

  bool send_appends(bool force, ceph::ref_t<FutureImpl> flush_sentinel);
  void handle_append_flushed(uint64_t tid, int r);
  void append_overflowed();

  void wake_up_flushes();
  void notify_handler_unlock(std::unique_lock<ceph::mutex>& locker,
                             bool notify_overflowed);
};

} // namespace journal

#endif // CEPH_JOURNAL_OBJECT_RECORDER_H
