// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_OBJECT_RECORDER_H
#define CEPH_JOURNAL_OBJECT_RECORDER_H

#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "journal/FutureImpl.h"
#include <list>
#include <map>
#include <set>
#include <boost/noncopyable.hpp>
#include "include/ceph_assert.h"

class SafeTimer;

namespace journal {

class ObjectRecorder;

typedef std::pair<FutureImpl::ref, bufferlist> AppendBuffer;
typedef std::list<AppendBuffer> AppendBuffers;

class ObjectRecorder : public RefCountedObjectInstance<ObjectRecorder>, boost::noncopyable {
public:
  struct Handler {
    virtual ~Handler() {
    }
    virtual void closed(ObjectRecorder *object_recorder) = 0;
    virtual void overflow(ObjectRecorder *object_recorder) = 0;
  };

  inline uint64_t get_object_number() const {
    return m_object_number;
  }
  inline const std::string &get_oid() const {
    return m_oid;
  }

  bool append_unlock(AppendBuffers &&append_buffers);
  void flush(Context *on_safe);
  void flush(const FutureImpl::ref &future);

  void claim_append_buffers(AppendBuffers *append_buffers);

  bool is_closed() const {
    ceph_assert(m_lock->is_locked());
    return (m_object_closed && m_in_flight_appends.empty());
  }
  bool close();

  inline CephContext *cct() const {
    return m_cct;
  }

  inline size_t get_pending_appends() const {
    Mutex::Locker locker(*m_lock);
    return m_append_buffers.size();
  }

private:
  friend factory;
  ObjectRecorder(librados::IoCtx &ioctx, const std::string &oid,
                 uint64_t object_number, std::shared_ptr<Mutex> lock,
                 ContextWQ *work_queue, SafeTimer &timer, Mutex &timer_lock,
                 Handler *handler, uint8_t order, uint32_t flush_interval,
                 uint64_t flush_bytes, double flush_age,
                 uint64_t max_in_flight_appends);
  ~ObjectRecorder() override;

  typedef std::set<uint64_t> InFlightTids;
  typedef std::map<uint64_t, AppendBuffers> InFlightAppends;

  struct FlushHandler : public FutureImpl::FlushHandler {
    ObjectRecorder::ref object_recorder;
    virtual void flush(const FutureImpl::ref &future) override {
      Mutex::Locker locker(*(object_recorder->m_lock));
      object_recorder->flush(future);
    }
    FlushHandler(ObjectRecorder::ref o) : object_recorder(std::move(o)) {}
  };
  struct C_AppendFlush : public Context {
    ObjectRecorder::ref object_recorder;
    uint64_t tid;
    C_AppendFlush(ObjectRecorder::ref o, uint64_t _tid)
        : object_recorder(std::move(o)), tid(_tid) {
    }
    void finish(int r) override {
      object_recorder->handle_append_flushed(tid, r);
    }
  };

  librados::IoCtx m_ioctx;
  std::string m_oid;
  uint64_t m_object_number;
  CephContext *m_cct;

  ContextWQ *m_op_work_queue;

  SafeTimer &m_timer;
  Mutex &m_timer_lock;

  Handler *m_handler;

  uint8_t m_order;
  uint64_t m_soft_max_size;

  uint32_t m_flush_interval;
  uint64_t m_flush_bytes;
  double m_flush_age;
  uint32_t m_max_in_flight_appends;

  std::weak_ptr<FlushHandler> m_flush_handler;

  Context *m_append_task = nullptr;

  mutable std::shared_ptr<Mutex> m_lock;
  AppendBuffers m_append_buffers;
  uint64_t m_append_tid;
  uint32_t m_pending_bytes;

  InFlightTids m_in_flight_tids;
  InFlightAppends m_in_flight_appends;
  uint64_t m_size;
  bool m_overflowed;
  bool m_object_closed;

  bufferlist m_prefetch_bl;

  bool m_in_flight_flushes;
  Cond m_in_flight_flushes_cond;

  AppendBuffers m_pending_buffers;
  uint64_t m_aio_sent_size = 0;
  bool m_aio_scheduled;

  void handle_append_task();
  void cancel_append_task();
  void schedule_append_task();

  bool append(const AppendBuffer &append_buffer, bool *schedule_append);
  bool flush_appends(bool force);
  void handle_append_flushed(uint64_t tid, int r);
  void append_overflowed();
  void send_appends(AppendBuffers *append_buffers);
  void send_appends_aio();

  void notify_handler_unlock();
};

} // namespace journal

#endif // CEPH_JOURNAL_OBJECT_RECORDER_H
