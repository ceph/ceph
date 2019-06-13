// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_OBJECT_RECORDER_H
#define CEPH_JOURNAL_OBJECT_RECORDER_H

#include "include/utime.h"
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
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include "include/ceph_assert.h"

class SafeTimer;

namespace journal {

class ObjectRecorder;
typedef boost::intrusive_ptr<ObjectRecorder> ObjectRecorderPtr;

typedef std::pair<FutureImplPtr, bufferlist> AppendBuffer;
typedef std::list<AppendBuffer> AppendBuffers;

class ObjectRecorder : public RefCountedObject, boost::noncopyable {
public:
  struct Handler {
    virtual ~Handler() {
    }
    virtual void closed(ObjectRecorder *object_recorder) = 0;
    virtual void overflow(ObjectRecorder *object_recorder) = 0;
  };

  ObjectRecorder(librados::IoCtx &ioctx, const std::string &oid,
                 uint64_t object_number, std::shared_ptr<Mutex> lock,
                 ContextWQ *work_queue, Handler *handler, uint8_t order,
                 int32_t max_in_flight_appends);
  ~ObjectRecorder() override;

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
  void flush(const FutureImplPtr &future);

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
    return m_pending_buffers.size();
  }

private:
  typedef std::set<uint64_t> InFlightTids;
  typedef std::map<uint64_t, AppendBuffers> InFlightAppends;

  struct FlushHandler : public FutureImpl::FlushHandler {
    ObjectRecorder *object_recorder;
    FlushHandler(ObjectRecorder *o) : object_recorder(o) {}
    void get() override {
      object_recorder->get();
    }
    void put() override {
      object_recorder->put();
    }
    void flush(const FutureImplPtr &future) override {
      object_recorder->flush(future);
    }
  };
  struct C_AppendFlush : public Context {
    ObjectRecorder *object_recorder;
    uint64_t tid;
    C_AppendFlush(ObjectRecorder *o, uint64_t _tid)
        : object_recorder(o), tid(_tid) {
      object_recorder->get();
    }
    void finish(int r) override {
      object_recorder->handle_append_flushed(tid, r);
      object_recorder->put();
    }
  };

  librados::IoCtx m_ioctx;
  std::string m_oid;
  uint64_t m_object_number;
  CephContext *m_cct;

  ContextWQ *m_op_work_queue;

  Handler *m_handler;

  uint8_t m_order;
  uint64_t m_soft_max_size;

  uint32_t m_flush_interval = 0;
  uint64_t m_flush_bytes = 0;
  double m_flush_age = 0;
  int32_t m_max_in_flight_appends;

  FlushHandler m_flush_handler;

  mutable std::shared_ptr<Mutex> m_lock;
  AppendBuffers m_pending_buffers;
  uint64_t m_pending_bytes = 0;
  utime_t m_last_flush_time;

  uint64_t m_append_tid;

  InFlightTids m_in_flight_tids;
  InFlightAppends m_in_flight_appends;
  uint64_t m_object_bytes = 0;
  bool m_overflowed;
  bool m_object_closed;

  bufferlist m_prefetch_bl;

  bool m_in_flight_flushes;
  Cond m_in_flight_flushes_cond;
  uint64_t m_in_flight_bytes = 0;

  bool send_appends(bool force, FutureImplPtr flush_sentinal);
  void handle_append_flushed(uint64_t tid, int r);
  void append_overflowed();

  void notify_handler_unlock();
};

} // namespace journal

#endif // CEPH_JOURNAL_OBJECT_RECORDER_H
