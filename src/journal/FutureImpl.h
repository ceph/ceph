// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_FUTURE_IMPL_H
#define CEPH_JOURNAL_FUTURE_IMPL_H

#include "include/int_types.h"
#include "common/RefCountedObj.h"
#include "include/Context.h"
#include "journal/Future.h"
#include <list>
#include <map>
#include <boost/noncopyable.hpp>
#include "include/ceph_assert.h"

class Context;

namespace journal {

class FutureImpl : public RefCountedObject, boost::noncopyable {
public:
  struct FlushHandler {
    using ref = std::shared_ptr<FlushHandler>;
    virtual void flush(const ceph::ref_t<FutureImpl> &future) = 0;
    virtual ~FlushHandler() = default;
  };

  void init(const ceph::ref_t<FutureImpl> &prev_future);

  inline uint64_t get_tag_tid() const {
    return m_tag_tid;
  }
  inline uint64_t get_entry_tid() const {
    return m_entry_tid;
  }
  inline uint64_t get_commit_tid() const {
    return m_commit_tid;
  }

  void flush(Context *on_safe = NULL);
  void wait(Context *on_safe);

  bool is_complete() const;
  int get_return_value() const;

  inline bool is_flush_in_progress() const {
    std::lock_guard locker{m_lock};
    return (m_flush_state == FLUSH_STATE_IN_PROGRESS);
  }
  inline void set_flush_in_progress() {
    auto h = std::move(m_flush_handler);
    ceph_assert(h);
    std::lock_guard locker{m_lock};
    m_flush_state = FLUSH_STATE_IN_PROGRESS;
  }

  bool attach(FlushHandler::ref flush_handler);
  inline void detach() {
    m_flush_handler.reset();
  }
  inline FlushHandler::ref get_flush_handler() const {
    return m_flush_handler;
  }

  void safe(int r);

private:
  friend std::ostream &operator<<(std::ostream &, const FutureImpl &);

  typedef std::map<FlushHandler::ref, ceph::ref_t<FutureImpl>> FlushHandlers;
  typedef std::list<Context *> Contexts;

  enum FlushState {
    FLUSH_STATE_NONE,
    FLUSH_STATE_REQUESTED,
    FLUSH_STATE_IN_PROGRESS
  };

  struct C_ConsistentAck : public Context {
    ceph::ref_t<FutureImpl> future;
    C_ConsistentAck(ceph::ref_t<FutureImpl> _future) : future(std::move(_future)) {}
    void complete(int r) override {
      future->consistent(r);
      future.reset();
    }
    void finish(int r) override {}
  };

  FRIEND_MAKE_REF(FutureImpl);
  FutureImpl(uint64_t tag_tid, uint64_t entry_tid, uint64_t commit_tid);
  ~FutureImpl() override = default;

  uint64_t m_tag_tid;
  uint64_t m_entry_tid;
  uint64_t m_commit_tid;

  mutable ceph::mutex m_lock = ceph::make_mutex("FutureImpl::m_lock", false);
  ceph::ref_t<FutureImpl> m_prev_future;
  bool m_safe = false;
  bool m_consistent = false;
  int m_return_value = 0;

  FlushHandler::ref m_flush_handler;
  FlushState m_flush_state = FLUSH_STATE_NONE;

  C_ConsistentAck m_consistent_ack;
  Contexts m_contexts;

  ceph::ref_t<FutureImpl> prepare_flush(FlushHandlers *flush_handlers);
  ceph::ref_t<FutureImpl> prepare_flush(FlushHandlers *flush_handlers, ceph::mutex &lock);

  void consistent(int r);
  void finish_unlock();
};

std::ostream &operator<<(std::ostream &os, const FutureImpl &future);

} // namespace journal

using journal::operator<<;

#endif // CEPH_JOURNAL_FUTURE_IMPL_H
