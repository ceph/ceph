// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_REPLAY_H
#define CEPH_LIBRBD_JOURNAL_REPLAY_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "librbd/JournalTypes.h"
#include <boost/variant.hpp>
#include <set>

namespace librbd {

class AioCompletion;
class ImageCtx;

class JournalReplay {
public:
  JournalReplay(ImageCtx &image_ctx);
  ~JournalReplay();

  int process(bufferlist::iterator it);
  int flush();

private:
  typedef std::set<AioCompletion *> AioCompletions;

  struct EventVisitor : public boost::static_visitor<void> {
    JournalReplay *journal_replay;

    EventVisitor(JournalReplay *_journal_replay)
      : journal_replay(_journal_replay) {
    }

    template <typename Event>
    inline void operator()(const Event &event) const {
      journal_replay->handle_event(event);
    }
  };

  ImageCtx &m_image_ctx;

  Mutex m_lock;
  Cond m_cond;

  AioCompletions m_aio_completions;
  int m_ret_val;

  void handle_event(const journal::AioDiscardEvent &event);
  void handle_event(const journal::AioWriteEvent &event);
  void handle_event(const journal::AioFlushEvent &event);
  void handle_event(const journal::OpFinishEvent &event);
  void handle_event(const journal::SnapCreateEvent &event);
  void handle_event(const journal::SnapRemoveEvent &event);
  void handle_event(const journal::SnapProtectEvent &event);
  void handle_event(const journal::SnapUnprotectEvent &event);
  void handle_event(const journal::SnapRollbackEvent &event);
  void handle_event(const journal::RenameEvent &event);
  void handle_event(const journal::ResizeEvent &event);
  void handle_event(const journal::FlattenEvent &event);
  void handle_event(const journal::UnknownEvent &event);

  AioCompletion *create_aio_completion();
  void handle_aio_completion(AioCompletion *aio_comp);

  static void aio_completion_callback(completion_t cb, void *arg);
};

} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_REPLAY_H
