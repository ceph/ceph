// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_REPLAY_H
#define CEPH_LIBRBD_JOURNAL_REPLAY_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "librbd/JournalTypes.h"
#include <boost/variant.hpp>
#include <map>

namespace librbd {

class AioCompletion;
class ImageCtx;

class JournalReplay {
public:
  JournalReplay(ImageCtx &image_ctx);
  ~JournalReplay();

  int process(bufferlist::iterator it, Context *on_safe = NULL);
  int flush();

private:
  typedef std::map<AioCompletion*,Context*> AioCompletions;

  struct EventVisitor : public boost::static_visitor<void> {
    JournalReplay *journal_replay;
    Context *on_safe;

    EventVisitor(JournalReplay *_journal_replay, Context *_on_safe)
      : journal_replay(_journal_replay), on_safe(_on_safe) {
    }

    template <typename Event>
    inline void operator()(const Event &event) const {
      journal_replay->handle_event(event, on_safe);
    }
  };

  ImageCtx &m_image_ctx;

  Mutex m_lock;
  Cond m_cond;

  AioCompletions m_aio_completions;
  int m_ret_val;

  void handle_event(const journal::AioDiscardEvent &event, Context *on_safe);
  void handle_event(const journal::AioWriteEvent &event, Context *on_safe);
  void handle_event(const journal::AioFlushEvent &event, Context *on_safe);
  void handle_event(const journal::OpFinishEvent &event, Context *on_safe);
  void handle_event(const journal::SnapCreateEvent &event, Context *on_safe);
  void handle_event(const journal::SnapRemoveEvent &event, Context *on_safe);
  void handle_event(const journal::SnapRenameEvent &event, Context *on_safe);
  void handle_event(const journal::SnapProtectEvent &event, Context *on_safe);
  void handle_event(const journal::SnapUnprotectEvent &event, Context *on_safe);
  void handle_event(const journal::SnapRollbackEvent &event, Context *on_safe);
  void handle_event(const journal::RenameEvent &event, Context *on_safe);
  void handle_event(const journal::ResizeEvent &event, Context *on_safe);
  void handle_event(const journal::FlattenEvent &event, Context *on_safe);
  void handle_event(const journal::UnknownEvent &event, Context *on_safe);

  AioCompletion *create_aio_completion(Context *on_safe);
  void handle_aio_completion(AioCompletion *aio_comp);

  static void aio_completion_callback(completion_t cb, void *arg);
};

} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_REPLAY_H
