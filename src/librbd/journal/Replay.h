// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_REPLAY_H
#define CEPH_LIBRBD_JOURNAL_REPLAY_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "include/unordered_set.h"
#include "include/unordered_map.h"
#include "include/rbd/librbd.hpp"
#include "common/Mutex.h"
#include "librbd/journal/Entries.h"
#include <boost/variant.hpp>
#include <list>

namespace librbd {

class AioCompletion;
class ImageCtx;

namespace journal {

template <typename ImageCtxT = ImageCtx>
class Replay {
public:
  static Replay *create(ImageCtxT &image_ctx) {
    return new Replay(image_ctx);
  }

  Replay(ImageCtxT &image_ctx);
  ~Replay();

  void process(bufferlist::iterator *it, Context *on_ready, Context *on_safe);
  void flush(Context *on_finish);

private:
  typedef std::list<Context *> Contexts;
  typedef ceph::unordered_set<Context *> ContextSet;
  typedef ceph::unordered_map<Context *, Context *> OpContexts;

  struct C_OpOnFinish : public Context {
    Replay *replay;
    C_OpOnFinish(Replay *replay) : replay(replay) {
    }
    virtual void finish(int r) override {
      replay->handle_op_context_callback(this, r);
    }
  };

  struct C_AioModifyComplete : public Context {
    Replay *replay;
    Context *on_safe;
    C_AioModifyComplete(Replay *replay, Context *on_safe)
      : replay(replay), on_safe(on_safe) {
    }
    virtual void finish(int r) {
      replay->handle_aio_modify_complete(on_safe, r);
    }
  };

  struct C_AioFlushComplete : public Context {
    Replay *replay;
    Context *on_flush_safe;
    Contexts on_safe_ctxs;
    C_AioFlushComplete(Replay *replay, Context *on_flush_safe,
                       Contexts &&on_safe_ctxs)
      : replay(replay), on_flush_safe(on_flush_safe),
        on_safe_ctxs(on_safe_ctxs) {
    }
    virtual void finish(int r) {
      replay->handle_aio_flush_complete(on_flush_safe, on_safe_ctxs, r);
    }
  };

  struct EventVisitor : public boost::static_visitor<void> {
    Replay *replay;
    Context *on_ready;
    Context *on_safe;

    EventVisitor(Replay *_replay, Context *_on_ready, Context *_on_safe)
      : replay(_replay), on_ready(_on_ready), on_safe(_on_safe) {
    }

    template <typename Event>
    inline void operator()(const Event &event) const {
      replay->handle_event(event, on_ready, on_safe);
    }
  };

  ImageCtxT &m_image_ctx;

  Mutex m_lock;

  uint64_t m_in_flight_aio = 0;
  Contexts m_aio_modify_unsafe_contexts;
  ContextSet m_aio_modify_safe_contexts;

  OpContexts m_op_contexts;

  Context *m_flush_ctx = nullptr;
  Context *m_on_aio_ready = nullptr;

  void handle_event(const AioDiscardEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const AioWriteEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const AioFlushEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const OpFinishEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const SnapCreateEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const SnapRemoveEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const SnapRenameEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const SnapProtectEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const SnapUnprotectEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const SnapRollbackEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const RenameEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const ResizeEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const FlattenEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const UnknownEvent &event, Context *on_ready,
                    Context *on_safe);

  void flush_aio();
  void handle_aio_modify_complete(Context *on_safe, int r);
  void handle_aio_flush_complete(Context *on_flush_safe, Contexts &on_safe_ctxs,
                                 int r);

  Context *create_op_context_callback(Context *on_safe);
  void handle_op_context_callback(Context *on_op_finish, int r);

  AioCompletion *create_aio_modify_completion(Context *on_ready,
                                              Context *on_safe,
                                              bool *flush_required);
  AioCompletion *create_aio_flush_completion(Context *on_ready,
                                             Context *on_safe);
  void handle_aio_completion(AioCompletion *aio_comp);

};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::Replay<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_REPLAY_H
