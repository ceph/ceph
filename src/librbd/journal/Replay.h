// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_REPLAY_H
#define CEPH_LIBRBD_JOURNAL_REPLAY_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "common/ceph_mutex.h"
#include "librbd/io/Types.h"
#include "librbd/journal/Types.h"
#include <boost/variant.hpp>
#include <list>
#include <unordered_set>
#include <unordered_map>

namespace librbd {

class ImageCtx;
namespace io { struct AioCompletion; }

namespace journal {

template <typename ImageCtxT = ImageCtx>
class Replay {
public:
  static Replay *create(ImageCtxT &image_ctx) {
    return new Replay(image_ctx);
  }

  Replay(ImageCtxT &image_ctx);
  ~Replay();

  int decode(bufferlist::const_iterator *it, EventEntry *event_entry);
  void process(const EventEntry &event_entry,
               Context *on_ready, Context *on_safe);

  void shut_down(bool cancel_ops, Context *on_finish);
  void flush(Context *on_finish);

  void replay_op_ready(uint64_t op_tid, Context *on_resume);

private:
  typedef std::unordered_set<int> ReturnValues;

  struct OpEvent {
    bool op_in_progress = false;
    bool finish_on_ready = false;
    Context *on_op_finish_event = nullptr;
    Context *on_start_ready = nullptr;
    Context *on_start_safe = nullptr;
    Context *on_finish_ready = nullptr;
    Context *on_finish_safe = nullptr;
    Context *on_op_complete = nullptr;
    ReturnValues op_finish_error_codes;
    ReturnValues ignore_error_codes;
  };

  typedef std::list<uint64_t> OpTids;
  typedef std::list<Context *> Contexts;
  typedef std::unordered_set<Context *> ContextSet;
  typedef std::unordered_map<uint64_t, OpEvent> OpEvents;

  struct C_OpOnComplete : public Context {
    Replay *replay;
    uint64_t op_tid;
    C_OpOnComplete(Replay *replay, uint64_t op_tid)
      : replay(replay), op_tid(op_tid) {
    }
    void finish(int r) override {
      replay->handle_op_complete(op_tid, r);
    }
  };

  struct C_AioModifyComplete : public Context {
    Replay *replay;
    Context *on_ready;
    Context *on_safe;
    std::set<int> filters;
    C_AioModifyComplete(Replay *replay, Context *on_ready,
                        Context *on_safe, std::set<int> &&filters)
      : replay(replay), on_ready(on_ready), on_safe(on_safe),
        filters(std::move(filters)) {
    }
    void finish(int r) override {
      replay->handle_aio_modify_complete(on_ready, on_safe, r, filters);
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
    void finish(int r) override {
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

  ceph::mutex m_lock = ceph::make_mutex("Replay<I>::m_lock");

  uint64_t m_in_flight_aio_flush = 0;
  uint64_t m_in_flight_aio_modify = 0;
  Contexts m_aio_modify_unsafe_contexts;
  ContextSet m_aio_modify_safe_contexts;

  OpEvents m_op_events;
  uint64_t m_in_flight_op_events = 0;

  bool m_shut_down = false;
  Context *m_flush_ctx = nullptr;
  Context *m_on_aio_ready = nullptr;

  void handle_event(const AioDiscardEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const AioWriteEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const AioWriteSameEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const AioCompareAndWriteEvent &event, Context *on_ready,
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
  void handle_event(const DemotePromoteEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const SnapLimitEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const UpdateFeaturesEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const MetadataSetEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const MetadataRemoveEvent &event, Context *on_ready,
                    Context *on_safe);
  void handle_event(const UnknownEvent &event, Context *on_ready,
                    Context *on_safe);

  void handle_aio_modify_complete(Context *on_ready, Context *on_safe,
                                  int r, std::set<int> &filters);
  void handle_aio_flush_complete(Context *on_flush_safe, Contexts &on_safe_ctxs,
                                 int r);

  Context *create_op_context_callback(uint64_t op_tid, Context *on_ready,
                                      Context *on_safe, OpEvent **op_event);
  void handle_op_complete(uint64_t op_tid, int r);

  io::AioCompletion *create_aio_modify_completion(Context *on_ready,
                                                  Context *on_safe,
                                                  io::aio_type_t aio_type,
                                                  bool *flush_required,
                                                  std::set<int> &&filters);
  io::AioCompletion *create_aio_flush_completion(Context *on_safe);
  void handle_aio_completion(io::AioCompletion *aio_comp);

  bool clipped_io(uint64_t image_offset, io::AioCompletion *aio_comp);

};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::Replay<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_REPLAY_H
