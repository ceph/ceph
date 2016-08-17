// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_STATE_H
#define CEPH_LIBRBD_IMAGE_STATE_H

#include "include/int_types.h"
#include "common/Mutex.h"
#include <list>
#include <string>
#include <utility>

class Context;
class RWLock;

namespace librbd {

class ImageCtx;
class ImageUpdateWatchers;
class UpdateWatchCtx;

template <typename ImageCtxT = ImageCtx>
class ImageState {
public:
  ImageState(ImageCtxT *image_ctx);
  ~ImageState();

  int open();
  void open(Context *on_finish);

  int close();
  void close(Context *on_finish);

  void handle_update_notification();

  bool is_refresh_required() const;

  int refresh();
  int refresh_if_required();
  void refresh(Context *on_finish);
  void acquire_lock_refresh(Context *on_finish);

  void snap_set(const std::string &snap_name, Context *on_finish);

  void prepare_lock(Context *on_ready);
  void handle_prepare_lock_complete();

  int register_update_watcher(UpdateWatchCtx *watcher, uint64_t *handle);
  int unregister_update_watcher(uint64_t handle);
  void flush_update_watchers(Context *on_finish);
  void shut_down_update_watchers(Context *on_finish);

private:
  enum State {
    STATE_UNINITIALIZED,
    STATE_OPEN,
    STATE_CLOSED,
    STATE_OPENING,
    STATE_CLOSING,
    STATE_REFRESHING,
    STATE_SETTING_SNAP,
    STATE_PREPARING_LOCK
  };

  enum ActionType {
    ACTION_TYPE_OPEN,
    ACTION_TYPE_CLOSE,
    ACTION_TYPE_REFRESH,
    ACTION_TYPE_SET_SNAP,
    ACTION_TYPE_LOCK
  };

  struct Action {
    ActionType action_type;
    uint64_t refresh_seq = 0;
    bool refresh_acquiring_lock = false;
    std::string snap_name;
    Context *on_ready = nullptr;

    Action(ActionType action_type) : action_type(action_type) {
    }
    inline bool operator==(const Action &action) const {
      if (action_type != action.action_type) {
        return false;
      }
      switch (action_type) {
      case ACTION_TYPE_REFRESH:
        return (refresh_seq == action.refresh_seq &&
                refresh_acquiring_lock == action.refresh_acquiring_lock);
      case ACTION_TYPE_SET_SNAP:
        return snap_name == action.snap_name;
      case ACTION_TYPE_LOCK:
        return false;
      default:
        return true;
      }
    }
  };

  typedef std::list<Context *> Contexts;
  typedef std::pair<Action, Contexts> ActionContexts;
  typedef std::list<ActionContexts> ActionsContexts;

  ImageCtxT *m_image_ctx;
  State m_state;

  mutable Mutex m_lock;
  ActionsContexts m_actions_contexts;

  uint64_t m_last_refresh;
  uint64_t m_refresh_seq;

  ImageUpdateWatchers *m_update_watchers;

  bool is_transition_state() const;
  bool is_closed() const;

  void refresh(bool acquiring_lock, Context *on_finish);

  void append_context(const Action &action, Context *context);
  void execute_next_action_unlock();
  void execute_action_unlock(const Action &action, Context *context);
  void complete_action_unlock(State next_state, int r);

  void send_open_unlock();
  void handle_open(int r);

  void send_close_unlock();
  void handle_close(int r);

  void send_refresh_unlock();
  void handle_refresh(int r);

  void send_set_snap_unlock();
  void handle_set_snap(int r);

  void send_prepare_lock_unlock();

};

} // namespace librbd

extern template class librbd::ImageState<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_STATE_H
