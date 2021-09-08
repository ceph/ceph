// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_REMOTE_POOL_POLLER_H
#define CEPH_RBD_MIRROR_REMOTE_POOL_POLLER_H

#include "include/rados/librados.hpp"
#include "tools/rbd_mirror/Types.h"
#include <string>

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;

namespace remote_pool_poller {

struct Listener {
  virtual ~Listener() {}

  virtual void handle_updated(const RemotePoolMeta& remote_pool_meta,
                              const PeerSpec& peer_spec) = 0;
};

}; // namespace remote_pool_poller

template <typename ImageCtxT>
class RemotePoolPoller {
public:
  static RemotePoolPoller* create(
      Threads<ImageCtxT>* threads,
      librados::IoCtx& remote_io_ctx,
      const std::string& site_name,
      const std::string& local_mirror_uuid,
      remote_pool_poller::Listener& listener,
      const PeerSpec& peer_spec) {
    return new RemotePoolPoller(threads, remote_io_ctx, site_name,
                                local_mirror_uuid, listener, peer_spec);
  }

  RemotePoolPoller(
      Threads<ImageCtxT>* threads,
      librados::IoCtx& remote_io_ctx,
      const std::string& site_name,
      const std::string& local_mirror_uuid,
      remote_pool_poller::Listener& listener,
      const PeerSpec& peer_spec)
    : m_threads(threads),
      m_remote_io_ctx(remote_io_ctx),
      m_site_name(site_name),
      m_local_mirror_uuid(local_mirror_uuid),
      m_listener(listener),
      m_peer_spec(peer_spec) {
  }
  ~RemotePoolPoller();

  void init(Context* on_finish);
  void shut_down(Context* on_finish);

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |/----------------------------\
   *    |                             |
   *    v                             |
   * MIRROR_UUID_GET                  |
   *    |                             |
   *    v                             |
   * MIRROR_PEER_PING                 |
   *    |                             |
   *    v                             |
   * MIRROR_PEER_LIST                 |
   *    |                             |
   *    v                             |
   * MIRROR_UUID_GET                  |
   *    |                             |
   *    v (skip if no changes)        |
   * NOTIFY_LISTENER                  |
   *    |                             |
   *    |   (repeat periodically)     |
   *    |\----------------------------/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  enum State {
    STATE_INITIALIZING,
    STATE_POLLING,
    STATE_SHUTTING_DOWN
  };

  Threads<ImageCtxT>* m_threads;
  librados::IoCtx& m_remote_io_ctx;
  std::string m_site_name;
  std::string m_local_mirror_uuid;
  remote_pool_poller::Listener& m_listener;
  PeerSpec m_peer_spec;

  bufferlist m_out_bl;

  RemotePoolMeta m_remote_pool_meta;
  bool m_updated = false;

  State m_state = STATE_INITIALIZING;
  Context* m_timer_task = nullptr;
  Context* m_on_finish = nullptr;

  void get_mirror_uuid();
  void handle_get_mirror_uuid(int r);

  void mirror_peer_ping();
  void handle_mirror_peer_ping(int r);

  void mirror_peer_list();
  void handle_mirror_peer_list(int r);

  void notify_listener();

  void schedule_task(int r);
  void handle_task();

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::RemotePoolPoller<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_REMOTE_POOL_POLLER_H
