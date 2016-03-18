// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/range/adaptor/map.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "Mirror.h"
#include "Threads.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: Mirror::" << __func__ << ": "

using std::chrono::seconds;
using std::list;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;
using librbd::mirror_peer_t;

namespace rbd {
namespace mirror {

void Mirror::C_ShutdownReplayer::finish(int r) {
  Mutex::Locker l(*m_replayers_lock);
  auto it = m_replayers->find(peer);
  if (it != m_replayers->end()) {
    dout(20) << "removing replayer for " << peer << dendl;
    m_replayers->erase(it);
  }
}

Mirror::Mirror(CephContext *cct) :
  m_cct(cct),
  m_lock("rbd::mirror::Mirror"),
  m_local(new librados::Rados()),
  m_replayers_lock("rbd::mirror::Mirror::replayers_map")
{
  cct->lookup_or_create_singleton_object<Threads>(m_threads,
                                                  "rbd_mirror::threads");
}

void Mirror::handle_signal(int signum)
{
  m_stopping.set(1);
}

int Mirror::init()
{
  int r = m_local->init_with_context(m_cct);
  if (r < 0) {
    derr << "could not initialize rados handle" << dendl;
    return r;
  }

  r = m_local->connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    return r;
  }

  // TODO: make interval configurable
  m_local_cluster_watcher.reset(new ClusterWatcher(m_local, m_lock));

  return r;
}

void Mirror::run()
{
  dout(20) << "enter" << dendl;
  while (!m_stopping.read()) {
    m_local_cluster_watcher->refresh_pools();
    Mutex::Locker l(m_lock);
    update_replayers(m_local_cluster_watcher->get_peer_configs());
    // TODO: make interval configurable
    m_cond.WaitInterval(g_ceph_context, m_lock, seconds(30));
  }
  dout(20) << "return" << dendl;
}

void Mirror::update_replayers(const map<peer_t, set<int64_t> > &peer_configs)
{
  dout(20) << "enter" << dendl;
  assert(m_lock.is_locked());
  for (auto &kv : peer_configs) {
    const peer_t &peer = kv.first;

    m_replayers_lock.Lock();
    auto it = m_replayers.find(peer);
    if (it == m_replayers.end()) {
      dout(20) << "starting replayer for " << peer << dendl;

      m_replayers.insert(std::make_pair(peer,
              unique_ptr<Replayer>(new Replayer(m_threads, m_local, peer))));
      m_replayers_lock.Unlock();

      FunctionContext *ctx = new FunctionContext(
          [this, peer](int r) {
            if (r < 0) {
              derr << "failed to initialize replayer for " << peer << dendl;
              m_replayers_lock.Lock();
              auto it = m_replayers.find(peer);
              assert(it != m_replayers.end());
              m_replayers.erase(it);
              m_replayers_lock.Unlock();
            }
          });
      m_replayers[peer]->init(ctx);
    } else {
      m_replayers_lock.Unlock();
    }
  }

  m_replayers_lock.Lock();
  for (auto it = m_replayers.begin(); it != m_replayers.end(); ++it) {
    peer_t peer = it->first;
    if (peer_configs.find(peer) == peer_configs.end()) {
      m_threads->work_queue->queue(new C_ShutdownReplayer(&m_replayers,
        &m_replayers_lock, peer));
    }
  }
  m_replayers_lock.Unlock();
}

} // namespace mirror
} // namespace rbd
