// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/range/adaptor/map.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "Mirror.h"
#include "Threads.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: "

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

Mirror::Mirror(CephContext *cct) :
  m_cct(cct),
  m_lock("rbd::mirror::Mirror"),
  m_local(new librados::Rados())
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
  while (!m_stopping.read()) {
    m_local_cluster_watcher->refresh_pools();
    Mutex::Locker l(m_lock);
    update_replayers(m_local_cluster_watcher->get_peer_configs());
    // TODO: make interval configurable
    m_cond.WaitInterval(g_ceph_context, m_lock, seconds(30));
  }
}

void Mirror::update_replayers(const map<peer_t, set<int64_t> > &peer_configs)
{
  assert(m_lock.is_locked());
  set<peer_t> peers;
  for (auto &kv : peer_configs) {
    const peer_t &peer = kv.first;
    if (m_replayers.find(peer) == m_replayers.end()) {
      unique_ptr<Replayer> replayer(new Replayer(m_threads, m_local, peer));
      // TODO: make async, and retry connecting within replayer
      int r = replayer->init();
      if (r < 0) {
	continue;
      }
      m_replayers.insert(std::make_pair(peer, std::move(replayer)));
    }
  }

  // TODO: make async
  for (auto it = m_replayers.begin(); it != m_replayers.end();) {
    peer_t peer = it->first;
    if (peers.find(peer) == peers.end()) {
      m_replayers.erase(it++);
    } else {
      ++it;
    }
  }
}

} // namespace mirror
} // namespace rbd
