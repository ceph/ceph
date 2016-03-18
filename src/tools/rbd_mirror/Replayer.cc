// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "Replayer.h"
#include "Threads.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: Replayer::" << __func__ << ": "

using std::chrono::seconds;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace rbd {
namespace mirror {

void Replayer::C_InitReplayer::finish(int r) {
  assert(r == 0);

  std::string uuid;

  r = replayer->m_remote->init2(replayer->m_peer.client_name.c_str(),
      replayer->m_peer.cluster_name.c_str(), 0);
  if (r < 0) {
    derr << "error initializing remote cluster handle for " <<
      replayer->m_peer << " : " << cpp_strerror(r) << dendl;
    goto call_on_finish;
  }

  r = replayer->m_remote->conf_read_file(nullptr);
  if (r < 0) {
    derr << "could not read ceph conf for " << replayer->m_peer
      << " : " << cpp_strerror(r) << dendl;
    goto call_on_finish;
  }

  r = replayer->m_remote->connect();
  if (r < 0) {
    derr << "error connecting to remote cluster " << replayer->m_peer
      << " : " << cpp_strerror(r) << dendl;
    goto call_on_finish;
  }

  dout(20) << "connected to " << replayer->m_peer << dendl;

  r = replayer->m_local->cluster_fsid(&uuid);
  if (r < 0) {
    derr << "error retrieving local cluster uuid: " << cpp_strerror(r)
      << dendl;
    goto call_on_finish;
  }
  replayer->m_client_id = uuid;

  // TODO: make interval configurable
  replayer->m_pool_watcher.reset(new PoolWatcher(
        replayer->m_remote, 30, replayer->m_lock, replayer->m_cond));
  replayer->m_pool_watcher->refresh_images();

  replayer->m_replayer_thread.create("replayer");

call_on_finish:
  on_finish->complete(r);
}

Replayer::Replayer(Threads *threads, RadosRef local_cluster,
                   const peer_t &peer) :
  m_threads(threads),
  m_lock(stringify("rbd::mirror::Replayer ") + stringify(peer)),
  m_peer(peer),
  m_local(local_cluster),
  m_remote(new librados::Rados),
  m_replayer_thread(this)
{
}

Replayer::~Replayer()
{
  m_stopping.set(1);
  {
    Mutex::Locker l(m_lock);
    m_cond.Signal();
  }
  if (m_replayer_thread.is_started()) {
    m_replayer_thread.join();
  }
}

void Replayer::init(Context *on_finish)
{
  dout(20) << "init replaying for " << m_peer << dendl;
  m_threads->work_queue->queue(new C_InitReplayer(this, on_finish), 0);
}

void Replayer::run()
{
  dout(20) << "enter" << dendl;

  while (!m_stopping.read()) {
    Mutex::Locker l(m_lock);
    set_sources(m_pool_watcher->get_images());
    m_cond.WaitInterval(g_ceph_context, m_lock, seconds(30));
  }

  // Stopping
  map<int64_t, set<string> > empty_sources;
  while (true) {
    Mutex::Locker l(m_lock);
    set_sources(empty_sources);
    if (m_images.empty()) {
      break;
    }
    m_cond.WaitInterval(g_ceph_context, m_lock, seconds(1));
  }
}

void Replayer::set_sources(const map<int64_t, set<string> > &images)
{
  dout(20) << "enter" << dendl;

  assert(m_lock.is_locked());
  for (auto it = m_images.begin(); it != m_images.end();) {
    int64_t pool_id = it->first;
    auto &pool_images = it->second;
    if (images.find(pool_id) == images.end()) {
      for (auto images_it = pool_images.begin();
	   images_it != pool_images.end();) {
	if (stop_image_replayer(images_it->second)) {
	  pool_images.erase(images_it++);
	}
      }
      if (pool_images.empty()) {
	m_images.erase(it++);
      }
      continue;
    }
    for (auto images_it = pool_images.begin();
	 images_it != pool_images.end();) {
      if (images.at(pool_id).find(images_it->first) ==
	  images.at(pool_id).end()) {
	if (stop_image_replayer(images_it->second)) {
	  pool_images.erase(images_it++);
	}
      } else {
	++images_it;
      }
    }
    ++it;
  }

  for (const auto &kv : images) {
    int64_t pool_id = kv.first;

    // TODO: clean up once remote peer -> image replayer refactored
    librados::IoCtx remote_ioctx;
    int r = m_remote->ioctx_create2(pool_id, remote_ioctx);
    if (r < 0) {
      derr << "failed to lookup remote pool " << pool_id << ": "
           << cpp_strerror(r) << dendl;
      continue;
    }

    librados::IoCtx local_ioctx;
    r = m_local->ioctx_create(remote_ioctx.get_pool_name().c_str(), local_ioctx);
    if (r < 0) {
      derr << "failed to lookup local pool " << remote_ioctx.get_pool_name()
           << ": " << cpp_strerror(r) << dendl;
      continue;
    }

    // create entry for pool if it doesn't exist
    auto &pool_replayers = m_images[pool_id];
    for (const auto &image_id : kv.second) {
      auto it = pool_replayers.find(image_id);
      if (it == pool_replayers.end()) {
	unique_ptr<ImageReplayer> image_replayer(new ImageReplayer(m_threads,
								   m_local,
								   m_remote,
								   m_client_id,
								   local_ioctx.get_id(),
								   pool_id,
								   image_id));
	it = pool_replayers.insert(
	  std::make_pair(image_id, std::move(image_replayer))).first;
      }
      start_image_replayer(it->second);
    }
  }
}

void Replayer::start_image_replayer(unique_ptr<ImageReplayer> &image_replayer)
{
  if (!image_replayer->is_stopped()) {
    return;
  }

  image_replayer->start();
}

bool Replayer::stop_image_replayer(unique_ptr<ImageReplayer> &image_replayer)
{
  if (image_replayer->is_stopped()) {
    return true;
  }

  if (image_replayer->is_running()) {
    image_replayer->stop();
  } else {
    // TODO: check how long it is stopping and alert if it is too long.
  }

  return false;
}

} // namespace mirror
} // namespace rbd
