// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "Replayer.h"

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

Replayer::Replayer(Threads *threads, RadosRef local_cluster,
                   const peer_t &peer, const std::vector<const char*> &args) :
  m_threads(threads),
  m_lock(stringify("rbd::mirror::Replayer ") + stringify(peer)),
  m_peer(peer),
  m_args(args),
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

int Replayer::init()
{
  dout(20) << "replaying for " << m_peer << dendl;

  int r = m_remote->init2(m_peer.client_name.c_str(),
			  m_peer.cluster_name.c_str(), 0);
  if (r < 0) {
    derr << "error initializing remote cluster handle for " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_remote->conf_read_file(nullptr);
  if (r < 0) {
    derr << "could not read ceph conf for " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_remote->conf_parse_env(nullptr);
  if (r < 0) {
    derr << "could not parse environment for " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  if (!m_args.empty()) {
    r = m_remote->conf_parse_argv(m_args.size(), &m_args[0]);
    if (r < 0) {
      derr << "could not parse command line args for " << m_peer
	   << " : " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  r = m_remote->connect();
  if (r < 0) {
    derr << "error connecting to remote cluster " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  dout(20) << "connected to " << m_peer << dendl;

  // TODO: make interval configurable
  m_pool_watcher.reset(new PoolWatcher(m_remote, 30, m_lock, m_cond));
  m_pool_watcher->refresh_images();

  m_replayer_thread.create("replayer");

  return 0;
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

    std::string mirror_uuid;
    r = librbd::cls_client::mirror_uuid_get(&local_ioctx, &mirror_uuid);
    if (r < 0) {
      derr << "failed to retrieve mirror uuid from pool "
        << local_ioctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
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
								   mirror_uuid,
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
