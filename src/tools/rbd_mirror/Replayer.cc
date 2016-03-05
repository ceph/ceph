// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "Replayer.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: "

using std::chrono::seconds;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace rbd {
namespace mirror {

namespace {

class ThreadPoolSingleton : public ThreadPool {
public:
  explicit ThreadPoolSingleton(CephContext *cct)
    : ThreadPool(cct, "rbd::mirror::thread_pool", "tp_rbd_mirror",
		 1 /* TODO: add config option or reuse cct->_conf->rbd_op_threads */,
                 "rbd_mirror_op_threads") {
    start();
  }
  virtual ~ThreadPoolSingleton() {
    stop();
  }
};

} // anonymous namespace

Replayer::Replayer(RadosRef local_cluster, const peer_t &peer) :
  m_lock(stringify("rbd::mirror::Replayer ") + stringify(peer)),
  m_peer(peer),
  m_local(local_cluster),
  m_remote(new librados::Rados),
  m_replayer_thread(this)
{
  ThreadPoolSingleton *thread_pool_singleton;
  CephContext *cct = static_cast<CephContext *>(m_local->cct());
  cct->lookup_or_create_singleton_object<ThreadPoolSingleton>(
    thread_pool_singleton, "rbd::mirror::thread_pool");
  m_op_work_queue = new ContextWQ("rbd::mirror::op_work_queue",
				  60 /* TODO: add config option or reuse cct->_conf->rbd_op_thread_timeout? */,
				  thread_pool_singleton);
}

Replayer::~Replayer()
{
  m_stopping.set(1);
  {
    Mutex::Locker l(m_lock);
    m_cond.Signal();
  }
  m_replayer_thread.join();

  m_op_work_queue->drain();
  delete m_op_work_queue;
}

int Replayer::init()
{
  dout(20) << __func__ << "Replaying for " << m_peer << dendl;

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

  r = m_remote->connect();
  if (r < 0) {
    derr << "error connecting to remote cluster " << m_peer
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }

  dout(20) << __func__ << "connected to " << m_peer << dendl;

  std::string uuid;
  r = m_local->cluster_fsid(&uuid);
  if (r < 0) {
    derr << "error retrieving local cluster uuid: " << cpp_strerror(r)
	 << dendl;
    return r;
  }
  m_client_id = uuid;

  // TODO: make interval configurable
  m_pool_watcher.reset(new PoolWatcher(m_remote, 30, m_lock, m_cond));
  m_pool_watcher->refresh_images();

  return 0;
}

void Replayer::run()
{
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
    // create entry for pool if it doesn't exist
    auto &pool_replayers = m_images[pool_id];
    for (const auto &image_id : kv.second) {
      auto it = pool_replayers.find(image_id);
      if (it == pool_replayers.end()) {
	unique_ptr<ImageReplayer> image_replayer(
	  new ImageReplayer(m_local, m_remote, m_client_id, pool_id, image_id,
			    m_op_work_queue));
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
    // TODO: check how long it is stopping and alert if it is too long?
  }

  return false;
}

} // namespace mirror
} // namespace rbd
