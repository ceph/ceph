// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>

#include "common/debug.h"
#include "common/errno.h"

#include "cls/rbd/cls_rbd_client.h"
#include "include/rbd_types.h"
#include "librbd/internal.h"

#include "PoolWatcher.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: PoolWatcher::" << __func__ << ": "

using std::list;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;
using librbd::cls_client::mirror_image_list;

namespace rbd {
namespace mirror {

PoolWatcher::PoolWatcher(RadosRef cluster, double interval_seconds,
			 Mutex &lock, Cond &cond) :
  m_lock(lock),
  m_refresh_cond(cond),
  m_stopping(false),
  m_cluster(cluster),
  m_timer(g_ceph_context, m_lock, false),
  m_interval(interval_seconds)
{
  m_timer.init();
}

PoolWatcher::~PoolWatcher()
{
  Mutex::Locker l(m_lock);
  m_stopping = true;
  m_timer.shutdown();
}

const PoolWatcher::PoolImageIds& PoolWatcher::get_images() const
{
  assert(m_lock.is_locked());
  return m_images;
}

void PoolWatcher::refresh_images(bool reschedule)
{
  dout(20) << "enter" << dendl;
  PoolImageIds images;
  list<pair<int64_t, string> > pools;
  int r = m_cluster->pool_list2(pools);
  if (r < 0) {
    derr << "error listing pools: " << cpp_strerror(r) << dendl;
    return;
  }

  for (auto kv : pools) {
    int64_t pool_id = kv.first;
    string pool_name = kv.second;
    int64_t base_tier;
    r = m_cluster->pool_get_base_tier(pool_id, &base_tier);
    if (r == -ENOENT) {
      dout(10) << "pool " << pool_name << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      derr << "Error retrieving base tier for pool " << pool_name << dendl;
      continue;
    }
    if (pool_id != base_tier) {
      // pool is a cache; skip it
      continue;
    }

    IoCtx ioctx;
    r = m_cluster->ioctx_create2(pool_id, ioctx);
    if (r == -ENOENT) {
      dout(10) << "pool " << pool_name << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      derr << "Error accessing pool " << pool_name << cpp_strerror(r) << dendl;
      continue;
    }

    rbd_mirror_mode_t mirror_mode;
    r = librbd::mirror_mode_get(ioctx, &mirror_mode);
    if (r < 0) {
      derr << "could not tell whether mirroring was enabled for " << pool_name
	   << " : " << cpp_strerror(r) << dendl;
      continue;
    }
    if (mirror_mode == RBD_MIRROR_MODE_DISABLED) {
      dout(20) << "pool " << pool_name << " has mirroring disabled" << dendl;
      continue;
    }

    std::set<ImageIds> image_ids;
    std::string last_read = "";
    int max_read = 1024;
    do {
      std::map<std::string, std::string> mirror_images;
      r =  mirror_image_list(&ioctx, last_read, max_read, &mirror_images);
      if (r < 0) {
        derr << "error listing mirrored image directory: "
             << cpp_strerror(r) << dendl;
        continue;
      }
      for (auto it = mirror_images.begin(); it != mirror_images.end(); ++it) {
        image_ids.insert(ImageIds(it->first, it->second));
      }
      if (!mirror_images.empty()) {
        last_read = mirror_images.rbegin()->first;
      }
      r = mirror_images.size();
    } while (r == max_read);

    if (!image_ids.empty()) {
      images[pool_id] = std::move(image_ids);
    }
  }

  Mutex::Locker l(m_lock);
  m_images = std::move(images);
  if (!m_stopping && reschedule) {
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&PoolWatcher::refresh_images, this, true));
    m_timer.add_event_after(m_interval, ctx);
  }
  m_refresh_cond.Signal();
  // TODO: perhaps use a workqueue instead, once we get notifications
  // about new/removed mirrored images
}

} // namespace mirror
} // namespace rbd
