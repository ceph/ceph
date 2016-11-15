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
#define dout_prefix *_dout << "rbd::mirror::PoolWatcher: " << this << " " \
                           << __func__ << ": "

using std::list;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;
using librbd::cls_client::mirror_image_list;

namespace rbd {
namespace mirror {

PoolWatcher::PoolWatcher(librados::IoCtx &remote_io_ctx,
                         double interval_seconds,
			 Mutex &lock, Cond &cond) :
  m_lock(lock),
  m_refresh_cond(cond),
  m_timer(g_ceph_context, m_lock, false),
  m_interval(interval_seconds)
{
  m_remote_io_ctx.dup(remote_io_ctx);
  m_timer.init();
}

PoolWatcher::~PoolWatcher()
{
  Mutex::Locker l(m_lock);
  m_stopping = true;
  m_timer.shutdown();
}

bool PoolWatcher::is_blacklisted() const {
  assert(m_lock.is_locked());
  return m_blacklisted;
}

const PoolWatcher::ImageIds& PoolWatcher::get_images() const
{
  assert(m_lock.is_locked());
  return m_images;
}

void PoolWatcher::refresh_images(bool reschedule)
{
  ImageIds image_ids;
  int r = refresh(&image_ids);

  Mutex::Locker l(m_lock);
  if (r >= 0) {
    m_images = std::move(image_ids);
  } else if (r == -EBLACKLISTED) {
    derr << "blacklisted during image refresh" << dendl;
    m_blacklisted = true;
  }

  if (!m_stopping && reschedule) {
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&PoolWatcher::refresh_images, this, true));
    m_timer.add_event_after(m_interval, ctx);
  }
  m_refresh_cond.Signal();
  // TODO: perhaps use a workqueue instead, once we get notifications
  // about new/removed mirrored images
}

int PoolWatcher::refresh(ImageIds *image_ids) {
  dout(20) << "enter" << dendl;

  std::string pool_name = m_remote_io_ctx.get_pool_name();
  rbd_mirror_mode_t mirror_mode;
  int r = librbd::mirror_mode_get(m_remote_io_ctx, &mirror_mode);
  if (r < 0) {
    derr << "could not tell whether mirroring was enabled for "
         << pool_name << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  if (mirror_mode == RBD_MIRROR_MODE_DISABLED) {
    dout(20) << "pool " << pool_name << " has mirroring disabled" << dendl;
    return 0;
  }

  std::map<std::string, std::string> images_map;
  r = librbd::list_images_v2(m_remote_io_ctx, images_map);
  if (r < 0) {
    derr << "error retrieving image names from pool " << pool_name << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  std::map<std::string, std::string> image_id_to_name;
  for (const auto& img_pair : images_map) {
    image_id_to_name.insert(std::make_pair(img_pair.second, img_pair.first));
  }

  std::string last_read = "";
  int max_read = 1024;
  do {
    std::map<std::string, std::string> mirror_images;
    r =  mirror_image_list(&m_remote_io_ctx, last_read, max_read,
                           &mirror_images);
    if (r < 0) {
      derr << "error listing mirrored image directory: "
           << cpp_strerror(r) << dendl;
      return r;
    }
    for (auto it = mirror_images.begin(); it != mirror_images.end(); ++it) {
      boost::optional<std::string> image_name(boost::none);
      auto it2 = image_id_to_name.find(it->first);
      if (it2 != image_id_to_name.end()) {
        image_name = it2->second;
      }
      image_ids->insert(ImageId(it->first, image_name, it->second));
    }
    if (!mirror_images.empty()) {
      last_read = mirror_images.rbegin()->first;
    }
    r = mirror_images.size();
  } while (r == max_read);

  return 0;
}

} // namespace mirror
} // namespace rbd
