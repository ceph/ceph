// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "Driver.h"
#include "Watcher.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd::ggate::Watcher: " << this \
                           << " " << __func__ << ": "

namespace rbd {
namespace ggate {

Watcher::Watcher(Driver *drv, librados::IoCtx &ioctx, librbd::Image &image,
                 size_t size)
  : m_drv(drv), m_ioctx(ioctx), m_image(image), m_size(size) {
}

void Watcher::handle_notify() {
  dout(20) << dendl;

  librbd::image_info_t info;

  if (m_image.stat(info, sizeof(info)) == 0) {
    size_t new_size = info.size;

    if (new_size != m_size) {
      int r = m_drv->resize(new_size);
      if (r < 0) {
        derr << "resize failed: " << cpp_strerror(r) << dendl;
        m_drv->shut_down();
      }
      r = m_image.invalidate_cache();
      if (r < 0) {
        derr << "invalidate rbd cache failed: " << cpp_strerror(r) << dendl;
        m_drv->shut_down();
      }
      m_size = new_size;
    }
  }
}

} // namespace ggate
} // namespace rbd
