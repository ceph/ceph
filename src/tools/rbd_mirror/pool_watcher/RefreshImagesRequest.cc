// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/pool_watcher/RefreshImagesRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include <map>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::pool_watcher::RefreshImagesRequest " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace pool_watcher {

static const uint32_t MAX_RETURN = 1024;

using librbd::util::create_rados_callback;

template <typename I>
void RefreshImagesRequest<I>::send() {
  mirror_image_list();
}

template <typename I>
void RefreshImagesRequest<I>::mirror_image_list() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_list_start(&op, m_start_after, MAX_RETURN);

  librados::AioCompletion *aio_comp = create_rados_callback<
    RefreshImagesRequest<I>,
    &RefreshImagesRequest<I>::handle_mirror_image_list>(this);
  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RefreshImagesRequest<I>::handle_mirror_image_list(int r) {
  dout(10) << "r=" << r << dendl;

  std::map<std::string, std::string> ids;
  if (r == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_list_finish(&it, &ids);
  }

  if (r < 0 && r != -ENOENT) {
    derr << "failed to list mirrored images: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!ids.empty()) {
    m_local_to_global_ids.insert(ids.begin(), ids.end());
    if (ids.size() == MAX_RETURN) {
      m_start_after = ids.rbegin()->first;
      mirror_image_list();
      return;
    }
  }

  dir_list();
}

template <typename I>
void RefreshImagesRequest<I>::dir_list() {
  dout(10) << dendl;

  m_out_bl.clear();
  m_start_after = "";

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_list_start(&op, m_start_after, MAX_RETURN);

  librados::AioCompletion *aio_comp = create_rados_callback<
    RefreshImagesRequest<I>,
    &RefreshImagesRequest<I>::handle_dir_list>(this);
  int r = m_remote_io_ctx.aio_operate(RBD_DIRECTORY, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RefreshImagesRequest<I>::handle_dir_list(int r) {
  dout(10) << "r=" << r << dendl;

  std::map<std::string, std::string> name_to_ids;
  if (r == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::dir_list_finish(&it, &name_to_ids);
  }

  if (r < 0 && r != -ENOENT) {
    derr << "failed to list images: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!name_to_ids.empty()) {
    for (auto &pair : name_to_ids) {
      auto it = m_local_to_global_ids.find(pair.second);
      if (it != m_local_to_global_ids.end()) {
        // mirrored image must exist within directory to be treated as
        // a valid image
        m_image_ids->insert(ImageId(it->second, it->first, pair.first));
      }
    }

    if (name_to_ids.size() == MAX_RETURN) {
      m_start_after = name_to_ids.rbegin()->first;
      dir_list();
      return;
    }
  }

  finish(0);
}

template <typename I>
void RefreshImagesRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace pool_watcher
} // namespace mirror
} // namespace rbd

template class rbd::mirror::pool_watcher::RefreshImagesRequest<librbd::ImageCtx>;
