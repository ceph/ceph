// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_H

#include <map>
#include <string>
#include <vector>

#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"
#include "types.h"

namespace rbd {
namespace mirror {

/**
 * Replays changes from a remote cluster for a single image.
 */
class ImageReplayer {
public:
  ImageReplayer(RadosRef local, RadosRef remote,
		int64_t remote_pool_id, const std::string &remote_image_id);
  ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  int start();
  void stop();

private:
  Mutex m_lock;
  int64_t m_remote_pool_id;
  std::string m_pool_name;
  std::string m_image_id;
  RadosRef m_local, m_remote;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
