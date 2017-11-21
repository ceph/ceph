// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_DELETER_TYPES_H
#define CEPH_RBD_MIRROR_IMAGE_DELETER_TYPES_H

namespace rbd {
namespace mirror {
namespace image_deleter {

enum ErrorResult {
  ERROR_RESULT_COMPLETE,
  ERROR_RESULT_RETRY,
  ERROR_RESULT_RETRY_IMMEDIATELY
};

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_DELETER_TYPES_H
