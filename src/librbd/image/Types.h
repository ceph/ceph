// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_IMAGE_TYPES_H
#define LIBRBD_IMAGE_TYPES_H

namespace librbd {
namespace image {

enum {
  CREATE_FLAG_SKIP_MIRROR_ENABLE  = 1 << 0,
  CREATE_FLAG_FORCE_MIRROR_ENABLE = 1 << 1,
  CREATE_FLAG_MIRROR_ENABLE_MASK = (CREATE_FLAG_SKIP_MIRROR_ENABLE |
                                    CREATE_FLAG_FORCE_MIRROR_ENABLE),
};

} // namespace image
} // librbd

#endif // LIBRBD_IMAGE_TYPES_H
