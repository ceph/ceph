// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Utils.h"

namespace rbd {
namespace mirror {
namespace image_sync {
namespace util {

namespace {

static const std::string SNAP_NAME_PREFIX(".rbd-mirror");

} // anonymous namespace

std::string get_snapshot_name_prefix(const std::string& local_mirror_uuid) {
  return SNAP_NAME_PREFIX + "." + local_mirror_uuid + ".";
}

} // namespace util
} // namespace image_sync
} // namespace mirror
} // namespace rbd
