// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>

namespace rbd {
namespace mirror {
namespace image_sync {
namespace util {

std::string get_snapshot_name_prefix(const std::string& local_mirror_uuid);

} // namespace util
} // namespace image_sync
} // namespace mirror
} // namespace rbd
