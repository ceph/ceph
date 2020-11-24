// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_UTILS_H
#define CEPH_LIBRBD_MIGRATION_UTILS_H

#include "include/common_fwd.h"
#include "librbd/migration/Types.h"
#include <string>

namespace librbd {
namespace migration {
namespace util {

int parse_url(CephContext* cct, const std::string& url, UrlSpec* url_spec);

} // namespace util
} // namespace migration
} // namespace librbd

#endif // CEPH_LIBRBD_MIGRATION_UTILS_H
