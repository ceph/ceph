// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_UTILS_H
#define CEPHFS_MIRROR_UTILS_H

#include "Types.h"

namespace cephfs {
namespace mirror {

int connect(std::string_view client_name, std::string_view cluster_name,
            RadosRef *cluster, std::string_view mon_host={}, std::string_view cephx_key={},
            std::vector<const char *> args={});

int mount(RadosRef cluster, const Filesystem &filesystem, bool cross_check_fscid,
          MountRef *mount);

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_UTILS_H
