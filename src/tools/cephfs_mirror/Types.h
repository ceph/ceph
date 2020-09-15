// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_TYPES_H
#define CEPHFS_MIRROR_TYPES_H

#include <set>
#include <iostream>
#include <string_view>

#include "include/rados/librados.hpp"

namespace cephfs {
namespace mirror {

static const std::string CEPHFS_MIRROR_OBJECT("cephfs_mirror");

// specification of a filesystem -- pool id the metadata pool id.
struct FilesystemSpec {
  FilesystemSpec() = default;
  FilesystemSpec(std::string_view fs_name, uint64_t pool_id)
    : fs_name(fs_name),
      pool_id(pool_id) {
  }

  std::string fs_name;
  uint64_t pool_id;

  bool operator==(const FilesystemSpec &rhs) const {
    return (fs_name == rhs.fs_name &&
            pool_id == rhs.pool_id);
  }

  bool operator<(const FilesystemSpec &rhs) const {
    if (fs_name != rhs.fs_name) {
      return fs_name < rhs.fs_name;
    }

    return pool_id < rhs.pool_id;
  }
};

std::ostream& operator<<(std::ostream& out, const FilesystemSpec &spec);

typedef std::shared_ptr<librados::Rados> RadosRef;
typedef std::shared_ptr<librados::IoCtx> IoCtxRef;

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_TYPES_H
