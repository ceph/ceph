// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_TYPES_H
#define CEPHFS_MIRROR_TYPES_H

#include <set>
#include <iostream>
#include <string_view>

#include "include/rados/librados.hpp"
#include "include/cephfs/libcephfs.h"
#include "mds/mdstypes.h"

namespace cephfs {
namespace mirror {

static const std::string CEPHFS_MIRROR_OBJECT("cephfs_mirror");

typedef boost::variant<bool, uint64_t, std::string> AttributeValue;
typedef std::map<std::string, AttributeValue> Attributes;

// distinct filesystem identifier
struct Filesystem {
  fs_cluster_id_t fscid;
  std::string fs_name;

  bool operator==(const Filesystem &rhs) const {
    return (fscid == rhs.fscid &&
            fs_name == rhs.fs_name);
  }

  bool operator!=(const Filesystem &rhs) const {
    return !(*this == rhs);
  }

  bool operator<(const Filesystem &rhs) const {
    if (fscid != rhs.fscid) {
      return fscid < rhs.fscid;
    }

    return fs_name < rhs.fs_name;
  }
};

// specification of a filesystem -- pool id the metadata pool id.
struct FilesystemSpec {
  FilesystemSpec() = default;
  FilesystemSpec(const Filesystem &filesystem, uint64_t pool_id)
    : filesystem(filesystem),
      pool_id(pool_id) {
  }
  FilesystemSpec(fs_cluster_id_t fscid, std::string_view fs_name, uint64_t pool_id)
    : filesystem(Filesystem{fscid, std::string(fs_name)}),
      pool_id(pool_id) {
  }

  Filesystem filesystem;
  uint64_t pool_id;

  bool operator==(const FilesystemSpec &rhs) const {
    return (filesystem == rhs.filesystem &&
            pool_id == rhs.pool_id);
  }

  bool operator<(const FilesystemSpec &rhs) const {
    if (filesystem != rhs.filesystem) {
      return filesystem < rhs.filesystem;
    }

    return pool_id < rhs.pool_id;
  }
};

std::ostream& operator<<(std::ostream& out, const Filesystem &filesystem);
std::ostream& operator<<(std::ostream& out, const FilesystemSpec &spec);

typedef std::shared_ptr<librados::Rados> RadosRef;
typedef std::shared_ptr<librados::IoCtx> IoCtxRef;

// not a shared_ptr since the type is incomplete
typedef ceph_mount_info *MountRef;

// Performance Counters
enum {
  l_cephfs_mirror_snapshot_first = 4000,
  //TODO:
  l_cephfs_mirror_snapshot_last,
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_TYPES_H
