// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"

namespace cephfs {
namespace mirror {

std::ostream& operator<<(std::ostream& out, const Filesystem &filesystem) {
  out << "{fscid=" << filesystem.fscid << ", fs_name=" << filesystem.fs_name << "}";
  return out;
}

std::ostream& operator<<(std::ostream& out, const FilesystemSpec &spec) {
  out << "{filesystem=" << spec.filesystem << ", pool_id=" << spec.pool_id << "}";
  return out;
}

} // namespace mirror
} // namespace cephfs

