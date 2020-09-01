// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"

namespace cephfs {
namespace mirror {

std::ostream& operator<<(std::ostream& out, const FilesystemSpec &spec) {
  out << "{fs_name=" << spec.fs_name << ", pool_id=" << spec.pool_id << "}";
  return out;
}

} // namespace mirror
} // namespace cephfs

