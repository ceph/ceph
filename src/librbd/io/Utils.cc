// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/Utils.h"
#include "include/buffer.h"
#include "osd/osd_types.h"

namespace librbd {
namespace io {
namespace util {

bool assemble_write_same_extent(
    const ObjectExtent &object_extent, const ceph::bufferlist& data,
    ceph::bufferlist *ws_data, bool force_write) {
  size_t data_len = data.length();

  if (!force_write) {
    bool may_writesame = true;
    for (auto& q : object_extent.buffer_extents) {
      if (!(q.first % data_len == 0 && q.second % data_len == 0)) {
        may_writesame = false;
        break;
      }
    }

    if (may_writesame) {
      ws_data->append(data);
      return true;
    }
  }

  for (auto& q : object_extent.buffer_extents) {
    bufferlist sub_bl;
    uint64_t sub_off = q.first % data_len;
    uint64_t sub_len = data_len - sub_off;
    uint64_t extent_left = q.second;
    while (extent_left >= sub_len) {
      sub_bl.substr_of(data, sub_off, sub_len);
      ws_data->claim_append(sub_bl);
      extent_left -= sub_len;
      if (sub_off) {
	sub_off = 0;
	sub_len = data_len;
      }
    }
    if (extent_left) {
      sub_bl.substr_of(data, sub_off, extent_left);
      ws_data->claim_append(sub_bl);
    }
  }
  return false;
}

} // namespace util
} // namespace io
} // namespace librbd

