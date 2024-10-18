// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOCK_IMAGE_SYNC_SYNC_POINT_HANDLER_H
#define CEPH_MOCK_IMAGE_SYNC_SYNC_POINT_HANDLER_H

#include "tools/rbd_mirror/image_sync/Types.h"
#include <gmock/gmock.h>

struct Context;

namespace rbd {
namespace mirror {
namespace image_sync {

struct MockSyncPointHandler : public SyncPointHandler{
  MOCK_CONST_METHOD0(get_sync_points, SyncPoints());
  MOCK_CONST_METHOD0(get_snap_seqs, librbd::SnapSeqs());

  MOCK_METHOD4(update_sync_points, void(const librbd::SnapSeqs&,
                                        const SyncPoints&,
                                        bool, Context*));
};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

#endif // CEPH_MOCK_IMAGE_SYNC_SYNC_POINT_HANDLER_H
