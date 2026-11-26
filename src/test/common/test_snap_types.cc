// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/snap_types.h"
#include <gtest/gtest.h>

// An older MDS sends SnapRealmInfoNew struct_v 1, which has no flags field.
TEST(SnapRealmInfoNew, DecodeV1FromOldMDSSnapdirVisible)
{
  SnapRealmInfoNew old(SnapRealmInfo(1, 10, 10, 0), utime_t(), 5, 0);
  ceph::buffer::list bl;
  {
    ENCODE_START(1, 1, bl);
    encode(old.info, bl);
    encode(old.last_modified, bl);
    encode(old.change_attr, bl);
    ENCODE_FINISH(bl);
  }

  SnapRealmInfoNew ninfo;
  auto p = bl.cbegin();
  decode(ninfo, p);
  ASSERT_EQ(old.change_attr, ninfo.change_attr);
  ASSERT_TRUE(ninfo.flags & SnapRealmInfoNew::SNAPDIR_VISIBILITY);
}

// A current MDS that hides the snapdir must not be overridden by the v1 default.
TEST(SnapRealmInfoNew, DecodeV2SnapdirHidden)
{
  ceph::buffer::list bl;
  encode(SnapRealmInfoNew(SnapRealmInfo(1, 10, 10, 0), utime_t(), 5, 0), bl);

  SnapRealmInfoNew ninfo;
  auto p = bl.cbegin();
  decode(ninfo, p);
  ASSERT_EQ(0u, ninfo.flags);
}
