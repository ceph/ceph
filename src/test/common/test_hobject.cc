#include "common/hobject.h"
#include "gtest/gtest.h"

TEST(HObject, cmp)
{
  hobject_t c{object_t{"fooc"}, "food", CEPH_NOSNAP, 42, 0, "nspace"};
  hobject_t d{object_t{"food"}, "",     CEPH_NOSNAP, 42, 0, "nspace"};
  hobject_t e{object_t{"fooe"}, "food", CEPH_NOSNAP, 42, 0, "nspace"};
  ASSERT_EQ(-1, cmp(c, d));
  ASSERT_EQ(-1, cmp(d, e));
}
