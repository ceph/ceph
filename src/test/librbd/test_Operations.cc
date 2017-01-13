// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/ImageCtx.h"
#include "librbd/Operations.h"

void register_test_operations() {
}

class TestOperations : public TestFixture {
public:

};

TEST_F(TestOperations, DisableJournalingCorrupt) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, m_ioctx.remove("journal." + ictx->id));
  ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_JOURNALING,
                                                 false));
}

