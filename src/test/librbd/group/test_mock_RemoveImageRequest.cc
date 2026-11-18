// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/librbd/test_mock_fixture.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/Cond.h"
#include "librbd/group/RemoveImageRequest.h"

#include "librbd/group/RemoveImageRequest.cc"

namespace librbd {
namespace group {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;

class TestMockGroupRemoveImageRequest : public TestMockFixture {
public:
  void expect_group_image_set(cls::rbd::GroupImageLinkState state, int r) {
    bufferlist bl;
    encode(cls::rbd::GroupImageStatus{"image-id", m_ioctx.get_id(), state}, bl);
    expect_exec(util::group_header_name("group-id"), "group_image_set", bl, r);
  }

  void expect_image_group(const std::string& method, int r) {
    bufferlist bl;
    encode(cls::rbd::GroupSpec{"group-id", m_ioctx.get_id()}, bl);
    expect_exec(util::header_name("image-id"), method, bl, r);
  }

  void expect_group_image_remove(int r) {
    bufferlist bl;
    encode(cls::rbd::GroupImageSpec{"image-id", m_ioctx.get_id()}, bl);
    expect_exec(util::group_header_name("group-id"), "group_image_remove",
                bl, r);
  }

  void remove_image(int expected_r) {
    C_SaferCond on_finish;
    auto req = RemoveImageRequest<MockImageCtx>::create(
      m_ioctx, "group-id", m_ioctx, "image-id", &on_finish);
    req->send();
    ASSERT_EQ(expected_r, on_finish.wait());
  }

private:
  void expect_exec(const std::string& oid, const std::string& method,
                   const bufferlist& bl, int r) {
    EXPECT_CALL(get_mock_io_ctx(m_ioctx),
                exec_internal(StrEq(oid), _, StrEq("rbd"), StrEq(method),
                              ContentsEqual(bl), _, _, _))
      .WillOnce(Return(r));
  }
};

// Test sequence:
// 1. Start removal of an image from its group.
// 2. Remove the image backlink and then its group membership entry.
// Expected: Both sides are unlinked and the request succeeds.
TEST_F(TestMockGroupRemoveImageRequest, Success) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", 0);
  expect_group_image_remove(0);

  remove_image(0);
}

// Test sequence:
// 1. Start removal of an image from its group.
// 2. Return -ENOENT when removing the image backlink.
// Expected: The request still removes the group membership entry and succeeds.
TEST_F(TestMockGroupRemoveImageRequest, MissingImageOrGroupReference) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", -ENOENT);
  expect_group_image_remove(0);

  remove_image(0);
}

// Test sequence:
// 1. Start removal of an image from its group.
// 2. Return -EBADF because the image belongs to a different group.
// Expected: The old membership entry is restored and -EBADF is returned.
TEST_F(TestMockGroupRemoveImageRequest, ImageBelongsToAnotherGroup) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", -EBADF);
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, 0);

  remove_image(-EBADF);
}

// Test sequence:
// 1. Start removal of an image from its group.
// 2. Fail image unlink with -EIO.
// Expected: The membership entry is restored and -EIO is returned.
TEST_F(TestMockGroupRemoveImageRequest, ImageUnlinkError) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", -EIO);
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, 0);

  remove_image(-EIO);
}

// Test sequence:
// 1. Start removal of an image from its group.
// 2. Unlink the image, then fail removal of its group entry with -EIO.
// Expected: The image backlink and membership entry are restored; -EIO is
//   returned.
TEST_F(TestMockGroupRemoveImageRequest, GroupUnlinkError) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", 0);
  expect_group_image_remove(-EIO);
  expect_image_group("image_group_add", 0);
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, 0);

  remove_image(-EIO);
}

// Test sequence:
// 1. Start removal of an image from its group.
// 2. Report a missing image, fail group unlink, and report the image missing
//   again during restore.
// Expected: Group membership is restored and the original -EIO is returned.
TEST_F(TestMockGroupRemoveImageRequest, MissingImageGroupUnlinkError) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", -ENOENT);
  expect_group_image_remove(-EIO);
  expect_image_group("image_group_add", -ENOENT);
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, 0);

  remove_image(-EIO);
}

// Test sequence:
// 1. Start removal of an image from its group.
// 2. Fail the initial membership update with -EIO.
// Expected: The request stops before image unlink and returns -EIO.
TEST_F(TestMockGroupRemoveImageRequest, PreUnlinkError) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, -EIO);

  remove_image(-EIO);
}

} // namespace group
} // namespace librbd
