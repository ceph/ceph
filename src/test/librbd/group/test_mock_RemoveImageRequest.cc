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

TEST_F(TestMockGroupRemoveImageRequest, Success) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", 0);
  expect_group_image_remove(0);

  remove_image(0);
}

TEST_F(TestMockGroupRemoveImageRequest, MissingImageOrGroupReference) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", -ENOENT);
  expect_group_image_remove(0);

  remove_image(0);
}

TEST_F(TestMockGroupRemoveImageRequest, ImageBelongsToAnotherGroup) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", -EBADF);
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, 0);

  remove_image(-EBADF);
}

TEST_F(TestMockGroupRemoveImageRequest, ImageUnlinkError) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", -EIO);
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, 0);

  remove_image(-EIO);
}

TEST_F(TestMockGroupRemoveImageRequest, GroupUnlinkError) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", 0);
  expect_group_image_remove(-EIO);
  expect_image_group("image_group_add", 0);
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, 0);

  remove_image(-EIO);
}

TEST_F(TestMockGroupRemoveImageRequest, MissingImageGroupUnlinkError) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, 0);
  expect_image_group("image_group_remove", -ENOENT);
  expect_group_image_remove(-EIO);
  expect_image_group("image_group_add", -ENOENT);
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, 0);

  remove_image(-EIO);
}

TEST_F(TestMockGroupRemoveImageRequest, PreUnlinkError) {
  InSequence sequence;
  expect_group_image_set(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, -EIO);

  remove_image(-EIO);
}

} // namespace group
} // namespace librbd
