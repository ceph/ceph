// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/api/Trash.h"
#include <set>
#include <vector>

void register_test_trash() {
}

namespace librbd {

static bool operator==(const trash_image_info_t& lhs,
                       const trash_image_info_t& rhs) {
  return (lhs.id == rhs.id &&
          lhs.name == rhs.name &&
          lhs.source == rhs.source);
}

static bool operator==(const image_spec_t& lhs,
                       const image_spec_t& rhs) {
  return (lhs.id == rhs.id && lhs.name == rhs.name);
}

class TestTrash : public TestFixture {
public:

  TestTrash() {}
};

TEST_F(TestTrash, UserRemovingSource) {
  REQUIRE_FORMAT_V2();

  auto compare_lambda = [](const trash_image_info_t& lhs,
                           const trash_image_info_t& rhs) {
      if (lhs.id != rhs.id) {
        return lhs.id < rhs.id;
      } else if (lhs.name != rhs.name) {
        return lhs.name < rhs.name;
      }
      return lhs.source < rhs.source;
    };
  typedef std::set<trash_image_info_t, decltype(compare_lambda)> TrashEntries;

  librbd::RBD rbd;
  librbd::Image image;
  auto image_name1 = m_image_name;
  std::string image_id1;
  ASSERT_EQ(0, rbd.open(m_ioctx, image, image_name1.c_str()));
  ASSERT_EQ(0, image.get_id(&image_id1));
  ASSERT_EQ(0, image.close());

  auto image_name2 = get_temp_image_name();
  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, image_name2, m_image_size));

  std::string image_id2;
  ASSERT_EQ(0, rbd.open(m_ioctx, image, image_name2.c_str()));
  ASSERT_EQ(0, image.get_id(&image_id2));
  ASSERT_EQ(0, image.close());

  ASSERT_EQ(0, api::Trash<>::move(m_ioctx, RBD_TRASH_IMAGE_SOURCE_USER,
                                  image_name1, image_id1, 0));
  ASSERT_EQ(0, api::Trash<>::move(m_ioctx, RBD_TRASH_IMAGE_SOURCE_REMOVING,
                                  image_name2, image_id2, 0));

  TrashEntries trash_entries{compare_lambda};
  TrashEntries expected_trash_entries{compare_lambda};

  std::vector<trash_image_info_t> entries;
  ASSERT_EQ(0, api::Trash<>::list(m_ioctx, entries, true));
  trash_entries.insert(entries.begin(), entries.end());

  expected_trash_entries = {
    {.id = image_id1,
     .name = image_name1,
     .source = RBD_TRASH_IMAGE_SOURCE_USER},
  };
  ASSERT_EQ(expected_trash_entries, trash_entries);

  std::vector<image_spec_t> expected_images = {
    {.id = image_id2, .name = image_name2}
  };
  std::vector<image_spec_t> images;
  ASSERT_EQ(0, rbd.list2(m_ioctx, &images));
  ASSERT_EQ(expected_images, images);
}

} // namespace librbd
