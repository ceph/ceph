// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/rbd/librbd.h"
#include "include/rbd/librbd.hpp"
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/api/Image.h"

void register_test_diff_iterate() {
}

struct diff_extent {
  diff_extent(uint64_t _offset, uint64_t _length, bool _exists,
              uint64_t object_size) :
    offset(_offset), length(_length), exists(_exists)
  {
    if (object_size != 0) {
      offset -= offset % object_size;
      length = object_size;
    }
  }
  uint64_t offset;
  uint64_t length;
  bool exists;
  bool operator==(const diff_extent& o) const {
    return offset == o.offset && length == o.length && exists == o.exists;
  }
};

template <typename Policy>
class DiffIterateDeterministicTest : public TestFixture {
public:
  static constexpr bool whole_object = Policy::whole_object;

  static int format(librbd::ImageCtx* ictx, librbd::encryption_format_t format,
                    librbd::encryption_options_t opts, size_t opts_size,
                    bool c_api = false) {
    // format with insecure_fast_mode=true to save time and CPU cycles
    return librbd::api::Image<>::encryption_format(ictx, format, opts,
                                                   opts_size, true, c_api);
  }

  static int vector_cb(uint64_t off, size_t len, int exists, void *arg) {
    auto diff = static_cast<std::vector<diff_extent>*>(arg);
    diff->push_back(diff_extent(off, len, exists, 0));
    return 0;
  }

  static void validate_object_map(rbd_image_t image) {
    uint64_t flags;
    ASSERT_EQ(0, rbd_get_flags(image, &flags));
    ASSERT_EQ(0, flags & RBD_FLAG_OBJECT_MAP_INVALID);
  }

  static void validate_object_map(librbd::Image& image) {
    uint64_t flags;
    ASSERT_EQ(0, image.get_flags(&flags));
    ASSERT_EQ(0, flags & RBD_FLAG_OBJECT_MAP_INVALID);
  }

  void test_deterministic(uint64_t object_off, uint64_t len) {
    std::string name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, 20 << 20, 22));
    librbd::ImageCtx* ictx;
    ASSERT_EQ(0, open_image(name, &ictx));
    test_deterministic(static_cast<rbd_image_t>(ictx), object_off, len, 1);
    close_image(ictx);
  }

  void test_deterministic_pp(uint64_t object_off, uint64_t len) {
    std::string name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, 20 << 20, 22));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, name.c_str(), NULL));
    test_deterministic_pp(image, object_off, len, 1);
  }

#ifdef HAVE_LIBCRYPTSETUP

  void test_deterministic_luks1(uint64_t object_off, uint64_t len) {
    std::string name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, 24 << 20, 22));
    librbd::ImageCtx* ictx;
    ASSERT_EQ(0, open_image(name, &ictx));
    rbd_encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, "some passphrase", 15};
    ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                        sizeof(fopts), true));
    test_deterministic(static_cast<rbd_image_t>(ictx), object_off, len, 512);
    close_image(ictx);
  }

  void test_deterministic_luks1_pp(uint64_t object_off, uint64_t len) {
    std::string name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, 24 << 20, 22));
    librbd::ImageCtx* ictx;
    ASSERT_EQ(0, open_image(name, &ictx));
    librbd::encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, "some passphrase"};
    ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                        sizeof(fopts)));
    close_image(ictx);

    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, name.c_str(), NULL));
    ASSERT_EQ(0, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                       sizeof(fopts)));
    test_deterministic_pp(image, object_off, len, 512);
  }

  void test_deterministic_luks2(uint64_t object_off, uint64_t len) {
    std::string name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, 36 << 20, 22));
    librbd::ImageCtx* ictx;
    ASSERT_EQ(0, open_image(name, &ictx));
    rbd_encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, "some passphrase", 15};
    ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                        sizeof(fopts), true));
    test_deterministic(static_cast<rbd_image_t>(ictx), object_off, len, 4096);
    close_image(ictx);
  }

  void test_deterministic_luks2_pp(uint64_t object_off, uint64_t len) {
    std::string name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, 36 << 20, 22));
    librbd::ImageCtx* ictx;
    ASSERT_EQ(0, open_image(name, &ictx));
    librbd::encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, "some passphrase"};
    ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                        sizeof(fopts)));
    close_image(ictx);

    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, name.c_str(), NULL));
    ASSERT_EQ(0, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                       sizeof(fopts)));
    test_deterministic_pp(image, object_off, len, 4096);
  }

#endif // HAVE_LIBCRYPTSETUP

private:
  void test_deterministic(rbd_image_t image, uint64_t object_off,
                          uint64_t len, uint64_t block_size) {
    uint64_t off1 = 0;
    uint64_t off2 = 4 << 20;
    uint64_t size = 20 << 20;
    uint64_t extent_len = round_up_to(object_off + len, block_size);

    rbd_image_info_t info;
    ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
    ASSERT_EQ(size, info.size);
    ASSERT_EQ(5, info.num_objs);
    ASSERT_EQ(4 << 20, info.obj_size);
    ASSERT_EQ(22, info.order);

    uint64_t object_size = 0;
    if (whole_object) {
      object_size = 1 << info.order;
    }

    std::vector<diff_extent> extents;
    ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_EQ(-ENOENT, rbd_diff_iterate2(image, "snap1", 0, size, true,
                                         whole_object, vector_cb, &extents));

    ASSERT_EQ(0, rbd_snap_create(image, "snap1"));

    std::string buf(len, '1');
    ASSERT_EQ(len, rbd_write(image, off1 + object_off, len, buf.data()));
    ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    extents.clear();

    ASSERT_EQ(0, rbd_snap_create(image, "snap2"));

    ASSERT_EQ(len, rbd_write(image, off2 + object_off, len, buf.data()));
    ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    ASSERT_EQ(0, rbd_snap_create(image, "snap3"));

    // 1. beginning of time -> HEAD
    ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    // 2. snap1 -> HEAD
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap1", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    // 3. snap2 -> HEAD
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap2", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[0]);
    extents.clear();

    // 4. snap3 -> HEAD
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap3", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_NO_FATAL_FAILURE(validate_object_map(image));
    ASSERT_EQ(0, rbd_snap_set(image, "snap3"));

    // 5. beginning of time -> snap3
    ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    // 6. snap1 -> snap3
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap1", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    // 7. snap2 -> snap3
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap2", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[0]);
    extents.clear();

    // 8. snap3 -> snap3
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap3", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_NO_FATAL_FAILURE(validate_object_map(image));
    ASSERT_EQ(0, rbd_snap_set(image, "snap2"));

    // 9. beginning of time -> snap2
    ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    extents.clear();

    // 10. snap1 -> snap2
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap1", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    extents.clear();

    // 11. snap2 -> snap2
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap2", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    // 12. snap3 -> snap2
    ASSERT_EQ(-EINVAL, rbd_diff_iterate2(image, "snap3", 0, size, true,
                                         whole_object, vector_cb, &extents));

    ASSERT_NO_FATAL_FAILURE(validate_object_map(image));
    ASSERT_EQ(0, rbd_snap_set(image, "snap1"));

    // 13. beginning of time -> snap1
    ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    // 14. snap1 -> snap1
    ASSERT_EQ(0, rbd_diff_iterate2(image, "snap1", 0, size, true, whole_object,
                                   vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    // 15. snap2 -> snap1
    ASSERT_EQ(-EINVAL, rbd_diff_iterate2(image, "snap2", 0, size, true,
                                         whole_object, vector_cb, &extents));

    // 16. snap3 -> snap1
    ASSERT_EQ(-EINVAL, rbd_diff_iterate2(image, "snap3", 0, size, true,
                                         whole_object, vector_cb, &extents));

    ASSERT_NO_FATAL_FAILURE(validate_object_map(image));
  }

  void test_deterministic_pp(librbd::Image& image, uint64_t object_off,
                             uint64_t len, uint64_t block_size) {
    uint64_t off1 = 8 << 20;
    uint64_t off2 = 16 << 20;
    uint64_t size = 20 << 20;
    uint64_t extent_len = round_up_to(object_off + len, block_size);

    librbd::image_info_t info;
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(size, info.size);
    ASSERT_EQ(5, info.num_objs);
    ASSERT_EQ(4 << 20, info.obj_size);
    ASSERT_EQ(22, info.order);

    uint64_t object_size = 0;
    if (whole_object) {
      object_size = 1 << info.order;
    }

    std::vector<diff_extent> extents;
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_EQ(-ENOENT, image.diff_iterate2("snap1", 0, size, true,
                                           whole_object, vector_cb, &extents));

    ASSERT_EQ(0, image.snap_create("snap1"));

    ceph::bufferlist bl;
    bl.append(std::string(len, '1'));
    ASSERT_EQ(len, image.write(off1 + object_off, len, bl));
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    extents.clear();

    ASSERT_EQ(0, image.snap_create("snap2"));

    ASSERT_EQ(len, image.write(off2 + object_off, len, bl));
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    ASSERT_EQ(0, image.snap_create("snap3"));

    // 1. beginning of time -> HEAD
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    // 2. snap1 -> HEAD
    ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    // 3. snap2 -> HEAD
    ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[0]);
    extents.clear();

    // 4. snap3 -> HEAD
    ASSERT_EQ(0, image.diff_iterate2("snap3", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_NO_FATAL_FAILURE(validate_object_map(image));
    ASSERT_EQ(0, image.snap_set("snap3"));

    // 5. beginning of time -> snap3
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    // 6. snap1 -> snap3
    ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[1]);
    extents.clear();

    // 7. snap2 -> snap3
    ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off2, extent_len, true, object_size), extents[0]);
    extents.clear();

    // 8. snap3 -> snap3
    ASSERT_EQ(0, image.diff_iterate2("snap3", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_NO_FATAL_FAILURE(validate_object_map(image));
    ASSERT_EQ(0, image.snap_set("snap2"));

    // 9. beginning of time -> snap2
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    extents.clear();

    // 10. snap1 -> snap2
    ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(off1, extent_len, true, object_size), extents[0]);
    extents.clear();

    // 11. snap2 -> snap2
    ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    // 12. snap3 -> snap2
    ASSERT_EQ(-EINVAL, image.diff_iterate2("snap3", 0, size, true,
                                           whole_object, vector_cb, &extents));

    ASSERT_NO_FATAL_FAILURE(validate_object_map(image));
    ASSERT_EQ(0, image.snap_set("snap1"));

    // 13. beginning of time -> snap1
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    // 14. snap1 -> snap1
    ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, whole_object,
                                     vector_cb, &extents));
    ASSERT_EQ(0u, extents.size());

    // 15. snap2 -> snap1
    ASSERT_EQ(-EINVAL, image.diff_iterate2("snap2", 0, size, true,
                                           whole_object, vector_cb, &extents));

    // 16. snap3 -> snap1
    ASSERT_EQ(-EINVAL, image.diff_iterate2("snap3", 0, size, true,
                                           whole_object, vector_cb, &extents));

    ASSERT_NO_FATAL_FAILURE(validate_object_map(image));
  }
};

struct Exact {
  static constexpr bool whole_object = false;
};

struct WholeObject {
  static constexpr bool whole_object = true;
};

using DiffIterateDeterministicTestTypes = ::testing::Types<Exact, WholeObject>;
TYPED_TEST_SUITE(DiffIterateDeterministicTest, DiffIterateDeterministicTestTypes);

TYPED_TEST(DiffIterateDeterministicTest, Basic)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));

  EXPECT_NO_FATAL_FAILURE(this->test_deterministic(0, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic((1 << 20) - 256, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic((1 << 20) - 128, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic(1 << 20, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic((4 << 20) - 256, 256));
}

TYPED_TEST(DiffIterateDeterministicTest, BasicPP)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));

  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_pp(0, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_pp((3 << 20) - 2, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_pp((3 << 20) - 1, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_pp(3 << 20, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_pp((4 << 20) - 2, 2));
}

#ifdef HAVE_LIBCRYPTSETUP

TYPED_TEST(DiffIterateDeterministicTest, LUKS1)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1(0, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1((1 << 20) - 256, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1((1 << 20) - 128, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1(1 << 20, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1((4 << 20) - 256, 256));
}

TYPED_TEST(DiffIterateDeterministicTest, LUKS1PP)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1_pp(0, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1_pp((3 << 20) - 2, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1_pp((3 << 20) - 1, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1_pp(3 << 20, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks1_pp((4 << 20) - 2, 2));
}

TYPED_TEST(DiffIterateDeterministicTest, LUKS2)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2(0, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2((1 << 20) - 256, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2((1 << 20) - 128, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2(1 << 20, 256));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2((4 << 20) - 256, 256));
}

TYPED_TEST(DiffIterateDeterministicTest, LUKS2PP)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2_pp(0, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2_pp((3 << 20) - 2, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2_pp((3 << 20) - 1, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2_pp(3 << 20, 2));
  EXPECT_NO_FATAL_FAILURE(this->test_deterministic_luks2_pp((4 << 20) - 2, 2));
}

#endif // HAVE_LIBCRYPTSETUP
