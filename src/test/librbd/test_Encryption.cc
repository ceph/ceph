// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/rbd/librbd.h"
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/api/Image.h"
#include "librbd/api/Io.h"
#include "librbd/internal.h"
#include "librbd/io/ReadResult.h"

void register_test_encryption() {
}

#ifdef HAVE_LIBCRYPTSETUP

class TestEncryption : public TestFixture {
public:
  static int format(librbd::ImageCtx* ictx, librbd::encryption_format_t format,
                    librbd::encryption_options_t opts, size_t opts_size,
                    bool c_api = false) {
    // format with insecure_fast_mode=true to save time and CPU cycles
    return librbd::api::Image<>::encryption_format(ictx, format, opts,
                                                   opts_size, true, c_api);
  }

  static int load(librbd::ImageCtx* ictx, librbd::encryption_format_t format,
                  librbd::encryption_options_t opts, size_t opts_size) {
    librbd::encryption_spec_t spec = {format, opts, opts_size};
    return load(ictx, &spec, 1);
  }

  static int load(librbd::ImageCtx* ictx,
                  const librbd::encryption_spec_t* specs, size_t spec_count) {
    return librbd::api::Image<>::encryption_load(ictx, specs, spec_count,
                                                 false);
  }

  void clone_image(librbd::ImageCtx* parent_ictx,
                   const std::string& parent_snap_name,
                   const std::string& clone_name) {
    ASSERT_EQ(0, snap_create(*parent_ictx, parent_snap_name));
    ASSERT_EQ(0, snap_protect(*parent_ictx, parent_snap_name));

    librbd::ImageOptions opts;
    ASSERT_EQ(0, m_rbd.clone3(m_ioctx, parent_ictx->name.c_str(),
                              parent_snap_name.c_str(), m_ioctx,
                              clone_name.c_str(), opts));
  }
};

TEST_F(TestEncryption, BasicClone)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, _pool_name.c_str(), &ioctx);

  // create base image, write 'a's
  std::string name = get_temp_image_name();
  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, 256 << 20));

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(4, rbd_write(image, 0, 4, "aaaa"));
  ASSERT_EQ(0, rbd_flush(image));

  // clone, encrypt with LUKS1, write 'b's
  std::string child1_name = get_temp_image_name();
  ASSERT_NO_FATAL_FAILURE(clone_image(static_cast<librbd::ImageCtx*>(image),
                                      "snap", child1_name));

  rbd_image_t child1;
  ASSERT_EQ(0, rbd_open(ioctx, child1_name.c_str(), &child1, NULL));
  rbd_encryption_luks1_format_options_t child1_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "password",
          .passphrase_size = 8,
  };
  ASSERT_EQ(-EINVAL, rbd_encryption_load(
          child1, RBD_ENCRYPTION_FORMAT_LUKS1, &child1_opts,
          sizeof(child1_opts)));
  ASSERT_EQ(0, format(static_cast<librbd::ImageCtx*>(child1),
                      RBD_ENCRYPTION_FORMAT_LUKS1, &child1_opts,
                      sizeof(child1_opts), true));
  ASSERT_EQ(0, rbd_encryption_load(
          child1, RBD_ENCRYPTION_FORMAT_LUKS1, &child1_opts,
          sizeof(child1_opts)));
  ASSERT_EQ(4, rbd_write(child1, 64 << 20, 4, "bbbb"));
  ASSERT_EQ(0, rbd_flush(child1));

  // clone, encrypt with LUKS2 (same passphrase), write 'c's
  std::string child2_name = get_temp_image_name();
  ASSERT_NO_FATAL_FAILURE(clone_image(static_cast<librbd::ImageCtx*>(child1),
                                      "snap", child2_name));

  rbd_image_t child2;
  ASSERT_EQ(0, rbd_open(ioctx, child2_name.c_str(), &child2, NULL));
  rbd_encryption_luks2_format_options_t child2_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "password",
          .passphrase_size = 8,
  };
  ASSERT_EQ(0, format(static_cast<librbd::ImageCtx*>(child2),
                      RBD_ENCRYPTION_FORMAT_LUKS2, &child2_opts,
                      sizeof(child2_opts), true));
  rbd_encryption_luks_format_options_t child2_lopts = {
          .passphrase = "password",
          .passphrase_size = 8,
  };
  ASSERT_EQ(0, rbd_encryption_load(
          child2, RBD_ENCRYPTION_FORMAT_LUKS, &child2_lopts,
          sizeof(child2_lopts)));
  ASSERT_EQ(4, rbd_write(child2, 128 << 20, 4, "cccc"));
  ASSERT_EQ(0, rbd_flush(child2));

  // clone, encrypt with LUKS2 (different passphrase)
  std::string child3_name = get_temp_image_name();
  ASSERT_NO_FATAL_FAILURE(clone_image(static_cast<librbd::ImageCtx*>(child2),
                                      "snap", child3_name));

  rbd_image_t child3;
  ASSERT_EQ(0, rbd_open(ioctx, child3_name.c_str(), &child3, NULL));
  rbd_encryption_luks2_format_options_t child3_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "12345678",
          .passphrase_size = 8,
  };
  ASSERT_EQ(0, format(static_cast<librbd::ImageCtx*>(child3),
                      RBD_ENCRYPTION_FORMAT_LUKS2, &child3_opts,
                      sizeof(child3_opts), true));
  ASSERT_EQ(-EPERM, rbd_encryption_load(
        child3, RBD_ENCRYPTION_FORMAT_LUKS2, &child3_opts,
        sizeof(child3_opts)));

  // verify child3 data
  rbd_encryption_spec_t specs[] = {
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child3_opts,
            .opts_size = sizeof(child3_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child2_opts,
            .opts_size = sizeof(child2_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS1,
            .opts = &child1_opts,
            .opts_size = sizeof(child1_opts)}
  };

  ASSERT_EQ(0, rbd_encryption_load2(child3, specs, 3));

  char read_data[5] = {0};
  ASSERT_EQ(4, rbd_read(child3, 0, 4, read_data));
  ASSERT_STREQ("aaaa", read_data);
  ASSERT_EQ(4, rbd_read(child3, 64 << 20, 4, read_data));
  ASSERT_STREQ("bbbb", read_data);
  ASSERT_EQ(4, rbd_read(child3, 128 << 20, 4, read_data));
  ASSERT_STREQ("cccc", read_data);

  // clone without formatting
  std::string child4_name = get_temp_image_name();
  ASSERT_NO_FATAL_FAILURE(clone_image(static_cast<librbd::ImageCtx*>(child3),
                                      "snap", child4_name));

  rbd_image_t child4;
  ASSERT_EQ(0, rbd_open(ioctx, child4_name.c_str(), &child4, NULL));
  rbd_encryption_spec_t child4_specs[] = {
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child3_opts,
            .opts_size = sizeof(child3_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child3_opts,
            .opts_size = sizeof(child3_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child2_opts,
            .opts_size = sizeof(child2_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS1,
            .opts = &child1_opts,
            .opts_size = sizeof(child1_opts)}
  };

  ASSERT_EQ(0, rbd_encryption_load2(child4, child4_specs, 4));

  // flatten child4
  ASSERT_EQ(0, rbd_flatten(child4));

  // reopen child4 and load encryption
  ASSERT_EQ(0, rbd_close(child4));
  ASSERT_EQ(0, rbd_open(ioctx, child4_name.c_str(), &child4, NULL));
  ASSERT_EQ(0, rbd_encryption_load(
          child4, RBD_ENCRYPTION_FORMAT_LUKS2, &child3_opts,
          sizeof(child3_opts)));

  // verify flattend image
  ASSERT_EQ(4, rbd_read(child4, 0, 4, read_data));
  ASSERT_STREQ("aaaa", read_data);
  ASSERT_EQ(4, rbd_read(child4, 64 << 20, 4, read_data));
  ASSERT_STREQ("bbbb", read_data);
  ASSERT_EQ(4, rbd_read(child4, 128 << 20, 4, read_data));
  ASSERT_STREQ("cccc", read_data);

  ASSERT_EQ(0, rbd_close(child4));
  ASSERT_EQ(0, rbd_remove(ioctx, child4_name.c_str()));
  ASSERT_EQ(0, rbd_snap_unprotect(child3, "snap"));
  ASSERT_EQ(0, rbd_snap_remove(child3, "snap"));
  ASSERT_EQ(0, rbd_close(child3));
  ASSERT_EQ(0, rbd_remove(ioctx, child3_name.c_str()));
  ASSERT_EQ(0, rbd_snap_unprotect(child2, "snap"));
  ASSERT_EQ(0, rbd_snap_remove(child2, "snap"));
  ASSERT_EQ(0, rbd_close(child2));
  ASSERT_EQ(0, rbd_remove(ioctx, child2_name.c_str()));
  ASSERT_EQ(0, rbd_snap_unprotect(child1, "snap"));
  ASSERT_EQ(0, rbd_snap_remove(child1, "snap"));
  ASSERT_EQ(0, rbd_close(child1));
  ASSERT_EQ(0, rbd_remove(ioctx, child1_name.c_str()));
  ASSERT_EQ(0, rbd_snap_unprotect(image, "snap"));
  ASSERT_EQ(0, rbd_snap_remove(image, "snap"));
  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_remove(ioctx, name.c_str()));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestEncryption, LUKS1UnderLUKS2WithoutResize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  std::string parent_name = get_temp_image_name();
  std::string clone_name = get_temp_image_name();
  uint64_t data_size = 25 << 20;
  uint64_t luks1_meta_size = 4 << 20;
  uint64_t luks2_meta_size = 16 << 20;
  std::string parent_passphrase = "parent passphrase";
  std::string clone_passphrase = "clone passphrase";

  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, parent_name,
                               luks1_meta_size + data_size, 22));
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(parent_name, &ictx));
  librbd::encryption_luks1_format_options_t luks1_opts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, parent_passphrase};
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts,
                      sizeof(luks1_opts)));
  ceph::bufferlist bl;
  bl.append(std::string(data_size, 'a'));
  ASSERT_EQ(static_cast<ssize_t>(data_size),
            librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));
  ASSERT_NO_FATAL_FAILURE(clone_image(ictx, "snap", clone_name));
  close_image(ictx);

  ASSERT_EQ(0, open_image(clone_name, &ictx));
  librbd::encryption_luks2_format_options_t luks2_opts =
      {RBD_ENCRYPTION_ALGORITHM_AES256, clone_passphrase};
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts,
                      sizeof(luks2_opts)));
  librbd::encryption_luks_format_options_t opts1 = {parent_passphrase};
  librbd::encryption_luks_format_options_t opts2 = {clone_passphrase};
  librbd::encryption_spec_t specs[] = {
      {RBD_ENCRYPTION_FORMAT_LUKS, &opts2, sizeof(opts2)},
      {RBD_ENCRYPTION_FORMAT_LUKS, &opts1, sizeof(opts1)}};
  ASSERT_EQ(0, load(ictx, specs, std::size(specs)));
  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  EXPECT_EQ(data_size + luks1_meta_size - luks2_meta_size, size);
  uint64_t overlap;
  ASSERT_EQ(0, librbd::get_overlap(ictx, &overlap));
  EXPECT_EQ(data_size + luks1_meta_size - luks2_meta_size, overlap);
  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(
      data_size + luks1_meta_size - luks2_meta_size, 'a'));
  ceph::bufferlist read_bl;
  ASSERT_EQ(static_cast<ssize_t>(expected_bl.length()),
            librbd::api::Io<>::read(*ictx, 0, expected_bl.length(),
                                    librbd::io::ReadResult{&read_bl}, 0));
  EXPECT_TRUE(expected_bl.contents_equal(read_bl));
}

TEST_F(TestEncryption, LUKS2UnderLUKS1WithoutResize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  std::string parent_name = get_temp_image_name();
  std::string clone_name = get_temp_image_name();
  uint64_t data_size = 25 << 20;
  uint64_t luks1_meta_size = 4 << 20;
  uint64_t luks2_meta_size = 16 << 20;
  std::string parent_passphrase = "parent passphrase";
  std::string clone_passphrase = "clone passphrase";

  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, parent_name,
                               luks2_meta_size + data_size, 22));
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(parent_name, &ictx));
  librbd::encryption_luks2_format_options_t luks2_opts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, parent_passphrase};
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts,
                      sizeof(luks2_opts)));
  ceph::bufferlist bl;
  bl.append(std::string(data_size, 'a'));
  ASSERT_EQ(static_cast<ssize_t>(data_size),
            librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));
  ASSERT_NO_FATAL_FAILURE(clone_image(ictx, "snap", clone_name));
  close_image(ictx);

  ASSERT_EQ(0, open_image(clone_name, &ictx));
  librbd::encryption_luks1_format_options_t luks1_opts =
      {RBD_ENCRYPTION_ALGORITHM_AES256, clone_passphrase};
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts,
                      sizeof(luks1_opts)));
  librbd::encryption_luks_format_options_t opts1 = {parent_passphrase};
  librbd::encryption_luks_format_options_t opts2 = {clone_passphrase};
  librbd::encryption_spec_t specs[] = {
      {RBD_ENCRYPTION_FORMAT_LUKS, &opts2, sizeof(opts2)},
      {RBD_ENCRYPTION_FORMAT_LUKS, &opts1, sizeof(opts1)}};
  ASSERT_EQ(0, load(ictx, specs, std::size(specs)));
  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  EXPECT_EQ(data_size + luks2_meta_size - luks1_meta_size, size);
  uint64_t overlap;
  ASSERT_EQ(0, librbd::get_overlap(ictx, &overlap));
  EXPECT_EQ(data_size, overlap);
  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(data_size, 'a'));
  expected_bl.append_zero(luks2_meta_size - luks1_meta_size);
  ceph::bufferlist read_bl;
  ASSERT_EQ(static_cast<ssize_t>(expected_bl.length()),
            librbd::api::Io<>::read(*ictx, 0, expected_bl.length(),
                                    librbd::io::ReadResult{&read_bl}, 0));
  EXPECT_TRUE(expected_bl.contents_equal(read_bl));
}

TEST_F(TestEncryption, EncryptionFormatNoData)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  std::string name = get_temp_image_name();
  uint64_t luks1_meta_size = 4 << 20;
  std::string passphrase = "some passphrase";

  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, luks1_meta_size - 1, 22));
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(name, &ictx));
  librbd::encryption_luks1_format_options_t fopts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  ASSERT_EQ(-ENOSPC, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                            sizeof(fopts)));
  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(luks1_meta_size - 1, size);
  close_image(ictx);

  ASSERT_EQ(0, open_image(name, &ictx));
  ASSERT_EQ(0, resize(ictx, luks1_meta_size));
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                      sizeof(fopts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(0, size);
}

TEST_F(TestEncryption, EncryptionLoadBadSize)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  std::string name = get_temp_image_name();
  uint64_t luks1_meta_size = 4 << 20;
  std::string passphrase = "some passphrase";

  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name, luks1_meta_size, 22));
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(name, &ictx));
  librbd::encryption_luks1_format_options_t fopts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                      sizeof(fopts)));
  close_image(ictx);

  ASSERT_EQ(0, open_image(name, &ictx));
  librbd::encryption_luks_format_options_t opts = {passphrase};
  ASSERT_EQ(0, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts, sizeof(opts)));
  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(0, size);
  close_image(ictx);

  ASSERT_EQ(0, open_image(name, &ictx));
  ASSERT_EQ(0, resize(ictx, luks1_meta_size - 1));
  ASSERT_EQ(-EINVAL, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                          sizeof(opts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(luks1_meta_size - 1, size);
}

TEST_F(TestEncryption, EncryptionLoadBadStripePattern)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  uint64_t features;
  ASSERT_TRUE(get_features(&features));

  std::string name1 = get_temp_image_name();
  std::string name2 = get_temp_image_name();
  std::string name3 = get_temp_image_name();
  std::string passphrase = "some passphrase";

  int order = 22;
  ASSERT_EQ(0, m_rbd.create3(m_ioctx, name1.c_str(), 20 << 20, features,
                             &order, 2 << 20, 2));
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(name1, &ictx));
  librbd::encryption_luks1_format_options_t fopts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                      sizeof(fopts)));
  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(12 << 20, size);
  close_image(ictx);

  ASSERT_EQ(0, open_image(name1, &ictx));
  // name2 - incompatible striping pattern
  librbd::ImageOptions image_opts;
  ASSERT_EQ(0, image_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, 1 << 20));
  ASSERT_EQ(0, image_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, 3));
  librbd::NoOpProgressContext prog_ctx;
  ASSERT_EQ(0, librbd::api::Image<>::deep_copy(ictx, m_ioctx, name2.c_str(),
                                               image_opts, prog_ctx));
  // name3 - different but compatible striping pattern
  ASSERT_EQ(0, image_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, 2));
  ASSERT_EQ(0, librbd::api::Image<>::deep_copy(ictx, m_ioctx, name3.c_str(),
                                               image_opts, prog_ctx));
  close_image(ictx);

  ASSERT_EQ(0, open_image(name2, &ictx));
  librbd::encryption_luks_format_options_t opts = {passphrase};
  ASSERT_EQ(-EINVAL, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                          sizeof(opts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(20 << 20, size);
  close_image(ictx);

  ASSERT_EQ(0, open_image(name3, &ictx));
  ASSERT_EQ(0, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts, sizeof(opts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(12 << 20, size);
}

TEST_F(TestEncryption, EncryptionLoadFormatMismatch)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  std::string name1 = get_temp_image_name();
  std::string name2 = get_temp_image_name();
  std::string name3 = get_temp_image_name();
  std::string passphrase = "some passphrase";

  librbd::encryption_luks1_format_options_t luks1_opts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  librbd::encryption_luks2_format_options_t luks2_opts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  librbd::encryption_luks_format_options_t luks_opts = {passphrase};

#define LUKS_ONE {RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)}
#define LUKS_TWO {RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)}
#define LUKS_ANY {RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)}

  const std::vector<librbd::encryption_spec_t> bad_specs[] = {
      {},
      {LUKS_ONE},
      {LUKS_TWO},
      {LUKS_ONE, LUKS_ONE},
      {LUKS_ONE, LUKS_TWO},
      {LUKS_ONE, LUKS_ANY},
      {LUKS_TWO, LUKS_TWO},
      {LUKS_ANY, LUKS_TWO},
      {LUKS_ONE, LUKS_ONE, LUKS_ONE},
      {LUKS_ONE, LUKS_ONE, LUKS_TWO},
      {LUKS_ONE, LUKS_ONE, LUKS_ANY},
      {LUKS_ONE, LUKS_TWO, LUKS_ONE},
      {LUKS_ONE, LUKS_TWO, LUKS_TWO},
      {LUKS_ONE, LUKS_TWO, LUKS_ANY},
      {LUKS_ONE, LUKS_ANY, LUKS_ONE},
      {LUKS_ONE, LUKS_ANY, LUKS_TWO},
      {LUKS_ONE, LUKS_ANY, LUKS_ANY},
      {LUKS_TWO, LUKS_ONE, LUKS_TWO},
      {LUKS_TWO, LUKS_TWO, LUKS_ONE},
      {LUKS_TWO, LUKS_TWO, LUKS_TWO},
      {LUKS_TWO, LUKS_TWO, LUKS_ANY},
      {LUKS_TWO, LUKS_ANY, LUKS_TWO},
      {LUKS_ANY, LUKS_ONE, LUKS_TWO},
      {LUKS_ANY, LUKS_TWO, LUKS_ONE},
      {LUKS_ANY, LUKS_TWO, LUKS_TWO},
      {LUKS_ANY, LUKS_TWO, LUKS_ANY},
      {LUKS_ANY, LUKS_ANY, LUKS_TWO},
      {LUKS_ANY, LUKS_ANY, LUKS_ANY, LUKS_ANY}};

  const std::vector<librbd::encryption_spec_t> good_specs[] = {
      {LUKS_ANY},
      {LUKS_TWO, LUKS_ONE},
      {LUKS_TWO, LUKS_ANY},
      {LUKS_ANY, LUKS_ONE},
      {LUKS_ANY, LUKS_ANY},
      {LUKS_TWO, LUKS_ONE, LUKS_ONE},
      {LUKS_TWO, LUKS_ONE, LUKS_ANY},
      {LUKS_TWO, LUKS_ANY, LUKS_ONE},
      {LUKS_TWO, LUKS_ANY, LUKS_ANY},
      {LUKS_ANY, LUKS_ONE, LUKS_ONE},
      {LUKS_ANY, LUKS_ONE, LUKS_ANY},
      {LUKS_ANY, LUKS_ANY, LUKS_ONE},
      {LUKS_ANY, LUKS_ANY, LUKS_ANY}};

  static_assert(std::size(bad_specs) + std::size(good_specs) == 1 + 3 + 9 + 27 + 1);

#undef LUKS_ONE
#undef LUKS_TWO
#undef LUKS_ANY

  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name1, 40 << 20));
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(name1, &ictx));
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts,
                      sizeof(luks1_opts)));
  ASSERT_NO_FATAL_FAILURE(clone_image(ictx, "snap", name2));
  close_image(ictx);

  ASSERT_EQ(0, open_image(name2, &ictx));
  ASSERT_NO_FATAL_FAILURE(clone_image(ictx, "snap", name3));
  close_image(ictx);

  ASSERT_EQ(0, open_image(name3, &ictx));
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts,
                      sizeof(luks2_opts)));
  close_image(ictx);

  for (auto& specs : good_specs) {
    ASSERT_EQ(0, open_image(name3, &ictx));
    ASSERT_EQ(0, load(ictx, specs.data(), specs.size()));
    close_image(ictx);
  }

  ASSERT_EQ(0, open_image(name3, &ictx));
  for (auto& specs : bad_specs) {
    ASSERT_EQ(-EINVAL, load(ictx, specs.data(), specs.size()));
  }
}

TEST_F(TestEncryption, EncryptedResize)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  std::string name = get_temp_image_name();
  uint64_t luks2_meta_size = 16 << 20;
  uint64_t data_size = 10 << 20;
  std::string passphrase = "some passphrase";

  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, name,
                               luks2_meta_size + data_size, 22));
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(name, &ictx));
  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(luks2_meta_size + data_size, size);
  librbd::encryption_luks2_format_options_t fopts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                      sizeof(fopts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(data_size, size);
  ASSERT_EQ(0, resize(ictx, data_size * 3));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(data_size * 3, size);
  close_image(ictx);

  ASSERT_EQ(0, open_image(name, &ictx));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(luks2_meta_size + data_size * 3, size);
  librbd::encryption_luks_format_options_t opts = {passphrase};
  ASSERT_EQ(0, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts, sizeof(opts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(data_size * 3, size);
  ASSERT_EQ(0, resize(ictx, data_size / 2));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(data_size / 2, size);
  close_image(ictx);

  ASSERT_EQ(0, open_image(name, &ictx));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(luks2_meta_size + data_size / 2, size);
  ASSERT_EQ(0, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts, sizeof(opts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(data_size / 2, size);
  ASSERT_EQ(0, resize(ictx, 0));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(0, size);
  close_image(ictx);

  ASSERT_EQ(0, open_image(name, &ictx));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(luks2_meta_size, size);
  ASSERT_EQ(0, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts, sizeof(opts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(0, size);
  ASSERT_EQ(0, resize(ictx, data_size));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(data_size, size);
  close_image(ictx);

  ASSERT_EQ(0, open_image(name, &ictx));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(luks2_meta_size + data_size, size);
}

TEST_F(TestEncryption, EncryptedFlattenSmallData)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  std::string parent_name = get_temp_image_name();
  std::string clone_name = get_temp_image_name();
  uint64_t data_size = 5000;
  uint64_t luks2_meta_size = 16 << 20;
  std::string passphrase = "some passphrase";

  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, parent_name,
                               luks2_meta_size + data_size, 22));
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(parent_name, &ictx));
  librbd::encryption_luks2_format_options_t fopts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  ASSERT_EQ(0, format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                      sizeof(fopts)));
  ceph::bufferlist bl;
  bl.append(std::string(data_size, 'a'));
  ASSERT_EQ(static_cast<ssize_t>(data_size),
            librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));
  ASSERT_NO_FATAL_FAILURE(clone_image(ictx, "snap", clone_name));
  close_image(ictx);

  ASSERT_EQ(0, open_image(clone_name, &ictx));
  librbd::encryption_luks_format_options_t opts = {passphrase};
  ASSERT_EQ(0, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts, sizeof(opts)));
  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(data_size, size);
  uint64_t overlap;
  ASSERT_EQ(0, librbd::get_overlap(ictx, &overlap));
  ASSERT_EQ(data_size, overlap);
  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(data_size, 'a'));
  ceph::bufferlist read_bl1;
  ASSERT_EQ(static_cast<ssize_t>(expected_bl.length()),
            librbd::api::Io<>::read(*ictx, 0, expected_bl.length(),
                                    librbd::io::ReadResult{&read_bl1}, 0));
  ASSERT_TRUE(expected_bl.contents_equal(read_bl1));
  ASSERT_EQ(0, flatten(ictx));
  ceph::bufferlist read_bl2;
  ASSERT_EQ(static_cast<ssize_t>(expected_bl.length()),
            librbd::api::Io<>::read(*ictx, 0, expected_bl.length(),
                                    librbd::io::ReadResult{&read_bl2}, 0));
  ASSERT_TRUE(expected_bl.contents_equal(read_bl2));
  close_image(ictx);

  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ASSERT_EQ(0, load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts, sizeof(opts)));
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(data_size, size);
  ASSERT_EQ(0, librbd::get_overlap(ictx, &overlap));
  ASSERT_EQ(0, overlap);
  ceph::bufferlist read_bl3;
  ASSERT_EQ(static_cast<ssize_t>(expected_bl.length()),
            librbd::api::Io<>::read(*ictx, 0, expected_bl.length(),
                                    librbd::io::ReadResult{&read_bl3}, 0));
  ASSERT_TRUE(expected_bl.contents_equal(read_bl3));
}

struct LUKSOnePassphrase {
  int load(librbd::ImageCtx* ictx) {
    librbd::encryption_luks_format_options_t opts = {m_passphrase};
    return TestEncryption::load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                sizeof(opts));
  }

  int load_flattened(librbd::ImageCtx* ictx) {
    return load(ictx);
  }

  std::string m_passphrase = "some passphrase";
};

struct LUKSTwoPassphrases {
  int load(librbd::ImageCtx* ictx) {
    librbd::encryption_luks_format_options_t opts1 = {m_parent_passphrase};
    librbd::encryption_luks_format_options_t opts2 = {m_clone_passphrase};
    librbd::encryption_spec_t specs[] = {
        {RBD_ENCRYPTION_FORMAT_LUKS, &opts2, sizeof(opts2)},
        {RBD_ENCRYPTION_FORMAT_LUKS, &opts1, sizeof(opts1)}};
    return TestEncryption::load(ictx, specs, std::size(specs));
  }

  int load_flattened(librbd::ImageCtx* ictx) {
    librbd::encryption_luks_format_options_t opts = {m_clone_passphrase};
    return TestEncryption::load(ictx, RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                sizeof(opts));
  }

  std::string m_parent_passphrase = "parent passphrase";
  std::string m_clone_passphrase = "clone passphrase";
};

struct PlaintextUnderLUKS1 : LUKSOnePassphrase {
protected:
  void setup_parent(librbd::ImageCtx* ictx, uint64_t data_size) {
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(static_cast<ssize_t>(data_size),
              librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));

    // before taking a parent snapshot, (temporarily) add extra space
    // to the parent to account for upcoming LUKS1 header in the clone,
    // making the clone able to reach all parent data
    uint64_t luks1_meta_size = 4 << 20;
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size + luks1_meta_size));
  }

  void setup_clone(librbd::ImageCtx* ictx, uint64_t data_size) {
    librbd::encryption_luks1_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1,
                                        &fopts, sizeof(fopts)));
  }
};

struct PlaintextUnderLUKS2 : LUKSOnePassphrase {
  void setup_parent(librbd::ImageCtx* ictx, uint64_t data_size) {
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(static_cast<ssize_t>(data_size),
              librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));

    // before taking a parent snapshot, (temporarily) add extra space
    // to the parent to account for upcoming LUKS2 header in the clone,
    // making the clone able to reach all parent data
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size + luks2_meta_size));
  }

  void setup_clone(librbd::ImageCtx* ictx, uint64_t data_size) {
    librbd::encryption_luks2_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2,
                                        &fopts, sizeof(fopts)));
  }
};

struct UnformattedLUKS1 : LUKSOnePassphrase {
  void setup_parent(librbd::ImageCtx* ictx, uint64_t data_size) {
    uint64_t luks1_meta_size = 4 << 20;
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size + luks1_meta_size));
    librbd::encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1,
                                        &fopts, sizeof(fopts)));
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(static_cast<ssize_t>(data_size),
              librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));
  }

  void setup_clone(librbd::ImageCtx* ictx, uint64_t data_size) {}
};

struct LUKS1UnderLUKS1 : LUKSTwoPassphrases {
  void setup_parent(librbd::ImageCtx* ictx, uint64_t data_size) {
    uint64_t luks1_meta_size = 4 << 20;
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size + luks1_meta_size));
    librbd::encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_parent_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1,
                                        &fopts, sizeof(fopts)));
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(static_cast<ssize_t>(data_size),
              librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));
  }

  void setup_clone(librbd::ImageCtx* ictx, uint64_t data_size) {
    librbd::encryption_luks1_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_clone_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1,
                                        &fopts, sizeof(fopts)));
  }
};

struct LUKS1UnderLUKS2 : LUKSTwoPassphrases {
  void setup_parent(librbd::ImageCtx* ictx, uint64_t data_size) {
    uint64_t luks1_meta_size = 4 << 20;
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size + luks1_meta_size));
    librbd::encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_parent_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1,
                                        &fopts, sizeof(fopts)));
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(static_cast<ssize_t>(data_size),
              librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));

    // before taking a parent snapshot, (temporarily) add extra space
    // to the parent to account for upcoming LUKS2 header in the clone,
    // making the clone able to reach all parent data
    // space taken by LUKS1 header in the parent would be reused
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, TestEncryption::resize(
        ictx, data_size + luks2_meta_size - luks1_meta_size));
  }

  void setup_clone(librbd::ImageCtx* ictx, uint64_t data_size) {
    librbd::encryption_luks2_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_clone_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2,
                                        &fopts, sizeof(fopts)));
  }
};

struct UnformattedLUKS2 : LUKSOnePassphrase {
  void setup_parent(librbd::ImageCtx* ictx, uint64_t data_size) {
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size + luks2_meta_size));
    librbd::encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2,
                                        &fopts, sizeof(fopts)));
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(static_cast<ssize_t>(data_size),
              librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));
  }

  void setup_clone(librbd::ImageCtx* ictx, uint64_t data_size) {}
};

struct LUKS2UnderLUKS2 : LUKSTwoPassphrases {
  void setup_parent(librbd::ImageCtx* ictx, uint64_t data_size) {
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size + luks2_meta_size));
    librbd::encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_parent_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2,
                                        &fopts, sizeof(fopts)));
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(static_cast<ssize_t>(data_size),
              librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));
  }

  void setup_clone(librbd::ImageCtx* ictx, uint64_t data_size) {
    librbd::encryption_luks2_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_clone_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2,
                                        &fopts, sizeof(fopts)));
  }
};

struct LUKS2UnderLUKS1 : LUKSTwoPassphrases {
  void setup_parent(librbd::ImageCtx* ictx, uint64_t data_size) {
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size + luks2_meta_size));
    librbd::encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_parent_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS2,
                                        &fopts, sizeof(fopts)));
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(static_cast<ssize_t>(data_size),
              librbd::api::Io<>::write(*ictx, 0, data_size, std::move(bl), 0));
  }

  void setup_clone(librbd::ImageCtx* ictx, uint64_t data_size) {
    librbd::encryption_luks1_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_clone_passphrase};
    ASSERT_EQ(0, TestEncryption::format(ictx, RBD_ENCRYPTION_FORMAT_LUKS1,
                                        &fopts, sizeof(fopts)));

    // after loading encryption on the clone, one can get rid of
    // unneeded space allowance in the clone arising from LUKS2 header
    // in the parent being bigger than LUKS1 header in the clone
    ASSERT_EQ(0, load(ictx));
    ASSERT_EQ(0, TestEncryption::resize(ictx, data_size));
  }
};

template <typename FormatPolicy>
class EncryptedFlattenTest : public TestEncryption, FormatPolicy {
protected:
  void create_and_setup() {
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, m_parent_name, m_data_size,
                                 22));
    librbd::ImageCtx* ictx;
    ASSERT_EQ(0, open_image(m_parent_name, &ictx));
    ASSERT_NO_FATAL_FAILURE(FormatPolicy::setup_parent(ictx, m_data_size));
    ASSERT_NO_FATAL_FAILURE(clone_image(ictx, "snap", m_clone_name));
    close_image(ictx);

    ASSERT_EQ(0, open_image(m_clone_name, &ictx));
    ASSERT_NO_FATAL_FAILURE(FormatPolicy::setup_clone(ictx, m_data_size));
    close_image(ictx);
  }

  void open_and_load(librbd::ImageCtx** ictx) {
    ASSERT_EQ(0, open_image(m_clone_name, ictx));
    ASSERT_EQ(0, FormatPolicy::load(*ictx));
  }

  void open_and_load_flattened(librbd::ImageCtx** ictx) {
    ASSERT_EQ(0, open_image(m_clone_name, ictx));
    ASSERT_EQ(0, FormatPolicy::load_flattened(*ictx));
  }

  void verify_size_and_overlap(librbd::ImageCtx* ictx, uint64_t expected_size,
                               uint64_t expected_overlap) {
    uint64_t size;
    ASSERT_EQ(0, librbd::get_size(ictx, &size));
    EXPECT_EQ(expected_size, size);
    uint64_t overlap;
    ASSERT_EQ(0, librbd::get_overlap(ictx, &overlap));
    EXPECT_EQ(expected_overlap, overlap);
  }

  void verify_data(librbd::ImageCtx* ictx,
                   const ceph::bufferlist& expected_bl) {
    ceph::bufferlist read_bl;
    ASSERT_EQ(static_cast<ssize_t>(expected_bl.length()),
              librbd::api::Io<>::read(*ictx, 0, expected_bl.length(),
                                      librbd::io::ReadResult{&read_bl}, 0));
    EXPECT_TRUE(expected_bl.contents_equal(read_bl));
  }

  std::string m_parent_name = get_temp_image_name();
  std::string m_clone_name = get_temp_image_name();
  uint64_t m_data_size = 25 << 20;
};

using EncryptedFlattenTestTypes =
    ::testing::Types<PlaintextUnderLUKS1, PlaintextUnderLUKS2,
                     UnformattedLUKS1, LUKS1UnderLUKS1, LUKS1UnderLUKS2,
                     UnformattedLUKS2, LUKS2UnderLUKS2, LUKS2UnderLUKS1>;
TYPED_TEST_SUITE(EncryptedFlattenTest, EncryptedFlattenTestTypes);

TYPED_TEST(EncryptedFlattenTest, Simple)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_NO_FATAL_FAILURE(this->create_and_setup());

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(this->m_data_size, 'a'));

  librbd::ImageCtx* ictx;
  ASSERT_NO_FATAL_FAILURE(this->open_and_load(&ictx));
  this->verify_size_and_overlap(ictx, this->m_data_size, this->m_data_size);
  this->verify_data(ictx, expected_bl);
  ASSERT_EQ(0, this->flatten(ictx));
  this->verify_data(ictx, expected_bl);
  this->close_image(ictx);

  ASSERT_NO_FATAL_FAILURE(this->open_and_load_flattened(&ictx));
  this->verify_size_and_overlap(ictx, this->m_data_size, 0);
  this->verify_data(ictx, expected_bl);
}

TYPED_TEST(EncryptedFlattenTest, Grow)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_NO_FATAL_FAILURE(this->create_and_setup());

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(this->m_data_size, 'a'));
  expected_bl.append_zero(1);

  librbd::ImageCtx* ictx;
  ASSERT_NO_FATAL_FAILURE(this->open_and_load(&ictx));
  ASSERT_EQ(0, this->resize(ictx, this->m_data_size + 1));
  this->verify_size_and_overlap(ictx, this->m_data_size + 1,
                                this->m_data_size);
  this->verify_data(ictx, expected_bl);
  ASSERT_EQ(0, this->flatten(ictx));
  this->verify_data(ictx, expected_bl);
  this->close_image(ictx);

  ASSERT_NO_FATAL_FAILURE(this->open_and_load_flattened(&ictx));
  this->verify_size_and_overlap(ictx, this->m_data_size + 1, 0);
  this->verify_data(ictx, expected_bl);
}

TYPED_TEST(EncryptedFlattenTest, Shrink)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_NO_FATAL_FAILURE(this->create_and_setup());

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(this->m_data_size - 1, 'a'));

  librbd::ImageCtx* ictx;
  ASSERT_NO_FATAL_FAILURE(this->open_and_load(&ictx));
  ASSERT_EQ(0, this->resize(ictx, this->m_data_size - 1));
  this->verify_size_and_overlap(ictx, this->m_data_size - 1,
                                this->m_data_size - 1);
  this->verify_data(ictx, expected_bl);
  ASSERT_EQ(0, this->flatten(ictx));
  this->verify_data(ictx, expected_bl);
  this->close_image(ictx);

  ASSERT_NO_FATAL_FAILURE(this->open_and_load_flattened(&ictx));
  this->verify_size_and_overlap(ictx, this->m_data_size - 1, 0);
  this->verify_data(ictx, expected_bl);
}

TYPED_TEST(EncryptedFlattenTest, ShrinkToOne)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_NO_FATAL_FAILURE(this->create_and_setup());

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(1, 'a'));

  librbd::ImageCtx* ictx;
  ASSERT_NO_FATAL_FAILURE(this->open_and_load(&ictx));
  ASSERT_EQ(0, this->resize(ictx, 1));
  this->verify_size_and_overlap(ictx, 1, 1);
  this->verify_data(ictx, expected_bl);
  ASSERT_EQ(0, this->flatten(ictx));
  this->verify_data(ictx, expected_bl);
  this->close_image(ictx);

  ASSERT_NO_FATAL_FAILURE(this->open_and_load_flattened(&ictx));
  this->verify_size_and_overlap(ictx, 1, 0);
  this->verify_data(ictx, expected_bl);
}

TYPED_TEST(EncryptedFlattenTest, ShrinkToOneAfterSnapshot)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_NO_FATAL_FAILURE(this->create_and_setup());

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(1, 'a'));

  librbd::ImageCtx* ictx;
  ASSERT_NO_FATAL_FAILURE(this->open_and_load(&ictx));
  ASSERT_EQ(0, this->snap_create(*ictx, "snap"));
  ASSERT_EQ(0, this->resize(ictx, 1));
  this->verify_size_and_overlap(ictx, 1, 1);
  this->verify_data(ictx, expected_bl);
  ASSERT_EQ(0, this->flatten(ictx));
  this->verify_data(ictx, expected_bl);
  this->close_image(ictx);

  ASSERT_NO_FATAL_FAILURE(this->open_and_load_flattened(&ictx));
  this->verify_size_and_overlap(ictx, 1, 0);
  this->verify_data(ictx, expected_bl);
}

TYPED_TEST(EncryptedFlattenTest, MinOverlap)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_NO_FATAL_FAILURE(this->create_and_setup());

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(1, 'a'));
  expected_bl.append_zero(this->m_data_size - 1);

  librbd::ImageCtx* ictx;
  ASSERT_NO_FATAL_FAILURE(this->open_and_load(&ictx));
  ASSERT_EQ(0, this->resize(ictx, 1));
  ASSERT_EQ(0, this->resize(ictx, this->m_data_size));
  this->verify_size_and_overlap(ictx, this->m_data_size, 1);
  this->verify_data(ictx, expected_bl);
  ASSERT_EQ(0, this->flatten(ictx));
  this->verify_data(ictx, expected_bl);
  this->close_image(ictx);

  ASSERT_NO_FATAL_FAILURE(this->open_and_load_flattened(&ictx));
  this->verify_size_and_overlap(ictx, this->m_data_size, 0);
  this->verify_data(ictx, expected_bl);
}

TYPED_TEST(EncryptedFlattenTest, ZeroOverlap)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_NO_FATAL_FAILURE(this->create_and_setup());

  ceph::bufferlist expected_bl;
  expected_bl.append_zero(this->m_data_size);

  librbd::ImageCtx* ictx;
  ASSERT_NO_FATAL_FAILURE(this->open_and_load(&ictx));
  ASSERT_EQ(0, this->resize(ictx, 0));
  ASSERT_EQ(0, this->resize(ictx, this->m_data_size));
  this->verify_size_and_overlap(ictx, this->m_data_size, 0);
  this->verify_data(ictx, expected_bl);
  ASSERT_EQ(0, this->flatten(ictx));
  this->verify_data(ictx, expected_bl);
  this->close_image(ictx);

  ASSERT_NO_FATAL_FAILURE(this->open_and_load_flattened(&ictx));
  this->verify_size_and_overlap(ictx, this->m_data_size, 0);
  this->verify_data(ictx, expected_bl);
}

#endif // HAVE_LIBCRYPTSETUP
