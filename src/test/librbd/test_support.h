// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/int_types.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include <string>

static const uint64_t IMAGE_STRIPE_UNIT = 65536;
static const uint64_t IMAGE_STRIPE_COUNT = 16;

#define TEST_IO_SIZE 512
#define TEST_IO_TO_SNAP_SIZE 80

bool get_features(uint64_t *features);
bool is_feature_enabled(uint64_t feature);
int create_image_pp(librbd::RBD &rbd, librados::IoCtx &ioctx,
                    const std::string &name, uint64_t size);
int create_image_full_pp(librbd::RBD &rbd, librados::IoCtx &ioctx,
                         const std::string &name, uint64_t size,
                         uint64_t features, bool old_format, int *order);
int clone_image_pp(librbd::RBD &rbd, librbd::Image &p_image, librados::IoCtx &p_ioctx,
                   const char *p_name, const char *p_snap_name, librados::IoCtx &c_ioctx,
                   const char *c_name, uint64_t features);
int get_image_id(librbd::Image &image, std::string *image_id);
int create_image_data_pool(librados::Rados &rados, std::string &data_pool, bool *created);

bool is_librados_test_stub(librados::Rados &rados);

bool is_rbd_pwl_enabled(ceph::common::CephContext *ctx);

#define REQUIRE(x) {			  \
  if (!(x)) {				  \
    GTEST_SKIP() << "Skipping due to unmet REQUIRE"; \
  } 					  \
}

#define REQUIRE_FEATURE(feature) REQUIRE(is_feature_enabled(feature))
#define REQUIRE_FORMAT_V1() REQUIRE(!is_feature_enabled(0))
#define REQUIRE_FORMAT_V2() REQUIRE_FEATURE(0)
