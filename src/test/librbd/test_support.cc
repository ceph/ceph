// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include <sstream>

bool get_features(uint64_t *features) {
  const char *c = getenv("RBD_FEATURES");
  if (c == NULL) {
    return false;
  }

  std::stringstream ss(c);
  if (!(ss >> *features)) {
    return false;
  }
  return true;
}

bool is_feature_enabled(uint64_t feature) {
  uint64_t features;
  return (get_features(&features) && (features & feature) == feature);
}

int create_image_full_pp(librbd::RBD &rbd, librados::IoCtx &ioctx,
                         const std::string &name, uint64_t size,
                         uint64_t features, bool old_format, int *order)
{
  if (old_format) {
    librados::Rados rados(ioctx);
    int r = rados.conf_set("rbd_default_format", "1");
    if (r < 0) {
      return r;
    }
    return rbd.create(ioctx, name.c_str(), size, order);
  } else if ((features & RBD_FEATURE_STRIPINGV2) != 0) {
    uint64_t stripe_unit = IMAGE_STRIPE_UNIT;
    if (*order) {
      // use a conservative stripe_unit for non default order
      stripe_unit = (1ull << (*order-1));
    }

    printf("creating image with stripe unit: %" PRIu64 ", stripe count: %" PRIu64 "\n",
           stripe_unit, IMAGE_STRIPE_COUNT);
    return rbd.create3(ioctx, name.c_str(), size, features, order, stripe_unit,
                       IMAGE_STRIPE_COUNT);
  } else {
    return rbd.create2(ioctx, name.c_str(), size, features, order);
  }
}

int create_image_pp(librbd::RBD &rbd, librados::IoCtx &ioctx,
                    const std::string &name, uint64_t size) {
  int order = 0;
  uint64_t features = 0;
  if (!get_features(&features)) {
    // ensure old-format tests actually use the old format
    librados::Rados rados(ioctx);
    int r = rados.conf_set("rbd_default_format", "1");
    if (r < 0) {
      return r;
    }
    return rbd.create(ioctx, name.c_str(), size, &order);
  } else {
    return rbd.create2(ioctx, name.c_str(), size, features, &order);
  }
}

int clone_image_pp(librbd::RBD &rbd, librbd::Image &p_image, librados::IoCtx &p_ioctx,
                   const char *p_name, const char *p_snap_name, librados::IoCtx &c_ioctx,
                   const char *c_name, uint64_t features)
{
  uint64_t stripe_unit = p_image.get_stripe_unit();
  uint64_t stripe_count = p_image.get_stripe_count();

  librbd::image_info_t p_info;
  int r = p_image.stat(p_info, sizeof(p_info));
  if (r < 0) {
    return r;
  }

  int c_order = p_info.order;
  return rbd.clone2(p_ioctx, p_name, p_snap_name, c_ioctx, c_name,
                    features, &c_order, stripe_unit, stripe_count);
}

int get_image_id(librbd::Image &image, std::string *image_id)
{
  int r = image.get_id(image_id);
  if (r < 0) {
    return r;
  }
  return 0;
}

int create_image_data_pool(librados::Rados &rados, std::string &data_pool, bool *created) {
  std::string pool;
  int r = rados.conf_get("rbd_default_data_pool", pool);
  if (r != 0) {
    return r;
  } else if (pool.empty()) {
    return 0;
  }

  r = rados.pool_create(pool.c_str());
  if ((r == 0) || (r == -EEXIST)) {
    data_pool = pool;
    *created = (r == 0);
    return 0;
  }

  librados::IoCtx ioctx;
  r = rados.ioctx_create(pool.c_str(), ioctx);
  if (r < 0) {
    return r;
  }
  ioctx.application_enable("rbd", true);

  return r;
}
