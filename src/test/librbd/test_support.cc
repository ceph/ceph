// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_support.h"
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

int create_image_pp(librbd::RBD &rbd, librados::IoCtx &ioctx,
                    const std::string &name, uint64_t size) {
  uint64_t features = 0;
  get_features(&features);
  int order = 0;
  return rbd.create2(ioctx, name.c_str(), size, features, &order);
}
