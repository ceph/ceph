// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/int_types.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include <string>

bool get_features(uint64_t *features);
bool is_feature_enabled(uint64_t feature);
int create_image_pp(librbd::RBD &rbd, librados::IoCtx &ioctx,
                    const std::string &name, uint64_t size);

#define REQUIRE_FEATURE(feature) { 	  \
  if (!is_feature_enabled(feature)) { 	  \
    std::cout << "SKIPPING" << std::endl; \
    return SUCCEED(); 			  \
  } 					  \
}

#define REQUIRE_FORMAT_V1() { 	          \
  if (is_feature_enabled(0)) { 	          \
    std::cout << "SKIPPING" << std::endl; \
    return SUCCEED(); 			  \
  } 					  \
}

#define REQUIRE_FORMAT_V2() REQUIRE_FEATURE(0)
