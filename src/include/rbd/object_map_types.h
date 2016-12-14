// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_RBD_OBJECT_MAP_TYPES_H
#define CEPH_RBD_OBJECT_MAP_TYPES_H

#include "include/int_types.h"

static const uint8_t OBJECT_NONEXISTENT  = 0;
static const uint8_t OBJECT_EXISTS       = 1;
static const uint8_t OBJECT_PENDING      = 2;
static const uint8_t OBJECT_EXISTS_CLEAN = 3;

enum {
  OBJECT_MAP_BATCH_LEVEL0 = 0,
  OBJECT_MAP_BATCH_LEVEL1,
  OBJECT_MAP_BATCH_MAX,
};

static const uint8_t OBJECT_MAP_VIEW_LEVELS = OBJECT_MAP_BATCH_MAX;

#endif // CEPH_RBD_OBJECT_MAP_TYPES_H
