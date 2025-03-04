// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

/* This feature set defines a set of features supported by OSDs once a PG has
 * gone active.
 * Mechanically, pretty much the same as include/ceph_features.h */

using pg_feature_vec_t = uint64_t;
static constexpr pg_feature_vec_t PG_FEATURE_INCARNATION_1 = 0ull;

#define DEFINE_PG_FEATURE(bit, incarnation, name)			\
  static constexpr pg_feature_vec_t PG_FEATURE_##name = (1ull << bit);	\
  static constexpr pg_feature_vec_t PG_FEATUREMASK_##name =		\
    (1ull << bit) | PG_FEATURE_INCARNATION_##incarnation;

#define PG_HAVE_FEATURE(x, name)				\
  (((x) & (PG_FEATUREMASK_##name)) == (PG_FEATUREMASK_##name))

DEFINE_PG_FEATURE(0, 1, PCT)

static constexpr pg_feature_vec_t PG_FEATURE_NONE = 0ull;
static constexpr pg_feature_vec_t PG_FEATURE_CRIMSON_ALL = 0ull;
static constexpr pg_feature_vec_t PG_FEATURE_CLASSIC_ALL =
  PG_FEATURE_PCT;
