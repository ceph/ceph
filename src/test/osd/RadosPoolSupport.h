// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

/**
 * Declarative pool-support gating for ceph_test_rados.
 *
 * Replicated pools are never gated.  Erasure-coded pools may lack features
 * that some ops need; such ops are dropped (with a warning) instead of
 * failing the run.
 *
 * Adding a new pool support type:
 *   1. add a bit to PoolSupport,
 *   2. add one row to pool_support_infos[] giving its "osd pool get" name.
 *   (detect_pool_support() and the warning text pick it up automatically.)
 *
 * Adding a new requirement set:
 *   define a named constant next to SUPPORT_WRITE and say why it is needed.
 *
 * Adding a new op or feature that has a requirement is done in the op /
 * feature tables (see TestRados.cc); no code in this header changes.
 *
 * This header deliberately does not depend on RadosModel.h: the op id type
 * and the op table row type are template parameters, which keeps the logic
 * unit-testable without a cluster.
 */

#include <cstddef>
#include <iterator>
#include <map>
#include <string>
#include <vector>

#include "test/osd/RadosTestHelpers.h"

/// Capabilities an erasure-coded pool may or may not have.
enum PoolSupport : unsigned {
  SUPPORT_EC_OVERWRITES       = 1u << 0,
  SUPPORT_EC_OPTIMIZATIONS    = 1u << 1,
  SUPPORT_PRESERVE_ALLOCATION = 1u << 2,
  SUPPORT_OMAP                = 1u << 3,
};

struct PoolSupportInfo {
  unsigned flag;
  /// The "osd pool get" variable; also used in messages.
  const char *name;
  bool ec_only;
};

inline constexpr PoolSupportInfo pool_support_infos[] = {
  { SUPPORT_EC_OVERWRITES,       "allow_ec_overwrites",    true },
  { SUPPORT_EC_OPTIMIZATIONS,    "allow_ec_optimizations", true },
  { SUPPORT_PRESERVE_ALLOCATION, "preserve_allocation",    true },
  { SUPPORT_OMAP,                "supports_omap",          false },
};

/// Requirement sets.
inline constexpr unsigned SUPPORT_NONE = 0;
/// Omap operations.
inline constexpr unsigned SUPPORT_OMAP_OPS = SUPPORT_OMAP;
/// Partial-object writes to an EC pool need overwrites.
inline constexpr unsigned SUPPORT_WRITE = SUPPORT_EC_OVERWRITES;
/// Sparse reads and mapext rely on the OSD tracking allocated extents
inline constexpr unsigned SUPPORT_ALLOCATION_TRACKING =
  SUPPORT_EC_OPTIMIZATIONS | SUPPORT_PRESERVE_ALLOCATION;

/// Render a mask as "allow_ec_overwrites, preserve_allocation".
inline std::string pool_support_names(unsigned mask)
{
  std::string out;
  for (const auto& info : pool_support_infos) {
    if (mask & info.flag) {
      if (!out.empty()) {
	out += ", ";
      }
      out += info.name;
    }
  }
  return out;
}

/// Support bits that every pool type reports (not erasure-only).
inline constexpr unsigned all_pools_support_mask()
{
  unsigned mask = 0;
  for (const auto& info : pool_support_infos) {
    if (!info.ec_only) {
      mask |= info.flag;
    }
  }
  return mask;
}

struct PoolSupportLevel {
  bool is_ec = false;
  unsigned have = 0;

  unsigned missing(unsigned required) const
  {
    if (!is_ec) {
      required &= all_pools_support_mask();
    }
    return required & ~have;
  }
};

/**
 * Query every pool_support_infos[] variable and record the result.
 *
 * "osd pool get" returns -EACCES for an erasure-only variable on a
 * replicated pool.  That identifies the pool as replicated, and the remaining
 * erasure-only variables are not queried.
 *
 * Any other failed query is reported through @p warn and the variable is
 * treated as absent, except for variables every pool has (omap), which are
 * treated as present so a monitor error does not silently turn them off.
 */
template <class WarnFn>
PoolSupportLevel detect_pool_support(const std::string& pool,
				     librados::Rados& rados,
				     WarnFn warn)
{
  PoolSupportLevel level;
  bool replicated = false;

  for (const auto& info : pool_support_infos) {
    if (replicated && info.ec_only) {
      continue;
    }
    auto q = query_pool_flag(pool, info.name, rados);
    if (q) {
      if (*q) {
	level.have |= info.flag;
      }
    } else if (info.ec_only && q.error() == -EACCES) {
      replicated = true;
    } else {
      warn("Warning: failed to query " + std::string(info.name) +
	   " for pool " + pool + ": " + std::to_string(q.error()));
      if (!info.ec_only) {
	level.have |= info.flag;
      }
    }
  }
  level.is_ec = !replicated;
  return level;
}

/// Common message body for ops and features.
inline std::string pool_support_reason(const std::string& pool,
				       unsigned required, unsigned missing)
{
  return "pool " + pool + " requires " + pool_support_names(required) +
    " (missing: " + pool_support_names(missing) + ")";
}

template <class Op>
struct OpFilterResult {
  std::map<Op, unsigned> weights;
  std::vector<std::string> warnings;
};

/**
 * Drop ops whose requirements the pool does not meet.  Contains no op names:
 * @p table rows need members @c op, @c name and @c required_ec_support.
 */
template <class Op, class Row, std::size_t N>
OpFilterResult<Op> filter_unsupported_ops(const std::map<Op, unsigned>& weights,
					  const Row (&table)[N],
					  const std::string& pool,
					  const PoolSupportLevel& support)
{
  OpFilterResult<Op> result;
  for (const auto& [op, weight] : weights) {
    const Row *row = nullptr;
    for (const auto& candidate : table) {
      if (candidate.op == op) {
	row = &candidate;
	break;
      }
    }
    unsigned missing = row ? support.missing(row->required_ec_support) : 0;
    if (missing) {
      result.warnings.push_back(
	std::string("Warning: skipping op ") + row->name + ": " +
	pool_support_reason(pool, row->required_ec_support, missing));
    } else {
      result.weights.emplace(op, weight);
    }
  }
  return result;
}

/// Compile-time helper: true if every enumerator in [0, count) appears in
/// @p table exactly once and the table has exactly @p count rows.
template <class Row, std::size_t N>
constexpr bool op_table_is_complete(const Row (&table)[N], std::size_t count)
{
  if (N != count) {
    return false;
  }
  for (std::size_t id = 0; id < count; ++id) {
    std::size_t seen = 0;
    for (const auto& row : table) {
      if (static_cast<std::size_t>(row.op) == id) {
	++seen;
      }
    }
    if (seen != 1) {
      return false;
    }
  }
  return true;
}

/// Non-op features that need pool support (sparse reads, omap).
enum PoolFeature {
  POOL_FEATURE_SPARSE_READ,
  POOL_FEATURE_OMAP,
};

struct PoolFeatureRequirement {
  PoolFeature feature;
  const char *name;
  unsigned required_support;
};

struct UnsupportedFeature {
  PoolFeature feature;
  std::string warning;
};

/// Same check and message format as filter_unsupported_ops().
template <std::size_t N>
std::vector<UnsupportedFeature> unsupported_features(
  const PoolFeatureRequirement (&table)[N],
  const std::string& pool,
  const PoolSupportLevel& support)
{
  std::vector<UnsupportedFeature> out;
  for (const auto& req : table) {
    if (unsigned missing = support.missing(req.required_support)) {
      out.push_back({req.feature,
		     std::string("Warning: disabling ") + req.name + ": " +
		     pool_support_reason(pool, req.required_support, missing)});
    }
  }
  return out;
}
