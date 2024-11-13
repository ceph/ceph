// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "scrub_types.h"

#include <fmt/ranges.h>

template <>
struct fmt::formatter<librados::object_id_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &oid, FormatContext& ctx) const
  {
    return fmt::format_to(ctx.out(), "{}/{}/{}", oid.locator, oid.nspace, oid.name);
  }
};

template <>
struct fmt::formatter<librados::err_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &err, FormatContext& ctx) const
  {
    bool first = true;
#define F(FLAG_NAME)					\
    if (err.errors & librados::err_t::FLAG_NAME) {	\
      if (!first) {					\
	fmt::format_to(ctx.out(), "|");			\
      } else {						\
	first = false;					\
      }							\
      fmt::format_to(ctx.out(), #FLAG_NAME);		\
    }
    F(SHARD_MISSING);
    F(SHARD_STAT_ERR);
    F(SHARD_READ_ERR);
    F(DATA_DIGEST_MISMATCH_INFO);
    F(OMAP_DIGEST_MISMATCH_INFO);
    F(SIZE_MISMATCH_INFO);
    F(SHARD_EC_HASH_MISMATCH);
    F(SHARD_EC_SIZE_MISMATCH);
    F(INFO_MISSING);
    F(INFO_CORRUPTED);
    F(SNAPSET_MISSING);
    F(SNAPSET_CORRUPTED);
    F(OBJ_SIZE_INFO_MISMATCH);
    F(HINFO_MISSING);
    F(HINFO_CORRUPTED);
#undef F
    return ctx.out();
  }
};

template <>
struct fmt::formatter<librados::shard_info_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &err, FormatContext& ctx) const
  {
    return fmt::format_to(
      ctx.out(),
      "shard_info_t(error: {}, "
      "size: {}, "
      "omap_digest_present: {}, "
      "omap_digest: {}, "
      "data_digest_present: {}, "
      "data_digest: {}, "
      "selected_io: {}, "
      "primary: {})",
      static_cast<librados::err_t>(err),
      err.size,
      err.omap_digest_present,
      err.omap_digest,
      err.data_digest_present,
      err.data_digest,
      err.selected_oi,
      err.primary);
  }
};

template <>
struct fmt::formatter<shard_info_wrapper> :
  fmt::formatter<librados::shard_info_t> {};

template <>
struct fmt::formatter<librados::obj_err_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &err, FormatContext& ctx) const
  {
    bool first = true;
#define F(FLAG_NAME)					\
    if (err.errors & librados::obj_err_t::FLAG_NAME) {	\
      if (!first) {					\
	fmt::format_to(ctx.out(), "|");			\
      } else {						\
	first = false;					\
      }							\
      fmt::format_to(ctx.out(), #FLAG_NAME);		\
    }
    F(OBJECT_INFO_INCONSISTENCY);
    F(DATA_DIGEST_MISMATCH);
    F(OMAP_DIGEST_MISMATCH);
    F(SIZE_MISMATCH);
    F(ATTR_VALUE_MISMATCH);
    F(ATTR_NAME_MISMATCH);
    F(SNAPSET_INCONSISTENCY);
    F(HINFO_INCONSISTENCY);
    F(SIZE_TOO_LARGE);
#undef F
    return ctx.out();
  }
};

template <>
struct fmt::formatter<librados::osd_shard_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &shard, FormatContext& ctx) const
  {
    return fmt::format_to(
      ctx.out(),
      "osd_shard_t(osd: {}, shard: {})",
      shard.osd, shard.shard);
  }
};

template <>
struct fmt::formatter<librados::inconsistent_obj_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &err, FormatContext& ctx) const
  {
    return fmt::format_to(
      ctx.out(),
      "inconsistent_obj_t(error: {}, "
      "object: {}, "
      "version: {}, "
      "shards: {}, "
      "union_shards: {})",
      static_cast<librados::obj_err_t>(err),
      err.object,
      err.version,
      err.shards,
      err.union_shards);
  }
};

template <>
struct fmt::formatter<inconsistent_obj_wrapper> :
  fmt::formatter<librados::inconsistent_obj_t> {};

template <>
struct fmt::formatter<librados::inconsistent_snapset_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &err, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(), "inconsistent_snapset_t(errors: ");
    bool first = true;
#define F(FLAG_NAME)							\
    if (err.errors & librados::inconsistent_snapset_t::FLAG_NAME) {	\
      if (!first) {							\
	fmt::format_to(ctx.out(), "|");					\
      } else {								\
	first = false;							\
      }									\
      fmt::format_to(ctx.out(), #FLAG_NAME);				\
    }
    F(SNAPSET_MISSING);
    F(SNAPSET_CORRUPTED);
    F(CLONE_MISSING);
    F(SNAP_ERROR);
    F(HEAD_MISMATCH);
    F(HEADLESS_CLONE);
    F(SIZE_MISMATCH);
    F(OI_MISSING);
    F(INFO_MISSING);
    F(OI_CORRUPTED);
    F(INFO_CORRUPTED);
    F(EXTRA_CLONES);
#undef F
    return fmt::format_to(
      ctx.out(),
      ", object: {}, clones: {}, missing: {}",
      err.object, err.clones, err.missing);
  }
};

template <>
struct fmt::formatter<inconsistent_snapset_wrapper> :
  fmt::formatter<librados::inconsistent_snapset_t> {};
