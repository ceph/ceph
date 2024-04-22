// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_SCRUB_TYPES_H
#define CEPH_SCRUB_TYPES_H

#include <fmt/ranges.h>

#include "osd/osd_types.h"

// wrappers around scrub types to offer the necessary bits other than
// the minimal set that the lirados requires
struct object_id_wrapper : public librados::object_id_t {
  explicit object_id_wrapper(const hobject_t& hoid)
    : object_id_t{hoid.oid.name, hoid.nspace, hoid.get_key(), hoid.snap}
  {}
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
};

WRITE_CLASS_ENCODER(object_id_wrapper)

namespace librados {
inline void decode(object_id_t& obj, ceph::buffer::list::const_iterator& bp) {
  reinterpret_cast<object_id_wrapper&>(obj).decode(bp);
}
}

struct osd_shard_wrapper : public librados::osd_shard_t {
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bp);
};

WRITE_CLASS_ENCODER(osd_shard_wrapper)

namespace librados {
  inline void decode(librados::osd_shard_t& shard, ceph::buffer::list::const_iterator& bp) {
    reinterpret_cast<osd_shard_wrapper&>(shard).decode(bp);
  }
}

struct shard_info_wrapper : public librados::shard_info_t {
public:
  shard_info_wrapper() = default;
  explicit shard_info_wrapper(const ScrubMap::object& object) {
    set_object(object);
  }
  void set_object(const ScrubMap::object& object);
  void set_missing() {
    errors |= err_t::SHARD_MISSING;
  }
  void set_omap_digest_mismatch_info() {
    errors |= err_t::OMAP_DIGEST_MISMATCH_INFO;
  }
  void set_size_mismatch_info() {
    errors |= err_t::SIZE_MISMATCH_INFO;
  }
  void set_data_digest_mismatch_info() {
    errors |= err_t::DATA_DIGEST_MISMATCH_INFO;
  }
  void set_read_error() {
    errors |= err_t::SHARD_READ_ERR;
  }
  void set_stat_error() {
    errors |= err_t::SHARD_STAT_ERR;
  }
  void set_ec_hash_mismatch() {
    errors |= err_t::SHARD_EC_HASH_MISMATCH;
  }
  void set_ec_size_mismatch() {
    errors |= err_t::SHARD_EC_SIZE_MISMATCH;
  }
  void set_info_missing() {
    errors |= err_t::INFO_MISSING;
  }
  void set_info_corrupted() {
    errors |= err_t::INFO_CORRUPTED;
  }
  void set_snapset_missing() {
    errors |= err_t::SNAPSET_MISSING;
  }
  void set_snapset_corrupted() {
    errors |= err_t::SNAPSET_CORRUPTED;
  }
  void set_obj_size_info_mismatch() {
    errors |= err_t::OBJ_SIZE_INFO_MISMATCH;
  }
  void set_hinfo_missing() {
    errors |= err_t::HINFO_MISSING;
  }
  void set_hinfo_corrupted() {
    errors |= err_t::HINFO_CORRUPTED;
  }
  bool only_data_digest_mismatch_info() const {
    return errors == err_t::DATA_DIGEST_MISMATCH_INFO;
  }
  void clear_data_digest_mismatch_info() {
    errors &= ~err_t::DATA_DIGEST_MISMATCH_INFO;
  }
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bp);
};

WRITE_CLASS_ENCODER(shard_info_wrapper)

namespace librados {
  inline void decode(librados::shard_info_t& shard,
		     ceph::buffer::list::const_iterator& bp) {
    reinterpret_cast<shard_info_wrapper&>(shard).decode(bp);
  }
}

struct inconsistent_obj_wrapper : librados::inconsistent_obj_t {
  explicit inconsistent_obj_wrapper(const hobject_t& hoid);

  void merge(obj_err_t other) {
    errors |= other.errors;
  }

  void set_object_info_inconsistency() {
    errors |= obj_err_t::OBJECT_INFO_INCONSISTENCY;
  }
  void set_omap_digest_mismatch() {
    errors |= obj_err_t::OMAP_DIGEST_MISMATCH;
  }
  void set_data_digest_mismatch() {
    errors |= obj_err_t::DATA_DIGEST_MISMATCH;
  }
  void set_size_mismatch() {
    errors |= obj_err_t::SIZE_MISMATCH;
  }
  void set_attr_value_mismatch() {
    errors |= obj_err_t::ATTR_VALUE_MISMATCH;
  }
  void set_attr_name_mismatch() {
    errors |= obj_err_t::ATTR_NAME_MISMATCH;
  }
  void set_snapset_inconsistency() {
    errors |= obj_err_t::SNAPSET_INCONSISTENCY;
  }
  void set_hinfo_inconsistency() {
    errors |= obj_err_t::HINFO_INCONSISTENCY;
  }
  void set_size_too_large() {
    errors |= obj_err_t::SIZE_TOO_LARGE;
  }
  void add_shard(const pg_shard_t& pgs, const shard_info_wrapper& shard);
  void set_auth_missing(const hobject_t& hoid,
                        const std::map<pg_shard_t, ScrubMap>&,
			std::map<pg_shard_t, shard_info_wrapper>&,
			int &shallow_errors, int &deep_errors,
			const pg_shard_t &primary);
  void set_version(uint64_t ver) { version = ver; }
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bp);
};

WRITE_CLASS_ENCODER(inconsistent_obj_wrapper)

inline void decode(librados::inconsistent_obj_t& obj,
		   ceph::buffer::list::const_iterator& bp) {
  reinterpret_cast<inconsistent_obj_wrapper&>(obj).decode(bp);
}

struct inconsistent_snapset_wrapper : public librados::inconsistent_snapset_t {
  inconsistent_snapset_wrapper() = default;
  explicit inconsistent_snapset_wrapper(const hobject_t& head);
  void set_headless();
  // soid claims that it is a head or a snapdir, but its SS_ATTR
  // is missing.
  void set_snapset_missing();
  void set_info_missing();
  void set_snapset_corrupted();
  void set_info_corrupted();
  // snapset with missing clone
  void set_clone_missing(snapid_t);
  // Clones that are there
  void set_clone(snapid_t);
  // the snapset is not consistent with itself
  void set_snapset_error();
  void set_size_mismatch();

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bp);
};

WRITE_CLASS_ENCODER(inconsistent_snapset_wrapper)

namespace librados {
  inline void decode(librados::inconsistent_snapset_t& snapset,
		     ceph::buffer::list::const_iterator& bp) {
    reinterpret_cast<inconsistent_snapset_wrapper&>(snapset).decode(bp);
  }
}

struct scrub_ls_arg_t {
  uint32_t interval;
  uint32_t get_snapsets;
  librados::object_id_t start_after;
  uint64_t max_return;
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
};

WRITE_CLASS_ENCODER(scrub_ls_arg_t);

struct scrub_ls_result_t {
  epoch_t interval;
  std::vector<ceph::buffer::list> vals;
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
};

WRITE_CLASS_ENCODER(scrub_ls_result_t);

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

#endif
