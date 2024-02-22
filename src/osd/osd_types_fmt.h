// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatters for some types.h classes
 */

#include "common/hobject.h"
#include "osd/osd_types.h"
#include <fmt/chrono.h>
#include <fmt/ranges.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

template <>
struct fmt::formatter<osd_reqid_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const osd_reqid_t& req_id, FormatContext& ctx) const
  {
    return fmt::format_to(ctx.out(), "{}.{}:{}", req_id.name, req_id.inc,
			  req_id.tid);
  }
};

template <>
struct fmt::formatter<pg_shard_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const pg_shard_t& shrd, FormatContext& ctx) const
  {
    if (shrd.is_undefined()) {
      return fmt::format_to(ctx.out(), "?");
    }
    if (shrd.shard == shard_id_t::NO_SHARD) {
      return fmt::format_to(ctx.out(), "{}", shrd.get_osd());
    }
    return fmt::format_to(ctx.out(), "{}({})", shrd.get_osd(), shrd.shard);
  }
};

template <>
struct fmt::formatter<eversion_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const eversion_t& ev, FormatContext& ctx) const
  {
    return fmt::format_to(ctx.out(), "{}'{}", ev.epoch, ev.version);
  }
};

template <>
struct fmt::formatter<chunk_info_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const chunk_info_t& ci, FormatContext& ctx) const
  {
    return fmt::format_to(ctx.out(), "(len: {} oid: {} offset: {} flags: {})",
			  ci.length, ci.oid, ci.offset,
			  ci.get_flag_string(ci.flags));
  }
};

template <>
struct fmt::formatter<object_manifest_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const object_manifest_t& om, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(), "manifest({}", om.get_type_name());
    if (om.is_redirect()) {
      fmt::format_to(ctx.out(), " {}", om.redirect_target);
    } else if (om.is_chunked()) {
      fmt::format_to(ctx.out(), " {}", om.chunk_map);
    }
    return fmt::format_to(ctx.out(), ")");
  }
};

template <>
struct fmt::formatter<object_info_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const object_info_t& oi, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(), "{}({} {} {} s {} uv {}", oi.soid, oi.version,
		   oi.last_reqid, (oi.flags ? oi.get_flag_string() : ""), oi.size,
		   oi.user_version);
    if (oi.is_data_digest()) {
      fmt::format_to(ctx.out(), " dd {:x}", oi.data_digest);
    }
    if (oi.is_omap_digest()) {
      fmt::format_to(ctx.out(), " od {:x}", oi.omap_digest);
    }

    fmt::format_to(ctx.out(), " alloc_hint [{} {} {}]", oi.expected_object_size,
		   oi.expected_write_size, oi.alloc_hint_flags);

    if (oi.has_manifest()) {
      fmt::format_to(ctx.out(), " {}", oi.manifest);
    }
    return fmt::format_to(ctx.out(), ")");
  }
};

template <>
struct fmt::formatter<pg_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const pg_t& pg, FormatContext& ctx) const
  {
    return fmt::format_to(ctx.out(), "{}.{:x}", pg.pool(), pg.m_seed);
  }
};


template <>
struct fmt::formatter<spg_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const spg_t& spg, FormatContext& ctx) const
  {
    if (shard_id_t::NO_SHARD == spg.shard.id) {
      return fmt::format_to(ctx.out(), "{}", spg.pgid);
    } else {
      return fmt::format_to(ctx.out(), "{}s{}", spg.pgid, spg.shard.id);
    }
  }
};

template <>
struct fmt::formatter<pg_history_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const pg_history_t& pgh, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(),
		   "ec={}/{} lis/c={}/{} les/c/f={}/{}/{} sis={}",
		   pgh.epoch_created,
		   pgh.epoch_pool_created,
		   pgh.last_interval_started,
		   pgh.last_interval_clean,
		   pgh.last_epoch_started,
		   pgh.last_epoch_clean,
		   pgh.last_epoch_marked_full,
		   pgh.same_interval_since);

    if (pgh.prior_readable_until_ub != ceph::timespan::zero()) {
      return fmt::format_to(ctx.out(),
			    " pruub={}",
			    pgh.prior_readable_until_ub);
    } else {
      return ctx.out();
    }
  }
};

template <>
struct fmt::formatter<pg_info_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const pg_info_t& pgi, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(), "{}({}", pgi.pgid, (pgi.dne() ? " DNE" : ""));
    if (pgi.is_empty()) {
      fmt::format_to(ctx.out(), " empty");
    } else {
      fmt::format_to(ctx.out(), " v {}", pgi.last_update);
      if (pgi.last_complete != pgi.last_update) {
	fmt::format_to(ctx.out(), " lc {}", pgi.last_complete);
      }
      fmt::format_to(ctx.out(), " ({},{}]", pgi.log_tail, pgi.last_update);
    }
    if (pgi.is_incomplete()) {
      fmt::format_to(ctx.out(), " lb {}", pgi.last_backfill);
    }
    fmt::format_to(ctx.out(),
		   " local-lis/les={}/{}",
		   pgi.last_interval_started,
		   pgi.last_epoch_started);
    return fmt::format_to(ctx.out(),
			  " n={} {})",
			  pgi.stats.stats.sum.num_objects,
			  pgi.history);
  }
};

// snaps and snap-sets

template <>
struct fmt::formatter<SnapSet> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 'D') {
      verbose = true;
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const SnapSet& snps, FormatContext& ctx) const
  {
    if (verbose) {
      // similar to SnapSet::dump()
      fmt::format_to(ctx.out(),
		     "snaps{{{}: clns ({}): ",
		     snps.seq,
		     snps.clones.size());
      for (auto cln : snps.clones) {

	fmt::format_to(ctx.out(), "[{}: sz:", cln);

	auto cs = snps.clone_size.find(cln);
	if (cs != snps.clone_size.end()) {
	  fmt::format_to(ctx.out(), "{} ", cs->second);
	} else {
	  fmt::format_to(ctx.out(), "??");
	}

	auto co = snps.clone_overlap.find(cln);
	if (co != snps.clone_overlap.end()) {
	  fmt::format_to(ctx.out(), "olp:{} ", co->second);
	} else {
	  fmt::format_to(ctx.out(), "olp:?? ");
	}

	auto cln_snps = snps.clone_snaps.find(cln);
	if (cln_snps != snps.clone_snaps.end()) {
	  fmt::format_to(ctx.out(), "cl-snps:{} ]", cln_snps->second);
	} else {
	  fmt::format_to(ctx.out(), "cl-snps:?? ]");
	}
      }

      return fmt::format_to(ctx.out(), "}}");

    } else {
      return fmt::format_to(ctx.out(),
			    "{}={}:{}",
			    snps.seq,
			    snps.snaps,
			    snps.clone_snaps);
    }
  }

  bool verbose{false};
};

template <>
struct fmt::formatter<ScrubMap::object> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  ///\todo: consider passing the 'D" flag to control snapset dump
  template <typename FormatContext>
  auto format(const ScrubMap::object& so, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(),
		   "so{{ sz:{} dd:{} od:{} ",
		   so.size,
		   so.digest,
		   so.digest_present);

    // note the special handling of (1) OI_ATTR and (2) non-printables
    for (auto [k, v] : so.attrs) {
      std::string bkstr = v.to_str();
      if (k == std::string{OI_ATTR}) {
	/// \todo consider parsing the OI args here. Maybe add a specific format
	/// specifier
	fmt::format_to(ctx.out(), "{{{}:<<OI_ATTR>>({})}} ", k, bkstr.length());
      } else if (k == std::string{SS_ATTR}) {
	SnapSet sns{v};
	fmt::format_to(ctx.out(), "{{{}:{:D}}} ", k, sns);
      } else {
	fmt::format_to(ctx.out(), "{{{}:{}({})}} ", k, bkstr, bkstr.length());
      }
    }

    return fmt::format_to(ctx.out(), "}}");
  }
};

template <>
struct fmt::formatter<ScrubMap> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 'D') {
      debug_log = true;	 // list the objects
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const ScrubMap& smap, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(),
		   "smap{{ valid:{} incr-since:{} #:{}",
		   smap.valid_through,
		   smap.incr_since,
		   smap.objects.size());
    if (debug_log) {
      fmt::format_to(ctx.out(), " objects:");
      for (const auto& [ho, so] : smap.objects) {
	fmt::format_to(ctx.out(), "\n\th.o<{}>:<{}> ", ho, so);
      }
      fmt::format_to(ctx.out(), "\n");
    }
    return fmt::format_to(ctx.out(), "}}");
  }

  bool debug_log{false};
};

template <>
struct fmt::formatter<object_stat_sum_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const object_stat_sum_t &stats, FormatContext& ctx) const
  {
#define FORMAT(FIELD) fmt::format_to(ctx.out(), #FIELD"={}, ", stats.FIELD);
    fmt::format_to(ctx.out(), "object_stat_sum_t(");
    FORMAT(num_bytes);
    FORMAT(num_objects);
    FORMAT(num_object_clones);
    FORMAT(num_object_copies);
    FORMAT(num_objects_missing_on_primary);
    FORMAT(num_objects_missing);
    FORMAT(num_objects_degraded);
    FORMAT(num_objects_misplaced);
    FORMAT(num_objects_unfound);
    FORMAT(num_rd);
    FORMAT(num_rd_kb);
    FORMAT(num_wr);
    FORMAT(num_wr_kb);
    FORMAT(num_large_omap_objects);
    FORMAT(num_objects_manifest);
    FORMAT(num_omap_bytes);
    FORMAT(num_omap_keys);
    FORMAT(num_shallow_scrub_errors);
    FORMAT(num_deep_scrub_errors);
    FORMAT(num_scrub_errors);
    FORMAT(num_objects_recovered);
    FORMAT(num_bytes_recovered);
    FORMAT(num_keys_recovered);
    FORMAT(num_objects_dirty);
    FORMAT(num_whiteouts);
    FORMAT(num_objects_omap);
    FORMAT(num_objects_hit_set_archive);
    FORMAT(num_bytes_hit_set_archive);
    FORMAT(num_flush);
    FORMAT(num_flush_kb);
    FORMAT(num_evict);
    FORMAT(num_evict_kb);
    FORMAT(num_promote);
    FORMAT(num_flush_mode_high);
    FORMAT(num_flush_mode_low);
    FORMAT(num_evict_mode_some);
    FORMAT(num_evict_mode_full);
    FORMAT(num_objects_pinned);
    FORMAT(num_legacy_snapsets);
    return fmt::format_to(
      ctx.out(), "num_objects_repaired={})",
      stats.num_objects_repaired);
#undef FORMAT
  }
};
inline std::ostream &operator<<(std::ostream &lhs, const object_stat_sum_t &sum) {
  return lhs << fmt::format("{}", sum);
}

#if FMT_VERSION >= 90000
template <bool TrackChanges> struct fmt::formatter<pg_missing_set<TrackChanges>> : fmt::ostream_formatter {};
#endif
