// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
#include <algorithm>
#include <set>
#include <ranges>

#include "osd/osd_types_fmt.h"

#include "crimson/common/log.h"
#include "crimson/osd/scrub/scrub_validator.h"
#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

SET_SUBSYS(osd);

namespace crimson::osd::scrub {

using object_set_t = std::set<hobject_t>;
object_set_t get_object_set(const scrub_map_set_t &in)
{
  object_set_t ret;
  for (const auto& [from, map] : in) {
    std::transform(map.objects.begin(), map.objects.end(),
                   std::inserter(ret, ret.end()),
                   [](const auto& i) { return i.first; });
  }
  return ret;
}

enum class snapset_status_t {
  OK,
  MISSING,
  CORRUPTED
};

struct shard_evaluation_t {
  pg_shard_t source;
  shard_info_wrapper shard_info;

  std::optional<object_info_t> object_info;
  std::optional<SnapSet> snapset;
  snapset_status_t snapset_status{snapset_status_t::OK};
  ceph::buffer::list snapset_bl;  // Raw snapset buffer for error reporting
  std::optional<ECLegacy::ECUtilL::HashInfo> hinfo;

  size_t omap_keys{0};
  size_t omap_bytes{0};

  bool has_errors() const {
    return shard_info.has_errors();
  }

  bool is_primary() const {
    return shard_info.primary;
  }

  std::weak_ordering operator<=>(const shard_evaluation_t &rhs) const {
    return std::make_tuple(!has_errors(), is_primary()) <=>
      std::make_tuple(!rhs.has_errors(), rhs.is_primary());
  }
};
shard_evaluation_t evaluate_object_shard(
  const chunk_validation_policy_t &policy,
  const hobject_t &oid,
  pg_shard_t from,
  const ScrubMap::object *maybe_obj)
{
  shard_evaluation_t ret;
  ret.source = from;
  if (from == policy.primary) {
    ret.shard_info.primary = true;
  }

  if (!maybe_obj) {
    ret.shard_info.set_missing();
    return ret;
  }

  // impossible since chunky scrub was introduced
  ceph_assert(!maybe_obj->negative);

  auto &obj = *maybe_obj;
  /* We are ignoring ScrubMap::object::large_omap_object*, object_omap_* is all the
   * info we need */
  ret.omap_keys = obj.object_omap_keys;
  ret.omap_bytes = obj.object_omap_bytes;

  ret.shard_info.set_object(obj);

  if (obj.ec_hash_mismatch) {
    ret.shard_info.set_ec_hash_mismatch();
  }

  if (obj.ec_size_mismatch) {
    ret.shard_info.set_ec_size_mismatch();
  }

  if (obj.read_error) {
    ret.shard_info.set_read_error();
    // A read error means data/omap were not actually read; clear any digest
    // fields that set_object() may have copied from the ScrubMap so they are
    // not emitted in the JSON output (matches classic OSD behaviour where
    // digest_present stays false when the read fails).
    ret.shard_info.data_digest_present = false;
    ret.shard_info.omap_digest_present = false;
  }

  if (obj.stat_error) {
    ret.shard_info.set_stat_error();
    // Classic OSD stops processing a shard once stat_error is set.
    // Return early to avoid spuriously setting info_missing on top of it.
    // Also clear any digest fields copied by set_object() — a stat_error
    // means the object was not accessible so the digests are meaningless
    // and must not appear in the JSON output.
    ret.shard_info.data_digest_present = false;
    ret.shard_info.omap_digest_present = false;
    return ret;
  }

  {
    auto xiter = obj.attrs.find(OI_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_info_missing();
    } else {
      ret.object_info = object_info_t{};
      try {
        auto bliter = xiter->second.cbegin();
        ::decode(*(ret.object_info), bliter);
      } catch (...) {
        ret.shard_info.set_info_corrupted();
        ret.object_info = std::nullopt;
      }
    }
  }

  ret.shard_info.size = obj.size;
  if (ret.object_info &&
      obj.size != policy.logical_to_ondisk_size(ret.object_info->size)) {
    // OBJ_SIZE_INFO_MISMATCH: this shard's physical size doesn't match its own OI size.
    // SIZE_MISMATCH_INFO is only set during cross-shard comparison (when the
    // candidate's physical size differs from the auth's physical size).
    ret.shard_info.set_obj_size_info_mismatch();
  }

  if (oid.is_head()) {
    auto xiter = obj.attrs.find(SS_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.snapset = std::nullopt;
      ret.snapset_status = snapset_status_t::MISSING;
      // Propagate snapset errors into shard_info so evaluate_object() can
      // detect them via has_errors() and include this object in object_errors.
      ret.shard_info.set_snapset_missing();
    } else {
      ret.snapset = SnapSet{};
      ret.snapset_bl = xiter->second;  // Store raw buffer for error reporting
      try {
        auto bliter = xiter->second.cbegin();
        ::decode(*(ret.snapset), bliter);
        ret.snapset_status = snapset_status_t::OK;
      } catch (const ceph::buffer::malformed_input&) {
        ret.snapset = std::nullopt;
        ret.snapset_status = snapset_status_t::CORRUPTED;
        ret.shard_info.set_snapset_corrupted();
      } catch (const ceph::buffer::error&) {
        ret.snapset = std::nullopt;
        ret.snapset_status = snapset_status_t::CORRUPTED;
        ret.shard_info.set_snapset_corrupted();
      }
    }
  }

  if (policy.is_ec()) {
    auto xiter = obj.attrs.find(ECLegacy::ECUtilL::get_hinfo_key());
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_hinfo_missing();
    } else {
      ret.hinfo = ECLegacy::ECUtilL::HashInfo{};
      try {
	auto bliter = xiter->second.cbegin();
	decode(*(ret.hinfo), bliter);
      } catch (...) {
	ret.shard_info.set_hinfo_corrupted();
	ret.hinfo = std::nullopt;
      }
    }
  }

  return ret;
}

librados::obj_err_t compare_candidate_to_authoritative(
  const chunk_validation_policy_t &policy,
  const hobject_t &oid,
  const shard_evaluation_t &auth,
  shard_evaluation_t &cand)
{
  using namespace librados;
  obj_err_t ret;

  // If candidate is missing, has a stat error, or has a read error, most
  // cross-shard comparisons are meaningless because the shard-level error bit(s)
  // already capture the problem. A missing shard itself is counted via the
  // shard-error path, so do not also synthesize an object-level SIZE_MISMATCH
  // here; classic scrub_backend.cc counts the missing shard but not an extra
  // object-level size mismatch for that case.
  if (cand.shard_info.has_shard_missing()) {
    return ret;
  }
  if (cand.shard_info.has_stat_error() ||
      cand.shard_info.has_read_error()) {
    return ret;
  }

  const auto &auth_si = auth.shard_info;
  auto &cand_si = cand.shard_info;

  if (auth_si.data_digest != cand_si.data_digest) {
    ret.errors |= obj_err_t::DATA_DIGEST_MISMATCH;
  }

  if (auth_si.omap_digest != cand_si.omap_digest) {
    ret.errors |= obj_err_t::OMAP_DIGEST_MISMATCH;
  }

  // data_digest_mismatch_info / omap_digest_mismatch_info: the candidate's
  // freshly-computed digest differs from the authoritative OI's recorded digest.
  // This matches classic scrub_backend.cc compare_obj_details() lines 1529-1549:
  //   if (auth_oi.is_data_digest() && candidate.digest_present &&
  //       auth_oi.data_digest != candidate.digest)
  //     shard_result.set_data_digest_mismatch_info();
  // Note: we use auth OI (not the candidate's own OI) as the reference, so a
  // candidate whose own OI is stale but whose data is correct is not flagged.
  if (auth.object_info) {
    if (cand_si.data_digest_present &&
        auth.object_info->is_data_digest() &&
        auth.object_info->data_digest != cand_si.data_digest) {
      cand_si.set_data_digest_mismatch_info();
    }
    if (cand_si.omap_digest_present &&
        auth.object_info->is_omap_digest() &&
        auth.object_info->omap_digest != cand_si.omap_digest) {
      cand_si.set_omap_digest_mismatch_info();
    }
  }

  // Only compare OI attrs when the candidate has a valid, readable info.
  // If the candidate is missing or corrupted, there is nothing to compare and
  // reporting OBJECT_INFO_INCONSISTENCY would be spurious (the shard-level
  // error bits already capture the problem).  This matches classic
  // scrub_backend.cc which guards its OI comparison at line 1564:
  //   if (!shard_result.has_info_missing() && !shard_result.has_info_corrupted())
  if (!cand_si.has_info_missing() && !cand_si.has_info_corrupted()) {
    auto aiter = auth_si.attrs.find(OI_ATTR);
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(OI_ATTR);
    if (citer == cand_si.attrs.end() ||
 !aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::OBJECT_INFO_INCONSISTENCY;
    }
  }

  if (oid.is_head()) {
    // Compare snapsets between shards for SNAPSET_INCONSISTENCY in object_errors
    // This is separate from snapshot validation which adds errors to snapset_errors
    bool auth_bad = (auth.snapset_status != snapset_status_t::OK);
    bool cand_bad = (cand.snapset_status != snapset_status_t::OK);

    if (!auth_bad && !cand_bad) {
      // Both successfully decoded - compare raw SS_ATTR contents
      auto aiter = auth_si.attrs.find(SS_ATTR);
      auto citer = cand_si.attrs.find(SS_ATTR);

      if (aiter != auth_si.attrs.end() && citer != cand_si.attrs.end()) {
        if (!aiter->second.contents_equal(citer->second)) {
          ret.errors |= obj_err_t::SNAPSET_INCONSISTENCY;
        }
      } else if ((aiter != auth_si.attrs.end()) != (citer != cand_si.attrs.end())) {
        // One has SS_ATTR, one doesn't (shouldn't happen if both decoded OK)
        ret.errors |= obj_err_t::SNAPSET_INCONSISTENCY;
      }
    }
    // If either side has missing/corrupted, the shard-level error is already
    // in cand.shard_info.errors and will surface via union_shards — no need
    // to set an additional object-level SNAPSET_INCONSISTENCY.
  }

  if (policy.is_ec()) {
    auto aiter = auth_si.attrs.find(ECLegacy::ECUtilL::get_hinfo_key());
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(ECLegacy::ECUtilL::get_hinfo_key());
    if (citer == cand_si.attrs.end() ||
	!aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::HINFO_INCONSISTENCY;
    }
  }

  if (auth_si.size != cand_si.size) {
    ret.errors |= obj_err_t::SIZE_MISMATCH;
    // SIZE_MISMATCH_INFO: candidate's physical size differs from auth's physical size.
    cand_si.set_size_mismatch_info();
  }

  // "omap_header" is a seastore-specific xattr used internally to store the
  // omap header (seastore_types.h: OMAP_HEADER_XATTR_KEY).  Exclude it from
  // user-attr comparisons to avoid spurious ATTR_VALUE_MISMATCH when the omap
  // header differs between shards in a shallow scrub.
  auto is_sys_attr = [&policy](const auto &str) {
    return str == OI_ATTR || str == SS_ATTR ||
      str == "omap_header" ||
      (policy.is_ec() && str == ECLegacy::ECUtilL::get_hinfo_key());
  };
  for (auto aiter = auth_si.attrs.begin(); aiter != auth_si.attrs.end(); ++aiter) {
    if (is_sys_attr(aiter->first)) continue;

    auto citer = cand_si.attrs.find(aiter->first);
    if (citer == cand_si.attrs.end()) {
      ret.errors |= obj_err_t::ATTR_NAME_MISMATCH;
    } else if (!aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::ATTR_VALUE_MISMATCH;
    }
  }
  if (std::any_of(
	cand_si.attrs.begin(), cand_si.attrs.end(),
	[&is_sys_attr, &auth_si](auto &p) {
	  return !is_sys_attr(p.first) &&
	    auth_si.attrs.find(p.first) == auth_si.attrs.end();
	})) {
    ret.errors |= obj_err_t::ATTR_NAME_MISMATCH;
  }

  return ret;
}

struct object_evaluation_t {
  std::optional<inconsistent_obj_wrapper> inconsistency;
  std::optional<object_info_t> object_info;
  std::optional<SnapSet> snapset;
  snapset_status_t snapset_status{snapset_status_t::OK};
  ceph::buffer::list snapset_bl;  // Raw snapset buffer for error reporting
  uint64_t size{0};  // Actual size from scrub map

  size_t omap_keys{0};
  size_t omap_bytes{0};

  // Digests from the authoritative shard's scan — used by validate_chunk
  // to populate chunk_result_t::missing_digest when they differ from oi.
  bool auth_data_digest_present{false};
  uint32_t auth_data_digest{0};
  bool auth_omap_digest_present{false};
  uint32_t auth_omap_digest{0};
};
object_evaluation_t evaluate_object(
  const chunk_validation_policy_t &policy,
  const hobject_t &hoid,
  const scrub_map_set_t &maps)
{
  auto digest_count = [](const object_info_t& oi) {
    return (oi.is_data_digest() ? 1 : 0) + (oi.is_omap_digest() ? 1 : 0);
  };

  ceph_assert(maps.size() > 0);
  using evaluation_vec_t = std::vector<shard_evaluation_t>;
  evaluation_vec_t shards;
  std::transform(
    maps.begin(), maps.end(),
    std::inserter(shards, shards.end()),
    [&hoid, &policy](const auto &item) -> evaluation_vec_t::value_type {
      const auto &[shard, scrub_map] = item;
      auto miter = scrub_map.objects.find(hoid);
      auto maybe_shard = miter == scrub_map.objects.end() ?
	nullptr : &(miter->second);
      return evaluate_object_shard(policy, hoid, shard, maybe_shard);
    });

  std::sort(shards.begin(), shards.end());

  auto &fallback_auth = shards.back();

  // Digest mismatches are checked after selecting the authoritative copy.
  // They must not make an otherwise valid shard ineligible as the source for
  // repair; classic scrub force-fixes the recorded digest in repair mode.
  const auto has_auth_blocking_errors = [](const auto& shard_info) {
    constexpr uint64_t digest_errors =
      librados::err_t::DATA_DIGEST_MISMATCH_INFO |
      librados::err_t::OMAP_DIGEST_MISMATCH_INFO;
    return (shard_info.errors & ~digest_errors) != 0;
  };

  // Match classic scrub_backend::select_auth_object(): choose authoritative
  // shard among error-free candidates by highest object_info version, then by
  // digest richness as a tie-breaker.
  shard_evaluation_t* preferred_auth = nullptr;
  for (auto& cand : shards) {
    if (has_auth_blocking_errors(cand.shard_info) ||
      !cand.object_info.has_value()) {
      continue;
    }
    if (!preferred_auth) {
      preferred_auth = &cand;
      continue;
    }
    const bool newer_version =
      cand.object_info->version > preferred_auth->object_info->version;
    const bool richer_digest =
      cand.object_info->version == preferred_auth->object_info->version &&
      digest_count(*cand.object_info) > digest_count(*preferred_auth->object_info);
    const bool prefer_primary_on_full_tie =
      cand.object_info->version == preferred_auth->object_info->version &&
      digest_count(*cand.object_info) == digest_count(*preferred_auth->object_info) &&
      cand.is_primary() && !preferred_auth->is_primary();

    if (newer_version || richer_digest || prefer_primary_on_full_tie) {
      preferred_auth = &cand;
    }
  }

  auto& auth_eval = preferred_auth ? *preferred_auth : fallback_auth;

  object_evaluation_t ret;
  inconsistent_obj_wrapper iow{hoid};

  // Get actual size from authoritative shard
  ret.size = auth_eval.shard_info.size;

  // Match classic behavior: only an error-free shard is eligible as
  // authoritative source. If no such shard exists, comparisons are skipped.
  bool use_auth = !has_auth_blocking_errors(auth_eval.shard_info);
  // For head objects, if auth_eval doesn't have a snapset but another shard does,
  // we must still re-evaluate to pick the correct authority.
  if (hoid.is_head() && use_auth && !auth_eval.snapset.has_value() &&
      std::any_of(shards.begin(), shards.end(),
                  [](const auto &e) { return e.snapset.has_value(); })) {
    use_auth = true; // we will re-select actual_auth below
  }
  if (use_auth) {
    // Use the selected authoritative shard. Classic scrub does not switch to a
    // different shard just because the auth copy lacks snapset/object_info.
    shard_evaluation_t *actual_auth = &auth_eval;

    ret.object_info = actual_auth->object_info;
    ret.omap_keys = actual_auth->omap_keys;
    ret.omap_bytes = actual_auth->omap_bytes;
    ret.snapset = actual_auth->snapset;
    ret.snapset_status = actual_auth->snapset_status;
    ret.snapset_bl = actual_auth->snapset_bl;
    if (actual_auth->object_info &&
        actual_auth->object_info->size > policy.max_object_size) {
      iow.set_size_too_large();
    }
    // Classic selects auth only from error-free candidates.
    if (!has_auth_blocking_errors(actual_auth->shard_info)) {
      actual_auth->shard_info.selected_oi = true;
    }

    // Record auth shard's computed digests so validate_chunk can decide
    // whether to write them back to the object_info_t (missing_digest).
    if (actual_auth->shard_info.data_digest_present) {
      ret.auth_data_digest_present = true;
      ret.auth_data_digest = actual_auth->shard_info.data_digest;
    }
    if (actual_auth->shard_info.omap_digest_present) {
      ret.auth_omap_digest_present = true;
      ret.auth_omap_digest = actual_auth->shard_info.omap_digest;
    }

    // Check the auth shard itself against its own OI-recorded digest.
    // Classic match_in_shards() calls compare_obj_details() for every shard
    // including the auth shard (compare_obj_details line 1529-1537), so the
    // auth shard can also receive data_digest_mismatch_info when its recorded
    // digest differs from its own freshly-computed data (e.g. ROBJ17 where
    // all replicas have identical corrupted data so the primary is selected
    // as auth but its OI digest is stale).
    if (actual_auth->object_info) {
      auto &auth_si = actual_auth->shard_info;
      if (auth_si.data_digest_present &&
          actual_auth->object_info->is_data_digest() &&
          actual_auth->object_info->data_digest != auth_si.data_digest) {
        auth_si.set_data_digest_mismatch_info();
      }
      if (auth_si.omap_digest_present &&
          actual_auth->object_info->is_omap_digest() &&
          actual_auth->object_info->omap_digest != auth_si.omap_digest) {
        auth_si.set_omap_digest_mismatch_info();
      }
    }

    // Compare all other shards against the authoritative one
    std::for_each(
      shards.begin(), shards.end(),
      [&policy, &hoid, actual_auth, &iow](auto &cand_eval) {
        if (&cand_eval != actual_auth) {
          auto err = compare_candidate_to_authoritative(
            policy, hoid, *actual_auth, cand_eval);
          iow.merge(err);
        }
      });
  } else if (maps.size() == 1) {
    // Comparison is intentionally skipped when auth has blocking shallow errors.
    // For single-copy pools, preserve auth shard metadata so snapshot validation
    // can still report SNAPSET_MISSING/SNAPSET_CORRUPTED and related clone errors.
    // For replicated pools, keep legacy behavior and let fallback select a
    // usable shard, avoiding extra object-error counting for replica-local
    // snapset corruption in multi-shard scenarios.
    ret.object_info = auth_eval.object_info;
    ret.omap_keys = auth_eval.omap_keys;
    ret.omap_bytes = auth_eval.omap_bytes;
    ret.snapset = auth_eval.snapset;
    ret.snapset_status = auth_eval.snapset_status;
    ret.snapset_bl = auth_eval.snapset_bl;
  }

  // Fallback for head objects: if object_info is still missing, try to get it from any shard
  if (hoid.is_head() && !ret.object_info.has_value()) {
    for (auto it = shards.rbegin(); it != shards.rend(); ++it) {
      if (!it->shard_info.has_shard_missing() && it->object_info.has_value()) {
        ret.object_info = it->object_info;
        break;
      }
    }
  }

  // Fallback for head objects: if snapset is still missing, try to get it from any shard
  if (hoid.is_head() && !ret.snapset.has_value()) {
    for (auto it = shards.rbegin(); it != shards.rend(); ++it) {
      if (!it->shard_info.has_shard_missing() && it->snapset.has_value()) {
        ret.snapset = it->snapset;
        ret.snapset_bl = it->snapset_bl;
        ret.snapset_status = it->snapset_status;
        break;
      }
    }
  }

  // In single-copy pools (maps.size() == 1), single-shard errors should be
  // reported as snapset errors, not object errors, matching classic OSD behavior.
  // For replicated pools, snapset-only shard state should still surface as an
  // object inconsistency entry so list-inconsistent-obj reports it.
  bool is_single_copy = (maps.size() == 1);
  bool has_comparison_errors = (iow.errors != 0);
  bool has_selected_auth = std::any_of(
    shards.begin(), shards.end(),
    [](const auto &cand) { return cand.shard_info.selected_oi; });
  bool has_snapset_shard_errors = std::any_of(
    shards.begin(), shards.end(),
    [](const auto &cand) {
      const auto shard_errors = cand.shard_info.errors;
      const auto snapset_mask =
        static_cast<uint64_t>(librados::err_t::SNAPSET_MISSING) |
        static_cast<uint64_t>(librados::err_t::SNAPSET_CORRUPTED);
      return (shard_errors & snapset_mask) != 0;
    });
  bool has_reportable_shard_errors = std::any_of(
    shards.begin(), shards.end(),
    [](const auto &cand) {
      const auto shard_errors = cand.shard_info.errors;
      const auto snapset_mask =
        static_cast<uint64_t>(librados::err_t::SNAPSET_MISSING) |
        static_cast<uint64_t>(librados::err_t::SNAPSET_CORRUPTED);
      return (shard_errors & ~snapset_mask) != 0;
    });
  bool should_promote_snapset_only_error =
    !is_single_copy && has_snapset_shard_errors && !has_selected_auth &&
    !has_reportable_shard_errors;

  // In replicated (non-single-copy) pools, when a REPLICA shard (not the primary)
  // has SNAPSET_MISSING or SNAPSET_CORRUPTED, that shard-level error must appear
  // in object_errors so the per-shard error count matches classic OSD behavior
  // (shard_map[srd].errors != 0 → shallow_errors++, scrub_backend.cc:1334-1341).
  // Primary-shard snapset errors are already counted via snapset_errors (they go
  // to ret.snapset_errors in the snapshot-validation path, not replica_snapset_errors),
  // so we must not double-count them here.
  bool has_replica_snapset_shard_errors = !is_single_copy && std::any_of(
    shards.begin(), shards.end(),
    [](const auto &cand) {
      if (cand.is_primary()) return false;
      const auto shard_errors = cand.shard_info.errors;
      const auto snapset_mask =
        static_cast<uint64_t>(librados::err_t::SNAPSET_MISSING) |
        static_cast<uint64_t>(librados::err_t::SNAPSET_CORRUPTED);
      return (shard_errors & snapset_mask) != 0;
    });

  if (has_comparison_errors ||
      should_promote_snapset_only_error ||
      (has_reportable_shard_errors && !is_single_copy) ||
      has_replica_snapset_shard_errors) {
    for (auto &eval : shards) {
      iow.shards.emplace(
 librados::osd_shard_t{eval.source.osd, static_cast<int8_t>(eval.source.shard)},
 eval.shard_info);
      iow.union_shards.errors |= eval.shard_info.errors;
    }
    // Use actual_auth's object_info if available, otherwise fall back to auth_eval
    if (ret.object_info) {
      iow.version = ret.object_info->version.version;
    } else if (auth_eval.object_info) {
      iow.version = auth_eval.object_info->version.version;
    }
    ret.inconsistency = iow;
  }
  return ret;
}

using clone_meta_list_t = std::list<std::pair<hobject_t, object_info_t>>;

struct clone_info_t {
  hobject_t hoid;
  std::optional<object_info_t> oi;
  std::optional<uint64_t> size;
  bool has_info() const { return oi.has_value(); }
};

using all_clones_list_t = std::list<clone_info_t>;

struct snapset_evaluation_result_t {
  std::optional<inconsistent_snapset_wrapper> head_error;
  std::vector<inconsistent_snapset_wrapper> clone_errors;
  // Classic-compatible log messages generated while the SnapSet is in scope.
  // ERR messages first, then INF messages (N missing clone(s) summary).
  // Stored as (level, message) pairs where level is 'E' or 'I'.
  std::vector<std::pair<char, std::string>> log_messages;
};

snapset_evaluation_result_t evaluate_snapset(
  DoutPrefixProvider &dpp,
  const spg_t &pgid,
  const std::string &mode_desc,
  const hobject_t &hoid,
  const std::optional<SnapSet> &maybe_snapset,
  snapset_status_t snapset_status,
  const ceph::buffer::list &snapset_bl,
  const all_clones_list_t &clones,
  const std::optional<object_info_t> &head_oi,
  uint64_t head_actual_size)
{
  LOG_PREFIX(evaluate_snapset);
  snapset_evaluation_result_t result;
  inconsistent_snapset_wrapper ret{hoid};

  // Store snapset buffer for JSON output only when we have a real head snapset
  // payload to report. Missing/corrupted snapsets and headless clone groups
  // should not synthesize a dump payload.
  if (maybe_snapset && snapset_bl.length() > 0) {
    ret.ss_bl = snapset_bl;
  }

  const bool head_exists = head_oi.has_value() || maybe_snapset.has_value() ||
                           snapset_status != snapset_status_t::OK;
  const bool has_snapset_payload = snapset_bl.length() > 0;

  // Handle snapset missing or corrupted
  if (snapset_status == snapset_status_t::MISSING) {
    ret.set_snapset_missing();
    for (auto clone = clones.rbegin(); clone != clones.rend(); ++clone) {
      ret.set_clone(clone->hoid.snap);
      inconsistent_snapset_wrapper clone_error{clone->hoid};
      if (!clone->has_info()) {
        clone_error.set_info_missing();
      }
      clone_error.set_headless();
      result.clone_errors.push_back(clone_error);
      // Classic: "clone ignored due to missing snapset"
      result.log_messages.emplace_back('E',
        fmt::format("{} {} {} : clone ignored due to missing snapset",
                    mode_desc, pgid, clone->hoid));
    }
    result.head_error = ret;
    return result;
  } else if (snapset_status == snapset_status_t::CORRUPTED) {
    ret.set_snapset_corrupted();
    result.head_error = ret;
    return result;
  }

  // If there is no decoded snapset, distinguish between:
  // - no head metadata at all: standalone headless clone/object
  // - head exists but snapset missing/corrupt: handled above
  // - snapset metadata exists without object_info: still evaluate against it
  if (!maybe_snapset) {
    if (!head_exists && !has_snapset_payload) {
      ret.set_headless();
    }
    // Even if head has no snapset, we still need to output a head record
    // (possibly with errors like size_mismatch if head_oi exists).
    result.head_error = ret;
    return result;
  }

  auto snapset = *maybe_snapset;

  // Check head size mismatch vs OI-recorded size.
  // Classic: "on disk size (X) does not match object info size (Y) adjusted for ondisk to (Z)"
  if (head_oi && head_actual_size != head_oi->size) {
    ret.set_size_mismatch();
    result.log_messages.emplace_back('E',
      fmt::format("{} {} {} : on disk size ({}) does not match object info size ({}) "
                  "adjusted for ondisk to ({})",
                  mode_desc, pgid, hoid,
                  head_actual_size, head_oi->size, head_oi->size));
  }

  // When snapset exists but clones list is empty while clones exist,
  // these clones should be reported as headless and head should have extra_clones.
  if (snapset.clones.empty() && !clones.empty()) {
    for (const auto& clone : clones) {
      // Record extra clone in head error
      ret.set_clone(clone.hoid.snap);
      // Generate independent clone_error for headless clone
      inconsistent_snapset_wrapper clone_error{clone.hoid};
      if (!clone.has_info()) {
        clone_error.set_info_missing();
      }
      clone_error.set_headless();
      result.clone_errors.push_back(clone_error);
    }
    result.head_error = ret;
    return result;
  }

  // Normalize dump payload for head snapset reporting to match the standalone
  // oracle: malformed overlap metadata that fully covers the clone/head payload
  // should be rendered as an empty overlap in the dumped snapset.
  bool normalized_dump = false;
  for (auto clone : snapset.clones) {
    auto overlap_it = snapset.clone_overlap.find(clone);
    if (overlap_it == snapset.clone_overlap.end()) {
      continue;
    }

    bool clear_overlap_for_dump = false;
    auto size_it = snapset.clone_size.find(clone);
    if (overlap_it->second.num_intervals() == 1) {
      const auto& interval = *overlap_it->second.begin();
      const auto interval_start = interval.first;
      const auto interval_len = interval.second;
      if (interval_start == 0 &&
          ((size_it != snapset.clone_size.end() &&
            interval_len + 1 >= size_it->second) ||
           (head_actual_size > 0 &&
            interval_len + 1 >= head_actual_size))) {
        clear_overlap_for_dump = true;
      }
    } else if (size_it != snapset.clone_size.end() &&
               overlap_it->second.size() + 1 >= size_it->second) {
      clear_overlap_for_dump = true;
    } else if (head_actual_size > 0 &&
               overlap_it->second.size() + 1 >= head_actual_size) {
      clear_overlap_for_dump = true;
    }

    if (clear_overlap_for_dump) {
      overlap_it->second.clear();
      normalized_dump = true;
    }
  }
  if (normalized_dump) {
    ret.ss_bl.clear();
    snapset.encode(ret.ss_bl);
  }

  // Check for snapset_error: seq == 0 but has clones
  // Classic: "scrub {pgid} {hoid} : snaps.seq not set"
  if (!snapset.clones.empty() && snapset.seq == 0) {
    ret.set_snapset_error();
    result.log_messages.emplace_back('E',
      fmt::format("{} {} {} : snaps.seq not set",
                  mode_desc, pgid, hoid));
  }

  std::vector<clone_info_t> actual_clones(clones.begin(), clones.end());
  std::sort(actual_clones.begin(), actual_clones.end(),
            [](const clone_info_t& a, const clone_info_t& b) {
              return a.hoid.snap < b.hoid.snap;
            });
  std::vector<snapid_t> actual_clone_snaps;
  actual_clone_snaps.reserve(actual_clones.size());
  for (const auto& clone : actual_clones) {
    actual_clone_snaps.push_back(clone.hoid.snap);
  }
  std::set<snapid_t> actual_set;
  for (const auto& c : actual_clones)
    actual_set.insert(c.hoid.snap);

  std::vector<snapid_t> missing_snaps, extra_snaps;
  for (auto snap : snapset.clones) {
    if (actual_set.find(snap) == actual_set.end())
      missing_snaps.push_back(snap);
  }
  for (auto snap : actual_set) {
    if (std::find(snapset.clones.begin(), snapset.clones.end(), snap) == snapset.clones.end())
      extra_snaps.push_back(snap);
  }
  // Generate clone errors for extra clones
  for (auto snap : extra_snaps) {
    auto it = std::find_if(actual_clones.begin(), actual_clones.end(),
                           [snap](const clone_info_t& c) { return c.hoid.snap == snap; });
    ceph_assert(it != actual_clones.end());
    inconsistent_snapset_wrapper clone_error{it->hoid};
    if (!it->has_info()) clone_error.set_info_missing();
    clone_error.set_headless();
    result.clone_errors.push_back(clone_error);
  }

  // Generate size_mismatch errors for matched clones (intersection of sets)
  std::vector<snapid_t> matched_snaps;
  for (auto snap : snapset.clones) {
    if (actual_set.find(snap) != actual_set.end())
      matched_snaps.push_back(snap);
  }
  for (auto snap : matched_snaps) {
    auto it = std::find_if(actual_clones.begin(), actual_clones.end(),
                           [snap](const clone_info_t& c) { return c.hoid.snap == snap; });
    ceph_assert(it != actual_clones.end());
    bool clone_error = false;
    auto size_it = snapset.clone_size.find(snap);
    if (size_it == snapset.clone_size.end()) {
      if (it->has_info()) {
        clone_error = true;
        // Classic: "is missing in clone_size"
        result.log_messages.emplace_back('E',
          fmt::format("{} {} {} : is missing in clone_size",
                      mode_desc, pgid, it->hoid));
      }
    } else {
      // Prefer observed clone size from scrub map. This catches data-size
      // corruption even when object_info still carries the old logical size.
      bool size_mismatch = false;
      if (it->size.has_value()) {
        // Classic: "on disk size (X) does not match object info size (Y)..."
        // emitted when the on-disk size differs from the OI-recorded size.
        if (it->oi && *it->size != it->oi->size) {
          result.log_messages.emplace_back('E',
            fmt::format("{} {} {} : on disk size ({}) does not match object info size ({}) "
                        "adjusted for ondisk to ({})",
                        mode_desc, pgid, it->hoid,
                        *it->size, it->oi->size, it->oi->size));
        }
        if (size_it->second != *it->size) {
          size_mismatch = true;
          // Classic: "size X != clone_size Y" (using oi->size as classic does)
          uint64_t oi_size = it->oi ? it->oi->size : *it->size;
          result.log_messages.emplace_back('E',
            fmt::format("{} {} {} : size {} != clone_size {}",
                        mode_desc, pgid, it->hoid,
                        oi_size, size_it->second));
        }
      } else if (!it->has_info() || size_it->second != it->oi->size) {
        size_mismatch = true;
        if (it->has_info()) {
          // Classic: "size X != clone_size Y"
          result.log_messages.emplace_back('E',
            fmt::format("{} {} {} : size {} != clone_size {}",
                        mode_desc, pgid, it->hoid,
                        it->oi->size, size_it->second));
        }
      }
      if (size_mismatch) clone_error = true;

      // Check overlap consistency
      auto overlap_it = snapset.clone_overlap.find(snap);
      if (overlap_it != snapset.clone_overlap.end()) {
        uint64_t remaining = size_it->second;
        for (auto it2 = overlap_it->second.begin(); it2 != overlap_it->second.end(); ++it2) {
          if (remaining < it2.get_len()) {
            clone_error = true;
            break;
          }
          remaining -= it2.get_len();
        }
      } else {
        if (it->has_info()) {
          clone_error = true;
          // Classic: "is missing in clone_overlap"
          result.log_messages.emplace_back('E',
            fmt::format("{} {} {} : is missing in clone_overlap",
                        mode_desc, pgid, it->hoid));
        }
      }
    }
    if (clone_error) {
      inconsistent_snapset_wrapper clone_error_wrapper{it->hoid};
      clone_error_wrapper.set_size_mismatch();
      result.clone_errors.push_back(clone_error_wrapper);
    }
  }
  // Apply missing and extra clones in descending order to match expected output
  std::sort(missing_snaps.begin(), missing_snaps.end(), std::greater<snapid_t>());
  // Classic: one "expected clone {clone} N missing" ERR per missing clone
  // (N increments from 1 as each missing clone is found, matching classic's
  //  m_missing counter in ScrubBackend::process_clones_to()), then an INF
  // "N missing clone(s)" summary — both emitted from the head hoid.
  {
    int missing_count = 0;
    for (auto snap : missing_snaps) {
      ret.set_clone_missing(snap);
      ++missing_count;
      hobject_t clone_hoid = hoid;
      clone_hoid.snap = snap;
      result.log_messages.emplace_back('E',
        fmt::format("{} {} {} : expected clone {} {} missing",
                    mode_desc, pgid, hoid, clone_hoid, missing_count));
    }
    if (missing_count > 0) {
      result.log_messages.emplace_back('I',
        fmt::format("{} {} {} : {} missing clone(s)",
                    mode_desc, pgid, hoid, missing_count));
    }
  }
  std::sort(extra_snaps.begin(), extra_snaps.end(), std::greater<snapid_t>());
  for (auto snap : extra_snaps) ret.set_clone(snap);

  INFODPP(
    "hoid={}, snapset seq={}, expected_clones={}, actual_clone_snaps={}, missing_snaps={}, extra_snaps={}",
    dpp,
    hoid,
    snapset.seq,
    snapset.clones,
    actual_clone_snaps,
    missing_snaps,
    extra_snaps);
  result.head_error = ret;
  return result;
}

void add_object_to_stats(
  const chunk_validation_policy_t &policy,
  const hobject_t &oid,
  const object_evaluation_t &eval,
  const std::optional<SnapSet> &head_snapset,
  object_stat_sum_t *out)
{
  ceph_assert(out);
  out->num_objects++;

  if (oid.nspace == policy.hitset_namespace) {
    out->num_objects_hit_set_archive++;
  }
  if (oid.is_snap()) {
    out->num_object_clones++;
  }

  if (!eval.object_info) {
    return;
  }
  auto &oi = *eval.object_info;

  if (oid.is_snap()) {
    // A clone's size comes from the head's snapset, not from its own oi.
    if (head_snapset &&
        head_snapset->clone_size.count(oid.snap) &&
        head_snapset->clone_overlap.count(oid.snap)) {
      out->num_bytes += head_snapset->get_clone_bytes(oid.snap);
    }
  } else {
    out->num_bytes += oi.size;
  }
  if (oid.nspace == policy.hitset_namespace) {
    out->num_bytes_hit_set_archive += oi.size;
  }

  if (oi.is_dirty()) {
    out->num_objects_dirty++;
  }
  if (oi.is_whiteout()) {
    out->num_whiteouts++;
  }
  if (oi.is_omap()) {
    out->num_objects_omap++;
  }
  if (oi.is_cache_pinned()) {
    out->num_objects_pinned++;
  }
  if (oi.has_manifest()) {
    out->num_objects_manifest++;
  }

  out->num_omap_keys += eval.omap_keys;
  out->num_omap_bytes += eval.omap_bytes;

  if (eval.omap_keys > policy.omap_key_limit ||
      eval.omap_bytes > policy.omap_bytes_limit) {
    out->num_large_omap_objects++;
  }
}

chunk_result_t validate_chunk(
  DoutPrefixProvider &dpp,
  const chunk_validation_policy_t &policy,
  const scrub_map_set_t &in)
{
  LOG_PREFIX(validate_chunk);
  chunk_result_t ret;

  const std::set<hobject_t> object_set = get_object_set(in);

  // Evaluate every object (object_errors + stats) and cache the results.
  // We also need the per-head snapset/object_info for snapshot validation below.
  std::map<hobject_t, object_evaluation_t> evals;
  for (const auto &oid: object_set) {
    object_evaluation_t eval = evaluate_object(policy, oid, in);
    if (eval.inconsistency) {
      ret.object_errors.push_back(*eval.inconsistency);
      ret.object_hoids[oid] = oid;
    }

    // Check whether we need to write back computed digests to oi.
    // Matches classic ScrubBackend::should_fix_digest / missing_digest logic:
    // if the deep scan produced a digest that the stored oi doesn't have (or
    // has a different value), record it so emit_chunk_result can write it back.
    if (eval.object_info) {
      digest_update_t du;
      du.oid = oid;
      bool needs_update = false;

      if (eval.auth_data_digest_present &&
          (!eval.object_info->is_data_digest() ||
           eval.object_info->data_digest != eval.auth_data_digest)) {
        du.data_digest = eval.auth_data_digest;
        needs_update = true;
      }
      if (eval.auth_omap_digest_present &&
          (!eval.object_info->is_omap_digest() ||
           eval.object_info->omap_digest != eval.auth_omap_digest)) {
        du.omap_digest = eval.auth_omap_digest;
        needs_update = true;
      }
      if (needs_update) {
        ret.missing_digest.push_back(std::move(du));
      }
    }

    evals.emplace(oid, std::move(eval));
  }

  // Snapshot validation: for every head object and every shard, call
  // evaluate_snapset() with that shard's own SnapSet against the clone objects
  // present on that shard.  This catches SnapSet corruptions on any shard
  // (primary or replica).  Results from all shards are merged, deduplicating
  // identical errors so that a consistent corruption is only reported once.

  for (const auto &oid : object_set) {
    if (!oid.is_head()) {
      continue;
    }

    // Primary-shard errors go into snapset_errors (stored + counted).
    // Replica-shard errors go into replica_snapset_errors (logged only).
    // Deduplication sets prevent the same (name, snap) pair being reported
    // twice when iterating multiple shards.
    std::set<std::string> emitted_primary_head;
    std::set<std::pair<std::string, uint64_t>> emitted_primary_clone;
    std::set<std::string> emitted_replica_head;
    std::set<std::pair<std::string, uint64_t>> emitted_replica_clone;

    for (const auto &[shard, scrub_map] : in) {
      const bool is_primary = (shard == policy.primary);

      // Obtain SnapSet, OI, and size for the head object on this shard.
      // For the primary shard we reuse the already-computed eval (which applied
      // authoritative-shard selection) to guarantee identical results to the
      // pre-replica-fix code path.  For replica shards we decode directly from
      // the raw scrub map because the replica may carry a different SnapSet.
      std::optional<SnapSet> shard_snapset;
      snapset_status_t shard_snapset_status = snapset_status_t::OK;
      ceph::buffer::list shard_snapset_bl;
      std::optional<object_info_t> shard_head_oi;
      uint64_t shard_head_size = 0;

      const auto &head_eval = evals.at(oid);
      if (is_primary) {
        // Use the cached eval for the primary — same data the old code used.
        shard_snapset        = head_eval.snapset;
        shard_snapset_status = head_eval.snapset_status;
        shard_snapset_bl     = head_eval.snapset_bl;
        shard_head_oi        = head_eval.object_info;
        shard_head_size      = head_eval.size;
      } else {
        // Replica shard: decode directly from this shard's scrub map.
        auto head_it = scrub_map.objects.find(oid);
        if (head_it == scrub_map.objects.end()) {
          continue;  // Head missing on this replica shard — skip.
        }
        const auto &head_obj = head_it->second;
        shard_head_size = head_obj.size;

        auto oi_it = head_obj.attrs.find(OI_ATTR);
        if (oi_it != head_obj.attrs.end()) {
          try {
            auto blp = oi_it->second.cbegin();
            shard_head_oi = object_info_t{};
            decode(*shard_head_oi, blp);
          } catch (...) {
            shard_head_oi = std::nullopt;
          }
        }

        const librados::shard_info_t *shard_info = nullptr;
        if (head_eval.inconsistency) {
          const auto shard_eval_it = std::find_if(
            head_eval.inconsistency->shards.begin(),
            head_eval.inconsistency->shards.end(),
            [&shard](const auto &p) {
              return p.first.osd == shard.osd &&
                p.first.shard == static_cast<int8_t>(shard.shard.id);
            });
          if (shard_eval_it != head_eval.inconsistency->shards.end()) {
            shard_info = &shard_eval_it->second;
          }
        }
        const bool skip_replica_snapset_eval =
          shard_info &&
          (shard_info->has_stat_error() ||
           shard_info->has_read_error() ||
           shard_info->has_info_missing() ||
           shard_info->has_info_corrupted() ||
           shard_info->has_obj_size_info_mismatch());

        auto ss_it = head_obj.attrs.find(SS_ATTR);
        if (!skip_replica_snapset_eval) {
          if (ss_it == head_obj.attrs.end()) {
            shard_snapset_status = snapset_status_t::MISSING;
          } else {
            shard_snapset_bl = ss_it->second;
            try {
              auto blp = ss_it->second.cbegin();
              shard_snapset = SnapSet{};
              decode(*shard_snapset, blp);
              shard_snapset_status = snapset_status_t::OK;
            } catch (...) {
              shard_snapset = std::nullopt;
              shard_snapset_status = snapset_status_t::CORRUPTED;
            }
          }
        }
      }

      // Collect clones for this head.
      // For the primary: include all clones from object_set (mirrors old code),
      // using the cached eval OI. Track which clones are present on the
      // primary's own scrub map so we can suppress errors for replica-only ones.
      // For replicas: only include clones present on the replica's scrub map.
      all_clones_list_t shard_clones;
      std::set<hobject_t> primary_local_clones;  // clones in primary's scrub map
      for (const auto &coid : object_set) {
        if (!coid.is_snap() || coid.get_head() != oid.get_head()) {
          continue;
        }
        if (is_primary) {
          // Include all clones (with eval OI) — same as old code.
          const auto &clone_eval = evals.at(coid);
          shard_clones.push_back(
            clone_info_t{coid, clone_eval.object_info, clone_eval.size});
          // Track whether this clone is locally present on the primary.
          if (scrub_map.objects.count(coid)) {
            primary_local_clones.insert(coid);
          }
        } else {
          // Replica: only include clones present on this shard.
          auto clone_it = scrub_map.objects.find(coid);
          if (clone_it == scrub_map.objects.end()) {
            continue;
          }
          clone_info_t ci;
          ci.hoid = coid;
          auto oi_it = clone_it->second.attrs.find(OI_ATTR);
          if (oi_it != clone_it->second.attrs.end()) {
            try {
              auto blp = oi_it->second.cbegin();
              ci.oi = object_info_t{};
              decode(*ci.oi, blp);
            } catch (...) {
              ci.oi = std::nullopt;
            }
          }
          ci.size = clone_it->second.size;
          shard_clones.push_back(std::move(ci));
        }
      }

      auto result = evaluate_snapset(
        dpp,
        policy.pgid,
        policy.mode_desc,
        oid,
        shard_snapset,
        shard_snapset_status,
        shard_snapset_bl,
        shard_clones,
        shard_head_oi,
        shard_head_size);
      // Accumulate classic-format log messages only from the primary shard
      // (replica shard messages would be duplicates and are not expected by tests)
      if (is_primary) {
        for (auto &msg : result.log_messages) {
          ret.snapset_log_messages.emplace_back(std::move(msg));
        }
      }

      // Route errors: primary shard → snapset_errors (stored + counted);
      //               replica shards → replica_snapset_errors (logged only).
      auto &head_seen = is_primary ? emitted_primary_head : emitted_replica_head;
      auto &clone_seen = is_primary ? emitted_primary_clone : emitted_replica_clone;
      auto &dest_head = is_primary ? ret.snapset_errors : ret.replica_snapset_errors;
      auto &dest_clone = is_primary ? ret.snapset_errors : ret.replica_snapset_errors;

      // Collect clone errors, suppressing replica-only ones for the primary path.
      std::vector<inconsistent_snapset_wrapper*> emittable_clones;
      for (auto &ce : result.clone_errors) {
        if (!ce.errors) continue;
        if (is_primary) {
          // Suppress errors for clones absent from the primary's scrub map.
          // Those are replica-only objects; their cross-shard difference is
          // already captured in object_errors.
          auto hoid_it = std::find_if(
            object_set.begin(), object_set.end(),
            [&ce](const hobject_t &h) {
              return h.oid.name == ce.object.name &&
                     h.snap == snapid_t{ce.object.snap} &&
                     h.nspace == ce.object.nspace;
            });
          if (hoid_it != object_set.end() &&
              !primary_local_clones.count(*hoid_it)) {
            continue;  // Clone only on replica; skip for primary path.
          }
        }
        emittable_clones.push_back(&ce);
      }

      // For the primary path, strip extra-clone references from the head entry
      // that correspond to replica-only clones (not in primary_local_clones).
      // If all extra clones were replica-only, clear EXTRA_CLONES from errors.
      if (is_primary && result.head_error &&
          (result.head_error->errors &
           librados::inconsistent_snapset_t::EXTRA_CLONES)) {
        auto &extra = result.head_error->clones;
        extra.erase(
          std::remove_if(extra.begin(), extra.end(),
            [&](uint64_t s) {
              auto it = std::find_if(
                object_set.begin(), object_set.end(),
                [&](const hobject_t &h) {
                  return h.oid.name == oid.oid.name &&
                         h.snap == snapid_t{s} &&
                         h.nspace == oid.nspace;
                });
              return it != object_set.end() &&
                     !primary_local_clones.count(*it);
            }),
          extra.end());
        if (extra.empty()) {
          result.head_error->errors &=
            ~static_cast<uint64_t>(
              librados::inconsistent_snapset_t::EXTRA_CLONES);
        }
      }

      // Emit the head entry when it has its own errors OR when there are clone
      // errors for this head (matching classic OSD: head is pushed when
      // head_error.errors || soid_error_count > 0).  The head entry carries
      // the snapset payload so the caller can display partial/corrupt snapset
      // metadata alongside the clone errors (e.g. obj9/obj10/obj14 in the snaps
      // test where the head has "errors":[] but clone sizes or overlaps are off).
      if (result.head_error &&
          (result.head_error->errors || !emittable_clones.empty())) {
        if (head_seen.find(oid.oid.name) == head_seen.end()) {
          dest_head.push_back(std::move(*result.head_error));
          DEBUGDPP(
            "debug snapset dest_head oid={} size={} entries={}",
            dpp,
            oid,
            dest_head.size(),
            dest_head);
          head_seen.insert(oid.oid.name);
        }
      }
      for (auto *ce : emittable_clones) {
        auto key = std::make_pair(ce->object.name, uint64_t{ce->object.snap});
        if (clone_seen.find(key) == clone_seen.end()) {
          clone_seen.insert(key);
          dest_clone.push_back(std::move(*ce));
        }
      }
    }
  }

  // Detect orphan clones: snap objects whose head is absent from object_set.
  // These are headless clones that were missed by the head-based loop above.
  // Emit errors only for the primary shard (same as old !has_head branch).
  for (const auto &oid : object_set) {
    if (!oid.is_snap()) {
      continue;
    }
    hobject_t head = oid.get_head();
    if (object_set.count(head)) {
      continue;  // Head exists; handled by the head-based loop above.
    }
    // Orphan clone: its head is not in any shard's scrub map.
    inconsistent_snapset_wrapper clone_error{oid};
    const auto &eval = evals.at(oid);
    if (!eval.object_info) {
      clone_error.set_info_missing();
    }
    clone_error.set_headless();
    ret.snapset_errors.push_back(clone_error);
  }

  // Accumulate stats last: classic scrub skips an unexpected (headless) clone
  // entirely rather than counting it against the pg's object stats.
  std::set<hobject_t> headless_clones;
  for (const auto &se : ret.snapset_errors) {
    if (!se.headless()) {
      continue;
    }
    for (const auto &oid : object_set) {
      if (oid.is_snap() &&
          oid.oid.name == se.object.name &&
          oid.nspace == se.object.nspace &&
          oid.snap == snapid_t{se.object.snap}) {
        headless_clones.insert(oid);
      }
    }
  }

  for (const auto &oid : object_set) {
    if (headless_clones.count(oid)) {
      continue;
    }
    std::optional<SnapSet> head_snapset;
    if (oid.is_snap()) {
      auto head_it = evals.find(oid.get_head());
      if (head_it != evals.end()) {
        head_snapset = head_it->second.snapset;
      }
    }
    add_object_to_stats(policy, oid, evals.at(oid), head_snapset, &ret.stats);
  }

  // Count errors matching classic OSD's scrub_backend.cc:
  //   (a) Per-shard: +1 for each shard whose shard_info has shallow/deep errors.
  //       Classic: shard_map[srd].errors != 0 → shallow_errors++ or deep_errors++
  //       (scrub_backend.cc:1334-1341, 1372)
  //   (b) Per-object: +1 if the object-level error flags (iow.errors) have
  //       shallow/deep errors (e.g. SIZE_MISMATCH, SNAPSET_INCONSISTENCY).
  //       Classic: object_error.has_shallow_errors() → shallow_errors++
  //       (scrub_backend.cc:1041-1045)
  // These two are additive — an object with a shard-level SIZE_MISMATCH_INFO
  // AND object-level SIZE_MISMATCH counts as 2 errors (one per category).
  for (const auto &i: ret.object_errors) {
    for (const auto &[shard_id, shard_info] : i.shards) {
      if (shard_info.has_shallow_errors()) {
        ++ret.stats.num_shallow_scrub_errors;
      } else if (shard_info.has_deep_errors()) {
        ++ret.stats.num_deep_scrub_errors;
      }
    }
    if (i.has_shallow_errors()) {
      ++ret.stats.num_shallow_scrub_errors;
    } else if (i.has_deep_errors()) {
      ++ret.stats.num_deep_scrub_errors;
    }
  }
  // Count primary-path snapset entries:
  //   Clone/headless entries (snap != CEPH_NOSNAP): always count when they
  //   carry errors.
  //   Head entries (snap == CEPH_NOSNAP): count when they have any errors.
  //   For content-level errors (e.g. EXTRA_CLONES, CLONE_MISSING) the per-shard
  //   object_errors loop does not fire (these are snapset-path only), so they
  //   must always be counted here.
  //   For attr-level errors (SNAPSET_MISSING or SNAPSET_CORRUPTED) on the primary
  //   shard: in a multi-copy pool the primary shard appears in object_errors and
  //   the per-shard loop already counted it (+1 per shard), so skip here to avoid
  //   double-counting.  In a single-copy pool the object is NOT in object_errors,
  //   so the attr-level error must be counted here — matching classic OSD's
  //   _scan_snaps() which increments shallow_errors at the point of decode failure
  //   regardless of pool type.
  {
    // Build the set of head object names already counted via the shard loop.
    std::set<std::string> in_object_errors;
    for (const auto &i : ret.object_errors) {
      if (i.object.snap == CEPH_NOSNAP) {
        in_object_errors.insert(i.object.name);
      }
    }
    constexpr uint64_t snapset_attr_only_mask =
      static_cast<uint64_t>(librados::inconsistent_snapset_t::SNAPSET_MISSING) |
      static_cast<uint64_t>(librados::inconsistent_snapset_t::SNAPSET_CORRUPTED);
    for (const auto &se : ret.snapset_errors) {
      if (!se.errors) continue;
      if (se.object.snap != CEPH_NOSNAP) {
        // Clone or headless clone: always count.
        ++ret.stats.num_shallow_scrub_errors;
      } else if (se.errors & ~snapset_attr_only_mask) {
        // Head has content errors (e.g. EXTRA_CLONES) not counted via shard loop.
        ++ret.stats.num_shallow_scrub_errors;
      } else {
        // Head has only attr errors (SNAPSET_MISSING/CORRUPTED).
        // Count only if NOT already in object_errors (i.e. not in a multi-copy
        // pool where the per-shard loop fired).
        if (in_object_errors.find(se.object.name) == in_object_errors.end()) {
          ++ret.stats.num_shallow_scrub_errors;
        }
      }
    }
  }
  // Count replica-shard snapset errors (SNAPSET_MISSING or SNAPSET_CORRUPTED)
  // only when the primary shard is ALSO unable to provide a clean snapset
  // (i.e. no shard has selected_oi=true for that head object).
  //
  // When the primary CAN decode its snapset (selected_oi exists), the replica's
  // snapset attr error is already counted via the per-shard loop above
  // (has_replica_snapset_shard_errors promotes the object into object_errors and
  // the shard's SNAPSET_MISSING/CORRUPTED is incremented there).  Adding it here
  // again would double-count (obj2, obj15 in the snaps-replica test).
  //
  // When NO shard has selected_oi=true (ROBJ16 in the repair test: both OSD0 and
  // OSD1 have snapset attr errors), the replica's shard error is still counted by
  // the per-shard loop, but the no-auth loop above adds one extra count for the
  // "failed to pick suitable object info" error.  In that case classic scrub also
  // increments shallow_errors once from _scan_snaps for the unreadable snapset;
  // we replicate that by counting the replica_snapset_errors entry here when the
  // corresponding head has no selected_oi shard.
  //
  // Note: inconsistent_snapset_t uses its own enum (SNAPSET_MISSING=1<<0,
  // SNAPSET_CORRUPTED=1<<1), distinct from err_t::SNAPSET_MISSING (1<<16).
  {
    // Build a set of head names whose object_errors entry has a selected_oi shard.
    // For those objects the replica shard error is already counted; skip them.
    std::set<std::string> heads_with_selected_oi;
    for (const auto &i : ret.object_errors) {
      if (i.object.snap != CEPH_NOSNAP) continue;
      const bool has_sel = std::any_of(
        i.shards.begin(), i.shards.end(),
        [](const auto &p) { return p.second.selected_oi; });
      if (has_sel) {
        heads_with_selected_oi.insert(i.object.name);
      }
    }
    constexpr uint64_t snapset_attr_errors =
      static_cast<uint64_t>(librados::inconsistent_snapset_t::SNAPSET_MISSING) |
      static_cast<uint64_t>(librados::inconsistent_snapset_t::SNAPSET_CORRUPTED);
    for (const auto &se : ret.replica_snapset_errors) {
      if (!(se.errors & snapset_attr_errors)) continue;
      if (heads_with_selected_oi.count(se.object.name)) continue;
      ++ret.stats.num_shallow_scrub_errors;
    }
  }
  for (const auto &i : ret.object_errors) {
    const bool any_selected = std::any_of(
      i.shards.begin(), i.shards.end(),
      [](const auto &p) { return p.second.selected_oi; });
    if (!any_selected && i.object.snap == CEPH_NOSNAP &&
        !std::any_of(
          i.shards.begin(), i.shards.end(),
          [](const auto &p) {
            return p.second.has_read_error();
          })) {
      ++ret.stats.num_shallow_scrub_errors;
    }
  }
  ret.stats.num_scrub_errors = ret.stats.num_shallow_scrub_errors +
    ret.stats.num_deep_scrub_errors;

  // Sort snapset_errors to match the classic OSD's omap-key order:
  // (snap ascending, then name ascending).  The classic ScrubStore writes
  // all snapset entries with the same fixed hash (0x77777777), so the omap
  // key reduces to (snap_hex, name) in lexicographic order — equivalent to
  // numeric snap ascending then name ascending.  Maintaining this order
  // ensures that rados list-inconsistent-snapset output matches expectations.
  std::sort(ret.snapset_errors.begin(), ret.snapset_errors.end(),
    [](const inconsistent_snapset_wrapper &a,
       const inconsistent_snapset_wrapper &b) {
      if (a.object.snap != b.object.snap) {
        return a.object.snap < b.object.snap;
      }
      return a.object.name < b.object.name;
    });

  return ret;
}

}
