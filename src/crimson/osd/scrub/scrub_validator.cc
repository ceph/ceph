// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ranges>

#include "osd/osd_types_fmt.h"

#include "crimson/common/log.h"
#include "crimson/osd/scrub/scrub_validator.h"
#include "osd/ECUtil.h"

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

struct shard_evaluation_t {
  pg_shard_t source;
  shard_info_wrapper shard_info;

  std::optional<object_info_t> object_info;
  std::optional<SnapSet> snapset;
  std::optional<ECUtil::HashInfo> hinfo;

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
  if (!maybe_obj || maybe_obj->negative) {
    // impossible since chunky scrub was introduced
    ceph_assert(!maybe_obj->negative);
    ret.shard_info.set_missing();
    return ret;
  }

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
  }

  if (obj.stat_error) {
    ret.shard_info.set_stat_error();
  }

  {
    auto xiter = obj.attrs.find(OI_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_info_missing();
    } else {
      bufferlist bl;
      bl.push_back(xiter->second);
      ret.object_info = object_info_t{};
      try {
	auto bliter = bl.cbegin();
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
    ret.shard_info.set_size_mismatch_info();
  }

  if (oid.is_head()) {
    auto xiter = obj.attrs.find(SS_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_snapset_missing();
    } else {
      bufferlist bl;
      bl.push_back(xiter->second);
      ret.snapset = SnapSet{};
      try {
	auto bliter = bl.cbegin();
	::decode(*(ret.snapset), bliter);
      } catch (...) {
	ret.shard_info.set_snapset_corrupted();
	ret.snapset = std::nullopt;
      }
    }
  }

  if (policy.is_ec()) {
    auto xiter = obj.attrs.find(ECUtil::get_hinfo_key());
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_hinfo_missing();
    } else {
      bufferlist bl;
      bl.push_back(xiter->second);
      ret.hinfo = ECUtil::HashInfo{};
      try {
	auto bliter = bl.cbegin();
	decode(*(ret.hinfo), bliter);
      } catch (...) {
	ret.shard_info.set_hinfo_corrupted();
	ret.hinfo = std::nullopt;
      }
    }
  }

  if (ret.object_info) {
    if (ret.shard_info.data_digest_present &&
	ret.object_info->is_data_digest() &&
	(ret.object_info->data_digest != ret.shard_info.data_digest)) {
      ret.shard_info.set_data_digest_mismatch_info();
    }
    if (ret.shard_info.omap_digest_present &&
	ret.object_info->is_omap_digest() &&
	(ret.object_info->omap_digest != ret.shard_info.omap_digest)) {
      ret.shard_info.set_omap_digest_mismatch_info();
    }
  }

  return ret;
}

librados::obj_err_t compare_candidate_to_authoritative(
  const chunk_validation_policy_t &policy,
  const hobject_t &oid,
  const shard_evaluation_t &auth,
  const shard_evaluation_t &cand)
{
  using namespace librados;
  obj_err_t ret;

  if (cand.shard_info.has_shard_missing()) {
    return ret;
  }

  const auto &auth_si = auth.shard_info;
  const auto &cand_si = cand.shard_info;

  if (auth_si.data_digest != cand_si.data_digest) {
    ret.errors |= obj_err_t::DATA_DIGEST_MISMATCH;
  }

  if (auth_si.omap_digest != cand_si.omap_digest) {
    ret.errors |= obj_err_t::OMAP_DIGEST_MISMATCH;
  }

  {
    auto aiter = auth_si.attrs.find(OI_ATTR);
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(OI_ATTR);
    if (citer == cand_si.attrs.end() ||
	!aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::OBJECT_INFO_INCONSISTENCY;
    }
  }

  if (oid.is_head()) {
    auto aiter = auth_si.attrs.find(SS_ATTR);
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(SS_ATTR);
    if (citer == cand_si.attrs.end() ||
	!aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::SNAPSET_INCONSISTENCY;
    }
  }

  if (policy.is_ec()) {
    auto aiter = auth_si.attrs.find(ECUtil::get_hinfo_key());
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(ECUtil::get_hinfo_key());
    if (citer == cand_si.attrs.end() ||
	!aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::HINFO_INCONSISTENCY;
    }
  }

  if (auth_si.size != cand_si.size) {
    ret.errors |= obj_err_t::SIZE_MISMATCH;
  }

  auto is_sys_attr = [&policy](const auto &str) {
    return str == OI_ATTR || str == SS_ATTR ||
      (policy.is_ec() && str == ECUtil::get_hinfo_key());
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

  size_t omap_keys{0};
  size_t omap_bytes{0};
};
object_evaluation_t evaluate_object(
  const chunk_validation_policy_t &policy,
  const hobject_t &hoid,
  const scrub_map_set_t &maps)
{
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

  auto &auth_eval = shards.back();

  object_evaluation_t ret;
  inconsistent_obj_wrapper iow{hoid};
  if (!auth_eval.has_errors()) {
    ret.object_info = auth_eval.object_info;
    ret.omap_keys = auth_eval.omap_keys;
    ret.omap_bytes = auth_eval.omap_bytes;
    ret.snapset = auth_eval.snapset;
    if (auth_eval.object_info->size > policy.max_object_size) {
      iow.set_size_too_large();
    }
    auth_eval.shard_info.selected_oi = true;
    std::for_each(
      shards.begin(), shards.end() - 1,
      [&policy, &hoid, &auth_eval, &iow](auto &cand_eval) {
	auto err = compare_candidate_to_authoritative(
	  policy, hoid, auth_eval, cand_eval);
	iow.merge(err);
      });
  }

  if (iow.errors ||
      std::any_of(shards.begin(), shards.end(),
		  [](auto &cand) { return cand.has_errors(); })) {
    for (auto &eval : shards) {
      iow.shards.emplace(
	librados::osd_shard_t{eval.source.osd, eval.source.shard},
	eval.shard_info);
      iow.union_shards.errors |= eval.shard_info.errors;
    }
    if (auth_eval.object_info) {
      iow.version = auth_eval.object_info->version.version;
    }
    ret.inconsistency = iow;
  }
  return ret;
}

using clone_meta_list_t = std::list<std::pair<hobject_t, object_info_t>>;
std::optional<inconsistent_snapset_wrapper> evaluate_snapset(
  DoutPrefixProvider &dpp,
  const hobject_t &hoid,
  const std::optional<SnapSet> &maybe_snapset,
  const clone_meta_list_t &clones)
{
  LOG_PREFIX(evaluate_snapset);
  /* inconsistent_snapset_t has several error codes that seem to pertain to
   * specific objects rather than to the snapset specifically.  I'm choosing
   * to ignore those for now */
  inconsistent_snapset_wrapper ret{hoid};
  if (!maybe_snapset) {
    ret.set_headless();
    return ret;
  }
  const auto &snapset = *maybe_snapset;

  auto clone_iter = clones.begin();
  for (auto ss_clone_id : snapset.clones) {
    for (; clone_iter != clones.end() &&
	   clone_iter->first.snap < ss_clone_id;
	 ++clone_iter) {
      ret.set_clone(clone_iter->first.snap);
    }

    if (clone_iter != clones.end() &&
	clone_iter->first.snap == ss_clone_id) {
      auto ss_clone_size_iter = snapset.clone_size.find(ss_clone_id);
      if (ss_clone_size_iter == snapset.clone_size.end() ||
	  ss_clone_size_iter->second != clone_iter->second.size) {
	ret.set_size_mismatch();
      }
      ++clone_iter;
    } else {
      ret.set_clone_missing(ss_clone_id);
    }
  }

  for (; clone_iter != clones.end(); ++clone_iter) {
    ret.set_clone(clone_iter->first.snap);
  }

  if (ret.errors) {
    DEBUGDPP(
      "snapset {}, clones {}",
      dpp, snapset, clones);
    return ret;
  } else {
    return std::nullopt;
  }
}

void add_object_to_stats(
  const chunk_validation_policy_t &policy,
  const object_evaluation_t &eval,
  object_stat_sum_t *out)
{
  auto &ss = eval.snapset;
  if (!eval.object_info) {
    return;
  }
  auto &oi = *eval.object_info;
  ceph_assert(out);
  out->num_objects++;
  if (ss) {
    out->num_bytes += oi.size;
    for (auto clone : ss->clones) {
      out->num_bytes += ss->get_clone_bytes(clone);
      out->num_object_clones++;
    }
    if (oi.is_whiteout()) {
      out->num_whiteouts++;
    }
  }
  if (oi.is_dirty()) {
    out->num_objects_dirty++;
  }
  if (oi.is_cache_pinned()) {
    out->num_objects_pinned++;
  }
  if (oi.has_manifest()) {
    out->num_objects_manifest++;
  }

  if (eval.omap_keys > 0) {
    out->num_objects_omap++;
  }
  out->num_omap_keys += eval.omap_keys;
  out->num_omap_bytes += eval.omap_bytes;

  if (oi.soid.nspace == policy.hitset_namespace) {
    out->num_objects_hit_set_archive++;
    out->num_bytes_hit_set_archive += oi.size;
  }

  if (eval.omap_keys > policy.omap_key_limit ||
      eval.omap_bytes > policy.omap_bytes_limit) {
    out->num_large_omap_objects++;
  }
}

chunk_result_t validate_chunk(
  DoutPrefixProvider &dpp,
  const chunk_validation_policy_t &policy, const scrub_map_set_t &in)
{
  chunk_result_t ret;

  const std::set<hobject_t> object_set = get_object_set(in);

  std::list<std::pair<hobject_t, SnapSet>> heads;
  clone_meta_list_t clones;
  for (const auto &oid: object_set) {
    object_evaluation_t eval = evaluate_object(policy, oid, in);
    add_object_to_stats(policy, eval, &ret.stats);
    if (eval.inconsistency) {
      ret.object_errors.push_back(*eval.inconsistency);
    }
    if (oid.is_head()) {
      /* We're only going to consider the head object as "existing" if
       * evaluate_object was able to find a sensible, authoritative copy
       * complete with snapset */
      if (eval.snapset) {
	heads.emplace_back(oid, *eval.snapset);
      }
    } else {
      /* We're only going to consider the clone object as "existing" if
       * evaluate_object was able to find a sensible, authoritative copy
       * complete with an object_info */
      if (eval.object_info) {
	clones.emplace_back(oid, *eval.object_info);
      }
    }
  }

  const hobject_t max_oid = hobject_t::get_max();
  while (heads.size() || clones.size()) {
    const hobject_t &next_head = heads.size() ? heads.front().first : max_oid;
    const hobject_t &next_clone = clones.size() ? clones.front().first : max_oid;
    hobject_t head_to_process = std::min(next_head, next_clone).get_head();

    clone_meta_list_t clones_to_process;
    auto clone_iter = clones.begin();
    while (clone_iter != clones.end() && clone_iter->first < head_to_process)
      ++clone_iter;
    clones_to_process.splice(
      clones_to_process.end(), clones, clones.begin(), clone_iter);

    const auto head_meta = [&]() -> std::optional<SnapSet> {
      if (head_to_process == next_head) {
	auto ret = std::move(heads.front().second);
	heads.pop_front();
	return ret;
      } else {
	return std::nullopt;
      }
    }();

    if (auto result = evaluate_snapset(
	  dpp, head_to_process, head_meta, clones_to_process); result) {
      ret.snapset_errors.push_back(*result);
    }
  }

  for (const auto &i: ret.object_errors) {
    ret.stats.num_shallow_scrub_errors +=
      (i.has_shallow_errors() || i.union_shards.has_shallow_errors());
    ret.stats.num_deep_scrub_errors +=
      (i.has_deep_errors() || i.union_shards.has_deep_errors());
  }
  ret.stats.num_shallow_scrub_errors += ret.snapset_errors.size();
  ret.stats.num_scrub_errors = ret.stats.num_shallow_scrub_errors +
    ret.stats.num_deep_scrub_errors;

  return ret;
}

}
