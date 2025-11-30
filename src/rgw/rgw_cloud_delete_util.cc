// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_cloud_delete.h"

#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_obj_manifest.h"
#include "rgw_sal.h"
#include "rgw_zone.h"
#include "rgw_http_errors.h"
#include "driver/rados/rgw_lc_tier.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::cloud_delete {

void CloudDeleteEntry::dump(ceph::Formatter* f) const {
  encode_json("src_bucket", src_bucket, f);
  encode_json("src_key", src_key, f);
  encode_json("src_version_id", src_version_id, f);
  encode_json("target_bucket_name", target_bucket_name, f);
  /*
   * target_object_name is the cloud-side key the worker will HEAD/DELETE.
   * Matches the "expected name" path in handle_entry(): version id comes
   * from src_key.instance, falling back to src_version_id when unset.
   */
  {
    rgw_obj_key versioned_key = src_key;
    if (versioned_key.instance.empty() && !src_version_id.empty()) {
      versioned_key.set_instance(src_version_id);
    }
    encode_json("target_object_name",
                make_target_obj_name(src_bucket.name, versioned_key,
                                     target_by_bucket, false), f);
  }
  encode_json("placement_rule", placement_rule, f);
  encode_json("target_by_bucket", target_by_bucket, f);
  encode_json("retry_count", retry_count, f);
  encode_json("enqueue_time", utime_t{enqueue_time}, f);
  /*
   * next_retry_time is set to ceph::real_time::min() to mean "eligible
   * now". Converting that sentinel through utime_t wraps to garbage, so
   * emit it as a clear string instead.
   */
  if (next_retry_time == ceph::real_time::min()) {
    encode_json("next_retry_time", std::string("immediate"), f);
  } else {
    encode_json("next_retry_time", utime_t{next_retry_time}, f);
  }
  encode_json("pass_id", pass_id, f);
  encode_json("src_etag", src_etag, f);
}

/** Prepare cloud delete context by reading object attrs before deletion.
 *  Decodes the manifest and resolves tier config upfront so no blobs
 *  are carried through the delete path.
 *  Callers on hot paths should pass preloaded_attrs to avoid an extra read. */
CloudDeleteContext prepare_cloud_delete_context(
    const DoutPrefixProvider* dpp,
    rgw::sal::Driver* driver,
    rgw::sal::Object* obj,
    bool is_versioned_delete_marker_creation,
    const rgw_bucket& bucket,
    const rgw_owner& bucket_owner,
    optional_yield y,
    const rgw::sal::Attrs* preloaded_attrs)
{
  const bool used_preloaded_attrs = (preloaded_attrs != nullptr);

  // Early-out: no work when cloud delete is not configured
  if (!driver || !driver->get_rgwcloud_delete() || is_versioned_delete_marker_creation) return {};

  bool is_versioning_suspended = false;
  if (obj) {
    if (auto* src_bucket = obj->get_bucket(); src_bucket) {
      auto& binfo = src_bucket->get_info();
      is_versioning_suspended = binfo.versioned() && !binfo.versioning_enabled();
    }
  }

  const rgw::sal::Attrs* attrs = preloaded_attrs;
  if (!attrs) {
    if (obj->get_obj_attrs(y, dpp) < 0) {
      ldpp_dout(dpp, 5) << __func__
          << ": WARNING: failed to read object attrs for cloud delete check"
          << " (bucket=" << bucket.name << ", key=" << obj->get_key().name
          << ") - if this object is cloud-tiered, the remote copy may be"
          << " orphaned after local delete" << dendl;
      return {};
    }
    attrs = &obj->get_attrs();
  }

  auto manifest_it = attrs->find(RGW_ATTR_MANIFEST);
  if (manifest_it == attrs->end()) return {};

  RGWObjManifest manifest;
  try {
    auto bliter = manifest_it->second.cbegin();
    decode(manifest, bliter);
  } catch (const buffer::error&) {
    ldpp_dout(dpp, 1) << __func__
        << ": WARNING: failed to decode manifest for cloud delete check"
        << " (bucket=" << bucket.name << ", key=" << obj->get_key().name
        << ") - if this object is cloud-tiered, the remote copy may be"
        << " orphaned after local delete" << dendl;
    return {};
  }
  if (!manifest.is_tier_type_s3()) return {};

  RGWObjTier tier_config;
  manifest.get_tier_config(&tier_config);
  auto& s3 = tier_config.tier_placement.t.s3;

  /** Resolve allow_delete_through with retain_head_object gating.
   * Both settings must be enabled to activate cloud delete. */
  bool allow_delete = s3.allow_delete_through &&
                      tier_config.tier_placement.retain_head_object;
  auto* zone = driver->get_zone();
  if (!zone) {
    ldpp_dout(dpp, 1) << __func__
                      << ": WARNING: missing zone, skipping cloud delete"
                      << dendl;
    return {};
  }

  rgw_placement_rule rule = manifest.get_head_placement_rule();
  rule.storage_class = tier_config.tier_placement.storage_class;
  std::unique_ptr<rgw::sal::PlacementTier> live_tier;
  int r = zone->get_zonegroup().get_placement_tier(rule, &live_tier);
  if (r >= 0 && live_tier) {
    if (live_tier->is_tier_type_s3()) {
      allow_delete = live_tier->allow_delete_through() &&
                     live_tier->retain_head_object();
    } else {
      allow_delete = false;
    }
  } else {
    ldpp_dout(dpp, 10) << __func__
                       << ": using manifest fallback for cloud delete gating on "
                       << rule << " after live tier lookup failed: " << r
                       << dendl;
  }
  if (!allow_delete) return {};

  /** Pre-build the entry with everything known before the delete.
   *  src_version_id is filled in by maybe_enqueue_after_delete. */
  auto& zg = zone->get_zonegroup();

  CloudDeleteEntry entry;
  entry.src_bucket = bucket;
  entry.src_key = obj->get_key();
  std::string owner_id;
  if (const auto* acct = std::get_if<rgw_account_id>(&bucket_owner); acct) {
    owner_id = *acct;
  } else if (const auto* user = std::get_if<rgw_user>(&bucket_owner); user) {
    owner_id = user->id;
  }
  entry.target_bucket_name = s3.make_target_bucket_name(
      zg.get_name(),
      tier_config.tier_placement.storage_class,
      bucket.name, bucket.tenant, owner_id);
  entry.placement_rule = manifest.get_head_placement_rule();
  entry.placement_rule.storage_class = tier_config.tier_placement.storage_class;
  entry.target_by_bucket = s3.target_by_bucket;

  auto etag_it = attrs->find(RGW_ATTR_ETAG);
  if (etag_it != attrs->end()) {
    entry.src_etag = etag_it->second.to_str();
  }

  CloudDeleteContext ctx;
  ctx.entry = std::move(entry);
  if (auto* v = obj->get_version_tracker().version_for_check()) {
    ctx.check_objv = *v;
  }

  /**
   * Suspended versioning + DELETE without version-id:
   * enqueue on delete-marker only when current version is null data.
   */
  if (is_versioning_suspended && obj->get_key().instance.empty()) {
    bool current_is_null_data_version = false;
    if (!used_preloaded_attrs) {
      /**
       * get_obj_attrs() above resolves OLH and rewrites obj instance to
       * the current authoritative version id.
       */
      const auto& current_instance = obj->get_instance();
      current_is_null_data_version = current_instance.empty() || current_instance == "null";
    } else {
      /**
       * With preloaded attrs, obj instance may still reflect request key.
       * Resolve current version and gate enqueue precisely.
       */
      auto resolved = obj->clone();
      if (resolved &&
          resolved->get_obj_attrs(y, dpp) >= 0 &&
          !resolved->is_delete_marker()) {
        const auto& current_instance = resolved->get_instance();
        current_is_null_data_version = current_instance.empty() || current_instance == "null";
      }
    }
    ctx.enqueue_on_delete_marker = current_is_null_data_version;
  }

  return ctx;
}


void maybe_set_check_objv(CloudDeleteContext& cloud_ctx,
                          obj_version** check_objv)
{
  if (!check_objv || !cloud_ctx.check_objv) {
    return;
  }
  *check_objv = &cloud_ctx.check_objv.value();
}

int maybe_enqueue_after_delete(const DoutPrefixProvider* dpp,
                               rgw::sal::Driver* driver,
                               CloudDeleteContext& cloud_ctx,
                               const std::string& version_id,
                               bool delete_succeeded,
                               bool delete_marker,
                               optional_yield y)
{
  if (!delete_succeeded || !cloud_ctx.entry
      || (delete_marker && !cloud_ctx.enqueue_on_delete_marker)) {
    return 0;
  }

  auto* queue = driver ? driver->get_rgwcloud_delete() : nullptr;
  if (!queue) return 0;

  auto& entry = *cloud_ctx.entry;
  entry.src_version_id = version_id;
  entry.enqueue_time = ceph::real_clock::now();
  entry.next_retry_time = ceph::real_time::min();  // eligible immediately

  int ret = queue->enqueue(dpp, y, entry);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << __func__ << ": WARNING: failed to enqueue cloud delete: "
                      << ret << " (bucket=" << entry.src_bucket.name
                      << ", key=" << entry.src_key.name
                      << ") - remote object may be orphaned" << dendl;
  }
  return ret;
}

} // namespace rgw::cloud_delete
