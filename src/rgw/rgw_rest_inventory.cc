// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_inventory.h"

#include "common/Formatter.h"
#include "common/XMLFormatter.h"
#include "rgw_common.h"
#include "rgw_lc.h"
#include "rgw_process_env.h"
#include "rgw_rest_s3.h"
#include "rgw_sal.h"
#ifdef _ARROW_EXIST
#include "rgw_inventory.h"
#endif

#define dout_subsys ceph_subsys_rgw

namespace {

int load_inventory_attr(const DoutPrefixProvider* dpp,
                        const rgw::sal::Attrs& attrs,
                        rgw::inventory::BucketConfigurations& configs)
{
  int r = rgw::inventory::decode_bucket_inventory(attrs, &configs);
  if (r == -EIO) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode inventory attr" << dendl;
  }
  return r;
}

int save_inventory_attr(const DoutPrefixProvider* dpp, optional_yield y,
                        rgw::sal::Bucket* bucket, rgw::sal::Attrs& attrs,
                        const rgw::inventory::BucketConfigurations& configs)
{
  bufferlist bl;
  configs.encode(bl);
  attrs[RGW_ATTR_INVENTORY] = std::move(bl);
  int ret = bucket->merge_and_store_attrs(dpp, attrs, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: merge_and_store_attrs returned " << ret
                      << dendl;
  }
  return ret;
}

bool check_bucket_permission(RGWOp* op, req_state* s, uint64_t action)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] =
      rgw_check_policy_condition(op, s, false);
  if (has_s3_resource_tag) {
    rgw_iam_add_buckettags(op, s);
  }
  return verify_bucket_permission(op, s, action);
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// PUT /bucket?inventory&id=<id>
// AWS authorizes PutBucketInventoryConfiguration with s3:PutInventoryConfiguration
// ---------------------------------------------------------------------------
int RGWPutBucketInventory_ObjStore_S3::verify_permission(optional_yield y)
{
  return check_bucket_permission(this, s, rgw::IAM::s3PutInventoryConfiguration)
             ? 0 : -EACCES;
}

void RGWPutBucketInventory_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

int RGWPutBucketInventory_ObjStore_S3::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  return op_ret;
}

void RGWPutBucketInventory_ObjStore_S3::execute(optional_yield y)
{
#ifndef _ARROW_EXIST
  s->err.message = "S3 Inventory requires radosgw built with Parquet support";
  op_ret = -ERR_NOT_IMPLEMENTED;
#else
  op_ret = get_params(y);
  if (op_ret < 0) return;

  RGWXMLParser parser;
  if (!parser.init()) {
    op_ret = -EINVAL;
    return;
  }
  if (!parser.parse(data.c_str(), data.length(), 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }
  try {
    RGWXMLDecoder::decode_xml("InventoryConfiguration", config, &parser);
  } catch (RGWXMLDecoder::err& e) {
    ldpp_dout(this, 5) << "Bad inventory configuration: " << e << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  const std::string url_id = s->info.args.get("id");
  if (!url_id.empty() && url_id != config.id) {
    s->err.message = "The id in the request does not match the id in the body";
    op_ret = -EINVAL;
    return;
  }

  std::string err;
  op_ret = config.validate(&err);
  if (op_ret < 0) {
    s->err.message = err;
    return;
  }

  // v1: destination must exist in this cluster and differ from the source
  std::unique_ptr<rgw::sal::Bucket> dest;
  op_ret = rgw::inventory::load_destination_bucket(
      this, driver, s->bucket->get_tenant(), config, &dest, y);
  if (op_ret < 0) {
    s->err.message = "Destination bucket '" +
                     config.destination.s3.bucket_name() +
                     "' does not exist in this cluster";
    if (op_ret == -ENOENT) op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }
  if (dest->get_tenant() == s->bucket->get_tenant() &&
      dest->get_name() == s->bucket->get_name()) {
    s->err.message = "Destination bucket must differ from the source bucket";
    op_ret = -EINVAL;
    return;
  }

  // Fail early on a policy that won't allow delivery; it is checked again
  // at delivery time so later policy changes take effect.
  std::string policy_err;
  op_ret = rgw::inventory::verify_destination_policy(
      this, s->cct, s->bucket.get(), dest.get(), config, &policy_err);
  if (op_ret < 0) {
    s->err.message = policy_err;
    return;
  }

  rgw::inventory::BucketConfigurations existing;
  int r = load_inventory_attr(this, s->bucket_attrs, existing);
  if (r < 0 && r != -ENOENT) {
    op_ret = r;
    return;
  }
  op_ret = existing.add_or_replace(std::move(config));
  if (op_ret < 0) {
    s->err.message = "Maximum number of inventory configurations (1000) reached";
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                         &data, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned " << op_ret
                       << dendl;
    return;
  }

  op_ret = save_inventory_attr(this, y, s->bucket.get(), s->bucket_attrs,
                               existing);
  if (op_ret < 0) return;

  // Put the bucket on the lifecycle work list so the LC worker visits it.
  // config == nullptr registers the entry only; no LC rules are written.
  op_ret = driver->get_rgwlc()->set_bucket_config(
      this, y, s->bucket.get(), s->bucket_attrs, nullptr);
#endif
}

void RGWPutBucketInventory_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this);
}

// ---------------------------------------------------------------------------
// GET /bucket?inventory&id=<id>
// AWS authorizes GetBucketInventoryConfiguration with s3:GetInventoryConfiguration
// ---------------------------------------------------------------------------
int RGWGetBucketInventory_ObjStore_S3::verify_permission(optional_yield y)
{
  return check_bucket_permission(this, s, rgw::IAM::s3GetInventoryConfiguration)
             ? 0 : -EACCES;
}

void RGWGetBucketInventory_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketInventory_ObjStore_S3::execute(optional_yield y)
{
  const std::string id = s->info.args.get("id");
  if (id.empty()) {
    op_ret = -EINVAL;
    s->err.message = "id parameter is required";
    return;
  }

  rgw::inventory::BucketConfigurations all;
  op_ret = load_inventory_attr(this, s->bucket_attrs, all);
  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_SUCH_CONFIGURATION;
    return;
  }
  if (op_ret < 0) return;

  const auto* cfg = all.get(id);
  if (!cfg) {
    op_ret = -ERR_NO_SUCH_CONFIGURATION;
    return;
  }
  config = *cfg;
}

void RGWGetBucketInventory_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
    dump_errno(s);
    end_header(s, this);
    return;
  }
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  ceph::XMLFormatter f;
  f.open_object_section_with_attrs(
      "InventoryConfiguration",
      ceph::FormatterAttrs("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/",
                     nullptr));
  config.dump_xml(&f);
  f.close_section();
  rgw_flush_formatter_and_reset(s, &f);
}

// ---------------------------------------------------------------------------
// DELETE /bucket?inventory&id=<id>
// AWS authorizes DeleteBucketInventoryConfiguration with s3:PutInventoryConfiguration
// ---------------------------------------------------------------------------
int RGWDeleteBucketInventory_ObjStore_S3::verify_permission(optional_yield y)
{
  return check_bucket_permission(this, s, rgw::IAM::s3PutInventoryConfiguration)
             ? 0 : -EACCES;
}

void RGWDeleteBucketInventory_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteBucketInventory_ObjStore_S3::execute(optional_yield y)
{
  const std::string id = s->info.args.get("id");
  if (id.empty()) {
    op_ret = -EINVAL;
    s->err.message = "id parameter is required";
    return;
  }

  rgw::inventory::BucketConfigurations all;
  int r = load_inventory_attr(this, s->bucket_attrs, all);
  if (r == -ENOENT) {
    op_ret = 0;  // idempotent
    return;
  }
  if (r < 0) {
    op_ret = r;
    return;
  }
  if (!all.remove(id)) {
    op_ret = 0;  // idempotent
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                         nullptr, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned " << op_ret
                       << dendl;
    return;
  }

  op_ret = save_inventory_attr(this, y, s->bucket.get(), s->bucket_attrs, all);
  if (op_ret < 0) return;

  // Last configuration gone and no LC rules: drop the lifecycle work-list
  // entry. Not fatal if this fails; a stale entry just finds nothing to do.
  if (all.configs.empty() && s->bucket_attrs.count(RGW_ATTR_LC) == 0) {
    int lc_ret = driver->get_rgwlc()->remove_bucket_config(
        this, y, s->bucket.get(), false);
    if (lc_ret < 0) {
      ldpp_dout(this, 5) << "WARNING: could not remove lifecycle work-list "
                         << "entry: " << lc_ret << dendl;
    }
  }
}

void RGWDeleteBucketInventory_ObjStore_S3::send_response()
{
  const int http_ret = op_ret ? op_ret : STATUS_NO_CONTENT;
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s, this);
}

// ---------------------------------------------------------------------------
// GET /bucket?inventory  (list)
// AWS authorizes ListBucketInventoryConfigurations with s3:GetInventoryConfiguration
// ---------------------------------------------------------------------------
int RGWListBucketInventory_ObjStore_S3::verify_permission(optional_yield y)
{
  return check_bucket_permission(this, s, rgw::IAM::s3GetInventoryConfiguration)
             ? 0 : -EACCES;
}

void RGWListBucketInventory_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListBucketInventory_ObjStore_S3::execute(optional_yield y)
{
  int r = load_inventory_attr(this, s->bucket_attrs, configs);
  op_ret = (r == -ENOENT) ? 0 : r;  // no attr: empty list
}

void RGWListBucketInventory_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
    dump_errno(s);
    end_header(s, this);
    return;
  }
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  ceph::XMLFormatter f;
  f.open_object_section_with_attrs(
      "ListInventoryConfigurationsResult",
      ceph::FormatterAttrs("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/",
                     nullptr));
  for (const auto& [id, cfg] : configs.configs) {
    f.open_object_section("InventoryConfiguration");
    cfg.dump_xml(&f);
    f.close_section();
  }
  // TODO: pagination (ContinuationToken) beyond 100 configurations
  encode_xml("IsTruncated", false, &f);
  f.close_section();
  rgw_flush_formatter_and_reset(s, &f);
}
