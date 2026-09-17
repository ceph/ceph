// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_inventory.h"
#include "rgw_rest_s3.h"
#include "rgw_common.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

// -----------------------------------------------------------------------
// Helper: load BucketConfigurations from bucket attr
// -----------------------------------------------------------------------
static int load_inventory_attr(
    const DoutPrefixProvider* dpp,
    const rgw::sal::Attrs& attrs,
    rgw::inventory::BucketConfigurations& configs)
{
  auto it = attrs.find(RGW_ATTR_INVENTORY);
  if (it == attrs.end()) {
    return -ENOENT;
  }
  try {
    auto bl_it = it->second.cbegin();
    configs.decode(bl_it);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode inventory attr: "
                      << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

// -----------------------------------------------------------------------
// Helper: save BucketConfigurations to bucket attr
// -----------------------------------------------------------------------
static int save_inventory_attr(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    rgw::sal::Bucket* bucket,
    rgw::sal::Attrs& attrs,
    const rgw::inventory::BucketConfigurations& configs)
{
  bufferlist bl;
  configs.encode(bl);
  attrs[RGW_ATTR_INVENTORY] = bl;
  int ret = bucket->merge_and_store_attrs(dpp, attrs, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: merge_and_store_attrs returned "
                      << ret << dendl;
  }
  return ret;
}

// -----------------------------------------------------------------------
// PUT /bucket?inventory&id=<id>
// -----------------------------------------------------------------------
int RGWPutBucketInventory_ObjStore_S3::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] =
      rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketInventoryConfiguration)) {
    return -EACCES;
  }
  return 0;
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
  op_ret = get_params(y);
  if (op_ret < 0) return;

  // parse XML body
  RGWXMLParser parser;
  if (!parser.init()) { op_ret = -EINVAL; return; }

  char* buf = data.c_str();
  if (!parser.parse(buf, data.length(), 1)) {
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

  // validate the id from URL matches the id in the body
  std::string url_id = s->info.args.get("id");
  if (!url_id.empty() && url_id != config.id) {
    s->err.message = "The id in the request does not match the id in the body";
    op_ret = -EINVAL;
    return;
  }

  // validate config contents
  std::string err;
  op_ret = config.validate(&err);
  if (op_ret < 0) {
    s->err.message = err;
    return;
  }

  // load existing configs
  rgw::inventory::BucketConfigurations existing;
  int r = load_inventory_attr(this, s->bucket_attrs, existing);
  if (r < 0 && r != -ENOENT) {
    op_ret = r;
    return;
  }

  // add/replace this config
  op_ret = existing.add_or_replace(std::move(config));
  if (op_ret < 0) {
    s->err.message = "Maximum number of inventory configurations (1000) reached";
    return;
  }

  // forward to master in multi-site
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                          &data, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned "
                       << op_ret << dendl;
    return;
  }

  // save
  op_ret = save_inventory_attr(this, y, s->bucket.get(),
                                s->bucket_attrs, existing);
}

void RGWPutBucketInventory_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this);
}

// -----------------------------------------------------------------------
// GET /bucket?inventory&id=<id>
// -----------------------------------------------------------------------
int RGWGetBucketInventory_ObjStore_S3::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] =
      rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketInventoryConfiguration)) {
    return -EACCES;
  }
  return 0;
}

void RGWGetBucketInventory_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWGetBucketInventory_ObjStore_S3::execute(optional_yield y)
{
  std::string id = s->info.args.get("id");
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

  XMLFormatter f;
  f.open_object_section_with_attrs(
      "InventoryConfiguration",
      FormatterAttrs("xmlns",
                     "http://s3.amazonaws.com/doc/2006-03-01/", nullptr));
  config.dump_xml(&f);
  f.close_section();
  rgw_flush_formatter_and_reset(s, &f);
}

// -----------------------------------------------------------------------
// DELETE /bucket?inventory&id=<id>
// -----------------------------------------------------------------------
int RGWDeleteBucketInventory_ObjStore_S3::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] =
      rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3DeleteBucketInventoryConfiguration)) {
    return -EACCES;
  }
  return 0;
}

void RGWDeleteBucketInventory_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWDeleteBucketInventory_ObjStore_S3::execute(optional_yield y)
{
  std::string id = s->info.args.get("id");
  if (id.empty()) {
    op_ret = -EINVAL;
    s->err.message = "id parameter is required";
    return;
  }

  rgw::inventory::BucketConfigurations all;
  int r = load_inventory_attr(this, s->bucket_attrs, all);
  if (r == -ENOENT) {
    op_ret = 0; // already gone — idempotent
    return;
  }
  if (r < 0) { op_ret = r; return; }

  if (!all.remove(id)) {
    op_ret = 0; // config didn't exist — idempotent
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                          nullptr, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned "
                       << op_ret << dendl;
    return;
  }

  op_ret = save_inventory_attr(this, y, s->bucket.get(),
                                s->bucket_attrs, all);
}

void RGWDeleteBucketInventory_ObjStore_S3::send_response()
{
  int http_ret = op_ret ? op_ret : STATUS_NO_CONTENT;
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s, this);
}

// -----------------------------------------------------------------------
// LIST GET /bucket?inventory
// -----------------------------------------------------------------------
int RGWListBucketInventory_ObjStore_S3::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] =
      rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3ListBucketInventoryConfigurations)) {
    return -EACCES;
  }
  return 0;
}

void RGWListBucketInventory_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListBucketInventory_ObjStore_S3::execute(optional_yield y)
{
  int r = load_inventory_attr(this, s->bucket_attrs, configs);
  if (r == -ENOENT) {
    op_ret = 0; // empty list is valid
    return;
  }
  op_ret = r;
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

  XMLFormatter f;
  f.open_object_section_with_attrs(
      "ListInventoryConfigurationsResult",
      FormatterAttrs("xmlns",
                     "http://s3.amazonaws.com/doc/2006-03-01/", nullptr));
  for (const auto& [id, cfg] : configs.configs) {
    f.open_object_section("InventoryConfiguration");
    cfg.dump_xml(&f);
    f.close_section();
  }
  // TODO: IsTruncated + continuation token for >100 configs
  encode_xml("IsTruncated", false, &f);
  f.close_section();
  rgw_flush_formatter_and_reset(s, &f);
}
