// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_inventory.h"

#include "rgw_common.h"
#include "rgw_inventory.h"
#include "rgw_inventory_s3.h"
#include "rgw_lc.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

using rgw::inventory::BucketConfigurations;
using rgw::inventory::Configuration;

static int load_inventory_attr(const DoutPrefixProvider* dpp,
                               const rgw::sal::Attrs& attrs,
                               BucketConfigurations& configs)
{
  int r = rgw::inventory::decode_bucket_inventory(attrs, &configs);
  if (r == -EIO) {
    ldpp_dout(dpp, 1) << "ERROR: failed to decode inventory attr" << dendl;
  }
  return r;
}

// read-modify-write of the inventory attr. `modify` returns <0 on error,
// 0 to store the result, or 1 if there is nothing to store. retried when
// another write to the bucket instance races with ours
template <typename F>
static int update_inventory_attr(const DoutPrefixProvider* dpp,
                                 optional_yield y, rgw::sal::Bucket* bucket,
                                 const F& modify)
{
  return retry_raced_bucket_write(dpp, bucket, [&] {
    rgw::sal::Attrs attrs = bucket->get_attrs();
    BucketConfigurations configs;
    int r = load_inventory_attr(dpp, attrs, configs);
    if (r < 0 && r != -ENOENT) {
      return r;
    }
    r = modify(configs);
    if (r != 0) {
      return r < 0 ? r : 0;
    }
    bufferlist bl;
    configs.encode(bl);
    attrs[RGW_ATTR_INVENTORY] = std::move(bl);
    r = bucket->merge_and_store_attrs(dpp, attrs, y);
    if (r < 0 && r != -ECANCELED) {
      ldpp_dout(dpp, 1) << "ERROR: failed to store inventory attr: " << r
                        << dendl;
    }
    return r;
  }, y);
}

static bool check_bucket_permission(RGWOp* op, req_state* s, uint64_t action)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] =
      rgw_check_policy_condition(op, s, false);
  if (has_s3_resource_tag) {
    rgw_iam_add_buckettags(op, s);
  }
  return verify_bucket_permission(op, s, action);
}

namespace {

// PUT /bucket?inventory&id=<id>
class RGWPutBucketInventoryOp : public RGWOp {
  bufferlist data;
  Configuration config;

  int get_params();
public:
  int verify_permission(optional_yield y) override {
    return check_bucket_permission(
        this, s, rgw::IAM::s3PutInventoryConfiguration) ? 0 : -EACCES;
  }
  void pre_exec() override { rgw_bucket_object_pre_exec(s); }
  void execute(optional_yield y) override;
  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this);
  }
  const char* name() const override { return "put_bucket_inventory"; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_INVENTORY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

// GET /bucket?inventory&id=<id>
class RGWGetBucketInventoryOp : public RGWOp {
  Configuration config;
public:
  int verify_permission(optional_yield y) override {
    return check_bucket_permission(
        this, s, rgw::IAM::s3GetInventoryConfiguration) ? 0 : -EACCES;
  }
  void pre_exec() override { rgw_bucket_object_pre_exec(s); }
  void execute(optional_yield y) override;
  void send_response() override;
  const char* name() const override { return "get_bucket_inventory"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_INVENTORY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// DELETE /bucket?inventory&id=<id>
class RGWDeleteBucketInventoryOp : public RGWOp {
public:
  int verify_permission(optional_yield y) override {
    // AWS uses s3:PutInventoryConfiguration for delete as well
    return check_bucket_permission(
        this, s, rgw::IAM::s3PutInventoryConfiguration) ? 0 : -EACCES;
  }
  void pre_exec() override { rgw_bucket_object_pre_exec(s); }
  void execute(optional_yield y) override;
  void send_response() override {
    set_req_state_err(s, op_ret ? op_ret : STATUS_NO_CONTENT);
    dump_errno(s);
    end_header(s, this);
  }
  const char* name() const override { return "delete_bucket_inventory"; }
  RGWOpType get_type() override { return RGW_OP_DELETE_BUCKET_INVENTORY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

// GET /bucket?inventory
class RGWListBucketInventoryOp : public RGWOp {
  BucketConfigurations configs;
public:
  int verify_permission(optional_yield y) override {
    return check_bucket_permission(
        this, s, rgw::IAM::s3GetInventoryConfiguration) ? 0 : -EACCES;
  }
  void pre_exec() override { rgw_bucket_object_pre_exec(s); }
  void execute(optional_yield y) override {
    op_ret = load_inventory_attr(this, s->bucket_attrs, configs);
    if (op_ret == -ENOENT) {
      op_ret = 0;  // empty list
    }
  }
  void send_response() override;
  const char* name() const override { return "list_bucket_inventory"; }
  RGWOpType get_type() override { return RGW_OP_LIST_BUCKET_INVENTORY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

} // anonymous namespace

int RGWPutBucketInventoryOp::get_params()
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r;
  std::tie(r, data) = read_all_input(s, max_size, false);
  return r;
}

void RGWPutBucketInventoryOp::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

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
    RGWXMLDecoder::decode_xml("InventoryConfiguration", config, &parser, true);
  } catch (const RGWXMLDecoder::err& e) {
    ldpp_dout(this, 5) << "bad inventory configuration: " << e.what() << dendl;
    s->err.message = e.what();
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  // reject what we don't implement rather than ignore it and deliver
  // something else than what was asked for
  XMLObj* root = parser.find_first("InventoryConfiguration");
  XMLObj* dest_obj = root->find_first("Destination")->find_first(
      "S3BucketDestination");
  if (root->find_first("Filter") || dest_obj->find_first("Encryption")) {
    s->err.message = "Filter and Encryption are not supported";
    op_ret = -ERR_NOT_IMPLEMENTED;
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

  // only same-cluster destinations are supported
  std::unique_ptr<rgw::sal::Bucket> dest;
  op_ret = rgw::inventory::load_destination_bucket(
      this, driver, s->bucket->get_tenant(), config, &dest, y);
  if (op_ret == -ENOENT) {
    s->err.message = "Destination bucket '" +
                     config.destination.s3.bucket_name() +
                     "' does not exist";
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "ERROR: failed to load destination bucket "
                       << config.destination.s3.bucket_name() << ": "
                       << op_ret << dendl;
    return;
  }
  if (dest->get_key() == s->bucket->get_key()) {
    s->err.message = "Destination bucket must differ from the source bucket";
    op_ret = -EINVAL;
    return;
  }

  // checked again on every run
  op_ret = rgw::inventory::verify_destination_policy(
      this, s->bucket.get(), dest.get(), config, &err);
  if (op_ret < 0) {
    s->err.message = err;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                         &data, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 5) << "forward_request_to_master returned " << op_ret
                       << dendl;
    return;
  }

  op_ret = update_inventory_attr(this, y, s->bucket.get(),
      [this](BucketConfigurations& configs) {
        int r = configs.add_or_replace(config);
        if (r < 0) {
          s->err.message = "Maximum number of inventory configurations (" +
              std::to_string(BucketConfigurations::MAX_CONFIGS) + ") reached";
        }
        return r;
      });
  if (op_ret < 0) {
    return;
  }

  // add the bucket to the lc work list; nullptr config writes no lc rules
  op_ret = driver->get_rgwlc()->set_bucket_config(
      this, y, s->bucket.get(), s->bucket->get_attrs(), nullptr);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "ERROR: failed to add bucket to the lc list: "
                       << op_ret << dendl;
  }
}

void RGWGetBucketInventoryOp::execute(optional_yield y)
{
  BucketConfigurations all;
  op_ret = load_inventory_attr(this, s->bucket_attrs, all);
  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_SUCH_CONFIGURATION;
    return;
  }
  if (op_ret < 0) {
    return;
  }
  const auto* cfg = all.get(s->info.args.get("id"));
  if (!cfg) {
    op_ret = -ERR_NO_SUCH_CONFIGURATION;
    return;
  }
  config = *cfg;
}

void RGWGetBucketInventoryOp::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  if (op_ret) {
    return;
  }
  dump_start(s);
  s->formatter->open_object_section_in_ns("InventoryConfiguration",
                                          XMLNS_AWS_S3);
  config.dump_xml(s->formatter);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWDeleteBucketInventoryOp::execute(optional_yield y)
{
  const std::string id = s->info.args.get("id");
  if (id.empty()) {
    s->err.message = "id parameter is required";
    op_ret = -EINVAL;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                         nullptr, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 5) << "forward_request_to_master returned " << op_ret
                       << dendl;
    return;
  }

  // deleting a configuration that does not exist is not an error
  bool removed_last = false;
  op_ret = update_inventory_attr(this, y, s->bucket.get(),
      [&id, &removed_last](BucketConfigurations& configs) {
        if (!configs.remove(id)) {
          return 1;
        }
        removed_last = configs.configs.empty();
        return 0;
      });
  if (op_ret < 0) {
    return;
  }

  // no inventory and no lc rules left: remove the lc entry. a stale entry
  // is harmless, so errors are ignored
  if (removed_last && !s->bucket->get_attrs().contains(RGW_ATTR_LC)) {
    int r = driver->get_rgwlc()->remove_bucket_config(
        this, y, s->bucket.get(), false);
    if (r < 0) {
      ldpp_dout(this, 5) << "WARNING: failed to remove bucket from the lc "
                         << "list: " << r << dendl;
    }
  }
}

void RGWListBucketInventoryOp::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  if (op_ret) {
    return;
  }
  dump_start(s);
  auto f = s->formatter;
  f->open_object_section_in_ns("ListInventoryConfigurationsResult",
                               XMLNS_AWS_S3);
  for (const auto& [id, cfg] : configs.configs) {
    f->open_object_section("InventoryConfiguration");
    cfg.dump_xml(f);
    f->close_section();
  }
  // at most MAX_CONFIGS (1000) configurations are stored per bucket, so
  // they are all returned in one response
  encode_xml("IsTruncated", "false", f);
  f->close_section();
  rgw_flush_formatter_and_reset(s, f);
}

RGWOp* RGWHandler_REST_BucketInventory_S3::create_get_op()
{
  return new RGWGetBucketInventoryOp();
}

RGWOp* RGWHandler_REST_BucketInventory_S3::create_list_op()
{
  return new RGWListBucketInventoryOp();
}

RGWOp* RGWHandler_REST_BucketInventory_S3::create_put_op()
{
  return new RGWPutBucketInventoryOp();
}

RGWOp* RGWHandler_REST_BucketInventory_S3::create_delete_op()
{
  return new RGWDeleteBucketInventoryOp();
}
