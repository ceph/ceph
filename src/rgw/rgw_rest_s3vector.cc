// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_op.h"
#include "rgw_rest_s3vector.h"
#include "rgw_rest_s3.h"
#include "rgw_s3vector.h"
#include "rgw_process_env.h"
#include "common/async/yield_context.h"
#include "common/ceph_json.h"
#include "rgw_arn.h"
#include "rgw_account.h"
#include "rgw_zone.h"
#include "rgw_s3vector_background.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw
using rgw::IAM::Policy;
namespace {

class RGWS3VectorBase : public RGWDefaultResponseOp {
protected:
  bufferlist in_data;
  std::vector<rgw::s3vector::validation_error_t> validation_errors;
  std::unique_ptr<rgw::sal::VectorBucket> vector_bucket;
  rgw::s3vector::bucket_namespace_t bucket_ns;
public:
  explicit RGWS3VectorBase(bufferlist&& data) : in_data(std::move(data)) {}
protected:
  template<typename RestConfig>
  int do_init_processing(rgw::s3vector::VectorBucketConfig<RestConfig>& configuration,
                         optional_yield y) {
    auto& rest_config = configuration.rest_config;
    auto& backend_config = configuration.backend_config;
    backend_config.storage_mode.reset();

    // the namespace of the request is the one of the user making it
    bucket_ns.tenant = s->bucket_tenant;
    if (const auto* account_id = std::get_if<rgw_account_id>(&s->owner.id); account_id) {
      bucket_ns.account = *account_id;
    }

    if (in_data.length() == 0) {
      ldpp_dout(this, 1) << "ERROR: JSON s3vector payload missing" << dendl;
      return -EINVAL;
    }

    JSONParser parser;
    if (!parser.parse(in_data.c_str(), in_data.length())) {
      ldpp_dout(this, 1) << "ERROR: failed to parse JSON s3vector payload" << dendl;
      return -EINVAL;
    }
    try {
      decode_json_obj(rest_config, &parser);
    } catch (const JSONDecoder::err& e) {
      ldpp_dout(this, 1) << "ERROR: failed to decode JSON s3vector payload: " << e.what() << dendl;
      return -EINVAL;
    }

    const auto backend_name = s->cct->_conf.get_val<std::string>("rgw_s3vector_backend");
    if (rgw::s3vector::get_backend_type(backend_name, backend_config.backend_type) < 0) {
      return -EINVAL;
    }

    int rc = 0;
    if constexpr (requires { rest_config.vector_bucket_arn; }) {
      rc = resolve_namespace(rest_config.vector_bucket_arn, y);
    } else if constexpr (requires { rest_config.index_arn; }) {
      rc = resolve_namespace(rest_config.index_arn, y);
    }
    if (rc < 0) {
      return rc;
    }
    // Resolve storage mode before entering LanceDB's alternate stack.
    // ListVectorBuckets has no individual target bucket.
    if constexpr (requires { rest_config.vector_bucket_name; }) {
      if (!rest_config.vector_bucket_name.empty()) {
        std::unique_ptr<rgw::sal::VectorBucket> bucket;
        int ret = driver->load_vector_bucket(this,
            rgw_bucket(bucket_ns.name(), rest_config.vector_bucket_name),
            &bucket, y);
        if (ret < 0 && ret != -ENOENT) {
          return ret;
        }
        rgw::s3vector::StorageMode mode;
        ret = rgw::s3vector::get_bucket_storage_mode(this,
            ret == 0 && rgw::s3vector::is_rgw_backend(backend_config.backend_type)
                ? bucket.get() : nullptr,
            mode);
        if (ret < 0) {
          return ret;
        }
        backend_config.storage_mode = mode;
      }
    }
    return 0;
  }

  // a vector bucket that is identified by name is looked up in the namespace of
  // the user making the request. one that is identified by ARN is looked up in
  // the namespace of the ARN, which is how the vector buckets of other accounts,
  // and tenants, are reached when their policies allow it. an ARN without an
  // account refers to the namespace of the user
  int resolve_namespace(const boost::optional<rgw::ARN>& arn, optional_yield y) {
    if (!arn || arn->account.empty() || arn->account == bucket_ns.name()) {
      return 0;
    }
    if (!rgw::account::validate_id(arn->account)) {
      // the owner of the vector bucket does not belong to an account, and the
      // ARN holds its tenant
      bucket_ns.tenant = arn->account;
      bucket_ns.account.clear();
      return 0;
    }
    RGWAccountInfo info;
    rgw::sal::Attrs attrs;
    RGWObjVersionTracker objv;
    const int ret = driver->load_account_by_id(this, y, arn->account, info, attrs, objv);
    if (ret < 0) {
      ldpp_dout(this, 5) << "WARNING: failed to load the account of ARN " << *arn
                         << ". error: " << ret << dendl;
      // a vector bucket of an account that does not exist, does not exist
      return (ret == -ENOENT) ? -ENOENT : ret;
    }
    bucket_ns.tenant = info.tenant;
    bucket_ns.account = arn->account;
    return 0;
  }

  // Check access to the RGW backend, which writes through SAL and bypasses
  // S3 API permission checks.
  // an object operation is verified against any object of the bucket, since
  // LanceDB spreads the data of a vector bucket over objects whose names are not
  // known here. the operations needed by each request were taken from the SAL
  // calls that it makes: note that "s3:GetObject" covers the head of an object as
  // well, and that removing vectors writes deletion files, and deletes nothing
  // Native vector data uses its own bucket's permissions; a separate S3 backing
  // bucket uses that bucket's ACL and policies.
  int verify_backend_permission(const std::string& bucket_name,
                                  const rgw::s3vector::BackendConfig& backend_config,
                                  std::initializer_list<uint64_t> s3_ops,
                                  optional_yield y) {
    if (!backend_config.storage_mode) {
      return -EINVAL;
    }
    if (!rgw::s3vector::is_rgw_backend(backend_config.backend_type)) {
      return 0;
    }

    if (rgw::s3vector::uses_vector_bucket_storage(*backend_config.storage_mode)) {
      // The caller has already authorized the S3 Vectors operation. Native
      // storage needs no additional S3 permissions; an owner-only check here
      // would reject access granted by the vector bucket's policy.
      return 0;
    }

    std::unique_ptr<rgw::sal::Bucket> s3_bucket;
    int ret = driver->load_bucket(this,
        rgw_bucket(bucket_ns.tenant, bucket_name), &s3_bucket, y);
    if (ret < 0) {
      return 0;
    }

    RGWAccessControlPolicy bucket_acl;
    auto& attrs = s3_bucket->get_attrs();
    auto aiter = attrs.find(RGW_ATTR_ACL);
    if (aiter != attrs.end()) {
      auto iter = aiter->second.cbegin();
      try {
        bucket_acl.decode(iter);
      } catch (buffer::error&) {
        ldpp_dout(this, 0) << "ERROR: could not decode bucket ACL for " << bucket_name << dendl;
        return -EIO;
      }
    } else {
      bucket_acl.create_default(s3_bucket->get_info().owner, "");
    }

    auto bucket_policy = get_iam_policy_from_attr(
        s->cct, s3_bucket->get_attrs(), bucket_ns.tenant);

    for (const uint64_t s3_op : s3_ops) {
      const rgw::ARN arn = (s3_op == rgw::IAM::s3ListBucket) ?
          rgw::ARN(s3_bucket->get_key()) : rgw::ARN(s3_bucket->get_key(), "*");
      if (!verify_bucket_permission(this, s, arn,
          s->user_acl, bucket_acl, bucket_policy,
          s->iam_identity_policies, s->session_policies, s3_op)) {
        return -EACCES;
      }
    }
    return 0;
  }

  int load_vector_bucket_policy_context(const std::string& bucket_name,
                                        optional_yield y) {
    const rgw_bucket bucket_id(bucket_ns.name(), bucket_name);
    int ret = driver->load_vector_bucket(this, bucket_id, &vector_bucket, y);
    if (ret < 0) {
      if (ret == -ENOENT) {
        rgw::s3vector::notify_session_delete(this, bucket_ns, bucket_id.name);
      }
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << ret << dendl;
      return ret;
    }

    s->bucket_name = bucket_name;
    s->bucket_attrs = vector_bucket->get_attrs();
    auto acl = s->bucket_attrs.find(RGW_ATTR_ACL);
    if (acl != s->bucket_attrs.end()) {
      auto iter = acl->second.cbegin();
      try {
        s->bucket_acl.decode(iter);
      } catch (buffer::error&) {
        return -EIO;
      }
    } else {
      s->bucket_acl.create_default(vector_bucket->get_info().owner, "");
    }
    s->bucket_owner = s->bucket_acl.get_owner();

    try {
      s->iam_policy = get_iam_policy_from_attr(
          s->cct, s->bucket_attrs, bucket_ns.name());
    } catch (const std::exception& e) {
      ldpp_dout(this, 0) << "ERROR: failed to read vector bucket policy: "
                         << e.what() << dendl;
      return -EACCES;
    }
    return 0;
  }

  bool can_bypass_bucket_policy() {

    // If the user is the root account of the bucket owner,
    // and x-amz-confirm-remove-self-bucket-access was not set,
    // then the user can access / modify the bucket policy without policy check.
    return s->auth.identity->is_root_of(s->bucket_owner.id) &&
           s->bucket_attrs.find(RGW_ATTR_IAM_POLICY_REMOVE_SELF_ACCESS) == s->bucket_attrs.end();
  }

  int verify_vector_bucket_permission(const std::string& bucket_name,
                                      uint64_t iam_op,
                                      optional_yield y) {
    int ret = load_vector_bucket_policy_context(bucket_name, y);
    if (ret < 0) {
      return ret;
    }

    const auto arn = rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name, bucket_ns.name(), bucket_name);
    if (!verify_bucket_permission(this, s, arn, s->user_acl, s->bucket_acl,
                                   s->iam_policy, s->iam_identity_policies,
                                   s->session_policies, iam_op)) {
      return -EACCES;
    }
    return 0;
  }

  void send_validation_error_response() {
    s->err.http_ret = 400;
    s->err.err_code = "ValidationException";
    s->err.message = "The requested action isn't valid.";
    set_req_state_err(s, op_ret);
    dump_errno(s);

    JSONFormatter f;
    f.open_object_section("");
    ::encode_json("code", std::string("ValidationException"), &f);
    ::encode_json("message", std::string("The requested action isn't valid."), &f);
    f.open_array_section("fieldList");
    for (const auto& err : validation_errors) {
      f.open_object_section("");
      ::encode_json("path", err.path, &f);
      ::encode_json("message", err.message, &f);
      f.close_section();
    }
    f.close_section(); // fieldList
    f.close_section(); // root
    std::stringstream ss;
    f.flush(ss);
    const auto body = ss.str();
    end_header(s, this, "application/json", body.size(), false, true);
    dump_body(s, body);
  }
};

class RGWS3VectorCreateIndex : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::create_index_t> configuration;
public:
  explicit RGWS3VectorCreateIndex(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector CreateIndex" << dendl;
    int ret = verify_vector_bucket_permission(configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsCreateIndex, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject, rgw::IAM::s3PutObject}, y);
  }

  const char* name() const override { return "s3vector_create_index"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.CreateIndex", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_CREATE_INDEX; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = rgw::s3vector::create_index(configuration, driver, bucket_ns, this, y, validation_errors);
  }

  void send_response() override {
    if (op_ret < 0 && !validation_errors.empty()) {
      send_validation_error_response();
      return;
    }
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    dump_start(s);

    rgw::s3vector::create_index_reply_t reply{
      rgw::s3vector::index_arn(s->zonegroup_name, bucket_ns.name(), configuration.rest_config.vector_bucket_name, configuration.rest_config.index_name).to_string()
    };
    const auto f = s->formatter;
    reply.dump(f);
    rgw_flush_formatter_and_reset(s, f);
  }
};

class RGWS3VectorCreateVectorBucket : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::create_vector_bucket_t> configuration;
public:
  explicit RGWS3VectorCreateVectorBucket(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector CreateVectorBucket" << dendl;
    if (s->auth.identity->is_anonymous()) {
      return -EACCES;
    }

    const rgw::ARN arn = rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name, bucket_ns.name(), configuration.rest_config.vector_bucket_name);
    if (!verify_user_permission(this, s, arn, rgw::IAM::s3vectorsCreateVectorBucket, false)) {
      return -EACCES;
    }

    if (s->auth.identity->get_tenant() != s->bucket_tenant) {
      //AssumeRole is meant for cross account access
      if (s->auth.identity->get_identity_type() != TYPE_ROLE) {
        ldpp_dout(this, 5) << "WARNING: user cannot create an s3vector bucket in a different tenant"
                        << " (user_id.tenant=" << s->user->get_tenant()
                        << " requested=" << s->bucket_tenant << ")"
                        << dendl;
        return -EACCES;
      }
    }

    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3PutObject}, y);
  }

  const char* name() const override { return "s3vector_create_vector_bucket"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.CreateVectorBucket", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_CREATE_VECTOR_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  // create the vector bucket metadata entity, recording the (immutable) storage mode
  int create_bucket_entity(rgw::s3vector::StorageMode mode, bool vector_storage, optional_yield y) {
    const auto& zonegroup = s->penv.site->get_zonegroup();

    rgw::sal::VectorBucket::CreateParams createparams;
    // as with ordinary buckets, a vector bucket belongs to the account of the user
    // creating it, when it has one, and to the user itself otherwise
    createparams.owner = s->owner.id;
    createparams.zonegroup_id = zonegroup.id;
    createparams.placement_rule.storage_class = s->info.storage_class;
    // Only native RGW storage needs placement and a bucket index.
    if (vector_storage) {
      if (int r = select_bucket_placement(this, zonegroup, s->user->get_info(),
                                          createparams.placement_rule); r < 0) {
        return r;
      }
      createparams.zone_placement = rgw::find_zone_placement(
          this, s->penv.site->get_zone_params(), createparams.placement_rule);
      if (!createparams.zone_placement) {
        return -ERR_INVALID_LOCATION_CONSTRAINT;
      }
    }
    if (vector_storage && createparams.zone_placement->index_type != rgw::BucketIndexType::Normal) {
      return -ERR_INVALID_LOCATION_CONSTRAINT;
    }
    // Persist the creation choice so later config changes cannot move the data.
    bufferlist bl;
    bl.append(rgw::s3vector::to_string(mode));
    createparams.attrs[RGW_ATTR_S3VECTOR_STORAGE] = std::move(bl);

    return vector_bucket->create(this, createparams, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(bucket_ns.name(), configuration.rest_config.vector_bucket_name);
    const int ret = driver->load_vector_bucket(this, bucket_id, &vector_bucket, y);
    if (ret == 0) {
      ldpp_dout(this, 5) << "WARNING: s3vector bucket " << bucket_id << " already exists" << dendl;
      op_ret = -EEXIST;
      return;
    }
    if (ret != -ENOENT) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << ret << dendl;
      op_ret = ret;
      return;
    }

    if (!configuration.backend_config.storage_mode) {
      op_ret = -EINVAL;
      return;
    }
    const auto mode = *configuration.backend_config.storage_mode;
    // the storage mode only governs the "rgw" backend; with the "local" backend the
    // data is on the local filesystem and no bucket is involved
    const auto backend_type = configuration.backend_config.backend_type;

    // Native storage needs its bucket index before LanceDB can connect.
    const bool vector_storage =
        rgw::s3vector::is_rgw_backend(backend_type) &&
        rgw::s3vector::uses_vector_bucket_storage(mode);

    if (vector_storage) {
      op_ret = create_bucket_entity(mode, vector_storage, y);
      if (op_ret < 0) {
        ldpp_dout(this, 1) << "ERROR: failed to create s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
        return;
      }
    }

    // Check separate backends before creating metadata so a failed request
    // leaves no bucket behind.
    op_ret = rgw::s3vector::create_vector_bucket(configuration, driver, bucket_ns, this, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to initialize the backend of s3vector bucket " << bucket_id <<
        ". error: " << op_ret << dendl;
      // Roll back the native bucket created by this request.
      if (vector_storage) {
        if (const int r = vector_bucket->remove(this, true, y); r < 0) {
          ldpp_dout(this, 1) << "WARNING: failed to roll back s3vector bucket " << bucket_id
            << " after backend failure. error: " << r << dendl;
        }
      }
      return;
    }

    if (!vector_storage) {
      op_ret = create_bucket_entity(mode, vector_storage, y);
      if (op_ret < 0) {
        ldpp_dout(this, 1) << "ERROR: failed to create s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
        return;
      }
    }
  }

  void send_response() override {
    if (op_ret == -EEXIST || op_ret == -ERR_BUCKET_EXISTS) {
      s->err.message = "A vector bucket with this name already exists. Choose a different name and try again.";
    }

    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    s->format = RGWFormat::JSON;
    end_header(s, this, to_mime_type(s->format));
    if (op_ret < 0) {
      return;
    }

    dump_start(s);
    JSONFormatter f; /* use json formatter for system requests output */

    if (op_ret == 0 && s->system_request) {

      ceph_assert(vector_bucket);
      ceph_assert(!vector_bucket->empty());
      const RGWBucketInfo& info = vector_bucket->get_info();
      const obj_version& ep_objv = vector_bucket->get_version();
      f.open_object_section("info");
      encode_json("entry_point_object_ver", ep_objv, &f);
      encode_json("object_ver", info.objv_tracker.read_version, &f);
      encode_json("bucket_info", info, &f);
      f.close_section();
    }

    rgw::s3vector::create_vector_bucket_reply_t reply{
      rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name,
        bucket_ns.name(),
        configuration.rest_config.vector_bucket_name).to_string()
    };
    reply.dump(&f);
    rgw_flush_formatter_and_reset(s, &f);
  }
};

class RGWS3VectorDeleteIndex : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::delete_index_t> configuration;
public:
  explicit RGWS3VectorDeleteIndex(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteIndex" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsDeleteIndex, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject, rgw::IAM::s3DeleteObject}, y);
  }

  const char* name() const override { return "s3vector_delete_index"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.DeleteIndex", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_DELETE_INDEX; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = rgw::s3vector::delete_index(configuration, driver, bucket_ns, this, y);
  }
};

class RGWS3VectorDeleteVectorBucket : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::delete_vector_bucket_t> configuration;
public:
  explicit RGWS3VectorDeleteVectorBucket(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteVectorBucket" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsDeleteVectorBucket, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject}, y);
  }

  const char* name() const override { return "s3vector_delete_vector_bucket"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.DeleteVectorBucket", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_DELETE_VECTOR_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    int ret = do_init_processing(configuration, y);
    if (ret < 0) {
      return ret;
    }
    if (!configuration.rest_config.vector_bucket_arn) {
      configuration.rest_config.vector_bucket_arn = rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name,
        bucket_ns.name(),
        configuration.rest_config.vector_bucket_name
      );
    }

    s->bucket_name = configuration.rest_config.vector_bucket_arn->resource;
    ldpp_dout(this, 20) << "INFO: s3vector bucket ARN: " << configuration.rest_config.vector_bucket_arn.get() << dendl;
    return 0;
  }

  void execute(optional_yield y) override {
    {
      JSONFormatter f;
      configuration.rest_config.dump(&f);
      std::stringstream ss;
      f.flush(ss);
      ldpp_dout(this, 20) << "INFO: executing s3vector DeleteVectorBucket with: " << ss.str() << dendl;
    }
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = vector_bucket->remove(this, false, y);
    if (op_ret < 0) {
      const rgw_bucket bucket_id(bucket_ns.name(), configuration.rest_config.vector_bucket_name);
      ldpp_dout(this, 1) << "ERROR: failed to delete s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
    }
  }
};

class RGWS3VectorDeleteVectorBucketPolicy : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::delete_vector_bucket_policy_t> configuration;
public:
  explicit RGWS3VectorDeleteVectorBucketPolicy(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteVectorBucketPolicy" << dendl;
    int ret = load_vector_bucket_policy_context(configuration.rest_config.vector_bucket_name, y);
    if (ret < 0) {
      return ret;
    }

    if (can_bypass_bucket_policy()) {
      return 0;
    }
    const auto arn = rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name, bucket_ns.name(), configuration.rest_config.vector_bucket_name);
    if (!verify_bucket_permission(this, s, arn, s->user_acl, s->bucket_acl,
                                   s->iam_policy, s->iam_identity_policies,
                                   s->session_policies,
                                   rgw::IAM::s3vectorsDeleteVectorBucketPolicy)) {
      return -EACCES;
    }
    return 0;
  }

  const char* name() const override { return "s3vector_delete_vector_bucket_policy"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.DeleteVectorBucketPolicy", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_DELETE_VECTOR_BUCKET_POLICY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = retry_raced_bucket_write(this, vector_bucket.get(), [this] {
      rgw::sal::Attrs& attrs = vector_bucket->get_attrs();
      attrs.erase(RGW_ATTR_IAM_POLICY);
      attrs.erase(RGW_ATTR_IAM_POLICY_REMOVE_SELF_ACCESS);
      op_ret = vector_bucket->put_info(this, false, real_time(), s->yield);
      return op_ret;
    }, y);
  }

void send_response() override {
    if (!op_ret) {
        /* A successful DeleteVectorBucketPolicy returns 204 on success */
        op_ret = STATUS_NO_CONTENT;
    }
    if (op_ret) {
        set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");
}
};

class RGWS3VectorPutVectors : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::put_vectors_t> configuration;
public:
  explicit RGWS3VectorPutVectors(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector PutVectors" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsPutVectors, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject, rgw::IAM::s3PutObject}, y);
  }

  const char* name() const override { return "s3vector_put_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.PutVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_PUT_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = rgw::s3vector::put_vectors(configuration, driver, bucket_ns, this, y, validation_errors);
  }

  void send_response() override {
    if (op_ret < 0 && !validation_errors.empty()) {
      send_validation_error_response();
      return;
    }
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");
  }
};

class RGWS3VectorGetVectors : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::get_vectors_t> configuration;
  rgw::s3vector::get_vectors_reply_t reply;
public:
  explicit RGWS3VectorGetVectors(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetVectors" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsGetVectors, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject}, y);
  }

  const char* name() const override { return "s3vector_get_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.GetVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_GET_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = rgw::s3vector::get_vectors(configuration, driver, bucket_ns, this, y, reply);
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    dump_start(s);

    const auto f = s->formatter;
    reply.dump(f);
    rgw_flush_formatter_and_reset(s, f);
  }
};

class RGWS3VectorListVectors : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::list_vectors_t> configuration;
  rgw::s3vector::list_vectors_reply_t reply;
public:
  explicit RGWS3VectorListVectors(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector ListVectors" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsListVectors, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject}, y);
  }

  const char* name() const override { return "s3vector_list_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.ListVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_LIST_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = rgw::s3vector::list_vectors(configuration, driver, bucket_ns, this, y, reply);
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    dump_start(s);

    const auto f = s->formatter;
    reply.dump(f);
    rgw_flush_formatter_and_reset(s, f);
  }
};

// the vector bucket listing is ordered by bucket name, but has no prefix filtering
// (the "match_prefix" of the underlying omap listing is not exposed by cls_user).
// the markers are used instead to narrow the listing down to the range of the prefix.
// both markers are exclusive

// the greatest name that is guaranteed to be smaller than any name with that prefix
std::string prefix_start_marker(std::string_view prefix) {
  // a proper prefix of a string is always smaller than the string itself
  return std::string(prefix.substr(0, prefix.size() - 1));
}

// the smallest name that is guaranteed to be greater than any name with that prefix.
// empty if no such name exists, in which case the listing is not bounded from above
std::string prefix_end_marker(std::string_view prefix) {
  std::string marker(prefix);
  while (!marker.empty()) {
    const auto last = static_cast<unsigned char>(marker.back());
    if (last != 0xff) {
      marker.back() = static_cast<char>(last + 1);
      return marker;
    }
    marker.pop_back();
  }
  return marker;
}

class RGWS3VectorListVectorBuckets : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::list_vector_buckets_t> configuration;
  rgw::sal::BucketList listing;
  std::string zonegroup_name;
public:
  explicit RGWS3VectorListVectorBuckets(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector ListVectorBuckets" << dendl;
    if (s->auth.identity->is_anonymous()) {
      return -EACCES;
    }

    zonegroup_name = s->penv.site->get_zonegroup().api_name;
    const auto arn = rgw::s3vector::vector_bucket_arn(zonegroup_name, bucket_ns.name(), "*");
    if (!verify_user_permission(this, s, arn, rgw::IAM::s3vectorsListVectorBuckets, false)) {
      return -EACCES;
    }
    return 0;
  }

  const char* name() const override { return "s3vector_list_vector_buckets"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.ListVectorBuckets", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_LIST_VECTOR_BUCKETS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    {
      JSONFormatter f;
      configuration.rest_config.dump(&f);
      std::stringstream ss;
      f.flush(ss);
      ldpp_dout(this, 20) << "INFO: executing s3vector ListVectorBuckets with: " << ss.str() << dendl;
    }

    std::string start_marker = configuration.rest_config.next_token;
    std::string end_marker;
    if (!configuration.rest_config.prefix.empty()) {
      start_marker = std::max(start_marker, prefix_start_marker(configuration.rest_config.prefix));
      end_marker = prefix_end_marker(configuration.rest_config.prefix);
    }

    op_ret = driver->list_vector_buckets(this, s->owner.id, bucket_ns.name(),
        start_marker, end_marker, configuration.rest_config.max_results, listing, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: failed to execute ListVectorBuckets. error: " << op_ret << dendl;
      return;
    }
    if (!configuration.rest_config.prefix.empty()) {
      // the markers only bound the listing to the range of the prefix, so the
      // names at the edges of that range still have to be filtered out here
      // won't be needed once https://tracker.ceph.com/issues/79532 is implemented
      std::erase_if(listing.buckets, [this](const RGWBucketEnt& bucket) {
        if (bucket.bucket.name.starts_with(configuration.rest_config.prefix)) {
          return false;
        }
        ldpp_dout(this, 20) << "INFO: on s3vector listing, vector bucket: " << bucket.bucket.name << " is filtered out" << dendl;
        return true;
      });
    }
  }

  void send_response() override {
    if (op_ret < 0) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    // convert BucketList to AWS S3 Vector Buckets format
    JSONFormatter f;
    f.open_object_section("");
    if (!listing.next_marker.empty()) {
      ::encode_json("nextToken", listing.next_marker, &f);
    }
    f.open_array_section("vectorBuckets");
    for (const auto& bucket : listing.buckets) {
      f.open_object_section("");
      // the AWS API encodes timestamps as seconds since the epoch
      ::encode_json("creationTime", static_cast<uint64_t>(ceph::real_clock::to_time_t(bucket.creation_time)), &f);
      const auto arn = rgw::s3vector::vector_bucket_arn(zonegroup_name, bucket_ns.name(), bucket.bucket.name);
      ::encode_json("vectorBucketArn", arn.to_string(), &f);
      ::encode_json("vectorBucketName", bucket.bucket.name, &f);
      f.close_section();
    }
    f.close_section(); // vectorBuckets
    f.close_section(); // root object
    std::stringstream ss;
    f.flush(ss);
    dump_body(s, ss.str());
  }
};

class RGWS3VectorGetVectorBucket : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::get_vector_bucket_t> configuration;
public:
  explicit RGWS3VectorGetVectorBucket(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetVectorBucket" << dendl;
    return verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsGetVectorBucket, y);
  }

  const char* name() const override { return "s3vector_get_vector_bucket"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.GetVectorBucket", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_GET_VECTOR_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    int ret = do_init_processing(configuration, y);
    if (ret < 0) {
      return ret;
    }
    if (!configuration.rest_config.vector_bucket_arn) {
      configuration.rest_config.vector_bucket_arn = rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name,
        bucket_ns.name(),
        configuration.rest_config.vector_bucket_name
      );
    }

    s->bucket_name = configuration.rest_config.vector_bucket_arn->resource;
    ldpp_dout(this, 20) << "INFO: s3vector bucket ARN: " << configuration.rest_config.vector_bucket_arn.get() << dendl;
    return 0;
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    {
      JSONFormatter f;
      configuration.rest_config.dump(&f);
      std::stringstream ss;
      f.flush(ss);
      ldpp_dout(this, 20) << "INFO: executing s3vector GetVectorBucket with: " << ss.str() << dendl;
    }
    const rgw_bucket bucket_id(bucket_ns.name(), configuration.rest_config.vector_bucket_name);
    op_ret = driver->load_vector_bucket(this, bucket_id, &vector_bucket, y);
    if (op_ret < 0) {
      if (op_ret == -ENOENT) {
        rgw::s3vector::notify_session_delete(this, bucket_ns, bucket_id.name);
      }
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
    }
  }

  void send_response() override {
    if (op_ret < 0) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    // convert BucketList to AWS S3 Vector Buckets format
    JSONFormatter f;
    f.open_object_section("");
    f.open_object_section("vectorBucket");
    // the AWS API encodes timestamps as seconds since the epoch
    ::encode_json("creationTime", static_cast<uint64_t>(ceph::real_clock::to_time_t(vector_bucket->get_creation_time())), &f);
    ::encode_json("vectorBucketArn", configuration.rest_config.vector_bucket_arn->to_string(), &f);
    ::encode_json("vectorBucketName", vector_bucket->get_name(), &f);
    f.close_section(); // vectorBucket
    f.close_section(); // root object
    std::stringstream ss;
    f.flush(ss);
    dump_body(s, ss.str());
  }
};

class RGWS3VectorGetIndex : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::get_index_t> configuration;
  rgw::s3vector::get_index_reply_t reply;
public:
  explicit RGWS3VectorGetIndex(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetIndex" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsGetIndex, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject}, y);
  }

  const char* name() const override { return "s3vector_get_index"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.GetIndex", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_GET_INDEX; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = rgw::s3vector::get_index(configuration, s->zonegroup_name, driver, bucket_ns, this, y, reply);
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    dump_start(s);

    const auto f = s->formatter;
    reply.dump(f);
    rgw_flush_formatter_and_reset(s, f);
  }
};

class RGWS3VectorListIndexes : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::list_indexes_t> configuration;
  rgw::s3vector::list_indexes_reply_t reply;
public:
  explicit RGWS3VectorListIndexes(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector ListIndexes" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsListIndexes, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject}, y);
  }

  const char* name() const override { return "s3vector_list_indexes"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.ListIndexes", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_LIST_INDEXES; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    if (!configuration.rest_config.vector_bucket_arn) {
      configuration.rest_config.vector_bucket_arn = rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name,
        bucket_ns.name(),
        configuration.rest_config.vector_bucket_name
      );
    }
    op_ret = rgw::s3vector::list_indexes(configuration, driver, bucket_ns, this, y, reply);
    if (op_ret == -ENOENT) {
      // the backend of the bucket does not exist. this happens when the bucket was
      // created, but its backend was never initialized. such a bucket has no indexes,
      // so an empty list is returned, and the bucket could still be deleted
      ldpp_dout(this, 5) << "WARNING: s3vector bucket " << vector_bucket->get_key() <<
        " has no backend. listing no indexes" << dendl;
      op_ret = 0;
    }
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    dump_start(s);

    const auto f = s->formatter;
    reply.dump(f);
    rgw_flush_formatter_and_reset(s, f);
  }
};

class RGWS3VectorPutVectorBucketPolicy : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::put_vector_bucket_policy_t> configuration;
public:
  explicit RGWS3VectorPutVectorBucketPolicy(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector PutVectorBucketPolicy" << dendl;
    int ret = load_vector_bucket_policy_context(configuration.rest_config.vector_bucket_name, y);
    if (ret < 0) {
      return ret;
    }

    if (can_bypass_bucket_policy()) {
      return 0;
    }
    const auto arn = rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name, bucket_ns.name(), configuration.rest_config.vector_bucket_name);
    if (!verify_bucket_permission(this, s, arn, s->user_acl, s->bucket_acl,
                                   s->iam_policy, s->iam_identity_policies,
                                   s->session_policies,
                                   rgw::IAM::s3vectorsPutVectorBucketPolicy)) {
      return -EACCES;
    }
    return 0;
  }

  const char* name() const override { return "s3vector_put_vector_bucket_policy"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.PutVectorBucketPolicy", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_PUT_VECTOR_BUCKET_POLICY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    try {
      const Policy p(s->cct, &bucket_ns.name(), configuration.rest_config.policy,
                      s->cct->_conf.get_val<bool> ("rgw_policy_reject_invalid_principals"));

      if(s->public_access_block.BlockPublicPolicy && rgw::IAM::is_public(this, p)) {
        op_ret = -EACCES;
        return;
      }
      op_ret = retry_raced_bucket_write(this, vector_bucket.get(), [&p, this] {
              rgw::sal::Attrs& attrs = vector_bucket->get_attrs();
              attrs[RGW_ATTR_IAM_POLICY].clear();
              attrs[RGW_ATTR_IAM_POLICY].append(p.text);
              if (s->info.env->exists("HTTP_X_AMZ_CONFIRM_REMOVE_SELF_BUCKET_ACCESS")) {
                attrs[RGW_ATTR_IAM_POLICY_REMOVE_SELF_ACCESS].clear();
              } else {
                attrs.erase(RGW_ATTR_IAM_POLICY_REMOVE_SELF_ACCESS);
              }
              vector_bucket->set_attrs(attrs);
              return vector_bucket->put_info(this, false, real_time(), s->yield); }, y);
    } catch (rgw::IAM::PolicyParseException& e) {
      ldpp_dout(this, 5) << "failed to parse the policy" << e.what() << dendl;
      op_ret = -EINVAL;
      s->err.message = e.what();
    }
  }

 void send_response() override {
    if (!op_ret) {
      op_ret = STATUS_NO_CONTENT;
    }
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");
    dump_body(s, configuration.rest_config.policy);
}
};

class RGWS3VectorGetVectorBucketPolicy : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::get_vector_bucket_policy_t> configuration;
  bufferlist policy;
public:
  explicit RGWS3VectorGetVectorBucketPolicy(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetVectorBucketPolicy" << dendl;
    int ret = load_vector_bucket_policy_context(configuration.rest_config.vector_bucket_name, y);
    if (ret < 0) {
      return ret;
    }

    if (can_bypass_bucket_policy()) {
      return 0;
    }
    const auto arn = rgw::s3vector::vector_bucket_arn(
        s->zonegroup_name, bucket_ns.name(), configuration.rest_config.vector_bucket_name);
    if (!verify_bucket_permission(this, s, arn, s->user_acl, s->bucket_acl,
                                   s->iam_policy, s->iam_identity_policies,
                                   s->session_policies,
                                   rgw::IAM::s3vectorsGetVectorBucketPolicy)) {
      return -EACCES;
    }
    return 0;
  }

  const char* name() const override { return "s3vector_get_vector_bucket_policy"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.GetVectorBucketPolicy", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_GET_VECTOR_BUCKET_POLICY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    const rgw::sal::Attrs& attrs = vector_bucket->get_attrs();
    auto attr = attrs.find(RGW_ATTR_IAM_POLICY);
    if (attr == attrs.end()) {
      ldpp_dout(this, 20) << "can't find vector bucket IAM POLICY attr bucket_name = "
        << configuration.rest_config.vector_bucket_name << dendl;
      op_ret = -ERR_NO_SUCH_BUCKET_POLICY;
      s->err.message = "The vector bucket policy does not exist";
      return;
    } else {
      policy = attr->second;

      if (policy.length() == 0) {
        ldpp_dout(this, 10) << "The vector bucket policy does not exist, bucket_policy = "
          << configuration.rest_config.vector_bucket_name << dendl;
          op_ret = -ERR_NO_SUCH_BUCKET_POLICY;
          return;
      }
    }
  }
  void send_response() override {
    if (op_ret) {
        set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");
    if (op_ret < 0) {
        return;
    }
    dump_start(s);
    const auto f = s->formatter;
    f->open_object_section("GetVectorBucketPolicyResult");
    encode_json("policy", policy.to_str(), f);
    f->close_section();
    rgw_flush_formatter_and_reset(s, f);
}
};

class RGWS3VectorDeleteVectors : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::delete_vectors_t> configuration;
public:
  explicit RGWS3VectorDeleteVectors(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteVectors" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsDeleteVectors, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject, rgw::IAM::s3PutObject}, y);
  }

  const char* name() const override { return "s3vector_delete_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.DeleteVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_DELETE_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = rgw::s3vector::delete_vectors(configuration, driver, bucket_ns, this, y);
  }
};

class RGWS3VectorQueryVectors : public RGWS3VectorBase {
  rgw::s3vector::VectorBucketConfig<rgw::s3vector::query_vectors_t> configuration;
  rgw::s3vector::query_vectors_reply_t reply;
  std::optional<JSONParser> filter_parser;
public:
  explicit RGWS3VectorQueryVectors(bufferlist&& data) : RGWS3VectorBase(std::move(data)) {}
private:
  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector QueryVectors" << dendl;
    int ret = verify_vector_bucket_permission(
        configuration.rest_config.vector_bucket_name, rgw::IAM::s3vectorsQueryVectors, y);
    if (ret < 0) {
      return ret;
    }
    return verify_backend_permission(
        configuration.rest_config.vector_bucket_name, configuration.backend_config, {rgw::IAM::s3ListBucket, rgw::IAM::s3GetObject}, y);
  }

  const char* name() const override { return "s3vector_query_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.QueryVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_QUERY_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    int ret = do_init_processing(configuration, y);
    if (ret < 0) {
      return ret;
    }
    if (!configuration.rest_config.filter.empty()) {
      filter_parser.emplace();
      if (!filter_parser->parse(configuration.rest_config.filter.c_str(), configuration.rest_config.filter.size())) {
        ldpp_dout(this, 1) << "ERROR: failed to parse filter JSON: " << configuration.rest_config.filter << dendl;
        return -EINVAL;
      }
    }
    return 0;
  }

  void execute(optional_yield y) override {
    ceph_assert(vector_bucket);
    ceph_assert(!vector_bucket->empty());
    op_ret = rgw::s3vector::query_vectors(configuration, filter_parser, driver, bucket_ns, this, y, reply, validation_errors);
  }

  void send_response() override {
    if (op_ret < 0 && !validation_errors.empty()) {
      send_validation_error_response();
      return;
    }
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    dump_start(s);

    const auto f = s->formatter;
    reply.dump(f);
    rgw_flush_formatter_and_reset(s, f);
  }
};

}

int RGWHandler_REST_s3Vector::init(rgw::sal::Driver* driver, req_state *s, rgw::io::BasicClient *cio) {
  s->dialect = "s3vectors";
  if (const auto ret = RGWHandler_REST::init(driver, s, cio); ret < 0) {
    return ret;
  }
  if (const auto ret = RGWHandler_REST::allocate_formatter(s, RGWFormat::JSON, false); ret < 0) {
    return ret;
  }
  // an s3vector request is signed as a non-s3 one, without the
  // "x-amz-content-sha256" header. so, the payload must be read, and its hash
  // calculated, before the request is authenticated.
  // the payload of PutVectors holds the vectors themselves, so its size is
  // limited separately from the parameters of the other REST requests
  const auto max_size = s->cct->_conf.get_val<Option::size_t>("rgw_s3vector_max_request_size");
  int ret;
  std::tie(ret, bl_post_body) = rgw_rest_read_all_input(s, max_size, false);
  if (ret == -ERANGE) {
    ldpp_dout(s, 1) << "ERROR: s3vector payload of " << s->init_state.url_bucket <<
      " is larger than the maximum of " << max_size << " bytes" << dendl;
    return -ERR_TOO_LARGE;
  }
  if (ret < 0) {
    ldpp_dout(s, 1) << "ERROR: failed to read s3vector payload of " <<
      s->init_state.url_bucket << ". error: " << ret << dendl;
    return ret;
  }
  // PayloadHash is present if the request was forwarded from another zone
  if (!s->info.args.exists("PayloadHash")) {
    auto sha256_hash = calc_hash_sha256_open_stream();
    calc_hash_sha256_update_stream(sha256_hash, bl_post_body.c_str(), bl_post_body.length());
    s->info.args.append("PayloadHash", calc_hash_sha256_close_stream(&sha256_hash));
  }
  return 0;
}

int RGWHandler_REST_s3Vector::postauth_init(optional_yield y) {
  // the URL of an s3vector request holds the name of the operation, and not a
  // bucket, so it cannot hold a tenant either. the tenant of the request is the
  // one of the user making it, as implied by its credentials
  const auto& tenant = s->auth.identity->get_tenant();
  if (const auto ret = rgw_validate_tenant_name(tenant); ret < 0) {
    ldpp_dout(s, 1) << "ERROR: invalid tenant name: " << tenant << ". error: " << ret << dendl;
    return ret;
  }
  s->bucket_tenant = tenant;
  return 0;
}

int RGWHandler_REST_s3Vector::authorize(const DoutPrefixProvider* dpp, optional_yield y) {
  return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
}

RGWOp* RGWHandler_REST_s3Vector::op_post() {
  const auto& op_name = s->init_state.url_bucket;
  if (op_name == "CreateIndex")
    return new RGWS3VectorCreateIndex(std::move(bl_post_body));
  if (op_name == "CreateVectorBucket")
    return new RGWS3VectorCreateVectorBucket(std::move(bl_post_body));
  if (op_name == "PutVectors")
    return new RGWS3VectorPutVectors(std::move(bl_post_body));
  if (op_name == "PutVectorBucketPolicy")
    return new RGWS3VectorPutVectorBucketPolicy(std::move(bl_post_body));
  if (op_name == "DeleteVectors")
    return new RGWS3VectorDeleteVectors(std::move(bl_post_body));
  if (op_name == "DeleteIndex")
    return new RGWS3VectorDeleteIndex(std::move(bl_post_body));
  if (op_name == "DeleteVectorBucket")
    return new RGWS3VectorDeleteVectorBucket(std::move(bl_post_body));
  if (op_name == "DeleteVectorBucketPolicy")
    return new RGWS3VectorDeleteVectorBucketPolicy(std::move(bl_post_body));
  if (op_name == "GetIndex")
    return new RGWS3VectorGetIndex(std::move(bl_post_body));
  if (op_name == "GetVectors")
    return new RGWS3VectorGetVectors(std::move(bl_post_body));
  if (op_name == "GetVectorBucket")
    return new RGWS3VectorGetVectorBucket(std::move(bl_post_body));
  if (op_name == "GetVectorBucketPolicy")
    return new RGWS3VectorGetVectorBucketPolicy(std::move(bl_post_body));
  if (op_name == "ListIndexes")
    return new RGWS3VectorListIndexes(std::move(bl_post_body));
  if (op_name == "ListVectors")
    return new RGWS3VectorListVectors(std::move(bl_post_body));
  if (op_name == "ListVectorBuckets")
    return new RGWS3VectorListVectorBuckets(std::move(bl_post_body));
  if (op_name == "QueryVectors")
    return new RGWS3VectorQueryVectors(std::move(bl_post_body));
  return nullptr;
}

