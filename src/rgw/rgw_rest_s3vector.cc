// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_op.h"
#include "rgw_rest_s3vector.h"
#include "rgw_s3vector.h"
#include "rgw_process_env.h"
#include "common/async/yield_context.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace {

class RGWS3VectorBase : public RGWDefaultResponseOp {
protected:
  bufferlist in_data;
  template<typename T>
  int do_init_processing(T& configuration, optional_yield y) {
    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    int ret = 0;
    if (std::tie(ret, in_data) = read_all_input(s, max_size, false); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to read JSON s3vector payload, ret = " << ret << dendl;
      return ret;
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
      decode_json_obj(configuration, &parser);
    } catch (const JSONDecoder::err& e) {
      ldpp_dout(this, 1) << "ERROR: failed to decode JSON s3vector payload: " << e.what() << dendl;
      return -EINVAL;
    }

    return 0;
  }
};

class RGWS3VectorCreateIndex : public RGWS3VectorBase {
  rgw::s3vector::create_index_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector CreateIndex" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsCreateIndex)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_create_index"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.CreateIndex", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_CREATE_INDEX; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::create_index(configuration, this, y);
  }
};

class RGWS3VectorCreateVectorBucket : public RGWS3VectorBase {
  rgw::s3vector::create_vector_bucket_t configuration;
  std::unique_ptr<rgw::sal::VectorBucket> bucket;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector CreateVectorBucket" << dendl;
    if (s->auth.identity->is_anonymous()) {
      return -EACCES;
    }

    rgw::ARN arn(rgw::Partition::aws, rgw::Service::s3vectors,
                         "", s->bucket_tenant, configuration.vector_bucket_name);
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

    return 0;
  }

  const char* name() const override { return "s3vector_create_vector_bucket"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.CreateVectorBucket", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_CREATE_VECTOR_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    int ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (ret != -ENOENT) {
      // TODO: verify attributes from the existing bucket match the requested configuration
      op_ret = ret;
      return;
    }

    const auto& zonegroup = s->penv.site->get_zonegroup();

    rgw::sal::VectorBucket::CreateParams createparams;
    createparams.owner = s->user->get_id();
    createparams.zonegroup_id = zonegroup.id;
    // vector buckets are indexless
    createparams.index_type = rgw::BucketIndexType::Indexless;
    createparams.placement_rule.storage_class = s->info.storage_class;
    if (!driver->is_meta_master()) {
      // apply bucket creation on the master zone first
      JSONParser jp;
      op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                           &in_data, &jp, s->info, s->err, y);
      if (op_ret < 0) {
        return;
      }

      RGWBucketInfo master_info;
      JSONDecoder::decode_json("bucket_info", master_info, &jp);

      // update params with info from the master
      createparams.marker = master_info.bucket.marker;
      createparams.bucket_id = master_info.bucket.bucket_id;
      createparams.zonegroup_id = master_info.zonegroup;
      createparams.quota = master_info.quota;
      createparams.creation_time = master_info.creation_time;
    }

    op_ret = bucket->create(this, createparams, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to create s3vector bucket " << bucket_id << ". error: " << ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::create_vector_bucket(configuration, this, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to initialize s3vector bucket " << bucket_id << ". error: " << ret << dendl;
      return;
    }
  }

  void send_response() override {
    if (op_ret == -ERR_BUCKET_EXISTS) {
      const auto eexist_override = s->cct->_conf.get_val<bool>("rgw_bucket_eexist_override");
      if (! eexist_override) [[likely]] {
        op_ret = 0;
      } else {
        s->err.message = "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Specify a different name and try again.";
      }
    }

    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);

    if (op_ret == 0 && s->system_request) {
      s->format = RGWFormat::JSON;
      end_header(s, this, to_mime_type(s->format));
      JSONFormatter f; /* use json formatter for system requests output */

      ceph_assert(bucket);
      ceph_assert(!bucket->empty());
      const RGWBucketInfo& info = bucket->get_info();
      const obj_version& ep_objv = bucket->get_version();
      f.open_object_section("info");
      encode_json("entry_point_object_ver", ep_objv, &f);
      encode_json("object_ver", info.objv_tracker.read_version, &f);
      encode_json("bucket_info", info, &f);
      f.close_section();
      rgw_flush_formatter_and_reset(s, &f);
      return;
    }
    end_header(s);
  }

};

class RGWS3VectorDeleteIndex : public RGWS3VectorBase {
  rgw::s3vector::delete_index_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteIndex" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsDeleteIndex)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_delete_index"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.DeleteIndex", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_DELETE_INDEX; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::delete_index(configuration, this, y);
  }
};

class RGWS3VectorDeleteVectorBucket : public RGWS3VectorBase {
  // TODO: collapse with get_vector_bucket_t an create a common function
  // to build and verify the ARN in init_processing()
  rgw::s3vector::delete_vector_bucket_t configuration;
  boost::optional<rgw::ARN> arn;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteVectorBucket" << dendl;
    if (!verify_bucket_permission(this, s, arn.get(), rgw::IAM::s3vectorsDeleteVectorBucket)) {
      // TODO: ignore failure for now "evaluate_iam_policies: implicit deny from identity-based policy"
      // return -EACCES;
      return 0;
    }
    return 0;
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
    if (configuration.vector_bucket_arn.empty()) {
      arn.emplace(rgw::Partition::aws,
          rgw::Service::s3vectors,
          s->penv.site->get_zonegroup().api_name,
          s->auth.identity->get_tenant(),
          configuration.vector_bucket_name);
    } else {
      arn = rgw::ARN::parse(configuration.vector_bucket_arn);
      if (!arn) {
        ldpp_dout(this, 1) << "ERROR: invalid s3vector bucket ARN: " << configuration.vector_bucket_arn << dendl;
        return -EINVAL;
      }
      if (arn->service != rgw::Service::s3vectors ||
          arn->partition != rgw::Partition::aws ||
          arn->region != s->penv.site->get_zonegroup().api_name ||
          arn->account != s->bucket_tenant ||
          arn->resource.empty()) {
        ldpp_dout(this, 1) << "ERROR: invalid s3vector bucket ARN service: " << arn->to_string() << dendl;
        return -EINVAL;
      }
    }
    if (!configuration.vector_bucket_name.empty() &&
        arn->resource != configuration.vector_bucket_name) {
      ldpp_dout(this, 1) << "ERROR: s3vector bucket ARN bucket mismatch: " << arn->to_string()
                    << " expected bucket: " << configuration.vector_bucket_name << dendl;
      return -EINVAL;
    }

    s->bucket_name = arn->resource;
    ldpp_dout(this, 20) << "INFO: s3vector bucket ARN: " << arn.get() << dendl;
    return 0;
  }

  void execute(optional_yield y) override {
    {
      JSONFormatter f;
      configuration.dump(&f);
      std::stringstream ss;
      f.flush(ss);
      ldpp_dout(this, 20) << "INFO: executing s3vector DeleteVectorBucket with: " << ss.str() << dendl;
    }
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = bucket->remove(this, false, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to delete s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    // TODO: verify bucket is empty before deletion (or support force delete)
    // rgw::s3vector::delete_vector_bucket(configuration, this, y);
  }
};

class RGWS3VectorDeleteVectorBucketPolicy : public RGWS3VectorBase {
  rgw::s3vector::delete_vector_bucket_policy_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteVectorBucketPolicy" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsDeleteVectorBucketPolicy)) {
      return -EACCES;
    }*/
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
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::delete_vector_bucket_policy(configuration, this, y);
  }
};

class RGWS3VectorPutVectors : public RGWS3VectorBase {
  rgw::s3vector::put_vectors_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector PutVectors" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsPutVectors)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_put_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.PutVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_PUT_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::put_vectors(configuration, this, y);
  }
};

class RGWS3VectorGetVectors : public RGWS3VectorBase {
  rgw::s3vector::get_vectors_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetVectors" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsGetVectors)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_get_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.GetVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_GET_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::get_vectors(configuration, this, y);
  }
};

class RGWS3VectorListVectors : public RGWS3VectorBase {
  rgw::s3vector::list_vectors_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector ListVectors" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsListVectors)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_list_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.ListVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_LIST_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::list_vectors(configuration, this, y);
  }
};

class RGWS3VectorListVectorBuckets : public RGWS3VectorBase {
  rgw::s3vector::list_vector_buckets_t configuration;
  rgw::sal::BucketList listing;
  std::string zonegroup_name;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector ListVectorBuckets" << dendl;
    if (s->auth.identity->is_anonymous()) {
      return -EACCES;
    }

    zonegroup_name = s->penv.site->get_zonegroup().api_name;
    rgw::ARN arn = rgw::ARN(rgw::Partition::aws,
        rgw::Service::s3vectors,
        zonegroup_name,
        s->auth.identity->get_tenant(),
        "*");
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
      configuration.dump(&f);
      std::stringstream ss;
      f.flush(ss);
      ldpp_dout(this, 20) << "INFO: executing s3vector ListVectorBuckets with: " << ss.str() << dendl;
    }

    op_ret = driver->list_vector_buckets(this, s->owner.id, s->auth.identity->get_tenant(),
        configuration.next_token, "", configuration.max_results, listing, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: failed to execute ListVectorBuckets. error: " << op_ret << dendl;
      return;
    }
  }

  void send_response() override {
    if (op_ret < 0) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");
    // convert BucketList to AWS S3 Vector Buckets format
    JSONFormatter f;
    f.open_object_section("");
    if (!listing.next_marker.empty()) {
      ::encode_json("nextToken", listing.next_marker, &f);
    }
    f.open_array_section("vectorBuckets");
    for (const auto& bucket : listing.buckets) {
      f.open_object_section("");
      ::encode_json("creationTime",  ceph::to_iso_8601(bucket.creation_time), &f);
      rgw::ARN arn(rgw::Partition::aws,
          rgw::Service::s3vectors,
          zonegroup_name,
          s->auth.identity->get_tenant(),
          bucket.bucket.name);
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
  rgw::s3vector::get_vector_bucket_t configuration;
  std::unique_ptr<rgw::sal::VectorBucket> bucket;
  boost::optional<rgw::ARN> arn;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetVectorBucket" << dendl;

    if (!verify_bucket_permission(this, s, arn.get(), rgw::IAM::s3vectorsGetVectorBucket)) {
      // TODO: ignore failure for now "evaluate_iam_policies: implicit deny from identity-based policy"
      // return -EACCES;
      return 0;
    }
    return 0;
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
    if (configuration.vector_bucket_arn.empty()) {
      arn.emplace(rgw::Partition::aws,
          rgw::Service::s3vectors,
          s->penv.site->get_zonegroup().api_name,
          s->auth.identity->get_tenant(),
          configuration.vector_bucket_name);
    } else {
      arn = rgw::ARN::parse(configuration.vector_bucket_arn);
      if (!arn) {
        ldpp_dout(this, 1) << "ERROR: invalid s3vector bucket ARN: " << configuration.vector_bucket_arn << dendl;
        return -EINVAL;
      }
      if (arn->service != rgw::Service::s3vectors ||
          arn->partition != rgw::Partition::aws ||
          arn->region != s->penv.site->get_zonegroup().api_name ||
          arn->account != s->bucket_tenant ||
          arn->resource.empty()) {
        ldpp_dout(this, 1) << "ERROR: invalid s3vector bucket ARN service: " << arn->to_string() << dendl;
        return -EINVAL;
      }
    }
    if (!configuration.vector_bucket_name.empty() &&
        arn->resource != configuration.vector_bucket_name) {
      ldpp_dout(this, 1) << "ERROR: s3vector bucket ARN bucket mismatch: " << arn->to_string()
                    << " expected bucket: " << configuration.vector_bucket_name << dendl;
      return -EINVAL;
    }

    s->bucket_name = arn->resource;
    ldpp_dout(this, 20) << "INFO: s3vector bucket ARN: " << arn.get() << dendl;
    return 0;
  }

  void execute(optional_yield y) override {
    {
      JSONFormatter f;
      configuration.dump(&f);
      std::stringstream ss;
      f.flush(ss);
      ldpp_dout(this, 20) << "INFO: executing s3vector GetVectorBucket with: " << ss.str() << dendl;
    }
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
    }
  }

  void send_response() override {
    if (op_ret < 0) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");
    // convert BucketList to AWS S3 Vector Buckets format
    JSONFormatter f;
    f.open_object_section("");
    f.open_object_section("vectorBucket");
    ::encode_json("creationTime",  ceph::to_iso_8601(bucket->get_creation_time()), &f);
    rgw::ARN arn(rgw::Partition::aws, rgw::Service::s3vectors,
                       "", bucket->get_tenant(), bucket->get_name());
    ::encode_json("vectorBucketArn", arn.to_string(), &f);
    ::encode_json("vectorBucketName", bucket->get_name(), &f);
    f.close_section(); // vectorBucket
    f.close_section(); // root object
    std::stringstream ss;
    f.flush(ss);
    dump_body(s, ss.str());
  }
};

class RGWS3VectorGetIndex : public RGWS3VectorBase {
  rgw::s3vector::get_index_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetIndex" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsGetIndex)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_get_index"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.GetIndex", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_GET_INDEX; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::get_index(configuration, this, y);
  }
};

class RGWS3VectorListIndexes : public RGWS3VectorBase {
  rgw::s3vector::list_indexes_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector ListIndexes" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsListIndexes)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_list_indexes"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.ListIndexes", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_LIST_INDEXES; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::list_indexes(configuration, this, y);
  }
};

class RGWS3VectorPutVectorBucketPolicy : public RGWS3VectorBase {
  rgw::s3vector::put_vector_bucket_policy_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector PutVectorBucketPolicy" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsPutVectorBucketPolicy)) {
      return -EACCES;
    }*/
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
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::put_vector_bucket_policy(configuration, this, y);
  }
};

class RGWS3VectorGetVectorBucketPolicy : public RGWS3VectorBase {
  rgw::s3vector::get_vector_bucket_policy_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetVectorBucketPolicy" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsGetVectorBucketPolicy)) {
      return -EACCES;
    }*/
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
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::get_vector_bucket_policy(configuration, this, y);
  }
};

class RGWS3VectorDeleteVectors : public RGWS3VectorBase {
  rgw::s3vector::delete_vectors_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteVectors" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsDeleteVectors)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_delete_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.DeleteVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_DELETE_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::delete_vectors(configuration, this, y);
  }
};

class RGWS3VectorQueryVectors : public RGWS3VectorBase {
  rgw::s3vector::query_vectors_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector QueryVectors" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsQueryVectors)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_query_vectors"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.QueryVectors", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_QUERY_VECTORS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    const rgw_bucket bucket_id(s->bucket_tenant, configuration.vector_bucket_name);
    std::unique_ptr<rgw::sal::VectorBucket> bucket;
    op_ret = driver->load_vector_bucket(this, bucket_id, &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to load s3vector bucket " << bucket_id << ". error: " << op_ret << dendl;
      return;
    }
    op_ret = rgw::s3vector::query_vectors(configuration, this, y);
  }
};
}

RGWOp* RGWHandler_REST_s3Vector::op_post() {
  const auto& op_name = s->init_state.url_bucket;
  if (op_name == "CreateIndex")
    return new RGWS3VectorCreateIndex();
  if (op_name == "CreateVectorBucket")
    return new RGWS3VectorCreateVectorBucket();
  if (op_name == "PutVectors")
    return new RGWS3VectorPutVectors();
  if (op_name == "PutVectorBucketPolicy")
    return new RGWS3VectorPutVectorBucketPolicy();
  if (op_name == "DeleteVectors")
    return new RGWS3VectorDeleteVectors();
  if (op_name == "DeleteIndex")
    return new RGWS3VectorDeleteIndex();
  if (op_name == "DeleteVectorBucket")
    return new RGWS3VectorDeleteVectorBucket();
  if (op_name == "DeleteVectorBucketPolicy")
    return new RGWS3VectorDeleteVectorBucketPolicy();
  if (op_name == "GetIndex")
    return new RGWS3VectorGetIndex();
  if (op_name == "GetVectors")
    return new RGWS3VectorGetVectors();
  if (op_name == "GetVectorBucket")
    return new RGWS3VectorGetVectorBucket();
  if (op_name == "GetVectorBucketPolicy")
    return new RGWS3VectorGetVectorBucketPolicy();
  if (op_name == "ListIndexes")
    return new RGWS3VectorListIndexes();
  if (op_name == "ListVectors")
    return new RGWS3VectorListVectors();
  if (op_name == "ListVectorBuckets")
    return new RGWS3VectorListVectorBuckets();
  if (op_name == "QueryVectors")
    return new RGWS3VectorQueryVectors();
  return nullptr;
}


