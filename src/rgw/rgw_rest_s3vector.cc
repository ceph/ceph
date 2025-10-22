// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_op.h"
#include "rgw_rest_s3vector.h"
#include "rgw_s3vector.h"
#include "common/async/yield_context.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace {

class RGWS3VectorBase : public RGWDefaultResponseOp {
protected:
  template<typename T>
  int do_init_processing(T& configuration, optional_yield y) {
    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    bufferlist data;
    int ret = 0;
    if (std::tie(ret, data) = read_all_input(s, max_size, false); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to read JSON s3vector payload, ret = " << ret << dendl;
      return ret;
    }
    if (data.length() == 0) {
      ldpp_dout(this, 1) << "ERROR: JSON s3vector payload missing" << dendl;
      return -EINVAL;
    }

    JSONParser parser;
    if (!parser.parse(data.c_str(), data.length())) {
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
    op_ret = rgw::s3vector::create_index(configuration, this, y);
  }
};

class RGWS3VectorCreateVectorBucket : public RGWS3VectorBase {
  rgw::s3vector::create_vector_bucket_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector CreateVectorBucket" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsCreateVectorBucket)) {
      return -EACCES;
    }*/
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
    op_ret = rgw::s3vector::create_vector_bucket(configuration, this, y);
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
    op_ret = rgw::s3vector::delete_index(configuration, this, y);
  }
};

class RGWS3VectorDeleteVectorBucket : public RGWS3VectorBase {
  rgw::s3vector::delete_vector_bucket_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector DeleteVectorBucket" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsDeleteVectorBucket)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_delete_vector_bucket"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.DeleteVectorBucket", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_DELETE_VECTOR_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    op_ret = rgw::s3vector::delete_vector_bucket(configuration, this, y);
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
    op_ret = rgw::s3vector::list_vectors(configuration, this, y);
  }
};

class RGWS3VectorListVectorBuckets : public RGWS3VectorBase {
  rgw::s3vector::list_vector_buckets_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector ListVectorBuckets" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsListVectorBuckets)) {
      return -EACCES;
    }*/
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
    op_ret = rgw::s3vector::list_vector_buckets(configuration, this, y);
  }
};

class RGWS3VectorGetVectorBucket : public RGWS3VectorBase {
  rgw::s3vector::get_vector_bucket_t configuration;

  int verify_permission(optional_yield y) override {
    ldpp_dout(this, 10) << "INFO: verifying permission for s3vector GetVectorBucket" << dendl;
    // TODO: implement permission check
    /*if (!verify_bucket_permission(this, s, rgw::IAM::s3vectorsGetVectorBucket)) {
      return -EACCES;
    }*/
    return 0;
  }

  const char* name() const override { return "s3vector_get_vector_bucket"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.S3VECTOR.GetVectorBucket", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_S3VECTOR_GET_VECTOR_BUCKET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  int init_processing(optional_yield y) override {
    return do_init_processing(configuration, y);
  }

  void execute(optional_yield y) override {
    op_ret = rgw::s3vector::get_vector_bucket(configuration, this, y);
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


