// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_arn.h"
#include "rgw_auth_s3.h"
#include "rgw_url.h"
#include "rgw_bucket_logging.h"
#include "rgw_rest_bucket_logging.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace {
  int verify_bucket_logging_params(const DoutPrefixProvider* dpp,  const req_state* s) {
    bool exists;
    const auto no_value = s->info.args.get("logging", &exists);
    if (!exists) {
      ldpp_dout(dpp, 1) << "ERROR: missing required param 'logging'" << dendl;
      return -EINVAL;
    }
    if (no_value.length() > 0) {
      ldpp_dout(dpp, 1) << "ERROR: param 'logging' should not have any value" << dendl;
      return -EINVAL;
    }
    if (s->bucket_name.empty()) {
      ldpp_dout(dpp, 1) << "ERROR: logging request must be on a bucket" << dendl;
      return -EINVAL;
    }
    return 0;
  }
}

// GET /<bucket name>/?logging
// reply is XML encoded
class RGWGetBucketLoggingOp : public RGWOp {
  rgw::bucketlogging::configuration configuration;

public:
  int verify_permission(optional_yield y) override {
    auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
    if (has_s3_resource_tag)
      rgw_iam_add_buckettags(this, s);

    if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketLogging)) {
      return -EACCES;
    }

    return 0;
  }

  void execute(optional_yield y) override {
    op_ret = verify_bucket_logging_params(this, s);
    if (op_ret < 0) {
      return;
    }

    std::unique_ptr<rgw::sal::Bucket> src_bucket;
    {
      const rgw_bucket src_bucket_id(s->bucket_tenant, s->bucket_name);
      op_ret = driver->load_bucket(this, src_bucket_id,
                                 &src_bucket, y);
      if (op_ret < 0) {
        ldpp_dout(this, 1) << "ERROR: failed to get bucket '" << src_bucket_id << "', ret = " << op_ret << dendl;
        return;
      }
    }
    const auto src_bucket_id = src_bucket->get_key();
    if (auto iter = src_bucket->get_attrs().find(RGW_ATTR_BUCKET_LOGGING); iter != src_bucket->get_attrs().end()) {
      try {
        configuration.enabled = true;
        decode(configuration, iter->second);
      } catch (buffer::error& err) {
        ldpp_dout(this, 1) << "WARNING: failed to decode logging attribute '" << RGW_ATTR_BUCKET_LOGGING
          << "' for bucket '" << src_bucket_id << "', error: " << err.what() << dendl;
        op_ret = -EIO;
        return;
      }
    } else {
      ldpp_dout(this, 5) << "WARNING: no logging configuration on bucket '" << src_bucket_id << "'" << dendl;
      return;
    }
    ldpp_dout(this, 20) << "INFO: found logging configuration on bucket '" << src_bucket_id << "'"
      << "'. configuration: " << configuration.to_json_str() << dendl;
  }

  void send_response() override {
    dump_errno(s);
    end_header(s, this, to_mime_type(s->format));
    dump_start(s);

    s->formatter->open_object_section_in_ns("BucketLoggingStatus", XMLNS_AWS_S3);
    configuration.dump_xml(s->formatter);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
  const char* name() const override { return "get_bucket_logging"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// PUT /<bucket name>/?logging
// actual configuration is XML encoded in the body of the message
class RGWPutBucketLoggingOp : public RGWDefaultResponseOp {
  // following data members are set in verify_permission()
  // and usd in execute()
  rgw::bucketlogging::configuration configuration;
  std::unique_ptr<rgw::sal::Bucket> target_bucket;

  int init_processing(optional_yield y) override {
    if (const auto ret = verify_bucket_logging_params(this, s); ret < 0) {
      return ret;
    }

    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    bufferlist data;
    int ret = 0;
    if (std::tie(ret, data) = read_all_input(s, max_size, false); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to read XML logging payload, ret = " << ret << dendl;
      return ret;
    }
    if (data.length() == 0) {
      ldpp_dout(this, 1) << "ERROR: XML logging payload missing" << dendl;
      return -EINVAL;
    }

    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()){
      ldpp_dout(this, 1) << "ERROR: failed to initialize XML parser" << dendl;
      return -EINVAL;
    }
    if (!parser.parse(data.c_str(), data.length(), 1)) {
      ldpp_dout(this, 1) << "ERROR: failed to parse XML logging payload" << dendl;
      return -ERR_MALFORMED_XML;
    }
    configuration.default_obj_roll_time = get_cct()->_conf->rgw_bucket_logging_obj_roll_time;
    try {
      RGWXMLDecoder::decode_xml("BucketLoggingStatus", configuration, &parser, true);
    } catch (RGWXMLDecoder::err& err) {
      ldpp_dout(this, 1) << "ERROR: failed to parse XML logging payload. error: " << err << dendl;
      return -ERR_MALFORMED_XML;
    }

    if (!configuration.enabled) {
      // when disabling logging, there is no target bucket
      return 0;
    }

    rgw_bucket target_bucket_id;
    if (const auto ret = rgw::bucketlogging::get_bucket_id(configuration.target_bucket, s->bucket_tenant, target_bucket_id); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to parse target bucket '" << configuration.target_bucket << "', ret = " << ret << dendl;
      return ret;
    }

    if (const auto ret = driver->load_bucket(this, target_bucket_id,
                                 &target_bucket, y); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to get target bucket '" << target_bucket_id << "', ret = " << ret << dendl;
      return ret;
    }

    return 0;
  }

  int verify_permission(optional_yield y) override {
    auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
    if (has_s3_resource_tag)
      rgw_iam_add_buckettags(this, s);

    if (!s->auth.identity->is_owner_of(s->bucket->get_info().owner)) {
      ldpp_dout(this, 1) << "ERROR: user '" << s->auth.identity->get_aclowner().id << "' is not the owner of bucket '" <<
        s->bucket_name << "'. owner is '" << s->bucket->get_info().owner << "'" << dendl;
      return -EACCES;
    }

    if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketLogging)) {
      return -EACCES;
    }

    if (!configuration.enabled) {
      // when disabling logging, there is no target bucket
      return 0;
    }

    const auto target_resource_arn = rgw::ARN(target_bucket->get_key(), configuration.target_prefix);
    return rgw::bucketlogging::verify_target_bucket_policy(this, target_bucket.get(), target_resource_arn, s);
  }

  const char* name() const override { return "put_bucket_logging"; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  void execute(optional_yield y) override {

    std::unique_ptr<rgw::sal::Bucket> src_bucket;
    {
      const rgw_bucket src_bucket_id{s->bucket_tenant, s->bucket_name};
      op_ret = driver->load_bucket(this, src_bucket_id,
                                   &src_bucket, y);
      if (op_ret < 0) {
        ldpp_dout(this, 1) << "ERROR: failed to get bucket '" << src_bucket_id << "', ret = " << op_ret << dendl;
        return;
      }
    }

    if (!configuration.enabled) {
      op_ret = rgw::bucketlogging::source_bucket_cleanup(this, driver, src_bucket.get(), true, y);
      return;
    }

    const auto src_bucket_id = src_bucket->get_key();
    const auto& target_bucket_id = target_bucket->get_key();
    if (target_bucket_id == src_bucket_id) {
      // target bucket must be different from source bucket (cannot change later on)
      ldpp_dout(this, 1) << "ERROR: target bucket '" << target_bucket_id << "' must be different from source bucket" << dendl;
      op_ret = -EINVAL;
      return;
    }
    const auto& target_info = target_bucket->get_info();
    if (target_info.zonegroup != src_bucket->get_info().zonegroup) {
      // target bucket must be in the same zonegroup as source bucket (cannot change later on)
      ldpp_dout(this, 1) << "ERROR: target bucket '" << target_bucket_id << "' zonegroup '" <<
        target_info.zonegroup << "' is different from the source bucket '" << src_bucket_id  <<
        "' zonegroup '" << src_bucket->get_info().zonegroup << "'" << dendl;
      op_ret = -EINVAL;
      return;
    }

    // following checks need to also be done on every write to the log bucket
    if (const auto ret = rgw::bucketlogging::verify_target_bucket_attributes(this, target_bucket.get()); ret < 0) {
      op_ret = ret;
      return;
    }

    std::optional<rgw::bucketlogging::configuration> old_conf;
    bufferlist conf_bl;
    encode(configuration, conf_bl);
    op_ret = retry_raced_bucket_write(this, src_bucket.get(), [this, &conf_bl, &src_bucket, &old_conf, y] {
      auto& attrs = src_bucket->get_attrs();
      auto it = attrs.find(RGW_ATTR_BUCKET_LOGGING);
      if (it != attrs.end()) {
        try {
          rgw::bucketlogging::configuration tmp_conf;
          tmp_conf.enabled = true;
          decode(tmp_conf, it->second);
          old_conf = std::move(tmp_conf);
        } catch (buffer::error& err) {
          ldpp_dout(this, 1) << "WARNING: failed to decode existing logging attribute '" << RGW_ATTR_BUCKET_LOGGING
              << "' for bucket '" << src_bucket->get_key() << "', error: " << err.what() << dendl;
        }
        if (!old_conf || (old_conf && *old_conf != configuration)) {
          // conf changed (or was unknown) - update
          it->second = conf_bl;
          return src_bucket->merge_and_store_attrs(this, attrs, y);
        }
        // nothing to update
        return 0;
      }
      // conf was added
      attrs.insert(std::make_pair(RGW_ATTR_BUCKET_LOGGING, conf_bl));
      return src_bucket->merge_and_store_attrs(this, attrs, y);
    }, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to set logging attribute '" << RGW_ATTR_BUCKET_LOGGING << "' to bucket '" <<
        src_bucket_id << "', ret = " << op_ret << dendl;
      return;
    }
    if (!old_conf) {
      ldpp_dout(this, 20) << "INFO: new logging configuration added to bucket '" << src_bucket_id << "'. configuration: " <<
        configuration.to_json_str() << dendl;
      if (const auto ret = rgw::bucketlogging::update_bucket_logging_sources(this, target_bucket, src_bucket_id, true, y); ret < 0) {
        ldpp_dout(this, 1) << "WARNING: failed to add source bucket '" << src_bucket_id << "' to logging sources of target bucket '" <<
          target_bucket_id << "', ret = " << ret << dendl;
      }
    } else if (*old_conf != configuration) {
      // conf changed - do cleanup
      if (const auto ret = commit_logging_object(*old_conf, target_bucket, this, y); ret < 0) {
        ldpp_dout(this, 1) << "WARNING: could not commit pending logging object when updating logging configuration of bucket '" <<
          src_bucket->get_key() << "', ret = " << ret << dendl;
      } else {
        ldpp_dout(this, 20) << "INFO: committed pending logging object when updating logging configuration of bucket '" <<
          src_bucket->get_key() << "'" << dendl;
      }
      if (old_conf->target_bucket != configuration.target_bucket) {
        rgw_bucket old_target_bucket_id;
        if (const auto ret = rgw::bucketlogging::get_bucket_id(old_conf->target_bucket, s->bucket_tenant, old_target_bucket_id); ret < 0) {
          ldpp_dout(this, 1) << "ERROR: failed to parse target bucket '" << old_conf->target_bucket << "', ret = " << ret << dendl;
          return;
        }
        if (const auto ret = rgw::bucketlogging::update_bucket_logging_sources(this, driver, old_target_bucket_id, src_bucket_id, false, y); ret < 0) {
          ldpp_dout(this, 1) << "WARNING: failed to remove source bucket '" << src_bucket_id << "' from logging sources of original target bucket '" <<
            old_target_bucket_id << "', ret = " << ret << dendl;
        }
        if (const auto ret = rgw::bucketlogging::update_bucket_logging_sources(this, target_bucket, src_bucket_id, true, y); ret < 0) {
          ldpp_dout(this, 1) << "WARNING: failed to add source bucket '" << src_bucket_id << "' to logging sources of target bucket '" <<
            target_bucket_id << "', ret = " << ret << dendl;
        }
      }
      ldpp_dout(this, 20) << "INFO: wrote logging configuration to bucket '" << src_bucket_id << "'. configuration: " <<
        configuration.to_json_str() << dendl;
    } else {
      ldpp_dout(this, 20) << "INFO: logging configuration of bucket '" << src_bucket_id << "' did not change" << dendl;
    }
  }
};

// Post /<bucket name>/?logging
class RGWPostBucketLoggingOp : public RGWDefaultResponseOp {
  // following data members are set in verify_permission()
  // and usd in execute()
  rgw::bucketlogging::configuration configuration;
  std::unique_ptr<rgw::sal::Bucket> target_bucket;

  int init_processing(optional_yield y) override {
    if (const auto ret = verify_bucket_logging_params(this, s); ret < 0) {
      return ret;
    }

    std::unique_ptr<rgw::sal::Bucket> src_bucket;
    {
      const rgw_bucket src_bucket_id{s->bucket_tenant, s->bucket_name};
      if (const auto ret = driver->load_bucket(this, src_bucket_id,
                                 &src_bucket, y); ret < 0) {
        ldpp_dout(this, 1) << "ERROR: failed to get bucket '" << src_bucket_id << "', ret = " << ret << dendl;
        return ret;
      }
    }
    const auto src_bucket_id = src_bucket->get_key();
    const auto& bucket_attrs = src_bucket->get_attrs();
    auto iter = bucket_attrs.find(RGW_ATTR_BUCKET_LOGGING);
    if (iter == bucket_attrs.end()) {
      ldpp_dout(this, 1) << "WARNING: no logging configured on bucket '" << src_bucket_id << "'" << dendl;
      return 0;
    }
    try {
      configuration.enabled = true;
      decode(configuration, iter->second);
    } catch (buffer::error& err) {
      ldpp_dout(this, 1) << "WARNING: failed to decode logging attribute '" << RGW_ATTR_BUCKET_LOGGING
        << "' for bucket '" << src_bucket_id << "', error: " << err.what() << dendl;
      return -EINVAL;
    }

    rgw_bucket target_bucket_id;
    if (const auto ret = rgw::bucketlogging::get_bucket_id(configuration.target_bucket, s->bucket_tenant, target_bucket_id); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to parse target bucket '" << configuration.target_bucket << "', ret = " << ret << dendl;
      return ret;
    }

    if (const auto ret = driver->load_bucket(this, target_bucket_id,
                                 &target_bucket, y); ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to get target bucket '" << target_bucket_id << "', ret = " << ret << dendl;
      return ret;
    }
    return 0;
  }

  int verify_permission(optional_yield y) override {
    auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
    if (has_s3_resource_tag)
      rgw_iam_add_buckettags(this, s);

    if (!verify_bucket_permission(this, s, rgw::IAM::s3PostBucketLogging)) {
      return -EACCES;
    }

    const auto target_resource_arn = rgw::ARN(target_bucket->get_key(), configuration.target_prefix);
    return rgw::bucketlogging::verify_target_bucket_policy(this, target_bucket.get(), target_resource_arn, s);
  }

  const char* name() const override { return "post_bucket_logging"; }
  RGWOpType get_type() override { return RGW_OP_POST_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  void execute(optional_yield y) override {
    const auto& target_bucket_id = target_bucket->get_key();
    std::string obj_name;
    RGWObjVersionTracker objv_tracker;
    op_ret = target_bucket->get_logging_object_name(obj_name, configuration.target_prefix, null_yield, this, &objv_tracker);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to get pending logging object name from target bucket '" << target_bucket_id << "'" << dendl;
      return;
    }
    const auto old_obj = obj_name;
    op_ret = rgw::bucketlogging::rollover_logging_object(configuration, target_bucket, obj_name, this, null_yield, true, &objv_tracker);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to flush pending logging object '" << old_obj
               << "' to target bucket '" << target_bucket_id << "'" << dendl;
      return;
    }
    ldpp_dout(this, 20) << "INFO: flushed pending logging object '" << old_obj
                << "' to target bucket '" << target_bucket_id << "'" << dendl;
  }
};

RGWOp* RGWHandler_REST_BucketLogging_S3::create_post_op() {
  return new RGWPostBucketLoggingOp();
}

RGWOp* RGWHandler_REST_BucketLogging_S3::create_put_op() {
  return new RGWPutBucketLoggingOp();
}

RGWOp* RGWHandler_REST_BucketLogging_S3::create_get_op() {
  return new RGWGetBucketLoggingOp();
}

