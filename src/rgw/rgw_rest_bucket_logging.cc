// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <string_view>

#include "common/dout.h"
#include "common/dout_fmt.h"
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
  int verify_bucket_logging_params(const DoutPrefixProvider *dpp,
                                   const req_state *s)
  {
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

  void update_mtime_attribute(const DoutPrefixProvider *dpp,
                              rgw::sal::Attrs& attrs)
  {
    bufferlist mtime_bl;
    const auto mtime = ceph::coarse_real_time::clock::now();
    encode(mtime, mtime_bl);
    attrs[RGW_ATTR_BUCKET_LOGGING_MTIME] = std::move(mtime_bl);
    ldpp_dout(dpp, 20) << "INFO: logging config modified at: " << mtime << dendl;
  }

  void decode_mtime_attribute(const DoutPrefixProvider *dpp,
                              const rgw_bucket& bucket_id,
                              const rgw::sal::Attrs& attrs,
                              std::optional<ceph::real_time>& mtime)
  {
    if (const auto mtime_it = attrs.find(RGW_ATTR_BUCKET_LOGGING_MTIME);
        mtime_it != attrs.end()) {
      try {
        ceph::real_time tmp_mtime;
        decode(tmp_mtime, mtime_it->second);
        mtime = std::move(tmp_mtime);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 5) << "WARNING: failed to decode logging mtime attribute '"
          << RGW_ATTR_BUCKET_LOGGING_MTIME << "' for bucket '" << bucket_id
          << "', error: " << err.what() << dendl;
      }

      return;
    }

    ldpp_dout(dpp, 5) << "WARNING: no logging mtime attribute '"
      << RGW_ATTR_BUCKET_LOGGING_MTIME << "' for bucket '" << bucket_id << "'" << dendl;
  }

  void update_logging_sources_or_warn(const DoutPrefixProvider *dpp,
                                      std::unique_ptr<rgw::sal::Bucket>& target_bucket,
                                      const rgw_bucket& src_bucket_id,
                                      const bool add,
                                      const int log_level,
                                      optional_yield y)
  {
    if (const auto r = rgw::bucketlogging::update_bucket_logging_sources(
          dpp, target_bucket, src_bucket_id, add, y);
        r < 0) {
      const auto& target_bucket_id = target_bucket->get_key();
      ldpp_dout_fmt(dpp, log_level,
                    "WARNING: failed to update logging sources for target bucket '{}' "
                    "and source bucket '{}', ret = {}",
                    fmt::streamed(target_bucket_id), fmt::streamed(src_bucket_id), r);
    }
  }

  int flush_pending_logging_object(const DoutPrefixProvider *dpp,
                                   rgw::sal::Driver *driver,
                                   const rgw::bucketlogging::configuration& conf,
                                   std::unique_ptr<rgw::sal::Bucket>& target_bucket,
                                   rgw::sal::Bucket& src_bucket,
                                   std::string& last_committed,
                                   optional_yield y)
  {
    RGWObjVersionTracker objv_tracker;

    std::string obj_name;

    const auto& target_bucket_id = target_bucket->get_key();
    if (const auto r = target_bucket->get_logging_object_name(
          obj_name, conf.target_prefix, y, dpp, nullptr);
        r < 0 && r != -ENOENT) {
      ldpp_dout_fmt(dpp, 1,
                    "ERROR: failed to get name of logging object of logging bucket '{}' and prefix '{}', ret = {}",
                    fmt::streamed(target_bucket_id), conf.target_prefix, r);

      return r;
    }

    const auto region = driver->get_zone()->get_zonegroup().get_api_name();
    const auto rollover_ret =
      rgw::bucketlogging::rollover_logging_object(
        conf,
        target_bucket,
        obj_name,
        dpp,
        region,
        &src_bucket,
        y,
        false, // rollover should happen even if commit failed
        &objv_tracker,
        false,
        &last_committed);
    if (rollover_ret < 0) {
      ldpp_dout_fmt(dpp, 1,
                    "WARNING: failed to flush pending logging object '{}' "
                    "to target bucket '{}' when updating logging configuration of bucket '{}', ret = {}",
                    obj_name, fmt::streamed(target_bucket_id),
                    fmt::streamed(src_bucket.get_key()), rollover_ret);
      return 0;
    }

    ldpp_dout_fmt(dpp, 20,
                  "INFO: flushed pending logging object '{}' to target bucket '{}' "
                  "when updating logging configuration of bucket '{}'",
                  last_committed, fmt::streamed(target_bucket_id), fmt::streamed(src_bucket.get_key()));

    return 0;
  }
}

// GET /<bucket name>/?logging
// reply is XML encoded
class RGWGetBucketLoggingOp : public RGWOp {
  rgw::bucketlogging::configuration configuration;
  std::optional<ceph::real_time> mtime;

public:
  int verify_permission(optional_yield y) override {
    return rgw_verify_bucket_permission_for_policy(this, s,
        rgw::IAM::s3GetBucketLogging);
  }

  void execute(optional_yield y) override {
    op_ret = verify_bucket_logging_params(this, s);
    if (op_ret < 0) {
      return;
    }

    std::unique_ptr<rgw::sal::Bucket> src_bucket;
    {
      const rgw_bucket src_bucket_id(s->bucket_tenant, s->bucket_name);
      op_ret = driver->load_bucket(this, src_bucket_id, &src_bucket, y);
      if (op_ret < 0) {
        ldpp_dout(this, 1) << "ERROR: failed to get bucket '" << src_bucket_id
          << "', ret = " << op_ret << dendl;
        return;
      }
    }

    const auto src_bucket_id = src_bucket->get_key();
    const auto& attrs = src_bucket->get_attrs();
    if (const auto iter = attrs.find(RGW_ATTR_BUCKET_LOGGING);
        iter != attrs.end()) {
      try {
        configuration.enabled = true;
        decode(configuration, iter->second);
        decode_mtime_attribute(this, src_bucket_id, attrs, mtime);
      } catch (buffer::error& err) {
        ldpp_dout(this, 1) << "WARNING: failed to decode logging attribute '"
          << RGW_ATTR_BUCKET_LOGGING << "' for bucket '" << src_bucket_id
          << "', error: " << err.what() << dendl;
        op_ret = -EIO;
        return;
      }

      ldpp_dout(this, 20) << "INFO: found logging configuration on bucket '"
        << src_bucket_id << "'. configuration: " << configuration.to_json_str()
        << dendl;
      return;
    }

    ldpp_dout(this, 5) << "WARNING: no logging configuration on bucket '"
      << src_bucket_id << "'" << dendl;
  }

  void send_response() override {
    set_req_state_err(s, op_ret);
    dump_errno(s);
    if (mtime) {
      dump_last_modified(s, *mtime);
    }
    end_header(s, this, to_mime_type(s->format));
    dump_start(s);

    s->formatter->open_object_section_in_ns("BucketLoggingStatus", XMLNS_AWS_S3);
    configuration.dump_xml(s->formatter);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }

  const char* name() const override { return "get_bucket_logging"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.LOGGING", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// PUT /<bucket name>/?logging
// actual configuration is XML encoded in the body of the message
class RGWPutBucketLoggingOp : public RGWDefaultResponseOp {
  // following data members are set in verify_permission()
  // and used in execute()
  rgw::bucketlogging::configuration configuration;
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  std::string old_obj; // used when conf change triggers a rollover

  int init_processing(optional_yield y) override {
    if (const auto r = verify_bucket_logging_params(this, s);
        r < 0) {
      return r;
    }

    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    auto [r, data] = read_all_input(s, max_size, false);
    if (r < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to read XML logging payload, ret = " << r << dendl;
      return r;
    }

    if (data.length() == 0) {
      ldpp_dout(this, 1) << "ERROR: XML logging payload missing" << dendl;
      return -EINVAL;
    }

    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
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
      ldpp_dout(this, 1) << "ERROR: failed to parse XML logging payload. error: "
        << err << dendl;
      return -ERR_MALFORMED_XML;
    }

    if (!configuration.enabled) {
      // when disabling logging, there is no target bucket
      return 0;
    }

    rgw_bucket target_bucket_id;
    if (const auto r = rgw::bucketlogging::get_bucket_id(
          configuration.target_bucket, s->bucket_tenant, target_bucket_id);
        r < 0) {
      ldpp_dout_fmt(this, 1, "ERROR: failed to parse logging bucket '{}', ret = {}",
                    configuration.target_bucket, r);

      return r;
    }

    if (const auto r = driver->load_bucket(
          this, target_bucket_id, &target_bucket, y);
        r < 0) {
      ldpp_dout_fmt(this, 1, "ERROR: failed to get logging bucket '{}', ret = {}",
                    fmt::streamed(target_bucket_id), r);

      return r;
    }

    return 0;
  }

  int verify_permission(optional_yield y) override {
    if (!s->auth.identity->is_owner_of(s->bucket->get_info().owner)) {
      ldpp_dout_fmt(this, 1,
                    "ERROR: user '{}' is not the owner of bucket '{}'. owner is '{}'",
                    fmt::streamed(s->auth.identity->get_aclowner().id),
                    s->bucket_name, fmt::streamed(s->bucket->get_info().owner));
      return -EACCES;
    }

    if (const auto r = rgw_verify_bucket_permission_for_policy(
          this, s, rgw::IAM::s3PutBucketLogging);
        r < 0) {
      return r;
    }

    if (!configuration.enabled) {
      // when disabling logging, there is no target bucket
      return 0;
    }

    const auto target_resource_arn = rgw::ARN(target_bucket->get_key(), configuration.target_prefix);
    return rgw::bucketlogging::verify_target_bucket_policy(this, target_bucket.get(), target_resource_arn, s);
  }

  const char* name() const override { return "put_bucket_logging"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.LOGGING", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  void execute(optional_yield y) override {

    std::unique_ptr<rgw::sal::Bucket> src_bucket;
    {
      const rgw_bucket src_bucket_id{s->bucket_tenant, s->bucket_name};
      op_ret = driver->load_bucket(this, src_bucket_id,
                                   &src_bucket, y);
      if (op_ret < 0) {
        ldpp_dout(this, 1) << "ERROR: failed to get bucket '" << src_bucket_id
          << "', ret = " << op_ret << dendl;
        return;
      }
    }

    if (!configuration.enabled) {
      op_ret = rgw::bucketlogging::source_bucket_cleanup(
        this, driver, src_bucket.get(), true, y, &old_obj);
      return;
    }

    const auto src_bucket_id = src_bucket->get_key();
    const auto& target_bucket_id = target_bucket->get_key();
    if (target_bucket_id == src_bucket_id) {
      // target bucket must be different from source bucket (cannot change later on)
      ldpp_dout_fmt(this, 1,
                    "ERROR: logging bucket '{}' must be different from source bucket",
                    fmt::streamed(target_bucket_id));
      op_ret = -EINVAL;
      return;
    }
    const auto& target_info = target_bucket->get_info();
    if (target_info.zonegroup != src_bucket->get_info().zonegroup) {
      // target bucket must be in the same zonegroup as source bucket (cannot change later on)
      ldpp_dout(this, 1) << "ERROR: logging bucket '" << target_bucket_id
        << "' zonegroup '" << target_info.zonegroup
        << "' is different from the source bucket '" << src_bucket_id
        << "' zonegroup '" << src_bucket->get_info().zonegroup << "'" << dendl;
      op_ret = -EINVAL;
      return;
    }

    // following checks need to also be done on every write to the log bucket
    op_ret = rgw::bucketlogging::verify_target_bucket_attributes(this, target_bucket.get());
    if (op_ret < 0) {
      return;
    }

    std::optional<rgw::bucketlogging::configuration> old_conf;
    bufferlist conf_bl;
    encode(configuration, conf_bl);
    op_ret = retry_raced_bucket_write(this, src_bucket.get(), [this, &conf_bl, &src_bucket, &old_conf, y] {
      auto& attrs = src_bucket->get_attrs();
      if (auto it = attrs.find(RGW_ATTR_BUCKET_LOGGING);
          it != attrs.end()) {
        try {
          rgw::bucketlogging::configuration tmp_conf;
          tmp_conf.enabled = true;
          decode(tmp_conf, it->second);
          old_conf = std::move(tmp_conf);
        } catch (buffer::error& err) {
          ldpp_dout(this, 1) << "WARNING: failed to decode existing logging attribute '"
            << RGW_ATTR_BUCKET_LOGGING << "' for bucket '" << src_bucket->get_key()
            << "', error: " << err.what() << dendl;
        }

        if (!old_conf || *old_conf != configuration) {
          // conf changed (or was unknown) - update
          it->second = conf_bl;
          update_mtime_attribute(this, attrs);
          return src_bucket->merge_and_store_attrs(this, attrs, y);
        }

        // nothing to update
        return 0;
      }

      // conf was added
      attrs.insert(std::make_pair(RGW_ATTR_BUCKET_LOGGING, conf_bl));
      update_mtime_attribute(this, attrs);
      return src_bucket->merge_and_store_attrs(this, attrs, y);
    }, y);
    if (op_ret < 0) {
      ldpp_dout_fmt(this, 1,
                    "ERROR: failed to set logging attribute '{}' to bucket '{}', ret = {}",
                    RGW_ATTR_BUCKET_LOGGING, fmt::streamed(src_bucket_id), op_ret);
      return;
    }
    if (!old_conf) {
      ldpp_dout_fmt(this, 20,
                    "INFO: new logging configuration added to bucket '{}'. configuration: {}",
                    fmt::streamed(src_bucket_id), configuration.to_json_str());
      update_logging_sources_or_warn(this, target_bucket, src_bucket_id,
                                     true, 1, y);
      return;
    }

    if (*old_conf == configuration) {
      ldpp_dout(this, 20) << "INFO: logging configuration of bucket '"
        << src_bucket_id << "' did not change" << dendl;
      return;
    }

    // conf changed - do cleanup
    op_ret = flush_pending_logging_object(this, driver, *old_conf,
                                          target_bucket, *src_bucket,
                                          old_obj, y);
    if (op_ret < 0) {
      return;
    }

    if (old_conf->target_bucket != configuration.target_bucket) {
      rgw_bucket old_target_bucket_id;
      if (const auto r = rgw::bucketlogging::get_bucket_id(
            old_conf->target_bucket, s->bucket_tenant, old_target_bucket_id);
          r < 0) {
        ldpp_dout_fmt(this, 1,
                      "ERROR: failed to parse logging bucket '{}', ret = {}",
                      old_conf->target_bucket, r);
        return;
      }

      if (const auto r = rgw::bucketlogging::update_bucket_logging_sources(
            this, driver, old_target_bucket_id, src_bucket_id, false, y);
          r < 0) {
        ldpp_dout_fmt(this, 1,
                      "WARNING: failed to remove source bucket '{}' from logging "
                      "sources of original logging bucket '{}', ret = {}",
                      fmt::streamed(src_bucket_id),
                      fmt::streamed(old_target_bucket_id), r);
      }

      update_logging_sources_or_warn(this, target_bucket, src_bucket_id,
                                     true, 1, y);
    }

    ldpp_dout(this, 20) << "INFO: wrote logging configuration to bucket '"
      << src_bucket_id << "'. configuration: " << configuration.to_json_str()
      << dendl;
  }

  void send_response() override {
    set_req_state_err(s, op_ret);
    dump_errno(s);
    end_header(s, this, to_mime_type(s->format));
    if (!old_obj.empty()) {
      dump_start(s);
      s->formatter->open_object_section_in_ns("PutBucketLoggingOutput", XMLNS_AWS_S3);
      s->formatter->dump_string("FlushedLoggingObject", old_obj);
      s->formatter->close_section();
      rgw_flush_formatter_and_reset(s, s->formatter);
    }
  }
};

// Post /<bucket name>/?logging
class RGWPostBucketLoggingOp : public RGWDefaultResponseOp {
  // following data members are set in verify_permission()
  // and used in execute()
  rgw::bucketlogging::configuration configuration;
  std::unique_ptr<rgw::sal::Bucket> target_bucket;
  std::unique_ptr<rgw::sal::Bucket> source_bucket;
  std::string old_obj;

  int init_processing(optional_yield y) override {
    if (const auto r = verify_bucket_logging_params(this, s);
        r < 0) {
      return r;
    }

    {
      const rgw_bucket src_bucket_id{s->bucket_tenant, s->bucket_name};
      if (const auto r = driver->load_bucket(
            this, src_bucket_id, &source_bucket, y);
          r < 0) {
        ldpp_dout_fmt(this, 1, "ERROR: failed to get bucket '{}', ret = {}",
                      fmt::streamed(src_bucket_id), r);

        return r;
      }
    }

    return rgw::bucketlogging::get_target_and_conf_from_source(
      this, driver, source_bucket.get(), s->bucket_tenant, configuration,
      target_bucket, y);
  }

  int verify_permission(optional_yield y) override {
    if (const auto r = rgw_verify_bucket_permission_for_policy(
          this, s, rgw::IAM::s3PostBucketLogging);
        r < 0) {
      return r;
    }

    const auto target_resource_arn = rgw::ARN(target_bucket->get_key(), configuration.target_prefix);
    return rgw::bucketlogging::verify_target_bucket_policy(this, target_bucket.get(), target_resource_arn, s);
  }

  const char* name() const override { return "post_bucket_logging"; }
  std::string canonical_name() const override { return fmt::format("REST.{}.LOGGING", s->info.method); }
  RGWOpType get_type() override { return RGW_OP_POST_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  void execute(optional_yield y) override {
    const auto& target_bucket_id = target_bucket->get_key();
    // make sure that the logging source attribute is up-to-date
    update_logging_sources_or_warn(this, target_bucket,
                                   source_bucket->get_key(),
                                   true, 5, y);

    std::string obj_name;
    RGWObjVersionTracker objv_tracker;
    op_ret = target_bucket->get_logging_object_name(
      obj_name, configuration.target_prefix, y, this, &objv_tracker);
    if (op_ret < 0 && op_ret != -ENOENT) {
      ldpp_dout_fmt(this, 1,
                    "ERROR: failed to get pending logging object name from logging bucket '{}', ret = {}",
                    fmt::streamed(target_bucket_id), op_ret);
      return;
    }
    if (op_ret == -ENOENT) {
      // no pending object - nothing to flush
      ldpp_dout_fmt(this, 5,
                    "INFO: no pending logging object in logging bucket '{}'. new object should be created",
                    fmt::streamed(target_bucket_id));
    }
    const auto region = driver->get_zone()->get_zonegroup().get_api_name();
    op_ret =
      rgw::bucketlogging::rollover_logging_object(
        configuration,
        target_bucket,
        obj_name,
        this,
        region,
        source_bucket.get(),
        y,
        true,
        &objv_tracker,
        false,
        &old_obj);
    if (op_ret < 0) {
      ldpp_dout_fmt(this, 1,
                    "ERROR: failed to flush pending logging object '{}' to logging bucket '{}'. "
                    "last committed object is '{}', ret = {}",
                    obj_name, fmt::streamed(target_bucket_id), old_obj, op_ret);
      return;
    }

    ldpp_dout_fmt(this, 20,
                  "INFO: flushed pending logging object '{}' to logging bucket '{}'",
                  old_obj, fmt::streamed(target_bucket_id));
  }

  void send_response() override {
    set_req_state_err(s, op_ret);
    dump_errno(s);
    end_header(s, this, to_mime_type(s->format));
    dump_start(s);
    s->formatter->open_object_section_in_ns("PostBucketLoggingOutput", XMLNS_AWS_S3);
    s->formatter->dump_string("FlushedLoggingObject", old_obj);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
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
