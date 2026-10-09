// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <cerrno>
#include <string>

#include "common/Clock.h"
#include "common/dout.h"
#include "rgw_bucket_logging.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_op_internal.h"
#include "rgw_process_env.h"
#include "rgw_sal.h"
#include "rgw_xml.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int RGWPutBucketObjectLock::verify_permission(optional_yield y)
{
  return rgw_verify_bucket_permission_for_policy(this, s,
      rgw::IAM::s3PutBucketObjectLockConfiguration);
}

void RGWPutBucketObjectLock::execute(optional_yield y)
{
  if (!s->bucket->get_info().versioning_enabled()) {
    s->err.message = "Object lock cannot be enabled unless the "
        "bucket has versioning enabled";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_BUCKET_STATE;
    return;
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }
  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }
  if (!parser.parse(data.c_str(), data.length(), 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("ObjectLockConfiguration", obj_lock, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }
  if (obj_lock.has_rule() && !obj_lock.retention_period_valid()) {
    s->err.message = "retention period must be a positive integer value";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_RETENTION_PERIOD;
    return;
  }

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->owner.id,
                                         &data, nullptr, s->info, s->err, y);
  if (op_ret < 0) {
    ldpp_dout(this, 20) << __func__ << "forward_request_to_master returned ret="
        << op_ret << dendl;
    return;
  }

  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
    if (!s->bucket->get_info().obj_lock_enabled()) {
      // automatically enable object lock if the bucket is versioning-enabled
      if (!s->bucket->get_info().versioning_enabled()) {
        s->err.message = "Object lock cannot be enabled unless the "
            "bucket has versioning enabled";
        ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
        return -ERR_INVALID_BUCKET_STATE;
      }
      s->bucket->get_info().flags |= BUCKET_OBJ_LOCK_ENABLED;
    }

    s->bucket->get_info().obj_lock = obj_lock;
    op_ret = s->bucket->put_info(this, false, real_time(), y);
    return op_ret;
  }, y);
}

int RGWGetBucketObjectLock::verify_permission(optional_yield y)
{
  return rgw_verify_bucket_permission_for_policy(this, s,
      rgw::IAM::s3GetBucketObjectLockConfiguration);
}

void RGWGetBucketObjectLock::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    op_ret = -ERR_NO_SUCH_OBJECT_LOCK_CONFIGURATION;
    return;
  }
}

static int rgw_check_object_retention_transition(
  const RGWObjectRetention& old_retention,
  const RGWObjectRetention& new_retention,
  const bool bypass_perm,
  const bool bypass_governance_mode,
  std::string& err_message)
{
  const auto old_until =
    ceph::real_clock::to_time_t(old_retention.get_retain_until_date());
  const auto new_until =
    ceph::real_clock::to_time_t(new_retention.get_retain_until_date());
  const auto old_mode = old_retention.get_mode();
  const auto new_mode = new_retention.get_mode();
  const bool can_bypass_governance =
    bypass_perm && bypass_governance_mode;

  if (new_until < old_until) {
    if (old_mode == "GOVERNANCE" && can_bypass_governance) {
      return 0;
    }

    err_message = "proposed retain-until date shortens an existing retention period and governance bypass check failed";
    return -EACCES;
  }

  if (old_mode == new_mode) {
    return 0;
  }

  if (new_mode == "GOVERNANCE") {
    err_message = "can't change retention mode from COMPLIANCE to GOVERNANCE";
    return -EACCES;
  }

  if (!can_bypass_governance) {
    err_message = "can't change retention mode from GOVERNANCE without governance bypass";
    return -EACCES;
  }

  return 0;
}

int RGWPutObjRetention::verify_permission(optional_yield y)
{
  const int ret = rgw_verify_object_permission_for_policy(this, s,
      rgw::IAM::s3PutObjectRetention);
  if (ret < 0) {
    return ret;
  }

  op_ret = get_params(y);
  if (op_ret) {
    return op_ret;
  }
  if (bypass_governance_mode) {
    bypass_perm = verify_object_permission(this, s,
        rgw::IAM::s3BypassGovernanceRetention);
  }
  return 0;
}

void RGWPutObjRetention::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "object retention can't be set if bucket object lock not configured";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_REQUEST;
    return;
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("Retention", obj_retention, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  if (ceph::real_clock::to_time_t(obj_retention.get_retain_until_date()) < ceph_clock_now()) {
    s->err.message = "the retain-until date must be in the future";
    ldpp_dout(this, 0) << "ERROR: " << s->err.message << dendl;
    op_ret = -EINVAL;
    return;
  }
  bufferlist bl;
  obj_retention.encode(bl);

  // check old retention
  op_ret = s->object->get_obj_attrs(s->yield, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: get obj attr error"<< dendl;
    return;
  }
  const auto& attrs = s->object->get_attrs();
  auto aiter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
  if (aiter != attrs.end()) {
    RGWObjectRetention old_obj_retention;
    try {
      decode(old_obj_retention, aiter->second);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: failed to decode RGWObjectRetention" << dendl;
      op_ret = -EIO;
      return;
    }

    op_ret = rgw_check_object_retention_transition(old_obj_retention,
                                                   obj_retention,
                                                   bypass_perm,
                                                   bypass_governance_mode,
                                                   s->err.message);
    if (op_ret < 0) {
      return;
    }
  }

  const auto etag = s->object->get_attrs()[RGW_ATTR_ETAG].to_str();
  op_ret = rgw::bucketlogging::log_record(driver,
      rgw::bucketlogging::LoggingType::Journal,
      s->object.get(),
      s,
      canonical_name(),
      etag,
      s->object->get_size(),
      this, y, false, false);
  if (op_ret < 0) {
    return;
  }

  op_ret = s->object->modify_obj_attrs(RGW_ATTR_OBJECT_RETENTION, bl,
                                       s->yield, this);
}

int RGWGetObjRetention::verify_permission(optional_yield y)
{
  return rgw_verify_object_permission_for_policy(this, s,
      rgw::IAM::s3GetObjectRetention);
}

void RGWGetObjRetention::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "bucket object lock not configured";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_REQUEST;
    return;
  }
  op_ret = s->object->get_obj_attrs(s->yield, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << s->object
                       << " ret=" << op_ret << dendl;
    return;
  }
  const auto& attrs = s->object->get_attrs();
  auto aiter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
  if (aiter == attrs.end()) {
    op_ret = -ERR_NO_SUCH_OBJECT_LOCK_CONFIGURATION;
    return;
  }

  bufferlist::const_iterator iter{&aiter->second};
  try {
    obj_retention.decode(iter);
  } catch (const buffer::error& e) {
    ldpp_dout(this, 0) << __func__ <<  "decode object retention config failed" << dendl;
    op_ret = -EIO;
    return;
  }
}

int RGWPutObjLegalHold::verify_permission(optional_yield y)
{
  return rgw_verify_object_permission_for_policy(this, s,
      rgw::IAM::s3PutObjectLegalHold);
}

void RGWPutObjLegalHold::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "object legal hold can't be set if bucket object lock not enabled";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_REQUEST;
    return;
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }

  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("LegalHold", obj_legal_hold, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  op_ret = s->object->get_obj_attrs(y, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << s->object
                       << " ret=" << op_ret << dendl;
    return;
  }
  const auto etag = s->object->get_attrs()[RGW_ATTR_ETAG].to_str();
  op_ret = rgw::bucketlogging::log_record(driver,
      rgw::bucketlogging::LoggingType::Journal,
      s->object.get(),
      s,
      canonical_name(),
      etag,
      s->object->get_size(),
      this, y, false, false);
  if (op_ret < 0) {
    return;
  }

  bufferlist bl;
  obj_legal_hold.encode(bl);
  // if instance is empty, we should modify the latest object
  op_ret = s->object->modify_obj_attrs(RGW_ATTR_OBJECT_LEGAL_HOLD, bl,
                                       s->yield, this);
}

int RGWGetObjLegalHold::verify_permission(optional_yield y)
{
  return rgw_verify_object_permission_for_policy(this, s,
      rgw::IAM::s3GetObjectLegalHold);
}

void RGWGetObjLegalHold::execute(optional_yield y)
{
  if (!s->bucket->get_info().obj_lock_enabled()) {
    s->err.message = "bucket object lock not configured";
    ldpp_dout(this, 4) << "ERROR: " << s->err.message << dendl;
    op_ret = -ERR_INVALID_REQUEST;
    return;
  }
  op_ret = s->object->get_obj_attrs(s->yield, this);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to get obj attrs, obj=" << s->object
                       << " ret=" << op_ret << dendl;
    return;
  }
  auto aiter = s->object->get_attrs().find(RGW_ATTR_OBJECT_LEGAL_HOLD);
  if (aiter == s->object->get_attrs().end()) {
    op_ret = -ERR_NO_SUCH_OBJECT_LOCK_CONFIGURATION;
    return;
  }

  bufferlist::const_iterator iter{&aiter->second};
  try {
    obj_legal_hold.decode(iter);
  } catch (const buffer::error& e) {
    ldpp_dout(this, 0) << __func__ <<  "decode object legal hold config failed" << dendl;
    op_ret = -EIO;
    return;
  }
}
