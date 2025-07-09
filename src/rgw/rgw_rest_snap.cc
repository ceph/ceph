// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"
#include "rgw_rest_snap.h"
#include "rgw_rest_s3.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

int RGWListBucketSnapshots_ObjStore_S3::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3ListBucketSnapshots)) {
    return -EACCES;
  }

  return 0;
}

void RGWListBucketSnapshots_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWListBucketSnapshots_ObjStore_S3::execute(optional_yield y)
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }
}

void RGWListBucketSnapshots_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
  if (op_ret < 0) {
    return;
  }
  auto& bucket_info = s->bucket->get_info();

  s->formatter->open_object_section_in_ns("ListBucketSnapshotsResponse", XMLNS_AWS_S3);
  bucket_info.local.snap_mgr.dump_xml(s->formatter);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWConfigureBucketSnapshots_ObjStore_S3::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketSnapshotsConfiguration)) {
    return -EACCES;
  }

  return 0;
}

void RGWConfigureBucketSnapshots_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

struct RGWConfigureBucketSnapshots_S3
{
  bool enabled{false};

  void decode_xml(XMLObj *obj) {
    RGWXMLDecoder::decode_xml("Enabled", enabled, obj, true);
  }
};

int RGWConfigureBucketSnapshots_ObjStore_S3::get_params(optional_yield y)
{
  RGWXMLParser parser;

  if (!parser.init()){
    return -EINVAL;
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r = 0;
  bufferlist data;

  std::tie(r, data) = read_all_input(s, max_size, false);

  if (r < 0)
    return r;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    return -ERR_MALFORMED_XML;
  }

  RGWConfigureBucketSnapshots_S3 conf;
  try {
    RGWXMLDecoder::decode_xml("SnapshotsConfiguration", conf, &parser);
  } catch (RGWXMLDecoder::err& err) {

    ldpp_dout(this, 5) << "Malformed tagging request: " << err << dendl;
    return -ERR_MALFORMED_XML;
  }

  enabled = conf.enabled;

  return 0;
}

void RGWConfigureBucketSnapshots_ObjStore_S3::execute(optional_yield y)
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  auto& snap_mgr = s->bucket->get_info().local.snap_mgr;

  if (snap_mgr.is_enabled()) {
    if (enabled) {
      /* already enabled, nothing to do */
      return;
    }

    /* for now don't allow disabling/suspending already-enabled snapshots,
     * need to validate that things work correctly
     */
    op_ret = -EINVAL;
    s->err.message = "Bucket already has snapshots enabled, cannot disable";
    return;
  }
  snap_mgr.set_enabled(enabled);

  op_ret = s->bucket->put_info(this, false, real_time(), y);
  if (op_ret < 0) {
    s->err.message = "Error";
  }
}

void RGWConfigureBucketSnapshots_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

int RGWCreateBucketSnapshot_ObjStore_S3::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3CreateBucketSnapshot)) {
    return -EACCES;
  }

  return 0;
}

void RGWCreateBucketSnapshot_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

struct RGWCreateBucketSnapshot_S3
{
  std::string name;
  std::string desc;

  void decode_xml(XMLObj *obj) {
    RGWXMLDecoder::decode_xml("Name", name, obj, true);
    RGWXMLDecoder::decode_xml("Description", desc, obj, false);
  }
};


int RGWCreateBucketSnapshot_ObjStore_S3::get_params(optional_yield y)
{
  RGWXMLParser parser;

  if (!parser.init()){
    return -EINVAL;
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r = 0;
  bufferlist data;

  std::tie(r, data) = read_all_input(s, max_size, false);

  if (r < 0)
    return r;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    return -ERR_MALFORMED_XML;
  }

  RGWCreateBucketSnapshot_S3 conf;
  try {
    RGWXMLDecoder::decode_xml("SnapshotConfiguration", conf, &parser);
  } catch (RGWXMLDecoder::err& err) {

    ldpp_dout(this, 5) << "Malformed tagging request: " << err << dendl;
    return -ERR_MALFORMED_XML;
  }

  snap_name = conf.name;
  desc = conf.desc;

  if (snap_name.empty()) {
    ldpp_dout(this, 5) << "Missing Name parameter" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWCreateBucketSnapshot_ObjStore_S3::execute(optional_yield y)
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  auto& snap_mgr = s->bucket->get_info().local.snap_mgr;
  if (!snap_mgr.is_enabled()) {
    op_ret = -EINVAL;
    s->err.message = "Snapshots are not enabled for this bucket";
    return;
  }
  snap.info.name = snap_name;
  snap.info.description = desc;
  snap.info.creation_time = real_clock::now();

  rgw_bucket_snap_id snap_id;

  op_ret = s->bucket->get_info().local.snap_mgr.create_snap(snap.info, &snap.id);
  if (op_ret < 0) {
    if (op_ret == -EEXIST) {
      op_ret = -ERR_BUCKET_SNAP_EXISTS;
      s->err.message = "A snapshot with the same name already exists";
    }
    ldpp_dout(this, 0) << "ERROR: failed writing bucket instance info: " + cpp_strerror(-op_ret) << dendl;
    return;
  }
  op_ret = s->bucket->put_info(this, false, real_time(), y);
  if (op_ret < 0) {
    s->err.message = "Error";
  }
}

void RGWCreateBucketSnapshot_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
  if (op_ret < 0) {
    return;
  }
  
  s->formatter->open_object_section_in_ns("CreateBucketSnapshotResponse", XMLNS_AWS_S3);
  encode_xml("Snapshot", snap, s->formatter);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}


int RGWRemoveBucketSnapshot_ObjStore_S3::verify_permission(optional_yield y)
{
  auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
  if (has_s3_resource_tag)
    rgw_iam_add_buckettags(this, s);

  if (!verify_bucket_permission(this, s, rgw::IAM::s3DeleteBucketSnapshot)) {
    return -EACCES;
  }

  return 0;
}

void RGWRemoveBucketSnapshot_ObjStore_S3::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

int RGWRemoveBucketSnapshot_ObjStore_S3::get_params(optional_yield y)
{
  bool exists;
  auto snap_str = s->info.args.get("snapId", &exists);

  if (!exists ||
      snap_str.empty() ||
      !snap_id.init_from_str(snap_str)) {
    err = "Invalid snapId param";
    ldpp_dout(this, 5) << __func__ << "(): " << err << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWRemoveBucketSnapshot_ObjStore_S3::execute(optional_yield y)
{
  if (!s->bucket_exists) {
    op_ret = -ERR_NO_SUCH_BUCKET;
    return;
  }

  op_ret = get_params(y);
  if (op_ret < 0) {
    return;
  }

  op_ret = s->bucket->get_info().local.snap_mgr.remove_snap(snap_id);
  if (op_ret < 0) {
    s->err.message = err;
    ldpp_dout(this, 0) << "ERROR: failed writing bucket instance info: " + cpp_strerror(-op_ret) << dendl;
    return;
  }
  op_ret = s->bucket->put_info(this, false, real_time(), y);
  if (op_ret < 0) {
    s->err.message = "Error";
  }
  auto lc = driver->get_rgwlc();
  op_ret = lc->set_bucket_snap(this, y, s->bucket.get(), snap_id);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << __func__ << " failed to set lc entry for snap_id=" << snap_id << dendl;
    return;
  }
}

void RGWRemoveBucketSnapshot_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}


