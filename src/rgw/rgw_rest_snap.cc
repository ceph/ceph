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

