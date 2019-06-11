
#include "svc_bilog_rados.h"
#include "svc_bi_rados.h"

#include "cls/rgw/cls_rgw_client.h"


#define dout_subsys ceph_subsys_rgw

RGWSI_BILog_RADOS::RGWSI_BILog_RADOS(CephContext *cct) : RGWServiceInstance(cct)
{
}

void RGWSI_BILog_RADOS::init(RGWSI_BucketIndex_RADOS *bi_rados_svc)
{
  svc.bi = bi_rados_svc;
}

int RGWSI_BILog_RADOS::log_trim(const RGWBucketInfo& bucket_info, int shard_id, string& start_marker, string& end_marker)
{
  RGWSI_RADOS::Pool index_pool;
  map<int, string> bucket_objs;

  BucketIndexShardsManager start_marker_mgr;
  BucketIndexShardsManager end_marker_mgr;

  int r = svc.bi->open_bucket_index(bucket_info, shard_id, &index_pool, &bucket_objs, nullptr);
  if (r < 0) {
    return r;
  }

  r = start_marker_mgr.from_string(start_marker, shard_id);
  if (r < 0) {
    return r;
  }

  r = end_marker_mgr.from_string(end_marker, shard_id);
  if (r < 0) {
    return r;
  }

  return CLSRGWIssueBILogTrim(index_pool.ioctx(), start_marker_mgr, end_marker_mgr, bucket_objs,
			      cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BILog_RADOS::log_start(const RGWBucketInfo& bucket_info, int shard_id)
{
  RGWSI_RADOS::Pool index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi->open_bucket_index(bucket_info, shard_id, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;

  return CLSRGWIssueResyncBucketBILog(index_pool.ioctx(), bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BILog_RADOS::log_stop(const RGWBucketInfo& bucket_info, int shard_id)
{
  RGWSI_RADOS::Pool index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi->open_bucket_index(bucket_info, shard_id, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;

  return CLSRGWIssueBucketBILogStop(index_pool.ioctx(), bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}


