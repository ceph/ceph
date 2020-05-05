// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_datalog_rados.h"
#include "svc_zone.h"
#include "svc_cls.h"

#include "rgw/rgw_bucket.h"


#define dout_subsys ceph_subsys_rgw

RGWSI_DataLog_RADOS::RGWSI_DataLog_RADOS(CephContext *cct) : RGWServiceInstance(cct) {
}

RGWSI_DataLog_RADOS::~RGWSI_DataLog_RADOS() {
}

int RGWSI_DataLog_RADOS::init(RGWSI_Zone *_zone_svc, RGWSI_Cls *_cls_svc,
			      R::RADOS* r)
{
  svc.zone = _zone_svc;
  svc.cls = _cls_svc;
  rados = r;

  return 0;
}

int RGWSI_DataLog_RADOS::do_start()
{
  log.reset(new RGWDataChangesLog(svc.zone, svc.cls, *rados));

  return 0;
}

void RGWSI_DataLog_RADOS::shutdown()
{
  log.reset();
}

void RGWSI_DataLog_RADOS::set_observer(rgw::BucketChangeObserver *observer)
{
  log->set_observer(observer);
}

int RGWSI_DataLog_RADOS::get_log_shard_id(rgw_bucket& bucket, int shard_id)
{
  return log->get_log_shard_id(bucket, shard_id);
}

const std::string& RGWSI_DataLog_RADOS::get_oid(int shard_id) const
{
  return log->get_oid(shard_id);
}

int RGWSI_DataLog_RADOS::get_info(int shard_id, RGWDataChangesLogInfo *info)
{
  return log->get_info(shard_id, info);
}

int RGWSI_DataLog_RADOS::add_entry(const RGWBucketInfo& bucket_info, int shard_id)
{
  return log->add_entry(bucket_info, shard_id);
}

int RGWSI_DataLog_RADOS::list_entries(int shard, const real_time& start_time, const real_time& end_time, int max_entries,
                 list<rgw_data_change_log_entry>& entries,
                 const string& marker,
                 string *out_marker,
                 bool *truncated)
{
  return log->list_entries(shard, start_time, end_time, max_entries,
                           entries, marker, out_marker, truncated);
}

int RGWSI_DataLog_RADOS::list_entries(const real_time& start_time, const real_time& end_time, int max_entries,
				      list<rgw_data_change_log_entry>& entries, RGWDataChangesLogMarker& marker, bool *ptruncated)
{
  return log->list_entries(start_time, end_time, max_entries,
			   entries, marker, ptruncated);
}

int RGWSI_DataLog_RADOS::trim_entries(int shard_id, const real_time& start_time, const real_time& end_time,
                                      const string& start_marker, const string& end_marker)
{
  return log->trim_entries(shard_id, start_time, end_time, start_marker, end_marker);
}
