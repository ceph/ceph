// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "svc_cls.h"
#include "svc_zone.h"

#include "rgw_zone.h"

#include "cls/otp/cls_otp_client.h"
#include "cls/log/cls_log_client.h"
#include "cls/lock/cls_lock_client.h"


#define dout_subsys ceph_subsys_rgw

using namespace std;

static string log_lock_name = "rgw_log_lock";

int RGWSI_Cls::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  int r = mfa.do_start(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to start mfa service" << dendl;
    return r;
  }

  return 0;
}

int RGWSI_Cls::MFA::get_mfa_ref(const DoutPrefixProvider *dpp, const rgw_user& user, rgw_rados_ref *ref)
{
  string oid = get_mfa_oid(user);
  rgw_raw_obj o(cls->zone_svc->get_zone_params().otp_pool, oid);

  auto r = rgw_get_rados_ref(dpp, cls->rados, o, ref);
  if (r < 0) {
    ldpp_dout(dpp, 4) << "failed to open rados context for " << o << dendl;
    return r;
  }

  return 0;
}

int RGWSI_Cls::MFA::check_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const string& otp_id, const string& pin, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_mfa_ref(dpp, user, &ref);
  if (r < 0) {
    return r;
  }

  rados::cls::otp::otp_check_t result;

  r = rados::cls::otp::OTP::check(cct, ref.ioctx, ref.obj.oid, otp_id, pin, &result);
  if (r < 0)
    return r;

  ldpp_dout(dpp, 20) << "OTP check, otp_id=" << otp_id << " result=" << (int)result.result << dendl;

  return (result.result == rados::cls::otp::OTP_CHECK_SUCCESS ? 0 : -EACCES);
}

void RGWSI_Cls::MFA::prepare_mfa_write(librados::ObjectWriteOperation *op,
                                 RGWObjVersionTracker *objv_tracker,
                                 const ceph::real_time& mtime)
{
  RGWObjVersionTracker ot;

  if (objv_tracker) {
    ot = *objv_tracker;
  }

  if (ot.write_version.tag.empty()) {
    if (ot.read_version.tag.empty()) {
      ot.generate_new_write_ver(cct);
    } else {
      ot.write_version = ot.read_version;
      ot.write_version.ver++;
    }
  }

  ot.prepare_op_for_write(op);
  struct timespec mtime_ts = real_clock::to_timespec(mtime);
  op->mtime2(&mtime_ts);
}

int RGWSI_Cls::MFA::create_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const rados::cls::otp::otp_info_t& config,
                         RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime, optional_yield y)
{
  rgw_rados_ref obj;
  int r = get_mfa_ref(dpp, user, &obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::create(&op, config);
  r = obj.operate(dpp, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "OTP create, otp_id=" << config.id << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_Cls::MFA::remove_mfa(const DoutPrefixProvider *dpp, 
                         const rgw_user& user, const string& id,
                         RGWObjVersionTracker *objv_tracker,
                         const ceph::real_time& mtime,
                         optional_yield y)
{
  rgw_rados_ref obj;
  int r = get_mfa_ref(dpp, user, &obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::remove(&op, id);
  r = obj.operate(dpp, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "OTP remove, otp_id=" << id << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_Cls::MFA::get_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, const string& id, rados::cls::otp::otp_info_t *result,
			    optional_yield y)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(dpp, user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get(nullptr, ref.ioctx, ref.obj.oid, id, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_Cls::MFA::list_mfa(const DoutPrefixProvider *dpp, const rgw_user& user, list<rados::cls::otp::otp_info_t> *result,
			     optional_yield y)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(dpp, user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get_all(nullptr, ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_Cls::MFA::otp_get_current_time(const DoutPrefixProvider *dpp, const rgw_user& user, ceph::real_time *result,
					 optional_yield y)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(dpp, user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get_current_time(ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_Cls::MFA::set_mfa(const DoutPrefixProvider *dpp, const string& oid, const list<rados::cls::otp::otp_info_t>& entries,
			    bool reset_obj, RGWObjVersionTracker *objv_tracker,
			    const real_time& mtime,
			    optional_yield y)
{
  rgw_rados_ref obj;
  int r = rgw_get_rados_ref(dpp, cls->rados,
			    { cls->zone_svc->get_zone_params().otp_pool, oid },
			    &obj);
  if (r < 0) {
    ldpp_dout(dpp, 4) << "failed to open rados context for " << oid << dendl;
    return r;
  }
  librados::ObjectWriteOperation op;
  if (reset_obj) {
    op.remove();
    op.set_op_flags2(LIBRADOS_OP_FLAG_FAILOK);
    op.create(false);
  }
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::set(&op, entries);
  r = obj.operate(dpp, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "OTP set entries.size()=" << entries.size() << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_Cls::MFA::list_mfa(const DoutPrefixProvider *dpp, const string& oid, list<rados::cls::otp::otp_info_t> *result,
			     RGWObjVersionTracker *objv_tracker, ceph::real_time *pmtime,
			     optional_yield y)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, cls->rados,
			    { cls->zone_svc->get_zone_params().otp_pool, oid },
			    &ref);
  if (r < 0) {
    ldpp_dout(dpp, 4) << "failed to open rados context for " << oid << dendl;
    return r;
  }
  librados::ObjectReadOperation op;
  struct timespec mtime_ts;
  if (pmtime) {
    op.stat2(nullptr, &mtime_ts, nullptr);
  }
  objv_tracker->prepare_op_for_read(&op);
  r = rados::cls::otp::OTP::get_all(&op, ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }
  if (pmtime) {
    *pmtime = ceph::real_clock::from_timespec(mtime_ts);
  }

  return 0;
}

void RGWSI_Cls::TimeLog::prepare_entry(cls_log_entry& entry,
                                       const real_time& ut,
                                       const string& section,
                                       const string& key,
                                       bufferlist& bl)
{
  cls_log_add_prepare_entry(entry, ut, section, key, bl);
}

int RGWSI_Cls::TimeLog::init_obj(const DoutPrefixProvider *dpp, const string& oid, rgw_rados_ref& obj)
{
  rgw_raw_obj o(cls->zone_svc->get_zone_params().log_pool, oid);
  return rgw_get_rados_ref(dpp, cls->rados, o, &obj);
}

int RGWSI_Cls::TimeLog::add(const DoutPrefixProvider *dpp,
                            const string& oid,
                            const real_time& ut,
                            const string& section,
                            const string& key,
                            bufferlist& bl,
			    optional_yield y)
{
  rgw_rados_ref obj;
  int r = init_obj(dpp, oid, obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  cls_log_add(op, ut, section, key, bl);

  return obj.operate(dpp, &op, y);
}

int RGWSI_Cls::TimeLog::add(const DoutPrefixProvider *dpp,
                            const string& oid,
                            std::vector<cls_log_entry>& entries,
                            librados::AioCompletion *completion,
                            bool monotonic_inc,
                            optional_yield y)
{
  rgw_rados_ref obj;

  int r = init_obj(dpp, oid, obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  cls_log_add(op, entries, monotonic_inc);

  if (!completion) {
    r = obj.operate(dpp, &op, y);
  } else {
    r = obj.aio_operate(completion, &op);
  }
  return r;
}

int RGWSI_Cls::TimeLog::list(const DoutPrefixProvider *dpp, 
                             const string& oid,
                             const real_time& start_time,
                             const real_time& end_time,
                             int max_entries, std::vector<cls_log_entry>& entries,
                             const string& marker,
                             string *out_marker,
                             bool *truncated,
                             optional_yield y)
{
  rgw_rados_ref obj;

  int r = init_obj(dpp, oid, obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;

  cls_log_list(op, start_time, end_time, marker, max_entries, entries,
	       out_marker, truncated);

  bufferlist obl;

  int ret = obj.operate(dpp, &op, &obl, y);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSI_Cls::TimeLog::info(const DoutPrefixProvider *dpp, 
                             const string& oid,
                             cls_log_header *header,
                             optional_yield y)
{
  rgw_rados_ref obj;

  int r = init_obj(dpp, oid, obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;

  cls_log_info(op, header);

  bufferlist obl;

  int ret = obj.operate(dpp, &op, &obl, y);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSI_Cls::TimeLog::info_async(const DoutPrefixProvider *dpp,
                                   rgw_rados_ref& obj,
                                   const string& oid,
                                   cls_log_header *header,
                                   librados::AioCompletion *completion)
{
  int r = init_obj(dpp, oid, obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;

  cls_log_info(op, header);

  int ret = obj.aio_operate(completion, &op, nullptr);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSI_Cls::TimeLog::trim(const DoutPrefixProvider *dpp, 
                             const string& oid,
                             const real_time& start_time,
                             const real_time& end_time,
                             const string& from_marker,
                             const string& to_marker,
                             librados::AioCompletion *completion,
                             optional_yield y)
{
  rgw_rados_ref obj;

  int r = init_obj(dpp, oid, obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  cls_log_trim(op, start_time, end_time, from_marker, to_marker);

  if (!completion) {
    r = obj.operate(dpp, &op, y);
  } else {
    r = obj.aio_operate(completion, &op);
  }
  return r;
}

int RGWSI_Cls::Lock::lock_exclusive(const DoutPrefixProvider *dpp,
                                    const rgw_pool& pool,
                                    const string& oid,
                                    timespan& duration,
                                    string& zone_id,
                                    string& owner_id,
                                    std::optional<string> lock_name)
{

  librados::IoCtx p;
  int r = rgw_init_ioctx(dpp, cls->rados, pool, p, true, false);
  if (r < 0) {
    return r;
  }

  uint64_t msec = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  utime_t ut(msec / 1000, msec % 1000);

  rados::cls::lock::Lock l(lock_name.value_or(log_lock_name));
  l.set_duration(ut);
  l.set_cookie(owner_id);
  l.set_tag(zone_id);
  l.set_may_renew(true);

  return l.lock_exclusive(&p, oid);
}

int RGWSI_Cls::Lock::unlock(const DoutPrefixProvider *dpp,
                            const rgw_pool& pool,
                            const string& oid,
                            string& zone_id,
                            string& owner_id,
                            std::optional<string> lock_name)
{
  librados::IoCtx p;
  int r = rgw_init_ioctx(dpp, cls->rados, pool, p, true, false);
  if (r < 0) {
    return r;
  }

  rados::cls::lock::Lock l(lock_name.value_or(log_lock_name));
  l.set_tag(zone_id);
  l.set_cookie(owner_id);

  return l.unlock(&p, oid);
}
