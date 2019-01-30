// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/expected.h"

#include "svc_cls.h"
#include "svc_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_error_code.h"
#include "rgw/rgw_zone.h"

#include "RADOS/cls/otp.h"
#include "RADOS/cls/log.h"

namespace bs = boost::system;
namespace R = RADOS;
namespace RCO = RADOS::CLS::OTP;
namespace RClg = RADOS::CLS::log;

using namespace std::literals;

#define dout_subsys ceph_subsys_rgw

bs::error_code RGWSI_Cls::do_start()
{
  auto ec =(mfa.do_start());
  if (ec) {
    ldout(cct, 0) << "ERROR: failed to start mfa service" << dendl;
  }
  return ec;
}

tl::expected<RGWSI_RADOS::Obj, bs::error_code>
RGWSI_Cls::MFA::get_mfa_obj(const rgw_user& user,
                            optional_yield y)
{
  auto oid = get_mfa_oid(user);
  rgw_raw_obj o(zone_svc->get_zone_params().otp_pool, oid);

  return rados_svc->obj(o, y);
}

bs::error_code
RGWSI_Cls::MFA::check_mfa(const rgw_user& user, std::string_view otp_id,
                          std::string_view pin, optional_yield y)
{
  auto obj = TRYE(get_mfa_obj(user, y));

  auto token = RCO::random_token(cct);
  auto op1 = RCO::check(otp_id, pin, token);
  auto ec = obj.operate(std::move(op1), nullptr, y);
  if (ec)
    return ec;

  rados::cls::otp::otp_check_t res;
  auto op2 = RCO::get_result(token, &res);
  ec = obj.operate(std::move(op1), nullptr, y);
  if (ec)
    return ec;
  ldout(cct, 20) << "OTP check, otp_id=" << otp_id << " result="
                 << (int)res.result << dendl;

  if (res.result == rados::cls::otp::OTP_CHECK_SUCCESS)
    return bs::error_code();

  return rgw_errc::mfa_failed;
}

void RGWSI_Cls::MFA::prepare_mfa_write(R::WriteOp& op,
                                       RGWObjVersionTracker *objv_tracker,
                                       ceph::real_time mtime)
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
  op.set_mtime(mtime);
}

bs::error_code
RGWSI_Cls::MFA::create_mfa(const rgw_user& user,
                           const rados::cls::otp::otp_info_t& config,
                           RGWObjVersionTracker *objv_tracker,
                           ceph::real_time mtime, optional_yield y)
{
  auto obj = TRYE(get_mfa_obj(user, y));

  R::WriteOp op;
  prepare_mfa_write(op, objv_tracker, mtime);
  RCO::create(op, config);
  auto ec = obj.operate(std::move(op), y);
  if (ec) {
    ldout(cct, 20) << "OTP create, otp_id=" << config.id << " result="
                   << ec << dendl;
  }

  return ec;
}

bs::error_code
RGWSI_Cls::MFA::remove_mfa(const rgw_user& user, const string& id,
                           RGWObjVersionTracker *objv_tracker,
                           ceph::real_time mtime,
                           optional_yield y)
{
  auto obj = TRYE(get_mfa_obj(user, y));

  R::WriteOp op;
  prepare_mfa_write(op, objv_tracker, mtime);
  RCO::remove(op, id);
  auto ec = obj.operate(std::move(op), y);
  if (ec) {
    ldout(cct, 20) << "OTP remove, otp_id=" << id << " result="
                   << ec << dendl;
  }

  return ec;
}

tl::expected<rados::cls::otp::otp_info_t,
             bs::error_code>
RGWSI_Cls::MFA::get_mfa(const rgw_user& user, std::string_view id,
                        optional_yield y)
{
  auto obj = TRY(get_mfa_obj(user, y));
  std::vector ids{ std::string(id) };
  std::vector<rados::cls::otp::otp_info_t> res;
  auto op = RCO::get(&ids, false, &res);
  auto ec = obj.operate(std::move(op), nullptr, y);
  if (ec)
    return tl::unexpected(ec);
  if (res.empty()) {
    return tl::unexpected(bs::errc::make_error_code(
			    bs::errc::no_such_file_or_directory));
  }
  return res[0];
}

tl::expected<std::vector<rados::cls::otp::otp_info_t>,
             bs::error_code>
RGWSI_Cls::MFA::list_mfa(const rgw_user& user, optional_yield y)
{
  auto obj = TRY(get_mfa_obj(user, y));
  std::vector<rados::cls::otp::otp_info_t> res;
  auto op = RCO::get(nullptr, true, &res);
  auto ec = obj.operate(std::move(op), nullptr, y);
  if (ec)
    return tl::unexpected(ec);
  return res;
}

tl::expected<ceph::real_time, bs::error_code>
RGWSI_Cls::MFA::otp_get_current_time(const rgw_user& user, optional_yield y)
{
  auto obj = TRY(get_mfa_obj(user, y));
  ceph::real_time t;
  auto op = RCO::get_current_time(&t);
  auto ec = obj.operate(std::move(op), nullptr, y);
  if (ec)
    return tl::unexpected(ec);
  return t;
}

bs::error_code
RGWSI_Cls::MFA::set_mfa(string_view oid,
                        const std::vector<rados::cls::otp::otp_info_t>& entries,
                        bool reset_obj, RGWObjVersionTracker *objv_tracker,
                        const real_time& mtime, optional_yield y)
{
  auto pool = TRYE(rados_svc->pool(zone_svc->get_zone_params().otp_pool, y));
  auto obj = rados_svc->obj(pool, oid, {});

  R::WriteOp op;
  if (reset_obj) {
    op.remove();
    op.set_failok();
    op.create(false);
  }
  prepare_mfa_write(op, objv_tracker, mtime);
  RCO::set(op, entries);
  return obj.operate(std::move(op), y);
}

tl::expected<std::pair<std::vector<rados::cls::otp::otp_info_t>,
                       ceph::real_time>, bs::error_code>
RGWSI_Cls::MFA::list_mfa(std::string_view oid, RGWObjVersionTracker *objv_tracker,
                         optional_yield y)
{
  auto pool = TRY(rados_svc->pool(zone_svc->get_zone_params().otp_pool, y));
  auto obj = rados_svc->obj(pool, oid, {});

  R::ReadOp op;
  ceph::real_time mtime;
  objv_tracker->prepare_op_for_read(op);
  op.stat(nullptr, &mtime);
  std::vector<rados::cls::otp::otp_info_t> res;
  RCO::get(op, nullptr, true, &res);
  auto ec = obj.operate(std::move(op), nullptr, y);
  if (ec)
    return tl::unexpected(ec);

  return std::make_pair(std::move(res), mtime);
}

tl::expected<RGWSI_RADOS::Obj, bs::error_code>
RGWSI_Cls::TimeLog::init_obj(std::string_view oid, optional_yield y)
{
  auto pool = TRY(rados_svc->pool(zone_svc->get_zone_params().log_pool, y));
  auto obj = rados_svc->obj(pool, oid, {});
  return obj;
}

bs::error_code
RGWSI_Cls::TimeLog::add(std::string_view oid, ceph::real_time t,
                        std::string_view section, std::string_view key,
                        bufferlist&& bl, optional_yield y)
{
  auto obj = TRYE(init_obj(oid, y));
  R::WriteOp op;
  RClg::add(op, t, section, key, std::move(bl));

  return obj.operate(std::move(op), y);
}

bs::error_code
RGWSI_Cls::TimeLog::add(std::string_view oid,
                        std::vector<cls_log_entry>&& entries,
                        bool monotonic_inc, optional_yield y)
{
  auto obj = TRYE(init_obj(oid, y));
  R::WriteOp op;
  RClg::add(op, std::move(entries), monotonic_inc);
  return obj.operate(std::move(op), y);
}

tl::expected<std::tuple<std::vector<cls_log_entry>, std::string, bool>,
             bs::error_code>
RGWSI_Cls::TimeLog::list(std::string_view oid, ceph::real_time start_time,
                         ceph::real_time end_time, int max_entries,
                         std::string_view marker, optional_yield y)
{
  auto obj = TRY(init_obj(oid, y));
  std::vector<cls_log_entry> l;
  std::string m;
  bool t;
  auto op = RClg::list(start_time, end_time, marker, max_entries,
		       &l, &m, &t);

  auto ec = obj.operate(std::move(op), nullptr, y);
  if (ec)
    return tl::unexpected(ec);
  return std::make_tuple(std::move(l), std::move(m), std::move(t));
}

tl::expected<cls_log_header, bs::error_code>
RGWSI_Cls::TimeLog::info(std::string_view oid, optional_yield y)
{
  auto obj = TRY(init_obj(oid, y));
  cls_log_header h;
  auto op = RClg::info(&h);
  auto ec = obj.operate(std::move(op), nullptr, y);
  if (ec)
    return tl::unexpected(ec);
  return h;
}


bs::error_code
RGWSI_Cls::TimeLog::trim(std::string_view oid, ceph::real_time start_time,
                         ceph::real_time end_time,
                         std::string_view from_marker,
                         std::string_view to_marker,
                         optional_yield y)
{
  auto obj = TRYE(init_obj(oid, y));
  bs::error_code ec;
  do {
    auto op = RClg::trim(start_time, end_time, from_marker, to_marker);
    ec = obj.operate(std::move(op), y);
  } while (!ec);
  if (ec == bs::errc::no_message_available)
    ec.clear();
  return ec;
}
