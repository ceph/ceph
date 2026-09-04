// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/mfa.h"

#include <iostream>
#include <string>
#include <vector>

extern "C" {
#include <liboath/oath.h>
}

#include "common/ceph_json.h"
#include "driver/rados/rgw_sal_rados.h"
#include "services/svc_cls.h"
#include "rgw_otp.h"
#include "rgw_rados.h"
#include "rgw_user.h"

using namespace rgw_admin;
using namespace std;

namespace {

static int scan_totp(const DoutPrefixProvider* dpp,
                     CephContext *cct, ceph::real_time& now, rados::cls::otp::otp_info_t& totp, vector<string>& pins,
                     time_t *pofs)
{
#define MAX_TOTP_SKEW_HOURS (24 * 7)
  time_t start_time = ceph::real_clock::to_time_t(now);
  time_t time_ofs = 0, time_ofs_abs = 0;
  time_t step_size = totp.step_size;
  if (step_size == 0) {
    step_size = OATH_TOTP_DEFAULT_TIME_STEP_SIZE;
  }
  uint32_t count = 0;
  int sign = 1;

  uint32_t max_skew = MAX_TOTP_SKEW_HOURS * 3600;

  while (time_ofs_abs < max_skew) {
    // coverity supression: oath_totp_validate2 is an external library function, cannot fix internally
    // Further, step_size is a small number and unlikely to overflow
    int rc = oath_totp_validate2(totp.seed_bin.c_str(), totp.seed_bin.length(),
                             start_time, 
                             // coverity[store_truncates_time_t:SUPPRESS]
                             step_size,
                             time_ofs,
                             1,
                             nullptr,
                             pins[0].c_str());
    if (rc != OATH_INVALID_OTP) {
      rc = oath_totp_validate2(totp.seed_bin.c_str(), totp.seed_bin.length(),
                               start_time, 
                               // coverity[store_truncates_time_t:SUPPRESS]
                               step_size,
                               time_ofs - step_size, /* smaller time_ofs moves time forward */
                               1,
                               nullptr,
                               pins[1].c_str());
      if (rc != OATH_INVALID_OTP) {
        *pofs = time_ofs - step_size + step_size * totp.window / 2;
        ldpp_dout(dpp, 20) << "found at time=" << start_time - time_ofs << " time_ofs=" << time_ofs << dendl;
        return 0;
      }
    }
    sign = -sign;
    time_ofs_abs = (++count) * step_size;
    time_ofs = sign * time_ofs_abs;
  }

  return -ENOENT;
}


} // anonymous namespace

int rgw_admin_mfa(const DoutPrefixProvider* dpp,
                  rgw::sal::Driver* driver,
                  ceph::Formatter* formatter,
                  RGWUser& ruser,
                  RGWUserAdminOpState& user_op,
                  std::unique_ptr<rgw::sal::User>& user,
                  rgw_admin_mfa_options& opts)
{
  auto& command = opts.command;
  auto& totp_serial = opts.totp_serial;
  auto& totp_seed = opts.totp_seed;
  auto& totp_seed_type = opts.totp_seed_type;
  auto& totp_pin = opts.totp_pin;
  RGWObjVersionTracker* objv_tracker = opts.objv_tracker;
  int totp_seconds = opts.totp_seconds;
  int totp_window = opts.totp_window;

  if (command == OPT::MFA_CREATE) {
    rados::cls::otp::otp_info_t config;

    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (totp_seed.empty()) {
      cerr << "ERROR: TOTP device seed was not provided (via --totp-seed)" << std::endl;
      return EINVAL;
    }


    rados::cls::otp::SeedType seed_type;
    if (totp_seed_type == "hex") {
      seed_type = rados::cls::otp::OTP_SEED_HEX;
    } else if (totp_seed_type == "base32") {
      seed_type = rados::cls::otp::OTP_SEED_BASE32;
    } else {
      cerr << "ERROR: invalid seed type: " << totp_seed_type << std::endl;
      return EINVAL;
    }

    config.id = totp_serial;
    config.seed = totp_seed;
    config.seed_type = seed_type;

    if (totp_seconds > 0) {
      config.step_size = totp_seconds;
    }

    if (totp_window > 0) {
      config.window = totp_window;
    }

    real_time mtime = real_clock::now();

    int ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->mutate(
        rgwrados::otp::get_meta_key(user->get_id()),
        mtime, objv_tracker,
        null_yield, dpp,
        MDLOG_STATUS_WRITE,
        [&] {
      return static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.create_mfa(dpp, user->get_id(), config, objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA creation failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    
    RGWUserInfo& user_info = user_op.get_user_info();
    user_info.mfa_ids.insert(totp_serial);
    user_op.set_mfa_ids(user_info.mfa_ids);
    string err;
    ret = ruser.modify(dpp, user_op, null_yield, &err);
    if (ret < 0) {
      cerr << "ERROR: failed storing user info, error: " << err << std::endl;
      return -ret;
    }
  }

  if (command == OPT::MFA_REMOVE) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    real_time mtime = real_clock::now();

    int ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->mutate(
        rgwrados::otp::get_meta_key(user->get_id()),
        mtime, objv_tracker,
        null_yield, dpp,
        MDLOG_STATUS_WRITE,
        [&] {
      return static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.remove_mfa(dpp, user->get_id(), totp_serial, objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA removal failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWUserInfo& user_info = user_op.get_user_info();
    user_info.mfa_ids.erase(totp_serial);
    user_op.set_mfa_ids(user_info.mfa_ids);
    string err;
    ret = ruser.modify(dpp, user_op, null_yield, &err);
    if (ret < 0) {
      cerr << "ERROR: failed storing user info, error: " << err << std::endl;
      return -ret;
    }
  }

  if (command == OPT::MFA_GET) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    rados::cls::otp::otp_info_t result;
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.get_mfa(dpp, user->get_id(), totp_serial, &result, null_yield);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("entry", result, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::MFA_LIST) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    list<rados::cls::otp::otp_info_t> result;
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.list_mfa(dpp, user->get_id(), &result, null_yield);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "MFA listing failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("entries", result, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::MFA_CHECK) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (totp_pin.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-pin)" << std::endl;
      return EINVAL;
    }

    list<rados::cls::otp::otp_info_t> result;
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.check_mfa(dpp, user->get_id(), totp_serial, totp_pin.front(), null_yield);
    if (ret < 0) {
      cerr << "MFA check failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    cout << "ok" << std::endl;
  }

  if (command == OPT::MFA_RESYNC) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (totp_pin.size() != 2) {
      cerr << "ERROR: missing two --totp-pin params (--totp-pin=<first> --totp-pin=<second>)" << std::endl;
      return EINVAL;
    }

    rados::cls::otp::otp_info_t config;
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.get_mfa(dpp, user->get_id(), totp_serial, &config, null_yield);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    ceph::real_time now;

    ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.otp_get_current_time(dpp, user->get_id(), &now, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed to fetch current time from osd: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    time_t time_ofs;

    ret = scan_totp(dpp, driver->ctx(), now, config, totp_pin, &time_ofs);
    if (ret < 0) {
      if (ret == -ENOENT) {
        cerr << "failed to resync, TOTP values not found in range" << std::endl;
      } else {
        cerr << "ERROR: failed to scan for TOTP values: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    // time offset is a small number and unlikely to overflow
    // coverity[store_truncates_time_t:SUPPRESS]
    config.time_ofs = time_ofs;

    /* now update the backend */
    real_time mtime = real_clock::now();

    ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->mutate(
        rgwrados::otp::get_meta_key(user->get_id()),
        mtime, objv_tracker,
        null_yield, dpp,
        MDLOG_STATUS_WRITE,
        [&] {
      return static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.create_mfa(dpp, user->get_id(), config, objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA update failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

 }

  return 0;
}
