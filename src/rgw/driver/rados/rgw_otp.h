// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>
#include <string>

struct rgw_user;
class RGWMetadataHandler;
class RGWSI_Cls;
class RGWSI_MDLog;
class RGWSI_SysObj;
class RGWZoneParams;

namespace rgwrados::otp {

// return the user's otp metadata key
std::string get_meta_key(const rgw_user& user);

// otp metadata handler factory
auto create_metadata_handler(RGWSI_SysObj& sysobj, RGWSI_Cls& cls,
                             RGWSI_MDLog& mdlog, const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>;

} // namespace rgwrados::otp
