// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>

#include "include/buffer.h"

class DoutPrefixProvider;
class RGWPeriod;
class RGWRealm;
class RGWRESTConn;
class JSONParser;
class req_info;
namespace ceph { class Formatter; }
namespace rgw { class SiteConfig; }
namespace rgw::sal { class ConfigStore; class Driver; class RealmWriter; }

int rgw_admin_send_to_remote_or_url(RGWRESTConn *conn, const std::string& url,
                                    std::optional<std::string> opt_region,
                                    const std::string& access, const std::string& secret,
                                    req_info& info, bufferlist& in_data,
                                    JSONParser& parser,
                                    const DoutPrefixProvider* dpp,
                                    rgw::sal::Driver* driver);

int rgw_admin_commit_period(rgw::sal::ConfigStore* cfgstore,
                            RGWRealm& realm, rgw::sal::RealmWriter& realm_writer,
                            RGWPeriod& period, std::string remote, const std::string& url,
                            std::optional<std::string> opt_region,
                            const std::string& access, const std::string& secret,
                            bool force, rgw::SiteConfig* site,
                            const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver);

int rgw_admin_update_period(rgw::sal::ConfigStore* cfgstore,
                            const std::string& realm_id, const std::string& realm_name,
                            const std::string& period_epoch, bool commit,
                            const std::string& remote, const std::string& url,
                            std::optional<std::string> opt_region,
                            const std::string& access, const std::string& secret,
                            ceph::Formatter *formatter, bool force, rgw::SiteConfig* site,
                            const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver);

int rgw_admin_do_period_pull(rgw::sal::ConfigStore* cfgstore,
                             RGWRESTConn *remote_conn, const std::string& url,
                             std::optional<std::string> opt_region,
                             const std::string& access_key, const std::string& secret_key,
                             const std::string& realm_id, const std::string& realm_name,
                             const std::string& period_id, const std::string& period_epoch,
                             RGWPeriod *period,
                             const DoutPrefixProvider* dpp,
                             rgw::sal::Driver* driver);
