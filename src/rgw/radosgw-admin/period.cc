// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/period.h"
#include <iostream>
#include <map>
#include <optional>
#include <string>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "global/global_context.h"
#include "radosgw-admin/admin_io.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_zone.h"

using ceph::Formatter;
using namespace rgw_admin;
using namespace std;

#include "compressor/Compressor.h"
#include "driver/rados/rgw_sal_rados.h"
#include "radosgw-admin/admin_remote.h"
#include "radosgw-admin/quota_ratelimit.h"
#include "rgw_http_client.h"
#include "rgw_http_errors.h"
#include "rgw_rest_client.h"
#include "rgw_rest_conn.h"
#include "services/svc_sync_modules.h"

int rgw_admin_period(const DoutPrefixProvider* dpp,
                rgw::sal::Driver* driver,
                rgw::sal::ConfigStore* cfgstore,
                rgw::SiteConfig& site,
                Formatter* formatter,
                rgw_admin_period_options& o)
{
  switch (o.command) {
    case OPT::PERIOD_DELETE:
      {
	if (o.period_id.empty()) {
	  cerr << "missing period id" << std::endl;
	  return EINVAL;
	}
        int ret = cfgstore->delete_period(dpp, null_yield, o.period_id);
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete period: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT::PERIOD_GET:
      {
        std::optional<epoch_t> epoch;
	if (!o.period_epoch.empty()) {
	  epoch = atoi(o.period_epoch.c_str());
	}
        if (o.staging) {
          RGWRealm realm;
          int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                    o.realm_id, o.realm_name, realm);
          if (ret < 0 ) {
            cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          o.realm_id = realm.get_id();
          o.realm_name = realm.get_name();
          o.period_id = RGWPeriod::get_staging_id(o.realm_id);
          epoch = 1;
        }
        if (o.period_id.empty()) {
          // use realm's current period
          RGWRealm realm;
          int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                    o.realm_id, o.realm_name, realm);
          if (ret < 0 ) {
            cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          o.period_id = realm.current_period;
        }

	RGWPeriod period;
        int ret = cfgstore->read_period(dpp, null_yield, o.period_id,
                                        epoch, period);
	if (ret < 0) {
	  cerr << "failed to load period: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("period", period, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::PERIOD_GET_CURRENT:
      {
        RGWRealm realm;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  o.realm_id, o.realm_name, realm);
	if (ret < 0) {
          std::cerr << "failed to load realm: " << cpp_strerror(ret) << std::endl;
	  return -ret;
	}

	formatter->open_object_section("period_get_current");
	encode_json("current_period", realm.current_period, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT::PERIOD_LIST:
      {
        Formatter::ObjectSection periods_list{*formatter, "periods_list"};
        Formatter::ArraySection periods{*formatter, "periods"};
        rgw::sal::ListResult<std::string> listing;
        std::array<std::string, 1000> period_ids; // list in pages of 1000
        do {
          int ret = cfgstore->list_period_ids(dpp, null_yield, listing.next,
                                              period_ids, listing);
          if (ret < 0) {
            std::cerr << "failed to list periods: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& id : listing.entries) {
            encode_json("id", id, formatter);
          }
        } while (!listing.next.empty());
      } // close sections periods and periods_list
      formatter->flush(cout);
      break;
    case OPT::PERIOD_UPDATE:
      {
        int ret = rgw_admin_update_period(cfgstore, o.realm_id, o.realm_name,
                                o.period_epoch, o.commit, o.remote, o.url,
                                o.opt_region, o.access_key, o.secret_key,
                                formatter, o.yes_i_really_mean_it, &site, dpp, driver);
	if (ret < 0) {
	  return -ret;
	}
      }
      break;
    case OPT::PERIOD_PULL:
      {
        if (o.url.empty()) {
          cerr << "A --url must be provided." << std::endl;
          return EINVAL;
        }
        // load realm for current period
        RGWRealm realm;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  o.realm_id, o.realm_name, realm);
        if (ret < 0 ) {
          cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        o.period_id = realm.current_period;

        RGWPeriod period;
        ret = rgw_admin_do_period_pull(cfgstore, nullptr, o.url,
                                 o.opt_region, o.access_key, o.secret_key,
                                 o.realm_id, o.realm_name, o.period_id, o.period_epoch,
                                 &period, dpp, driver);
        if (ret < 0) {
          cerr << "period pull failed: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("period", period, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT::GLOBAL_RATELIMIT_GET:
    case OPT::GLOBAL_RATELIMIT_SET:
    case OPT::GLOBAL_RATELIMIT_ENABLE:
    case OPT::GLOBAL_RATELIMIT_DISABLE:
      {
        if (o.realm_id.empty()) {
          if (!o.realm_name.empty()) {
            // look up o.realm_id for the given o.realm_name
            int ret = cfgstore->read_realm_id(dpp, null_yield,
                                              o.realm_name, o.realm_id);
            if (ret < 0) {
              cerr << "ERROR: failed to read realm for " << o.realm_name
                  << ": " << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          } else {
            // use default o.realm_id when none is given
            int ret = cfgstore->read_default_realm_id(dpp, null_yield,
                                                      o.realm_id);
            if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty o.realm_id
              cerr << "ERROR: failed to read default realm: "
                  << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          }
        }

        RGWPeriodConfig period_config;
        int ret = cfgstore->read_period_config(dpp, null_yield, o.realm_id,
                                               period_config);
        if (ret < 0 && ret != -ENOENT) {
          cerr << "ERROR: failed to read period config: "
              << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        bool ratelimit_configured = true;
        formatter->open_object_section("period_config");
        if (o.ratelimit_scope == "bucket") {
          ratelimit_configured = set_ratelimit_info(period_config.bucket_ratelimit, o.command,
                         o.max_read_ops, o.max_write_ops, o.max_list_ops, o.max_delete_ops,
                         o.max_read_bytes, o.max_write_bytes,
                         o.have_max_read_ops, o.have_max_write_ops, o.have_max_list_ops,
                         o.have_max_delete_ops, o.have_max_read_bytes, o.have_max_write_bytes);
          encode_json("bucket_ratelimit", period_config.bucket_ratelimit, formatter);
        } else if (o.ratelimit_scope == "user") {
          ratelimit_configured = set_ratelimit_info(period_config.user_ratelimit, o.command,
                         o.max_read_ops, o.max_write_ops, o.max_list_ops, o.max_delete_ops,
                         o.max_read_bytes, o.max_write_bytes,
                         o.have_max_read_ops, o.have_max_write_ops, o.have_max_list_ops,
                         o.have_max_delete_ops, o.have_max_read_bytes, o.have_max_write_bytes);
          encode_json("user_ratelimit", period_config.user_ratelimit, formatter);
        } else if (o.ratelimit_scope == "anonymous") {
          ratelimit_configured = set_ratelimit_info(period_config.anon_ratelimit, o.command,
                         o.max_read_ops, o.max_write_ops, o.max_list_ops,o.max_delete_ops,
                         o.max_read_bytes, o.max_write_bytes,
                         o.have_max_read_ops, o.have_max_write_ops, o.have_max_list_ops,
                         o.have_max_delete_ops, o.have_max_read_bytes, o.have_max_write_bytes);
          encode_json("anonymous_ratelimit", period_config.anon_ratelimit, formatter);
        } else if (o.ratelimit_scope.empty() && o.command == OPT::GLOBAL_RATELIMIT_GET) {
          // if no scope is given for GET, print both
          encode_json("bucket_ratelimit", period_config.bucket_ratelimit, formatter);
          encode_json("user_ratelimit", period_config.user_ratelimit, formatter);
          encode_json("anonymous_ratelimit", period_config.anon_ratelimit, formatter);
        } else {
          cerr << "ERROR: invalid rate limit scope specification. Please specify "
              "either --ratelimit-scope=bucket, or --ratelimit-scope=user or --ratelimit-scope=anonymous" << std::endl;
          return EINVAL;
        }
        if (!ratelimit_configured) {
          cerr << "ERROR: no rate limit values have been specified" << std::endl;
          return EINVAL;
        }

        formatter->close_section();

        if (o.command != OPT::GLOBAL_RATELIMIT_GET) {
          // write the modified period config
          constexpr bool exclusive = false;
          ret = cfgstore->write_period_config(dpp, null_yield, exclusive,
                                              o.realm_id, period_config);
          if (ret < 0) {
            cerr << "ERROR: failed to write period config: "
                << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          if (!o.realm_id.empty()) {
            cout << "Global ratelimit changes saved. Use 'period update' to apply "
                "them to the staging period, and 'period commit' to commit the "
                "new period." << std::endl;
          } else {
            cout << "Global ratelimit changes saved. They will take effect as "
                "the gateways are restarted." << std::endl;
          }
        }

        formatter->flush(cout);
      }
      break;
    case OPT::GLOBAL_QUOTA_GET:
    case OPT::GLOBAL_QUOTA_SET:
    case OPT::GLOBAL_QUOTA_ENABLE:
    case OPT::GLOBAL_QUOTA_DISABLE:
      {
        if (o.realm_id.empty()) {
          if (!o.realm_name.empty()) {
            // look up o.realm_id for the given o.realm_name
            int ret = cfgstore->read_realm_id(dpp, null_yield,
                                              o.realm_name, o.realm_id);
            if (ret < 0) {
              cerr << "ERROR: failed to read realm for " << o.realm_name
                  << ": " << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          } else {
            // use default o.realm_id when none is given
            int ret = cfgstore->read_default_realm_id(dpp, null_yield,
                                                      o.realm_id);
            if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty o.realm_id
              cerr << "ERROR: failed to read default realm: "
                  << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          }
        }

        RGWPeriodConfig period_config;
        int ret = cfgstore->read_period_config(dpp, null_yield, o.realm_id,
                                               period_config);
        if (ret < 0 && ret != -ENOENT) {
          cerr << "ERROR: failed to read period config: "
              << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        formatter->open_object_section("period_config");
        if (o.quota_scope == "bucket") {
          set_quota_info(period_config.quota.bucket_quota, o.command,
                         o.max_size, o.max_objects,
                         o.have_max_size, o.have_max_objects);
          encode_json("bucket quota", period_config.quota.bucket_quota, formatter);
        } else if (o.quota_scope == "user") {
          set_quota_info(period_config.quota.user_quota, o.command,
                         o.max_size, o.max_objects,
                         o.have_max_size, o.have_max_objects);
          encode_json("user quota", period_config.quota.user_quota, formatter);
        } else if (o.quota_scope.empty() && o.command == OPT::GLOBAL_QUOTA_GET) {
          // if no scope is given for GET, print both
          encode_json("bucket quota", period_config.quota.bucket_quota, formatter);
          encode_json("user quota", period_config.quota.user_quota, formatter);
        } else {
          cerr << "ERROR: invalid quota scope specification. Please specify "
              "either --quota-scope=bucket or --quota-scope=user" << std::endl;
          return EINVAL;
        }
        formatter->close_section();

        if (o.command != OPT::GLOBAL_QUOTA_GET) {
          // write the modified period config
          constexpr bool exclusive = false;
          ret = cfgstore->write_period_config(dpp, null_yield, exclusive,
                                              o.realm_id, period_config);
          if (ret < 0) {
            cerr << "ERROR: failed to write period config: "
                << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          if (!o.realm_id.empty()) {
            cout << "Global quota changes saved. Use 'period update' to apply "
                "them to the staging period, and 'period commit' to commit the "
                "new period." << std::endl;
          } else {
            cout << "Global quota changes saved. They will take effect as "
                "the gateways are restarted." << std::endl;
          }
        }

        formatter->flush(cout);
      }
      break;
  case OPT::PERIOD_PUSH:
    {
      RGWEnv env;
      req_info info(g_ceph_context, &env);
      info.method = "POST";
      info.request_uri = "/admin/realm/period";

      map<string, string> &params = info.args.get_params();
      if (!o.realm_id.empty())
        params["realm_id"] = o.realm_id;
      if (!o.realm_name.empty())
        params["realm_name"] = o.realm_name;
      if (!o.period_id.empty())
        params["period_id"] = o.period_id;
      if (!o.period_epoch.empty())
        params["epoch"] = o.period_epoch;

      // load the period
      RGWPeriod period;
      int ret = cfgstore->read_period(dpp, null_yield, o.period_id,
                                      std::nullopt, period);
      if (ret < 0) {
        cerr << "failed to load period: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      // json format into a bufferlist
      JSONFormatter jf(false);
      encode_json("period", period, &jf);
      bufferlist bl;
      jf.flush(bl);

      JSONParser p;
      ret = rgw_admin_send_to_remote_or_url(nullptr, o.url, o.opt_region,
                                  o.access_key, o.secret_key,
                                  info, bl, p, dpp, driver);
      if (ret < 0) {
        cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
    return 0;
  case OPT::PERIOD_COMMIT:
    {
      // read realm and o.staging period
      RGWRealm realm;
      std::unique_ptr<rgw::sal::RealmWriter> realm_writer;
      int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                o.realm_id, o.realm_name,
                                realm, &realm_writer);
      if (ret < 0) {
        cerr << "Error initializing realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      o.period_id = rgw::get_staging_period_id(realm.id);
      epoch_t epoch = 1;

      RGWPeriod period;
      ret = cfgstore->read_period(dpp, null_yield, o.period_id, epoch, period);
      if (ret < 0) {
        cerr << "failed to load period: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      ret = rgw_admin_commit_period(cfgstore, realm, *realm_writer, period,
                          o.remote, o.url, o.opt_region, o.access_key, o.secret_key,
                          o.yes_i_really_mean_it, &site, dpp, driver);
      if (ret < 0) {
        cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      encode_json("period", period, formatter);
      formatter->flush(cout);
    }
    return 0;

  default:
    return -EINVAL;
  }
  return 0;
}
