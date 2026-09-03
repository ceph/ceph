// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/realm.h"
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

#include "radosgw-admin/admin_remote.h"
#include "rgw_http_client.h"
#include "rgw_http_errors.h"
#include "rgw_rest_client.h"
#include "rgw_rest_conn.h"

int rgw_admin_realm(const DoutPrefixProvider* dpp,
                rgw::sal::Driver* driver,
                rgw::sal::ConfigStore* cfgstore,
                rgw::SiteConfig& site,
                Formatter* formatter,
                rgw_admin_realm_options& opts)
{
  switch (opts.command) {
    case OPT::REALM_CREATE:
      {
	if (opts.realm_name.empty()) {
	  cerr << "missing realm name" << std::endl;
	  return EINVAL;
	}

	RGWRealm realm;
        realm.name = opts.realm_name;

        constexpr bool exclusive = true;
	int ret = rgw::create_realm(dpp, null_yield, cfgstore,
                                    exclusive, realm);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create realm " << opts.realm_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opts.set_default) {
          ret = rgw::set_default_realm(dpp, null_yield, cfgstore, realm);
          if (ret < 0) {
            cerr << "failed to set realm " << opts.realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::REALM_DELETE:
      {
	if (opts.realm_id.empty() && opts.realm_name.empty()) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm;
        std::unique_ptr<rgw::sal::RealmWriter> writer;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  opts.realm_id, opts.realm_name, realm, &writer);
	if (ret < 0) {
	  cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = writer->remove(dpp, null_yield);
	if (ret < 0) {
	  cerr << "failed to remove realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT::REALM_GET:
      {
	RGWRealm realm;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  opts.realm_id, opts.realm_name, realm);
	if (ret < 0) {
	  if (ret == -ENOENT && opts.realm_name.empty() && opts.realm_id.empty()) {
	    cerr << "missing realm name or id, or default realm not found" << std::endl;
	  } else {
	    cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
          }
	  return -ret;
	}
	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::REALM_GET_DEFAULT:
      {
	string default_id;
	int ret = cfgstore->read_default_realm_id(dpp, null_yield, default_id);
	if (ret == -ENOENT) {
	  cout << "No default realm is set" << std::endl;
	  return -ret;
	} else if (ret < 0) {
	  cerr << "Error reading default realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	cout << "default realm: " << default_id << std::endl;
      }
      break;
    case OPT::REALM_LIST:
      {
        std::string default_id;
        int ret = cfgstore->read_default_realm_id(dpp, null_yield,
                                                  default_id);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default realm: " << cpp_strerror(-ret) << std::endl;
	}

        Formatter::ObjectSection realms_list{*formatter, "realms_list"};
        encode_json("default_info", default_id, formatter);

        Formatter::ArraySection realms{*formatter, "realms"};
        rgw::sal::ListResult<std::string> listing;
        std::array<std::string, 1000> names; // list in pages of 1000
        do {
          ret = cfgstore->list_realm_names(dpp, null_yield, listing.next,
                                           names, listing);
          if (ret < 0) {
            std::cerr << "failed to list realms: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& name : listing.entries) {
            encode_json("name", name, formatter);
          }
        } while (!listing.next.empty());
      } // close sections realms and realms_list
      formatter->flush(cout);
      break;
    case OPT::REALM_LIST_PERIODS:
      {
        // use realm's current period
        RGWRealm realm;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  opts.realm_id, opts.realm_name, realm);
        if (ret < 0) {
          cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        opts.period_id = realm.current_period;

        Formatter::ObjectSection periods_list{*formatter, "realm_periods_list"};
	encode_json("current_period", opts.period_id, formatter);

        Formatter::ArraySection periods{*formatter, "periods"};

        while (!opts.period_id.empty()) {
          RGWPeriod period;
          ret = cfgstore->read_period(dpp, null_yield, opts.period_id,
                                      std::nullopt, period);
          if (ret < 0) {
            cerr << "failed to load period id " << opts.period_id
                << ": " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          encode_json("id", opts.period_id, formatter);
          opts.period_id = period.predecessor_uuid;
        }
      } // close sections periods and realm_periods_list
      formatter->flush(cout);
      break;

    case OPT::REALM_RENAME:
      {
	if (opts.realm_new_name.empty()) {
	  cerr << "missing realm new name" << std::endl;
	  return EINVAL;
	}
	if (opts.realm_name.empty() && opts.realm_id.empty()) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}

        RGWRealm realm;
        std::unique_ptr<rgw::sal::RealmWriter> writer;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  opts.realm_id, opts.realm_name, realm, &writer);
	if (ret < 0) {
	  cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = writer->rename(dpp, null_yield, realm, opts.realm_new_name);
	if (ret < 0) {
	  cerr << "rename failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        cout << "Realm name updated. Note that this change only applies to "
            "the current cluster, so this command must be run separately "
            "on each of the realm's other clusters." << std::endl;
      }
      break;
    case OPT::REALM_SET:
      {
	if (opts.realm_id.empty() && opts.realm_name.empty()) {
	  cerr << "no realm name or id provided" << std::endl;
	  return EINVAL;
	}
	bool new_realm = false;
        RGWRealm realm;
        std::unique_ptr<rgw::sal::RealmWriter> writer;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  opts.realm_id, opts.realm_name, realm, &writer);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	} else if (ret == -ENOENT) {
	  new_realm = true;
	}
	ret = rgw_admin_read_decode_json(opts.infile, realm);
	if (ret < 0) {
	  return 1;
	}
	if (!opts.realm_name.empty() && realm.get_name() != opts.realm_name) {
	  cerr << "mismatch between --rgw-realm " << opts.realm_name << " and json input file name " <<
	    realm.get_name() << std::endl;
	  return EINVAL;
	}
	/* new realm */
	if (new_realm) {
	  cout << "clearing period and epoch for new realm" << std::endl;
	  realm.clear_current_period_and_epoch();
          constexpr bool exclusive = true;
          ret = rgw::create_realm(dpp, null_yield, cfgstore,
                                  exclusive, realm);
	  if (ret < 0) {
	    cerr << "ERROR: couldn't create new realm: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	} else {
          ret = writer->write(dpp, null_yield, realm);
	  if (ret < 0) {
	    cerr << "ERROR: couldn't write realm info: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	}

        if (opts.set_default) {
          ret = rgw::set_default_realm(dpp, null_yield, cfgstore, realm);
          if (ret < 0) {
            cerr << "failed to set realm " << opts.realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }
	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;

    case OPT::REALM_DEFAULT:
      {
        RGWRealm realm;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  opts.realm_id, opts.realm_name, realm);
	if (ret < 0) {
	  cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = rgw::set_default_realm(dpp, null_yield, cfgstore, realm);
	if (ret < 0) {
	  cerr << "failed to set realm as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::REALM_DEFAULT_RM:
      if (int ret = cfgstore->delete_default_realm_id(dpp, null_yield); ret < 0) {
        cerr << "failed to remove default realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      break;
    case OPT::REALM_PULL:
      {
        if (opts.url.empty()) {
          cerr << "A --url must be provided." << std::endl;
          return EINVAL;
        }
        RGWEnv env;
        req_info info(g_ceph_context, &env);
        info.method = "GET";
        info.request_uri = "/admin/realm";

        map<string, string> &params = info.args.get_params();
        if (!opts.realm_id.empty())
          params["id"] = opts.realm_id;
        if (!opts.realm_name.empty())
          params["name"] = opts.realm_name;

        bufferlist bl;
        JSONParser p;
        int ret = rgw_admin_send_to_remote_or_url(nullptr, opts.url, opts.opt_region, opts.access_key, opts.secret_key, info, bl, p, dpp, driver);
        if (ret < 0) {
          cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
          if (ret == -EACCES) {
            cerr << "If the realm has been changed on the master zone, the "
                "master zone's gateway may need to be restarted to recognize "
                "this user." << std::endl;
          }
          return -ret;
        }
        RGWRealm realm;
        try {
          decode_json_obj(realm, &p);
        } catch (const JSONDecoder::err& e) {
          cerr << "failed to decode JSON response: " << e.what() << std::endl;
          return EINVAL;
        }
        RGWPeriod period;
        auto& current_period = realm.get_current_period();
        if (!current_period.empty()) {
          // pull the latest epoch of the realm's current period
          ret = rgw_admin_do_period_pull(cfgstore, nullptr, opts.url, opts.opt_region,
                               opts.access_key, opts.secret_key,
                               opts.realm_id, opts.realm_name, current_period, "",
                               &period, dpp, driver);
          if (ret < 0) {
            cerr << "could not fetch period " << current_period << std::endl;
            return -ret;
          }
        }
        constexpr bool exclusive = false;
        ret = rgw::create_realm(dpp, null_yield, cfgstore,
                                exclusive, realm);
        if (ret < 0) {
          cerr << "Error storing realm " << realm.get_id() << ": "
            << cpp_strerror(ret) << std::endl;
          return -ret;
        }

        if (opts.set_default) {
          ret = rgw::set_default_realm(dpp, null_yield, cfgstore, realm);
          if (ret < 0) {
            cerr << "failed to set realm " << opts.realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("realm", realm, formatter);
        formatter->flush(cout);
      }
      break;


  default:
    return -EINVAL;
  }
  return 0;
}
