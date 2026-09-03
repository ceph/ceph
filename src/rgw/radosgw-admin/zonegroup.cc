// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/zonegroup.h"
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

#include "driver/rados/rgw_sal_rados.h"
#include "rgw_zone_features.h"
#include "services/svc_sync_modules.h"

int rgw_admin_zonegroup(const DoutPrefixProvider* dpp,
                rgw::sal::Driver* driver,
                rgw::sal::ConfigStore* cfgstore,
                rgw::SiteConfig& site,
                Formatter* formatter,
                rgw_admin_zonegroup_options& opts)
{
  switch (opts.command) {
    case OPT::ZONEGROUP_ADD:
      {
	if (opts.zonegroup_id.empty() && opts.zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}

        // load the zonegroup and zone params
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name,
                                      zonegroup, &zonegroup_writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup " << opts.zonegroup_name << " id "
              << opts.zonegroup_id << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> zone_writer;
        ret = rgw::read_zone(dpp, null_yield, cfgstore,
                             opts.zone_id, opts.zone_name, zone_params, &zone_writer);
	if (ret < 0) {
	  cerr << "unable to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        // update zone_params if necessary
        bool need_zone_update = false;

        if (zone_params.realm_id != zonegroup.realm_id) {
          if (!zone_params.realm_id.empty()) {
            cerr << "WARNING: overwriting zone realm_id=" << zone_params.realm_id
                << " to match zonegroup realm_id=" << zonegroup.realm_id << std::endl;
          }
          zone_params.realm_id = zonegroup.realm_id;
          need_zone_update = true;
        }

        for (auto a : opts.tier_config_add) {
          ret = zone_params.tier_config.set(a.first, a.second);
          if (ret < 0) {
            cerr << "ERROR: failed to set configurable: " << a << std::endl;
            return EINVAL;
          }
          need_zone_update = true;
        }

        if (need_zone_update) {
          ret = zone_writer->write(dpp, null_yield, zone_params);
          if (ret < 0) {
            cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }

        const bool *pis_master = (opts.is_master_set ? &opts.is_master : nullptr);
        const bool *pread_only = (opts.is_read_only_set ? &opts.read_only : nullptr);
        const bool *psync_from_all = (opts.sync_from_all_specified ? &opts.sync_from_all : nullptr);
        const string *predirect_zone = (opts.redirect_zone_set ? &opts.redirect_zone : nullptr);

        // validate --tier-type if specified
        const string *ptier_type = (opts.tier_type_specified ? &opts.tier_type : nullptr);
        if (ptier_type) {
#ifdef WITH_RADOSGW_RADOS
          auto sync_mgr = static_cast<rgw::sal::RadosStore*>(driver)->svc()->sync_modules->get_manager();
          if (!sync_mgr->get_module(*ptier_type, nullptr)) {
            ldpp_dout(dpp, -1) << "ERROR: could not find sync module: "
                << *ptier_type << ",  valid sync modules: "
                << sync_mgr->get_registered_module_names() << dendl;
            return EINVAL;
          }
#else
          ldpp_dout(dpp, -1) << "ERROR: --tier-type requires the RADOS backend" << dendl;
          return EINVAL;
#endif
        }

        if (opts.enable_features.empty()) { // enable all features by default
          opts.enable_features.insert(rgw::zone_features::supported.begin(),
                                 rgw::zone_features::supported.end());
        }

        // add/update the public zone information stored in the zonegroup
        ret = rgw::add_zone_to_group(dpp, zonegroup, zone_params,
                                     pis_master, pread_only, opts.endpoints,
                                     ptier_type, psync_from_all,
                                     opts.sync_from, opts.sync_from_rm,
                                     predirect_zone, opts.bucket_index_max_shards,
                                     opts.enable_features, opts.disable_features);
        if (ret < 0) {
          return -ret;
        }

        // write the updated zonegroup
        ret = zonegroup_writer->write(dpp, null_yield, zonegroup);
	if (ret < 0) {
	  cerr << "failed to write updated zonegroup " << zonegroup.get_name()
              << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_CREATE:
      {
	if (opts.zonegroup_name.empty()) {
	  cerr << "Missing zonegroup name" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  opts.realm_id, opts.realm_name, realm);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
        zonegroup.name = opts.zonegroup_name;
        zonegroup.is_master = opts.is_master;
        zonegroup.realm_id = realm.get_id();
        zonegroup.endpoints = opts.endpoints;
        zonegroup.api_name = (opts.api_name.empty() ? opts.zonegroup_name : opts.api_name);

        zonegroup.enabled_features = opts.enable_features;
        if (zonegroup.enabled_features.empty()) { // enable features by default
          zonegroup.enabled_features.insert(rgw::zone_features::enabled.begin(),
                                            rgw::zone_features::enabled.end());
        }
        for (const auto& feature : opts.disable_features) {
          auto i = zonegroup.enabled_features.find(feature);
          if (i == zonegroup.enabled_features.end()) {
            ldpp_dout(dpp, 1) << "WARNING: zone feature \"" << feature
                << "\" was not enabled in zonegroup " << opts.zonegroup_name << dendl;
            continue;
          }
          zonegroup.enabled_features.erase(i);
        }

        constexpr bool exclusive = true;
        ret = rgw::create_zonegroup(dpp, null_yield, cfgstore,
                                    exclusive, zonegroup);
	if (ret < 0) {
	  cerr << "failed to create zonegroup " << opts.zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opts.set_default) {
          ret = rgw::set_default_zonegroup(dpp, null_yield, cfgstore,
                                           zonegroup);
          if (ret < 0) {
            cerr << "failed to set zonegroup " << opts.zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_DEFAULT:
      {
	if (opts.zonegroup_id.empty() && opts.zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name,
                                      zonegroup);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        ret = rgw::set_default_zonegroup(dpp, null_yield, cfgstore,
                                         zonegroup);
	if (ret < 0) {
	  cerr << "failed to set zonegroup as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_DELETE:
      {
	if (opts.zonegroup_id.empty() && opts.zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name,
                                      zonegroup, &writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = writer->remove(dpp, null_yield);
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_GET:
      {
	RGWZoneGroup zonegroup;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name, zonegroup);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_LIST:
      {
        RGWZoneGroup default_zonegroup;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      {}, {}, default_zonegroup);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zonegroup: " << cpp_strerror(-ret) << std::endl;
	}

        Formatter::ObjectSection zonegroups_list{*formatter, "zonegroups_list"};
        encode_json("default_info", default_zonegroup.id, formatter);

        Formatter::ArraySection zonegroups{*formatter, "zonegroups"};
        rgw::sal::ListResult<std::string> listing;
        std::array<std::string, 1000> names; // list in pages of 1000
        do {
          ret = cfgstore->list_zonegroup_names(dpp, null_yield, listing.next,
                                               names, listing);
          if (ret < 0) {
            std::cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& name : listing.entries) {
            encode_json("name", name, formatter);
          }
        } while (!listing.next.empty());
      } // close sections zonegroups and zonegroups_list
      formatter->flush(cout);
      break;
    case OPT::ZONEGROUP_MODIFY:
      {
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name,
                                      zonegroup, &writer);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        bool need_update = false;

        if (!opts.master_zone.empty()) {
          zonegroup.master_zone = opts.master_zone;
          need_update = true;
        }

	if (opts.is_master_set) {
	  zonegroup.is_master = opts.is_master;
          need_update = true;
        }

        if (!opts.endpoints.empty()) {
          zonegroup.endpoints = opts.endpoints;
          need_update = true;
        }

        if (!opts.api_name.empty()) {
          zonegroup.api_name = opts.api_name;
          need_update = true;
        }

        if (!opts.realm_id.empty()) {
          zonegroup.realm_id = opts.realm_id;
          need_update = true;
        } else if (!opts.realm_name.empty()) {
          // get realm id from name
          ret = cfgstore->read_realm_id(dpp, null_yield, opts.realm_name,
                                        zonegroup.realm_id);
          if (ret < 0) {
            cerr << "failed to find realm by name " << opts.realm_name << std::endl;
            return -ret;
          }
          need_update = true;
        }

        if (opts.bucket_index_max_shards) {
          for (auto& [name, zone] : zonegroup.zones) {
            zone.bucket_index_max_shards = *opts.bucket_index_max_shards;
          }
          need_update = true;
        }

        for (const auto& feature : opts.enable_features) {
          zonegroup.enabled_features.insert(feature);
          need_update = true;
        }
        for (const auto& feature : opts.disable_features) {
          auto i = zonegroup.enabled_features.find(feature);
          if (i == zonegroup.enabled_features.end()) {
            ldpp_dout(dpp, 1) << "WARNING: zone feature \"" << feature
                << "\" was not enabled in zonegroup "
                << zonegroup.get_name() << dendl;
            continue;
          }
          zonegroup.enabled_features.erase(i);
          need_update = true;
        }

        if (need_update) {
	  ret = writer->write(dpp, null_yield, zonegroup);
	  if (ret < 0) {
	    cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (opts.set_default) {
          ret = rgw::set_default_zonegroup(dpp, null_yield, cfgstore,
                                           zonegroup);
          if (ret < 0) {
            cerr << "failed to set zonegroup " << opts.zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_SET:
      {
	RGWRealm realm;
        int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                  opts.realm_id, opts.realm_name, realm);
	bool default_realm_not_exist = (ret == -ENOENT && opts.realm_id.empty() && opts.realm_name.empty());

	if (ret < 0 && !default_realm_not_exist) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
	ret = rgw_admin_read_decode_json(opts.infile, zonegroup);
	if (ret < 0) {
	  return 1;
	}
	if (zonegroup.realm_id.empty() && !default_realm_not_exist) {
	  zonegroup.realm_id = realm.get_id();
	}
        // validate zonegroup features
        for (const auto& feature : zonegroup.enabled_features) {
          if (!rgw::zone_features::supports(feature)) {
            std::cerr << "ERROR: Unrecognized zonegroup feature \""
                << feature << "\"" << std::endl;
            return EINVAL;
          }
        }
        for (const auto& [name, zone] : zonegroup.zones) {
          // validate zone features
          for (const auto& feature : zone.supported_features) {
            if (!rgw::zone_features::supports(feature)) {
              std::cerr << "ERROR: Unrecognized zone feature \""
                  << feature << "\" in zone " << zone.name << std::endl;
              return EINVAL;
            }
          }
          // zone must support everything zonegroup does
          for (const auto& feature : zonegroup.enabled_features) {
            if (!zone.supports(feature)) {
              std::cerr << "ERROR: Zone " << name << " does not support feature \""
                  << feature << "\" required by zonegroup" << std::endl;
              return EINVAL;
            }
          }
        }

        // create/overwrite the zonegroup info
        constexpr bool exclusive = false;
        ret = rgw::create_zonegroup(dpp, null_yield, cfgstore,
                                    exclusive, zonegroup);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create zonegroup info: " << cpp_strerror(-ret) << std::endl;
	  return 1;
	}

        if (opts.set_default) {
          ret = rgw::set_default_zonegroup(dpp, null_yield, cfgstore,
                                           zonegroup);
          if (ret < 0) {
            cerr << "failed to set zonegroup " << opts.zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_REMOVE:
      {
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name,
                                      zonegroup, &writer);
        if (ret < 0) {
          cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        if (opts.zone_id.empty()) {
          if (opts.zone_name.empty()) {
            cerr << "no --zone-id or --rgw-zone name provided" << std::endl;
            return EINVAL;
          }
          // look up zone id by name
          for (auto& z : zonegroup.zones) {
            if (opts.zone_name == z.second.name) {
              opts.zone_id = z.second.id;
              break;
            }
          }
          if (opts.zone_id.empty()) {
            cerr << "zone name " << opts.zone_name << " not found in zonegroup "
                << zonegroup.get_name() << std::endl;
            return ENOENT;
          }
        }

        ret = rgw::remove_zone_from_group(dpp, zonegroup, opts.zone_id);
        if (ret < 0) {
          cerr << "failed to remove zone: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        ret = writer->write(dpp, null_yield, zonegroup);
        if (ret < 0) {
          cerr << "failed to write zonegroup: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_RENAME:
      {
	if (opts.zonegroup_new_name.empty()) {
	  cerr << " missing zonegroup new name" << std::endl;
	  return EINVAL;
	}
	if (opts.zonegroup_id.empty() && opts.zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name,
                                      zonegroup, &writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        zonegroup.api_name = opts.zonegroup_new_name;
        ret = writer->rename(dpp, null_yield, zonegroup, opts.zonegroup_new_name);
	if (ret < 0) {
	  cerr << "failed to rename zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_PLACEMENT_LIST:
      {
	RGWZoneGroup zonegroup;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name, zonegroup);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("placement_targets", zonegroup.placement_targets, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_PLACEMENT_GET:
      {
	if (opts.placement_id.empty()) {
	  cerr << "ERROR: --placement-id not specified" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name, zonegroup);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	auto p = zonegroup.placement_targets.find(opts.placement_id);
	if (p == zonegroup.placement_targets.end()) {
	  cerr << "failed to find a zonegroup placement target named '" << opts.placement_id << "'" << std::endl;
	  return -ENOENT;
	}
	encode_json("placement_targets", p->second, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_PLACEMENT_ADD:
    case OPT::ZONEGROUP_PLACEMENT_MODIFY:
    case OPT::ZONEGROUP_PLACEMENT_RM:
    case OPT::ZONEGROUP_PLACEMENT_DEFAULT:
      {
    if (opts.placement_id.empty()) {
      cerr << "ERROR: --placement-id not specified" << std::endl;
      return EINVAL;
    }

    rgw_placement_rule rule;
    rule.from_str(opts.placement_id);

    if (!rule.storage_class.empty() && opts.opt_storage_class &&
        rule.storage_class != *opts.opt_storage_class) {
      cerr << "ERROR: provided contradicting storage class configuration" << std::endl;
      return EINVAL;
    } else if (rule.storage_class.empty()) {
      rule.storage_class = opts.opt_storage_class.value_or(string());
    }

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                      opts.zonegroup_id, opts.zonegroup_name,
                                      zonegroup, &writer);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

    if (opts.command == OPT::ZONEGROUP_PLACEMENT_ADD ||
      opts.command == OPT::ZONEGROUP_PLACEMENT_MODIFY) {
      RGWZoneGroupPlacementTarget& target = zonegroup.placement_targets[opts.placement_id];
      if (!opts.tags.empty()) {
        target.tags.clear();
        for (auto& t : opts.tags) {
          target.tags.insert(t);
        }
      }

      target.name = opts.placement_id;
      for (auto& t : opts.tags_rm) {
        target.tags.erase(t);
      }
      for (auto& t : opts.tags_add) {
        target.tags.insert(t);
      }
      target.storage_classes.insert(rule.get_storage_class());

      /* Tier options */
      bool tier_class = false;
      std::string storage_class = rule.get_storage_class();
      RGWZoneGroupPlacementTier t;
      RGWZoneGroupPlacementTier *pt = &t;

	  auto ptiter = target.tier_targets.find(storage_class);
	  if (ptiter != target.tier_targets.end()) {
        pt = &ptiter->second;
        tier_class = true;
      } else if (opts.tier_type_specified) {
        if (RGWTierType::is_tier_type_supported(opts.tier_type)) {
          /* we support only cloud-s3 & cloud-s3-glacier tier-type for now.
           * Once set cant be reset. */
          tier_class = true;
          pt->tier_type = opts.tier_type;
          pt->storage_class = storage_class;
        } else {
	      cerr << "ERROR: Invalid tier-type specified" << std::endl;
	      return EINVAL;
        }
      }

      if (tier_class) {
        if (opts.tier_config_add.size() > 0) {
          JSONFormattable tconfig;
          for (auto add : opts.tier_config_add) {
            int r = tconfig.set(add.first, add.second);
            if (r < 0) {
              cerr << "ERROR: failed to set configurable: " << add << std::endl;
              return EINVAL;
            }
          }
          int r = pt->update_params(tconfig);
          if (r < 0) {
            cerr << "ERROR: failed to update tier_config options"<< std::endl;
          }
        }
        if (opts.tier_config_rm.size() > 0) {
          JSONFormattable tconfig;
          for (auto add : opts.tier_config_rm) {
            int r = tconfig.set(add.first, add.second);
            if (r < 0) {
              cerr << "ERROR: failed to set configurable: " << add << std::endl;
              return EINVAL;
            }
          }
          int r = pt->clear_params(tconfig);
          if (r < 0) {
            cerr << "ERROR: failed to update tier_config options"<< std::endl;
          }
        }

        target.tier_targets.emplace(std::make_pair(storage_class, *pt));
      }

      if (zonegroup.default_placement.empty()) {
        zonegroup.default_placement.init(rule.name, RGW_STORAGE_CLASS_STANDARD);
      }
    } else if (opts.command == OPT::ZONEGROUP_PLACEMENT_RM) {
      if (!opts.opt_storage_class || opts.opt_storage_class->empty()) {
        zonegroup.placement_targets.erase(opts.placement_id);
        if (zonegroup.default_placement.name == opts.placement_id) {
          // clear default placement
          zonegroup.default_placement.clear();
        }
      } else {
        auto iter = zonegroup.placement_targets.find(opts.placement_id);
        if (iter != zonegroup.placement_targets.end()) {
          RGWZoneGroupPlacementTarget& info = zonegroup.placement_targets[opts.placement_id];
          info.storage_classes.erase(*opts.opt_storage_class);

          if (zonegroup.default_placement == rule) {
            // clear default storage class
            zonegroup.default_placement.storage_class.clear();
          }

	      auto ptiter = info.tier_targets.find(*opts.opt_storage_class);
	      if (ptiter != info.tier_targets.end()) {
		    info.tier_targets.erase(ptiter);
	      }
        }
      }
    } else if (opts.command == OPT::ZONEGROUP_PLACEMENT_DEFAULT) {
      if (!zonegroup.placement_targets.count(opts.placement_id)) {
        cerr << "failed to find a zonegroup placement target named '"
             << opts.placement_id << "'" << std::endl;
        return -ENOENT;
      }
      zonegroup.default_placement = rule;
    }

    ret = writer->write(dpp, null_yield, zonegroup);
    if (ret < 0) {
      cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("placement_targets", zonegroup.placement_targets, formatter);
    formatter->flush(cout);
      }
      break;

  default:
    return -EINVAL;
  }
  return 0;
}
