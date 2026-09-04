// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/zone.h"
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
#include "rgw_zone_features.h"
#include "services/svc_sync_modules.h"

namespace {

#ifdef WITH_RADOSGW_RADOS
static int check_pool_support_omap(rgw::sal::Driver* driver, const rgw_pool& pool)
{
  librados::IoCtx io_ctx;
  int ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_rados_handle()->ioctx_create(pool.to_str().c_str(), io_ctx);
  if (ret < 0) {
     return 0;
  }

  ret = io_ctx.omap_clear("__omap_test_not_exist_oid__");
  if (ret == -EOPNOTSUPP) {
    io_ctx.close();
    return ret;
  }
  io_ctx.close();
  return 0;
}
#endif

} // anonymous namespace

int rgw_admin_zone(const DoutPrefixProvider* dpp,
                rgw::sal::Driver* driver,
                rgw::sal::ConfigStore* cfgstore,
                rgw::SiteConfig& site,
                Formatter* formatter,
                rgw_admin_zone_options& opts)
{
  switch (opts.command) {
    case OPT::ZONE_CREATE:
      {
        if (opts.zone_name.empty()) {
	  cerr << "zone name not provided" << std::endl;
	  return EINVAL;
        }

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
	/* if the user didn't provide zonegroup info , create stand alone zone */
	if (!opts.zonegroup_id.empty() || !opts.zonegroup_name.empty()) {
          int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                        opts.zonegroup_id, opts.zonegroup_name,
                                        zonegroup, &zonegroup_writer);
	  if (ret < 0) {
	    cerr << "failed to load zonegroup " << opts.zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  if (opts.realm_id.empty() && opts.realm_name.empty()) {
	    opts.realm_id = zonegroup.realm_id;
	  }
	}

        // create the local zone params
	RGWZoneParams zone_params;
        zone_params.id = opts.zone_id;
        zone_params.name = opts.zone_name;

        zone_params.system_key.id = opts.access_key;
        zone_params.system_key.key = opts.secret_key;
	zone_params.realm_id = opts.realm_id;
        for (const auto& a : opts.tier_config_add) {
          int r = zone_params.tier_config.set(a.first, a.second);
          if (r < 0) {
            cerr << "ERROR: failed to set configurable: " << a << std::endl;
            return EINVAL;
          }
        }

        if (zone_params.realm_id.empty()) {
          RGWRealm realm;
          int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                    opts.realm_id, opts.realm_name, realm);
          if (ret < 0 && ret != -ENOENT) {
            cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          zone_params.realm_id = realm.id;
          cerr << "NOTICE: set zone's realm_id=" << realm.id << std::endl;
        }

        constexpr bool exclusive = true;
        int ret = rgw::create_zone(dpp, null_yield, cfgstore,
                                   exclusive, zone_params);
	if (ret < 0) {
	  cerr << "failed to create zone " << opts.zone_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	if (zonegroup_writer) {
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
	    cerr << "failed to add zone " << opts.zone_name << " to zonegroup " << zonegroup.get_name()
		 << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (opts.set_default) {
          ret = rgw::set_default_zone(dpp, null_yield, cfgstore,
                                      zone_params);
          if (ret < 0) {
            cerr << "failed to set zone " << opts.zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zone", zone_params, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_DEFAULT:
      {
	if (opts.zone_id.empty() && opts.zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone_params;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone_params);
	if (ret < 0) {
	  cerr << "unable to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        ret = rgw::set_default_zone(dpp, null_yield, cfgstore,
                                    zone_params);
	if (ret < 0) {
	  cerr << "failed to set zone as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONE_DELETE:
      {
	if (opts.zone_id.empty() && opts.zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> writer;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone_params, &writer);
	if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        ret = rgw::delete_zone(dpp, null_yield, cfgstore,
                               zone_params, *writer);
	if (ret < 0) {
	  cerr << "failed to delete zone " << zone_params.get_name()
              << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONE_GET:
      {
	RGWZoneParams zone_params;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone_params);
	if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("zone", zone_params, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_SET:
      {
	RGWZoneParams zone;
        std::unique_ptr<rgw::sal::ZoneWriter> writer;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone, &writer);
        if (ret < 0 && ret != -ENOENT) {
	  cerr << "failed to load zone: " << cpp_strerror(ret) << std::endl;
          return -ret;
        }

        string orig_id = zone.get_id();

	ret = rgw_admin_read_decode_json(opts.infile, zone);
	if (ret < 0) {
	  return 1;
	}

	if (zone.realm_id.empty()) {
	  RGWRealm realm;
          ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                opts.realm_id, opts.realm_name, realm);
	  if (ret < 0 && ret != -ENOENT) {
	    cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  zone.realm_id = realm.get_id();
          cerr << "NOTICE: set zone's realm_id=" << zone.realm_id << std::endl;
	}

	if (!opts.zone_name.empty() && !zone.get_name().empty() && zone.get_name() != opts.zone_name) {
	  cerr << "ERROR: zone name " << opts.zone_name << " is different than the zone name " << zone.get_name() << " in the provided json " << std::endl;
	  return EINVAL;
	}

        if (zone.get_name().empty()) {
          zone.set_name(opts.zone_name);
          if (zone.get_name().empty()) {
            cerr << "no zone name specified" << std::endl;
            return EINVAL;
          }
        }

        opts.zone_name = zone.get_name();

        if (zone.get_id().empty()) {
          zone.set_id(orig_id);
        }

        constexpr bool exclusive = false;
        ret = rgw::create_zone(dpp, null_yield, cfgstore,
                               exclusive, zone);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opts.set_default) {
          ret = rgw::set_default_zone(dpp, null_yield, cfgstore, zone);
          if (ret < 0) {
            cerr << "failed to set zone " << opts.zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zone", zone, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_LIST:
      {
        RGWZoneParams default_zone_params;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 {}, {}, default_zone_params);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zone: " << cpp_strerror(-ret) << std::endl;
	}

        Formatter::ObjectSection zones_list{*formatter, "zones_list"};
        encode_json("default_info", default_zone_params.id, formatter);

        Formatter::ArraySection zones{*formatter, "zones"};
        rgw::sal::ListResult<std::string> listing;
        std::array<std::string, 1000> names; // list in pages of 1000
        do {
          ret = cfgstore->list_zone_names(dpp, null_yield, listing.next,
                                          names, listing);
          if (ret < 0) {
            std::cerr << "failed to list zones: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& name : listing.entries) {
            encode_json("name", name, formatter);
          }
        } while (!listing.next.empty());
      } // close sections zones and zones_list
      formatter->flush(cout);
      break;
    case OPT::ZONE_MODIFY:
      {
	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> zone_writer;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone_params, &zone_writer);
        if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        bool need_zone_update = false;
        if (!opts.access_key.empty()) {
          zone_params.system_key.id = opts.access_key;
          need_zone_update = true;
        }

        if (!opts.secret_key.empty()) {
          zone_params.system_key.key = opts.secret_key;
          need_zone_update = true;
        }

        if (!opts.realm_id.empty()) {
          zone_params.realm_id = opts.realm_id;
          need_zone_update = true;
        } else if (!opts.realm_name.empty()) {
          // get realm id from name
          ret = cfgstore->read_realm_id(dpp, null_yield,
                                        opts.realm_name, zone_params.realm_id);
          if (ret < 0) {
            cerr << "failed to find realm by name " << opts.realm_name << std::endl;
            return -ret;
          }
          need_zone_update = true;
        }

        for (const auto& add : opts.tier_config_add) {
          ret = zone_params.tier_config.set(add.first, add.second);
          if (ret < 0) {
            cerr << "ERROR: failed to set configurable: " << add << std::endl;
            return EINVAL;
          }
          need_zone_update = true;
        }

        for (const auto& rm : opts.tier_config_rm) {
          if (!rm.first.empty()) { /* otherwise will remove the entire config */
            zone_params.tier_config.erase(rm.first);
            need_zone_update = true;
          }
        }

        if (need_zone_update) {
          ret = zone_writer->write(dpp, null_yield, zone_params);
          if (ret < 0) {
            cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
        ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                  opts.zonegroup_id, opts.zonegroup_name,
                                  zonegroup, &zonegroup_writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
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
	  cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opts.set_default) {
          ret = rgw::set_default_zone(dpp, null_yield, cfgstore,
                                      zone_params);
          if (ret < 0) {
            cerr << "failed to set zone " << opts.zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zone", zone_params, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT::ZONE_RENAME:
      {
	if (opts.zone_new_name.empty()) {
	  cerr << " missing zone new name" << std::endl;
	  return EINVAL;
	}
	if (opts.zone_id.empty() && opts.zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> zone_writer;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone_params, &zone_writer);
	if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	ret = zone_writer->rename(dpp, null_yield, zone_params, opts.zone_new_name);
	if (ret < 0) {
	  cerr << "failed to rename zone " << opts.zone_name << " to " << opts.zone_new_name << ": " << cpp_strerror(-ret)
	       << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
        ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                  opts.zonegroup_id, opts.zonegroup_name,
                                  zonegroup, &zonegroup_writer);
	if (ret < 0) {
	  cerr << "WARNING: failed to load zonegroup " << opts.zonegroup_name << std::endl;
          return EXIT_SUCCESS;
	}

        auto z = zonegroup.zones.find(zone_params.id);
        if (z == zonegroup.zones.end()) {
          return EXIT_SUCCESS;
        }
        z->second.name = zone_params.name;

        ret = zonegroup_writer->write(dpp, null_yield, zonegroup);
        if (ret < 0) {
          cerr << "Error in zonegroup rename for " << opts.zone_name << ": " << cpp_strerror(-ret) << std::endl;
          return -ret;
	}
      }
      break;
#ifdef WITH_RADOSGW_RADOS
    case OPT::ZONE_PLACEMENT_ADD:
#endif
    case OPT::ZONE_PLACEMENT_MODIFY:
    case OPT::ZONE_PLACEMENT_RM:
      {
        if (opts.placement_id.empty()) {
          cerr << "ERROR: --placement-id not specified" << std::endl;
          return EINVAL;
        }
        // validate compression type
        if (opts.compression_type && *opts.compression_type != "random"
            && !Compressor::get_comp_alg_type(*opts.compression_type)) {
          std::cerr << "Unrecognized compression type" << std::endl;
          return EINVAL;
        }

	RGWZoneParams zone;
        std::unique_ptr<rgw::sal::ZoneWriter> writer;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone, &writer);
        if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

#ifdef WITH_RADOSGW_RADOS
        if (opts.command == OPT::ZONE_PLACEMENT_ADD ||
	    opts.command == OPT::ZONE_PLACEMENT_MODIFY) {
	  RGWZoneGroup zonegroup;
          ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                    opts.zonegroup_id, opts.zonegroup_name, zonegroup);
	  if (ret < 0) {
	    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }

	  auto ptiter = zonegroup.placement_targets.find(opts.placement_id);
	  if (ptiter == zonegroup.placement_targets.end()) {
	    cerr << "ERROR: placement id '" << opts.placement_id << "' is not configured in zonegroup placement targets" << std::endl;
	    return EINVAL;
	  }

	  string storage_class = rgw_placement_rule::get_canonical_storage_class(opts.opt_storage_class.value_or(string()));
	  if (ptiter->second.storage_classes.find(storage_class) == ptiter->second.storage_classes.end()) {
	    cerr << "ERROR: storage class '" << storage_class << "' is not defined in zonegroup '" << opts.placement_id << "' placement target" << std::endl;
	    return EINVAL;
	  }
	  if (ptiter->second.tier_targets.find(storage_class) != ptiter->second.tier_targets.end()) {
	    cerr << "ERROR: storage class '" << storage_class << "' is of tier type in zonegroup '" << opts.placement_id << "' placement target" << std::endl;
	    return EINVAL;
	  }

          RGWZonePlacementInfo& info = zone.placement_pools[opts.placement_id];

	  string opt_index_pool = opts.index_pool.value_or(string());
	  string opt_data_pool = opts.data_pool.value_or(string());

	  if (!opt_index_pool.empty()) {
	    info.index_pool = opt_index_pool;
	  }

	  if (info.index_pool.empty()) {
            cerr << "ERROR: index pool not configured, need to specify --index-pool" << std::endl;
            return EINVAL;
	  }

	  if (opt_data_pool.empty()) {
	    const RGWZoneStorageClass *porig_sc{nullptr};
	    if (info.storage_classes.find(storage_class, &porig_sc)) {
	      if (porig_sc->data_pool) {
		opt_data_pool = porig_sc->data_pool->to_str();
	      }
	    }
	    if (opt_data_pool.empty()) {
	      cerr << "ERROR: data pool not configured, need to specify --data-pool" << std::endl;
	      return EINVAL;
	    }
	  }

          rgw_pool dp = opt_data_pool;
          info.storage_classes.set_storage_class(storage_class, &dp,
              opts.compression_type.has_value() ? std::addressof(*opts.compression_type) : nullptr);

          if (opts.data_extra_pool) {
            info.data_extra_pool = *opts.data_extra_pool;
          }
          if (opts.index_type_specified) {
	    info.index_type = opts.placement_index_type;
          }
          if (opts.placement_inline_data_specified) {
            info.inline_data = opts.placement_inline_data;
          }

          ret = check_pool_support_omap(driver, info.get_data_extra_pool());
          if (ret < 0) {
             cerr << "ERROR: the data extra (non-ec) pool '" << info.get_data_extra_pool() 
                 << "' does not support omap" << std::endl;
             return ret;
          }
        } else 
#endif
	    if (opts.command == OPT::ZONE_PLACEMENT_RM) {
          if (!opts.opt_storage_class ||
              opts.opt_storage_class->empty()) {
            zone.placement_pools.erase(opts.placement_id);
          } else {
            auto iter = zone.placement_pools.find(opts.placement_id);
            if (iter != zone.placement_pools.end()) {
              RGWZonePlacementInfo& info = zone.placement_pools[opts.placement_id];
              info.storage_classes.remove_storage_class(*opts.opt_storage_class);
            }
          }
        }

        ret = writer->write(dpp, null_yield, zone);
        if (ret < 0) {
          cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("zone", zone, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT::ZONE_PLACEMENT_LIST:
      {
	RGWZoneParams zone;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("placement_pools", zone.placement_pools, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_PLACEMENT_GET:
      {
	if (opts.placement_id.empty()) {
	  cerr << "ERROR: --placement-id not specified" << std::endl;
	  return EINVAL;
	}

	RGWZoneParams zone;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 opts.zone_id, opts.zone_name, zone);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	auto p = zone.placement_pools.find(opts.placement_id);
	if (p == zone.placement_pools.end()) {
	  cerr << "ERROR: zone placement target '" << opts.placement_id << "' not found" << std::endl;
	  return ENOENT;
	}
	encode_json("placement_pools", p->second, formatter);
	formatter->flush(cout);
      }

  default:
    return -EINVAL;
  }
  return 0;
}
