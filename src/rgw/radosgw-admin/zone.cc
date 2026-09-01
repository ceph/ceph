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

static const DoutPrefixProvider* g_admin_dpp;
static rgw::sal::Driver* g_admin_driver;


#include "compressor/Compressor.h"
#include "driver/rados/rgw_sal_rados.h"
#include "rgw_zone_features.h"
#include "services/svc_sync_modules.h"

namespace {

#undef driver
#define driver g_admin_driver
#undef dpp
#define dpp g_admin_dpp


#ifdef WITH_RADOSGW_RADOS
static int check_pool_support_omap(const rgw_pool& pool)
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
                const rgw_admin_zone_options& o)
{
  g_admin_dpp = dpp;
  g_admin_driver = driver;
  auto command = o.command;

  auto& zonegroup_id = *o.zonegroup_id;
  auto& zonegroup_name = *o.zonegroup_name;
  auto& zone_id = *o.zone_id;
  auto& zone_name = *o.zone_name;
  auto& zone_new_name = *o.zone_new_name;
  auto& realm_id = *o.realm_id;
  auto& realm_name = *o.realm_name;
  auto& placement_id = *o.placement_id;
  auto& url = *o.url;
  auto& access_key = *o.access_key;
  auto& secret_key = *o.secret_key;
  auto& infile = *o.infile;
  auto& sync_from = *o.sync_from;
  auto& sync_from_rm = *o.sync_from_rm;
  auto& endpoints = *o.endpoints;
  auto& master_zone = *o.master_zone;
  auto& format = *o.format;
  auto& api_name = *o.api_name;
  auto& tier_type = *o.tier_type;
  auto& redirect_zone = *o.redirect_zone;
  auto& tier_config_add = *o.tier_config_add;
  auto& tier_config_rm = *o.tier_config_rm;
  auto& index_pool = *o.index_pool;
  auto& data_pool = *o.data_pool;
  auto& data_extra_pool = *o.data_extra_pool;
  auto& compression_type = *o.compression_type;
  auto& bucket_index_max_shards = *o.bucket_index_max_shards;
  auto& opt_storage_class = *o.opt_storage_class;
  auto& opt_region = *o.opt_region;
  auto tier_type_specified = o.tier_type_specified;
  auto sync_from_all_specified = o.sync_from_all_specified;
  auto redirect_zone_set = o.redirect_zone_set;
  auto placement_inline_data = o.placement_inline_data;
  auto placement_inline_data_specified = o.placement_inline_data_specified;
  auto set_default = o.set_default;
  auto read_only = o.read_only;
  auto is_master = o.is_master;
  auto is_master_set = o.is_master_set;
  auto is_read_only_set = o.is_read_only_set;
  auto sync_from_all = o.sync_from_all;
  auto yes_i_really_mean_it = o.yes_i_really_mean_it;
#ifdef WITH_RADOSGW_RADOS
  auto& placement_index_type = *o.placement_index_type;
  auto index_type_specified = o.index_type_specified;
  auto& enable_features = *o.enable_features;
  auto& disable_features = *o.disable_features;
#endif
  auto num_shards_specified = o.num_shards_specified;
  auto num_shards = o.num_shards;

  switch (command) {
    case OPT::ZONE_CREATE:
      {
        if (zone_name.empty()) {
	  cerr << "zone name not provided" << std::endl;
	  return EINVAL;
        }

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
	/* if the user didn't provide zonegroup info , create stand alone zone */
	if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
          int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                        zonegroup_id, zonegroup_name,
                                        zonegroup, &zonegroup_writer);
	  if (ret < 0) {
	    cerr << "failed to load zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  if (realm_id.empty() && realm_name.empty()) {
	    realm_id = zonegroup.realm_id;
	  }
	}

        // create the local zone params
	RGWZoneParams zone_params;
        zone_params.id = zone_id;
        zone_params.name = zone_name;

        zone_params.system_key.id = access_key;
        zone_params.system_key.key = secret_key;
	zone_params.realm_id = realm_id;
        for (const auto& a : tier_config_add) {
          int r = zone_params.tier_config.set(a.first, a.second);
          if (r < 0) {
            cerr << "ERROR: failed to set configurable: " << a << std::endl;
            return EINVAL;
          }
        }

        if (zone_params.realm_id.empty()) {
          RGWRealm realm;
          int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                    realm_id, realm_name, realm);
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
	  cerr << "failed to create zone " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	if (zonegroup_writer) {
          const bool *pis_master = (is_master_set ? &is_master : nullptr);
          const bool *pread_only = (is_read_only_set ? &read_only : nullptr);
          const bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
          const string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

          // validate --tier-type if specified
          const string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
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

          if (enable_features.empty()) { // enable all features by default
            enable_features.insert(rgw::zone_features::supported.begin(),
                                   rgw::zone_features::supported.end());
          }

          // add/update the public zone information stored in the zonegroup
          ret = rgw::add_zone_to_group(dpp, zonegroup, zone_params,
                                       pis_master, pread_only, endpoints,
                                       ptier_type, psync_from_all,
                                       sync_from, sync_from_rm,
                                       predirect_zone, bucket_index_max_shards,
                                       enable_features, disable_features);
          if (ret < 0) {
            return -ret;
          }

          // write the updated zonegroup
          ret = zonegroup_writer->write(dpp, null_yield, zonegroup);
	  if (ret < 0) {
	    cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name()
		 << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (set_default) {
          ret = rgw::set_default_zone(dpp, null_yield, cfgstore,
                                      zone_params);
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zone", zone_params, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_DEFAULT:
      {
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone_params;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 zone_id, zone_name, zone_params);
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
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> writer;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 zone_id, zone_name, zone_params, &writer);
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
                                 zone_id, zone_name, zone_params);
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
                                 zone_id, zone_name, zone, &writer);
        if (ret < 0 && ret != -ENOENT) {
	  cerr << "failed to load zone: " << cpp_strerror(ret) << std::endl;
          return -ret;
        }

        string orig_id = zone.get_id();

	ret = rgw_admin_read_decode_json(infile, zone);
	if (ret < 0) {
	  return 1;
	}

	if (zone.realm_id.empty()) {
	  RGWRealm realm;
          ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                realm_id, realm_name, realm);
	  if (ret < 0 && ret != -ENOENT) {
	    cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  zone.realm_id = realm.get_id();
          cerr << "NOTICE: set zone's realm_id=" << zone.realm_id << std::endl;
	}

	if (!zone_name.empty() && !zone.get_name().empty() && zone.get_name() != zone_name) {
	  cerr << "ERROR: zone name " << zone_name << " is different than the zone name " << zone.get_name() << " in the provided json " << std::endl;
	  return EINVAL;
	}

        if (zone.get_name().empty()) {
          zone.set_name(zone_name);
          if (zone.get_name().empty()) {
            cerr << "no zone name specified" << std::endl;
            return EINVAL;
          }
        }

        zone_name = zone.get_name();

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

        if (set_default) {
          ret = rgw::set_default_zone(dpp, null_yield, cfgstore, zone);
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
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
                                 zone_id, zone_name, zone_params, &zone_writer);
        if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        bool need_zone_update = false;
        if (!access_key.empty()) {
          zone_params.system_key.id = access_key;
          need_zone_update = true;
        }

        if (!secret_key.empty()) {
          zone_params.system_key.key = secret_key;
          need_zone_update = true;
        }

        if (!realm_id.empty()) {
          zone_params.realm_id = realm_id;
          need_zone_update = true;
        } else if (!realm_name.empty()) {
          // get realm id from name
          ret = cfgstore->read_realm_id(dpp, null_yield,
                                        realm_name, zone_params.realm_id);
          if (ret < 0) {
            cerr << "failed to find realm by name " << realm_name << std::endl;
            return -ret;
          }
          need_zone_update = true;
        }

        for (const auto& add : tier_config_add) {
          ret = zone_params.tier_config.set(add.first, add.second);
          if (ret < 0) {
            cerr << "ERROR: failed to set configurable: " << add << std::endl;
            return EINVAL;
          }
          need_zone_update = true;
        }

        for (const auto& rm : tier_config_rm) {
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
                                  zonegroup_id, zonegroup_name,
                                  zonegroup, &zonegroup_writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        const bool *pis_master = (is_master_set ? &is_master : nullptr);
        const bool *pread_only = (is_read_only_set ? &read_only : nullptr);
        const bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
        const string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

        // validate --tier-type if specified
        const string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
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

        if (enable_features.empty()) { // enable all features by default
          enable_features.insert(rgw::zone_features::supported.begin(),
                                 rgw::zone_features::supported.end());
        }

        // add/update the public zone information stored in the zonegroup
        ret = rgw::add_zone_to_group(dpp, zonegroup, zone_params,
                                     pis_master, pread_only, endpoints,
                                     ptier_type, psync_from_all,
                                     sync_from, sync_from_rm,
                                     predirect_zone, bucket_index_max_shards,
                                     enable_features, disable_features);
        if (ret < 0) {
          return -ret;
        }

        // write the updated zonegroup
        ret = zonegroup_writer->write(dpp, null_yield, zonegroup);
	if (ret < 0) {
	  cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = rgw::set_default_zone(dpp, null_yield, cfgstore,
                                      zone_params);
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zone", zone_params, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT::ZONE_RENAME:
      {
	if (zone_new_name.empty()) {
	  cerr << " missing zone new name" << std::endl;
	  return EINVAL;
	}
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> zone_writer;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 zone_id, zone_name, zone_params, &zone_writer);
	if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	ret = zone_writer->rename(dpp, null_yield, zone_params, zone_new_name);
	if (ret < 0) {
	  cerr << "failed to rename zone " << zone_name << " to " << zone_new_name << ": " << cpp_strerror(-ret)
	       << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
        ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                  zonegroup_id, zonegroup_name,
                                  zonegroup, &zonegroup_writer);
	if (ret < 0) {
	  cerr << "WARNING: failed to load zonegroup " << zonegroup_name << std::endl;
          return EXIT_SUCCESS;
	}

        auto z = zonegroup.zones.find(zone_params.id);
        if (z == zonegroup.zones.end()) {
          return EXIT_SUCCESS;
        }
        z->second.name = zone_params.name;

        ret = zonegroup_writer->write(dpp, null_yield, zonegroup);
        if (ret < 0) {
          cerr << "Error in zonegroup rename for " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
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
        if (placement_id.empty()) {
          cerr << "ERROR: --placement-id not specified" << std::endl;
          return EINVAL;
        }
        // validate compression type
        if (compression_type && *compression_type != "random"
            && !Compressor::get_comp_alg_type(*compression_type)) {
          std::cerr << "Unrecognized compression type" << std::endl;
          return EINVAL;
        }

	RGWZoneParams zone;
        std::unique_ptr<rgw::sal::ZoneWriter> writer;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 zone_id, zone_name, zone, &writer);
        if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

#ifdef WITH_RADOSGW_RADOS
        if (command == OPT::ZONE_PLACEMENT_ADD ||
	    command == OPT::ZONE_PLACEMENT_MODIFY) {
	  RGWZoneGroup zonegroup;
          ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                    zonegroup_id, zonegroup_name, zonegroup);
	  if (ret < 0) {
	    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }

	  auto ptiter = zonegroup.placement_targets.find(placement_id);
	  if (ptiter == zonegroup.placement_targets.end()) {
	    cerr << "ERROR: placement id '" << placement_id << "' is not configured in zonegroup placement targets" << std::endl;
	    return EINVAL;
	  }

	  string storage_class = rgw_placement_rule::get_canonical_storage_class(opt_storage_class.value_or(string()));
	  if (ptiter->second.storage_classes.find(storage_class) == ptiter->second.storage_classes.end()) {
	    cerr << "ERROR: storage class '" << storage_class << "' is not defined in zonegroup '" << placement_id << "' placement target" << std::endl;
	    return EINVAL;
	  }
	  if (ptiter->second.tier_targets.find(storage_class) != ptiter->second.tier_targets.end()) {
	    cerr << "ERROR: storage class '" << storage_class << "' is of tier type in zonegroup '" << placement_id << "' placement target" << std::endl;
	    return EINVAL;
	  }

          RGWZonePlacementInfo& info = zone.placement_pools[placement_id];

	  string opt_index_pool = index_pool.value_or(string());
	  string opt_data_pool = data_pool.value_or(string());

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
          info.storage_classes.set_storage_class(storage_class, &dp, compression_type.get_ptr());

          if (data_extra_pool) {
            info.data_extra_pool = *data_extra_pool;
          }
          if (index_type_specified) {
	    info.index_type = placement_index_type;
          }
          if (placement_inline_data_specified) {
            info.inline_data = placement_inline_data;
          }

          ret = check_pool_support_omap(info.get_data_extra_pool());
          if (ret < 0) {
             cerr << "ERROR: the data extra (non-ec) pool '" << info.get_data_extra_pool() 
                 << "' does not support omap" << std::endl;
             return ret;
          }
        } else 
#endif
	    if (command == OPT::ZONE_PLACEMENT_RM) {
          if (!opt_storage_class ||
              opt_storage_class->empty()) {
            zone.placement_pools.erase(placement_id);
          } else {
            auto iter = zone.placement_pools.find(placement_id);
            if (iter != zone.placement_pools.end()) {
              RGWZonePlacementInfo& info = zone.placement_pools[placement_id];
              info.storage_classes.remove_storage_class(*opt_storage_class);
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
                                 zone_id, zone_name, zone);
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
	if (placement_id.empty()) {
	  cerr << "ERROR: --placement-id not specified" << std::endl;
	  return EINVAL;
	}

	RGWZoneParams zone;
        int ret = rgw::read_zone(dpp, null_yield, cfgstore,
                                 zone_id, zone_name, zone);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	auto p = zone.placement_pools.find(placement_id);
	if (p == zone.placement_pools.end()) {
	  cerr << "ERROR: zone placement target '" << placement_id << "' not found" << std::endl;
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
