// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

// C++ harness for LanceDB ObjectStore integration tests.
// Initializes the SAL driver and test bucket, then calls into the Rust
// test runner (rgw_lancedb_object_store_run_tests) which exercises the
// ObjectStore trait through the real FFI boundary.

#include "rgw/rgw_sal.h"
#include "rgw/rgw_sal_config.h"
#include "rgw/rgw_bucket.h"
#include "rgw/rgw_zone.h"
#include "common/ceph_argparse.h"
#include "common/dout.h"
#include "common/async/context_pool.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/async/yield_context.h"

#include <iostream>
#include <string>

#define dout_subsys ceph_subsys_rgw

extern "C" {
  int rgw_lancedb_object_store_run_tests(void* driver, const void* dpp,
                                        const char* bucket, const char* tenant);
}

int main(int argc, char** argv) {
  bool has_conf = false;
  for (int i = 1; i < argc; i++) {
    if (std::string(argv[i]) == "-c" && i + 1 < argc) {
      has_conf = true;
      break;
    }
  }
  if (!has_conf) {
    std::cerr << "ERROR: -c <ceph.conf> is required." << std::endl;
    std::cerr << "Usage: " << argv[0] << " -c <path/to/ceph.conf>" << std::endl;
    return 1;
  }

  auto args = argv_to_vec(argc, const_cast<const char**>(argv));

  auto cct = rgw_global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY, 0);

  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  common_init_finish(g_ceph_context);

  DoutPrefix dpp(g_ceph_context, ceph_subsys_rgw, "lancedb-object-store-test: ");

  ceph::async::io_context_pool context_pool{
    cct->_conf->rgw_thread_pool_size};

  DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);
  std::cerr << "INFO: using backend '" << cfg.store_name << "'" << std::endl;

  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  auto cfgstore = DriverManager::create_config_store(&dpp, config_store_type);
  if (!cfgstore) {
    std::cerr << "ERROR: failed to create config store" << std::endl;
    return 1;
  }

  rgw::SiteConfig site;
  int r = site.load(&dpp, null_yield, cfgstore.get());
  if (r < 0) {
    std::cerr << "ERROR: failed to load site config (r=" << r << ")" << std::endl;
    return 1;
  }

  auto* driver = DriverManager::get_storage(&dpp,
                                            g_ceph_context,
                                            cfg,
                                            context_pool,
                                            site,
                                            false, false, false, false,
                                            false, false, false, false,
                                            false,
                                            null_yield,
                                            cfgstore.get(),
                                            false);
  if (!driver) {
    std::cerr << "ERROR: failed to initialize SAL driver" << std::endl;
    return 1;
  }

  // create test bucket
  std::string bucket_name = "lancedb-object-store-test-" + std::to_string(getpid());
  {
    rgw_bucket b;
    b.name = bucket_name;

    std::unique_ptr<rgw::sal::Bucket> bucket;
    r = driver->load_bucket(&dpp, b, &bucket, null_yield);

    if (bucket) {
      rgw::sal::Bucket::CreateParams params;
      rgw_user uid{"", "lancedb-object-store-test-user"};
      params.owner = uid;
      params.zonegroup_id = site.get_zonegroup().get_id();
      params.placement_rule = site.get_zonegroup().default_placement;
      params.zone_placement = rgw::find_zone_placement(
          &dpp, site.get_zone_params(), params.placement_rule);

      r = bucket->create(&dpp, params, null_yield);
      if (r < 0 && r != -EEXIST) {
        std::cerr << "ERROR: failed to create test bucket (r=" << r << ")" << std::endl;
        DriverManager::close_storage(driver);
        return 1;
      }
    } else {
      std::cerr << "ERROR: load_bucket returned no bucket object" << std::endl;
      DriverManager::close_storage(driver);
      return 1;
    }
  }

  // run LanceDB ObjectStore tests
  int ret = rgw_lancedb_object_store_run_tests(driver, &dpp,
                                      bucket_name.c_str(), nullptr);

  DriverManager::close_storage(driver);
  return ret;
}
