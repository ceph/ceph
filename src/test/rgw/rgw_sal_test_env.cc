// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

#include "rgw_sal_test_env.h"

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

#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#define dout_subsys ceph_subsys_rgw

namespace {

struct TestEnv {
  boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<NoDoutPrefix> dpp;
  std::unique_ptr<ceph::async::io_context_pool> context_pool;
  std::unique_ptr<rgw::sal::ConfigStore> cfgstore;
  rgw::SiteConfig site;
  rgw::sal::Driver* driver = nullptr;
  std::string backend;

  // buckets created via rgw_test_env_create_bucket() that have not been
  // removed yet -- torn down at exit so a panicking test leaks nothing
  std::mutex buckets_lock;
  std::set<std::pair<std::string, std::string>> buckets;
};

TestEnv g_env;
std::once_flag g_init_once;
int g_init_ret = -EINVAL;

void env_teardown();

// Runs once, under g_init_once.
void env_init()
{
  // libtest owns argv, so there is nothing to parse here; global_pre_init()
  // still picks up $CEPH_CONF and $CEPH_ARGS from the environment.
  std::vector<const char*> args;

  g_env.cct = rgw_global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                              CODE_ENVIRONMENT_UTILITY, 0);

  // region -> zonegroup conversion (must happen before common_init_finish)
  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  common_init_finish(g_ceph_context);

  g_env.dpp = std::make_unique<NoDoutPrefix>(g_ceph_context, ceph_subsys_rgw);

  // io_context_pool provides the worker threads RADOS async ops need
  g_env.context_pool = std::make_unique<ceph::async::io_context_pool>(
      g_env.cct->_conf->rgw_thread_pool_size);

  // backend is read from ceph.conf via rgw_backend_store
  DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);
  g_env.backend = cfg.store_name;
  std::cerr << "INFO: using backend '" << cfg.store_name << "'" << std::endl;

  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  g_env.cfgstore = DriverManager::create_config_store(g_env.dpp.get(),
                                                      config_store_type);
  if (!g_env.cfgstore) {
    std::cerr << "ERROR: failed to create config store" << std::endl;
    g_init_ret = -EIO;
    return;
  }

  int r = g_env.site.load(g_env.dpp.get(), null_yield, g_env.cfgstore.get());
  if (r < 0) {
    std::cerr << "ERROR: failed to load site config (r=" << r << ")" << std::endl;
    g_init_ret = r;
    return;
  }

  // all background threads disabled -- tests drive the driver synchronously
  g_env.driver = DriverManager::get_storage(g_env.dpp.get(),
                                            g_ceph_context,
                                            cfg,
                                            *g_env.context_pool,
                                            g_env.site,
                                            false,  // use_gc_thread
                                            false,  // use_lc_thread
                                            false,  // use_restore_thread
                                            false,  // quota_threads
                                            false,  // run_sync_thread
                                            false,  // run_reshard_thread
                                            false,  // run_notification_thread
                                            false,  // run_bucket_logging_thread
                                            false,  // background_tasks
                                            null_yield,
                                            g_env.cfgstore.get(),
                                            false); // use_cache
  if (!g_env.driver) {
    std::cerr << "ERROR: failed to initialize SAL driver" << std::endl;
    g_init_ret = -EIO;
    return;
  }

  // libtest has no global teardown hook, so hang teardown off atexit().  It
  // was registered after every static ctor ran, so it fires before any of
  // their dtors -- the driver is torn down while ceph is still up.
  std::atexit(env_teardown);

  g_init_ret = 0;
}

void env_teardown()
{
  if (!g_env.driver) {
    return;
  }

  decltype(g_env.buckets) leftovers;
  {
    std::lock_guard l{g_env.buckets_lock};
    leftovers.swap(g_env.buckets);
  }
  for (const auto& [name, tenant] : leftovers) {
    rgw_bucket b;
    b.name = name;
    b.tenant = tenant;
    std::unique_ptr<rgw::sal::Bucket> bucket;
    if (g_env.driver->load_bucket(g_env.dpp.get(), b, &bucket, null_yield) == 0
        && bucket) {
      bucket->remove(g_env.dpp.get(), true, null_yield);
    }
  }

  g_env.driver->shutdown();
  DriverManager::close_storage(g_env.driver);
  g_env.driver = nullptr;
}

} // namespace

int rgw_test_env_init(void)
{
  std::call_once(g_init_once, env_init);
  return g_init_ret;
}

CRgwDriver* rgw_test_env_driver(void)
{
  return reinterpret_cast<CRgwDriver*>(g_env.driver);
}

const CRgwDoutPrefix* rgw_test_env_dpp(void)
{
  if (!g_env.driver) {
    return nullptr;
  }
  return reinterpret_cast<const CRgwDoutPrefix*>(
      static_cast<const DoutPrefixProvider*>(g_env.dpp.get()));
}

const char* rgw_test_env_backend(void)
{
  return g_env.driver ? g_env.backend.c_str() : nullptr;
}

int rgw_test_env_create_bucket(const char* name, const char* tenant)
{
  if (!g_env.driver || !name) {
    return -EINVAL;
  }
  const std::string tenant_str = tenant ? tenant : "";

  rgw_bucket b;
  b.name = name;
  b.tenant = tenant_str;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = g_env.driver->load_bucket(g_env.dpp.get(), b, &bucket, null_yield);
  if (!bucket) {
    std::cerr << "ERROR: load_bucket returned no bucket object (r=" << r << ")"
              << std::endl;
    return r < 0 ? r : -EIO;
  }

  rgw::sal::Bucket::CreateParams params;
  params.owner = rgw_user{tenant_str, "sal-test-env-user"};
  params.zonegroup_id = g_env.site.get_zonegroup().get_id();
  params.placement_rule = g_env.site.get_zonegroup().default_placement;
  params.zone_placement = rgw::find_zone_placement(
      g_env.dpp.get(), g_env.site.get_zone_params(), params.placement_rule);

  r = bucket->create(g_env.dpp.get(), params, null_yield);
  if (r < 0 && r != -EEXIST) {
    std::cerr << "ERROR: failed to create bucket '" << name << "' (r=" << r
              << ")" << std::endl;
    return r;
  }

  std::lock_guard l{g_env.buckets_lock};
  g_env.buckets.emplace(name, tenant_str);
  return 0;
}

int rgw_test_env_remove_bucket(const char* name, const char* tenant)
{
  if (!g_env.driver || !name) {
    return -EINVAL;
  }
  const std::string tenant_str = tenant ? tenant : "";

  rgw_bucket b;
  b.name = name;
  b.tenant = tenant_str;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = g_env.driver->load_bucket(g_env.dpp.get(), b, &bucket, null_yield);
  if (r < 0 || !bucket) {
    return r < 0 ? r : -ENOENT;
  }

  r = bucket->remove(g_env.dpp.get(), true, null_yield);
  if (r < 0) {
    return r;
  }

  std::lock_guard l{g_env.buckets_lock};
  g_env.buckets.erase({name, tenant_str});
  return 0;
}
