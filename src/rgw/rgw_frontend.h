// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/RWLock.h"

#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_process_env.h"
#include "rgw_realm_reloader.h"

#include "rgw_auth_registry.h"
#include "rgw_sal_rados.h"

#define dout_context g_ceph_context

namespace rgw::dmclock {
  class SyncScheduler;
  class ClientConfig;
  class SchedulerCtx;
}

class RGWFrontendConfig {
  std::string config;
  std::multimap<std::string, std::string> config_map;
  std::string framework;

  int parse_config(const std::string& config,
                   std::multimap<std::string, std::string>& config_map);

public:
  explicit RGWFrontendConfig(const std::string& config)
    : config(config) {
  }

  int init() {
    const int ret = parse_config(config, config_map);
    return ret < 0 ? ret : 0;
  }

  void set_default_config(RGWFrontendConfig& def_conf);

  std::optional<std::string> get_val(const std::string& key);

  bool get_val(const std::string& key,
               const std::string& def_val,
               std::string* out);
  bool get_val(const std::string& key, int def_val, int *out);

  std::string get_val(const std::string& key,
                      const std::string& def_val) {
    std::string out;
    get_val(key, def_val, &out);
    return out;
  }

  const std::string& get_config() {
    return config;
  }

  std::multimap<std::string, std::string>& get_config_map() {
    return config_map;
  }

  std::string get_framework() const {
    return framework;
 }
};

class RGWFrontend {
public:
  virtual ~RGWFrontend() {}

  virtual int init() = 0;

  virtual int run() = 0;
  virtual void stop() = 0;
  virtual void join() = 0;

  virtual void pause_for_new_config() = 0;
  virtual void unpause_with_new_config() = 0;
};


class RGWProcessFrontend : public RGWFrontend {
protected:
  RGWFrontendConfig* conf;
  RGWProcess* pprocess;
  RGWProcessEnv& env;
  RGWProcessControlThread* thread;

public:
  RGWProcessFrontend(RGWProcessEnv& pe, RGWFrontendConfig* _conf)
    : conf(_conf), pprocess(nullptr), env(pe), thread(nullptr) {
  }

  ~RGWProcessFrontend() override {
    delete thread;
    delete pprocess;
  }

  int run() override {
    ceph_assert(pprocess); /* should have initialized by init() */
    thread = new RGWProcessControlThread(pprocess);
    thread->create("rgw_frontend");
    return 0;
  }

  void stop() override;

  void join() override {
    thread->join();
  }

  void pause_for_new_config() override {
    pprocess->pause();
  }

  void unpause_with_new_config() override {
    pprocess->unpause_with_new_config();
  }
}; /* RGWProcessFrontend */

class RGWLoadGenFrontend : public RGWProcessFrontend, public DoutPrefixProvider {
public:
  RGWLoadGenFrontend(RGWProcessEnv& pe, RGWFrontendConfig *_conf)
    : RGWProcessFrontend(pe, _conf) {}

  CephContext *get_cct() const {
    return env.driver->ctx();
  }

  unsigned get_subsys() const
  {
    return ceph_subsys_rgw;
  }

  std::ostream& gen_prefix(std::ostream& out) const
  {
    return out << "rgw loadgen frontend: ";
  }

  int init() override {
    int num_threads;
    conf->get_val("num_threads", g_conf()->rgw_thread_pool_size, &num_threads);
    std::string uri_prefix;
    conf->get_val("prefix", "", &uri_prefix);

    RGWLoadGenProcess *pp = new RGWLoadGenProcess(
        g_ceph_context, env, num_threads, std::move(uri_prefix), conf);

    pprocess = pp;

    std::string uid_str;
    conf->get_val("uid", "", &uid_str);
    if (uid_str.empty()) {
      derr << "ERROR: uid param must be specified for loadgen frontend"
	   << dendl;
      return -EINVAL;
    }

    rgw_user uid(uid_str);
    std::unique_ptr<rgw::sal::User> user = env.driver->get_user(uid);

    int ret = user->load_user(this, null_yield);
    if (ret < 0) {
      derr << "ERROR: failed reading user info: uid=" << uid << " ret="
	   << ret << dendl;
      return ret;
    }

    auto aiter = user->get_info().access_keys.begin();
    if (aiter == user->get_info().access_keys.end()) {
      derr << "ERROR: user has no S3 access keys set" << dendl;
      return -EINVAL;
    }

    pp->set_access_key(aiter->second);

    return 0;
  }
}; /* RGWLoadGenFrontend */

// FrontendPauser implementation for RGWRealmReloader
class RGWFrontendPauser : public RGWRealmReloader::Pauser {
  std::vector<RGWFrontend*> &frontends;
  RGWRealmReloader::Pauser* pauser;

 public:
  RGWFrontendPauser(std::vector<RGWFrontend*> &frontends,
                    RGWRealmReloader::Pauser* pauser = nullptr)
    : frontends(frontends), pauser(pauser) {}

  void pause() override {
    for (auto frontend : frontends)
      frontend->pause_for_new_config();
    if (pauser)
      pauser->pause();
  }
  void resume(rgw::sal::Driver* driver) override {
    for (auto frontend : frontends)
      frontend->unpause_with_new_config();
    if (pauser)
      pauser->resume(driver);
  }
};
