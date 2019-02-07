// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_FRONTEND_H
#define RGW_FRONTEND_H

#include <map>
#include <string>

#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_realm_reloader.h"

#include "rgw_civetweb.h"
#include "rgw_civetweb_log.h"
#include "civetweb/civetweb.h"
#include "rgw_auth_registry.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

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
  virtual void unpause_with_new_config(RGWRados* store,
                                       rgw_auth_registry_ptr_t auth_registry) = 0;
};


struct RGWMongooseEnv : public RGWProcessEnv {
  // every request holds a read lock, so we need to prioritize write locks to
  // avoid starving pause_for_new_config()
  static constexpr bool prioritize_write = true;
  RWLock mutex;

  explicit RGWMongooseEnv(const RGWProcessEnv &env)
    : RGWProcessEnv(env),
      mutex("RGWCivetWebFrontend", false, true, prioritize_write) {
  }
};


class RGWCivetWebFrontend : public RGWFrontend {
  RGWFrontendConfig* conf;
  struct mg_context* ctx;
  RGWMongooseEnv env;

  std::unique_ptr<rgw::dmclock::SyncScheduler> scheduler;
  std::unique_ptr<rgw::dmclock::ClientConfig> client_config;

  void set_conf_default(std::multimap<std::string, std::string>& m,
                        const std::string& key,
			const std::string& def_val) {
    if (m.find(key) == std::end(m)) {
      m.emplace(key, def_val);
    }
  }

  CephContext* cct() const { return env.store->ctx(); }
public:
  RGWCivetWebFrontend(RGWProcessEnv& env,
                      RGWFrontendConfig *conf,
		      rgw::dmclock::SchedulerCtx& sched_ctx);

  int init() override {
    return 0;
  }

  int run() override;

  int process(struct mg_connection* conn);

  void stop() override {
    if (ctx) {
      mg_stop(ctx);
    }
  }

  void join() override {
    return;
  }

  void pause_for_new_config() override {
    // block callbacks until unpause
    env.mutex.get_write();
  }

  void unpause_with_new_config(RGWRados* const store,
                               rgw_auth_registry_ptr_t auth_registry) override {
    env.store = store;
    env.auth_registry = std::move(auth_registry);
    // unpause callbacks
    env.mutex.put_write();
  }
}; /* RGWCivetWebFrontend */

class RGWProcessFrontend : public RGWFrontend {
protected:
  RGWFrontendConfig* conf;
  RGWProcess* pprocess;
  RGWProcessEnv env;
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

  void unpause_with_new_config(RGWRados* const store,
                               rgw_auth_registry_ptr_t auth_registry) override {
    env.store = store;
    env.auth_registry = auth_registry;
    pprocess->unpause_with_new_config(store, std::move(auth_registry));
  }
}; /* RGWProcessFrontend */

class RGWFCGXFrontend : public RGWProcessFrontend {
public:
  RGWFCGXFrontend(RGWProcessEnv& pe, RGWFrontendConfig* _conf)
    : RGWProcessFrontend(pe, _conf) {}

  int init() override {
    pprocess = new RGWFCGXProcess(g_ceph_context, &env,
				  g_conf()->rgw_thread_pool_size, conf);
    return 0;
  }
}; /* RGWFCGXFrontend */

class RGWLoadGenFrontend : public RGWProcessFrontend {
public:
  RGWLoadGenFrontend(RGWProcessEnv& pe, RGWFrontendConfig *_conf)
    : RGWProcessFrontend(pe, _conf) {}

  int init() override {
    int num_threads;
    conf->get_val("num_threads", g_conf()->rgw_thread_pool_size, &num_threads);
    RGWLoadGenProcess *pp = new RGWLoadGenProcess(g_ceph_context, &env,
						  num_threads, conf);

    pprocess = pp;

    string uid_str;
    conf->get_val("uid", "", &uid_str);
    if (uid_str.empty()) {
      derr << "ERROR: uid param must be specified for loadgen frontend"
	   << dendl;
      return -EINVAL;
    }

    rgw_user uid(uid_str);

    RGWUserInfo user_info;
    int ret = rgw_get_user_info_by_uid(env.store, uid, user_info, NULL);
    if (ret < 0) {
      derr << "ERROR: failed reading user info: uid=" << uid << " ret="
	   << ret << dendl;
      return ret;
    }

    map<string, RGWAccessKey>::iterator aiter = user_info.access_keys.begin();
    if (aiter == user_info.access_keys.end()) {
      derr << "ERROR: user has no S3 access keys set" << dendl;
      return -EINVAL;
    }

    pp->set_access_key(aiter->second);

    return 0;
  }
}; /* RGWLoadGenFrontend */

// FrontendPauser implementation for RGWRealmReloader
class RGWFrontendPauser : public RGWRealmReloader::Pauser {
  std::list<RGWFrontend*> &frontends;
  RGWRealmReloader::Pauser* pauser;

 public:
  RGWFrontendPauser(std::list<RGWFrontend*> &frontends,
                    RGWRealmReloader::Pauser* pauser = nullptr)
    : frontends(frontends), pauser(pauser) {}

  void pause() override {
    for (auto frontend : frontends)
      frontend->pause_for_new_config();
    if (pauser)
      pauser->pause();
  }
  void resume(RGWRados *store) override {
    /* Initialize the registry of auth strategies which will coordinate
     * the dynamic reconfiguration. */
    auto auth_registry = \
      rgw::auth::StrategyRegistry::create(g_ceph_context, store);

    for (auto frontend : frontends)
      frontend->unpause_with_new_config(store, auth_registry);
    if (pauser)
      pauser->resume(store);
  }
};

#endif /* RGW_FRONTEND_H */
