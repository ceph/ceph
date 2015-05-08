// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_FRONTEND_H
#define RGW_FRONTEND_H

#include "rgw_request.h"
#include "rgw_process.h"

#include "rgw_civetweb.h"
#include "rgw_civetweb_log.h"
#include "civetweb/civetweb.h"

#define dout_subsys ceph_subsys_rgw

class RGWFrontendConfig {
  string config;
  map<string, string> config_map;
  int parse_config(const string& config, map<string, string>& config_map);
  string framework;
public:
  RGWFrontendConfig(const string& _conf) : config(_conf) {}
  int init() {
    int ret = parse_config(config, config_map);
    if (ret < 0)
      return ret;
    return 0;
  }
  bool get_val(const string& key, const string& def_val, string *out);
  bool get_val(const string& key, int def_val, int *out);

  map<string, string>& get_config_map() { return config_map; }

  string get_framework() { return framework; }
};

class RGWFrontend {
public:
  virtual ~RGWFrontend() {}

  virtual int init() = 0;

  virtual int run() = 0;
  virtual void stop() = 0;
  virtual void join() = 0;
};

class RGWMongooseFrontend : public RGWFrontend {
  RGWFrontendConfig* conf;
  struct mg_context* ctx;
  RGWProcessEnv env;

  void set_conf_default(map<string, string>& m, const string& key,
			const string& def_val) {
    if (m.find(key) == m.end()) {
      m[key] = def_val;
    }
  }

public:
  RGWMongooseFrontend(RGWProcessEnv& pe, RGWFrontendConfig* _conf)
    : conf(_conf), ctx(nullptr), env(pe) {
  }

  int init() {
    return 0;
  }

  int run();

  void stop() {
    if (ctx) {
      mg_stop(ctx);
    }
  }

  void join() {
  }
}; /* RGWMongooseFrontend */

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

  ~RGWProcessFrontend() {
    delete thread;
    delete pprocess;
  }

  int run() {
    assert(pprocess); /* should have initialized by init() */
    thread = new RGWProcessControlThread(pprocess);
    thread->create("rgw_frontend");
    return 0;
  }

  void stop() {
    pprocess->close_fd();
    thread->kill(SIGUSR1);
  }

  void join() {
    thread->join();
  }
}; /* RGWProcessFrontend */

class RGWFCGXFrontend : public RGWProcessFrontend {
public:
  RGWFCGXFrontend(RGWProcessEnv& pe, RGWFrontendConfig* _conf)
    : RGWProcessFrontend(pe, _conf) {}

  int init() {
    pprocess = new RGWFCGXProcess(g_ceph_context, &env,
				  g_conf->rgw_thread_pool_size, conf);
    return 0;
  }
}; /* RGWFCGXFrontend */

class RGWLoadGenFrontend : public RGWProcessFrontend {
public:
  RGWLoadGenFrontend(RGWProcessEnv& pe, RGWFrontendConfig *_conf)
    : RGWProcessFrontend(pe, _conf) {}

  int init() {
    int num_threads;
    conf->get_val("num_threads", g_conf->rgw_thread_pool_size, &num_threads);
    RGWLoadGenProcess *pp = new RGWLoadGenProcess(g_ceph_context, &env,
						  num_threads, conf);

    pprocess = pp;

    string uid_str;
    conf->get_val("uid", "", &uid_str);
    if (uid_str.empty()) {
      derr << "ERROR: uid param must be specified for loadgen frontend"
	   << dendl;
      return EINVAL;
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

#endif /* RGW_FRONTEND_H */
