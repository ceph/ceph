#ifndef CEPH_RGW_SERVICE_H
#define CEPH_RGW_SERVICE_H


#include <string>
#include <vector>
#include <memory>

#include "rgw/rgw_common.h"


class CephContext;
class RGWServiceInstance;
class RGWServiceRegistry;

using RGWServiceInstanceRef = std::shared_ptr<RGWServiceInstance>;
using RGWServiceRegistryRef = std::shared_ptr<RGWServiceRegistry>;

class RGWService
{
  friend class RGWServiceRegistry;
  friend class RGWServiceInstance;

protected:
  RGWServiceRegistryRef svc_registry;
  CephContext *cct;
  std::string svc_type;

public:
  RGWService(CephContext *_cct, const std::string& _svc_type) : cct(_cct),
                                                           svc_type(_svc_type) {}
  virtual ~RGWService() = default;

  const std::string& type() {
    return svc_type;
  }
  virtual int create_instance(const string& conf, RGWServiceInstanceRef *instance) = 0;
};


using RGWServiceRef = std::shared_ptr<RGWService>;


class RGWServiceInstance
{
  friend class RGWServiceRegistry;
protected:
  CephContext *cct;
  std::shared_ptr<RGWService> svc;
  string svc_instance;
  uint64_t svc_id{0};

  struct dependency {
    string name;
    string conf;
  };

  virtual std::map<std::string, dependency> get_deps() {
    return std::map<std::string, dependency>();
  }
  virtual int load(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) = 0;
  virtual int init() {
    return 0;
  }
  virtual void shutdown() {}
public:
  RGWServiceInstance(RGWService *svc, CephContext *_cct) : cct(_cct) {}

  virtual ~RGWServiceInstance();

  CephContext *ctx() {
    return cct;
  }

  string get_title() {
    return svc->type() + ":" + svc_instance;
  }

  std::shared_ptr<RGWService>& get_svc() {
    return svc;
  }
};

class RGWServiceRegistry {
  CephContext *cct;

  map<string, RGWServiceRef> services;

  struct instance_info {
    string conf_id;
    uint64_t id;
    string title;
    string conf;
    RGWServiceInstanceRef ref;
  };
  map<uint64_t, instance_info> instances; /* registry_id -> instance */
  map<string, instance_info> instances_by_conf; /* conf_id -> instance */

  std::atomic<uint64_t> max_registry_id;

  string get_conf_id(const string& service_type, const string& conf);
  void register_all(CephContext *cct);

  int do_get_instance(RGWServiceRef& svc,
                      const string& conf,
                      RGWServiceInstanceRef *ref,
                      std::vector<RGWServiceInstanceRef> *new_instances);
  template <class T>
  int do_get_instance(const string& svc_name,
                   const string& conf,
                   T *ref,
                   std::vector<RGWServiceInstanceRef> *new_instances) {
    auto iter = services.find(svc_name);
    if (iter == services.end()) {
      return -ENOENT;
    }
    return do_get_instance(iter->second, conf, ref, new_instances);
  }
public:
  RGWServiceRegistry(CephContext *_cct) : cct(_cct) {
    register_all(cct);
  }
  bool find(const string& name, RGWServiceRef *svc);

  /* returns existing or creates a new one */
  int get_instance(RGWServiceRef& svc,
                   const string& conf,
                   RGWServiceInstanceRef *ref); /* returns existing or creates a new one */

  /* returns existing or creates a new one */
  template <class T>
  int get_instance(RGWServiceRef& svc,
                   const string& conf,
                   T *ref) {
    RGWServiceInstanceRef r;
    int ret = get_instance(svc, conf, &r);
    if (ret < 0) {
      return ret;
    }
    *ref = std::static_pointer_cast<typename T::element_type>(r);
    return 0;
  }

  template <class T>
  int get_instance(const string& svc_name,
                   const string& conf,
                   T *ref) {
    auto iter = services.find(svc_name);
    if (iter == services.end()) {
      return -ENOENT;
    }
    return get_instance(iter->second, conf, ref);
  }
  void remove_instance(RGWServiceInstance *instance);
};


#endif
