#ifndef CEPH_RGW_SERVICE_H
#define CEPH_RGW_SERVICE_H


#include <string>
#include <vector>
#include <memory>

#include "rgw/rgw_common.h"


class CephContext;
class JSONFormattable;
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
  virtual int create_instance(JSONFormattable& conf, RGWServiceInstanceRef *instance) = 0;
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

  virtual std::vector<std::string> get_deps() {
    return vector<std::string>();
  }
  virtual int init(JSONFormattable& conf) = 0;
public:
  RGWServiceInstance(RGWService *svc, CephContext *_cct) : cct(_cct) {}

  virtual ~RGWServiceInstance();

  string get_title() {
    return svc->type() + ":" + svc_instance;
  }
};

class RGWServiceRegistry : std::enable_shared_from_this<RGWServiceRegistry> {
  map<string, RGWServiceRef> services;

  struct instance_info {
    uint64_t id;
    string title;
    JSONFormattable conf;
    RGWServiceInstanceRef ref;
  };
  map<uint64_t, instance_info> instances; /* registry_id -> instance */

  std::atomic<uint64_t> max_registry_id;

  void register_all(CephContext *cct);
public:
  RGWServiceRegistry(CephContext *cct) {
    register_all(cct);
  }
  bool find(const string& name, RGWServiceRef *svc);

  int instantiate(RGWServiceRegistryRef& registry, RGWServiceRef& svc, JSONFormattable& conf);
  void remove_instance(RGWServiceInstance *instance);
};

#endif
