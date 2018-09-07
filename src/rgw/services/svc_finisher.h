#ifndef CEPH_RGW_SERVICES_FINISHER_H
#define CEPH_RGW_SERVICES_FINISHER_H


#include "rgw/rgw_service.h"

class Context;
class Finisher;

class RGWS_Finisher : public RGWService
{
public:
  RGWS_Finisher(CephContext *cct) : RGWService(cct, "finisher") {}

  int create_instance(const std::string& conf, RGWServiceInstanceRef *instance) override;
};

class RGWSI_Finisher : public RGWServiceInstance
{
public:
  class ShutdownCB;

private:
  Finisher *finisher{nullptr};
  bool finalized{false};

  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override {
    return 0;
  }
  int init() override;
  void shutdown() override;

  std::map<int, ShutdownCB *> shutdown_cbs;
  std::atomic<int> handles_counter;

public:
  RGWSI_Finisher(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}
  ~RGWSI_Finisher();

  class ShutdownCB {
  public:
      virtual ~ShutdownCB() {}
      virtual void call() = 0;
  };

  void register_caller(ShutdownCB *cb, int *phandle);
  void unregister_caller(int handle);

  void schedule_context(Context *c);
};

#endif
