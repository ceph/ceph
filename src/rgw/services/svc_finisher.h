#ifndef CEPH_RGW_SERVICES_FINISHER_H
#define CEPH_RGW_SERVICES_FINISHER_H


#include "rgw/rgw_service.h"

class Context;
class Finisher;

class RGWSI_Finisher : public RGWServiceInstance
{
  friend struct RGWServices_Def;
public:
  class ShutdownCB;

private:
  Finisher *finisher{nullptr};
  bool finalized{false};

  void shutdown() override;

  std::map<int, ShutdownCB *> shutdown_cbs;
  std::atomic<int> handles_counter;

protected:
  void init() {}
  int do_start() override;

public:
  RGWSI_Finisher(CephContext *cct): RGWServiceInstance(cct) {}
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
