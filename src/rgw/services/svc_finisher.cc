#include "common/Finisher.h"

#include "svc_finisher.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

int RGWS_Finisher::create_instance(const string& conf, RGWServiceInstanceRef *instance)
{
  instance->reset(new RGWSI_Finisher(this, cct));
  return 0;
}

std::map<string, RGWServiceInstance::dependency> RGWSI_Finisher::get_deps()
{
  std::map<string, RGWServiceInstance::dependency> dep;
  return dep;
}

int RGWSI_Finisher::init()
{
  finisher = new Finisher(cct);
  finisher->start();

  return 0;
}

void RGWSI_Finisher::shutdown()
{
  if (finisher) {
    finisher->stop();

    map<int, ShutdownCB *> cbs;
    cbs.swap(shutdown_cbs); /* move cbs out, in case caller unregisetrs */
    for (auto& iter : cbs) {
      iter.second->call();
    }
    delete finisher;
  }
}

void RGWSI_Finisher::register_caller(ShutdownCB *cb, int *phandle)
{
  *phandle = ++handles_counter;
  shutdown_cbs[*phandle] = cb;
}

void RGWSI_Finisher::unregister_caller(int handle)
{
  shutdown_cbs.erase(handle);
}

void RGWSI_Finisher::schedule_context(Context *c)
{
  finisher->queue(c);
}

