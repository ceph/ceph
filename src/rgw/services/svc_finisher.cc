// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/Finisher.h"

#include "svc_finisher.h"

using namespace std;

int RGWSI_Finisher::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  finisher = new Finisher(cct);
  finisher->start();

  return 0;
}

void RGWSI_Finisher::shutdown()
{
  if (finalized) {
    return;
  }

  if (finisher) {
    finisher->stop();

    map<int, ShutdownCB *> cbs;
    cbs.swap(shutdown_cbs); /* move cbs out, in case caller unregisters */
    for (auto& iter : cbs) {
      iter.second->call();
    }
    delete finisher;
  }

  finalized = true;
}

RGWSI_Finisher::~RGWSI_Finisher()
{
  shutdown();
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

