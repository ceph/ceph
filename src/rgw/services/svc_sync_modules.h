// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "rgw/rgw_service.h"


class RGWSyncModulesManager;

class RGWSI_SyncModules : public RGWServiceInstance
{
  RGWSyncModulesManager *sync_modules_manager{nullptr};

public:
  RGWSI_SyncModules(CephContext *cct): RGWServiceInstance(cct) {}
  ~RGWSI_SyncModules();

  RGWSyncModulesManager *get_manager() {
    return sync_modules_manager;
  }

  void init();
};
