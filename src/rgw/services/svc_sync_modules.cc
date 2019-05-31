// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "svc_sync_modules.h"

#include "rgw/rgw_sync_module.h"

void RGWSI_SyncModules::init()
{
  sync_modules_manager = new RGWSyncModulesManager();
  rgw_register_sync_modules(sync_modules_manager);
}

RGWSI_SyncModules::~RGWSI_SyncModules()
{
  delete sync_modules_manager;
}

