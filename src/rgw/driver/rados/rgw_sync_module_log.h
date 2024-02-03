// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_sync_module.h"

class RGWLogSyncModule : public RGWSyncModule {
public:
  RGWLogSyncModule() {}
  bool supports_data_export() override {
    return false;
  }
  int create_instance(const DoutPrefixProvider *dpp, CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};
