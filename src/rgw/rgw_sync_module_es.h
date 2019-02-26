// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_SYNC_MODULE_ES_H
#define CEPH_RGW_SYNC_MODULE_ES_H

#include "rgw_sync_module.h"

enum class ESType {
  /* string datatypes */
  String, /* Deprecated Since 5.X+ */
  Text,
  Keyword,

  /* Numeric Types */
  Long, Integer, Short, Byte, Double, Float, Half_Float, Scaled_Float,

  /* Date Type */
  Date,

  /* Boolean */
  Boolean,

  /* Binary; Must Be Base64 Encoded */
  Binary,

  /* Range Types */
  Integer_Range, Float_Range, Long_Range, Double_Range, Date_Range,

  /* A Few Specialized Types */
  Geo_Point,
  Ip
};


class RGWElasticSyncModule : public RGWSyncModule {
public:
  RGWElasticSyncModule() {}
  bool supports_data_export() override {
    return false;
  }
  int create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};

class RGWElasticDataSyncModule;
class RGWRESTConn;

class RGWElasticSyncModuleInstance : public RGWSyncModuleInstance {
  std::unique_ptr<RGWElasticDataSyncModule> data_handler;
public:
  RGWElasticSyncModuleInstance(CephContext *cct, const JSONFormattable& config);
  RGWDataSyncModule *get_data_handler() override;
  RGWRESTMgr *get_rest_filter(int dialect, RGWRESTMgr *orig) override;
  RGWRESTConn *get_rest_conn();
  std::string get_index_path();
  map<string, string>& get_request_headers();
  bool supports_user_writes() override {
    return true;
  }
};

#endif
