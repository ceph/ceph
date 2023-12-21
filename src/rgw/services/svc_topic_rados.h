// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_pubsub.h"
#include "rgw_service.h"
#include "svc_meta_be.h"

class RGWSI_Topic_RADOS : public RGWServiceInstance {
 public:
  struct Svc {
    RGWSI_Zone* zone{nullptr};
    RGWSI_Meta* meta{nullptr};
    RGWSI_MetaBackend* meta_be{nullptr};
    RGWSI_SysObj* sysobj{nullptr};
  } svc;

  RGWSI_Topic_RADOS(CephContext* cct) : RGWServiceInstance(cct) {}
  ~RGWSI_Topic_RADOS() {}

  void init(RGWSI_Zone* _zone_svc,
            RGWSI_Meta* _meta_svc,
            RGWSI_MetaBackend* _meta_be_svc,
            RGWSI_SysObj* _sysobj_svc);

  RGWSI_MetaBackend_Handler* get_be_handler();
  int do_start(optional_yield y, const DoutPrefixProvider* dpp) override;

 private:
  RGWSI_MetaBackend_Handler* be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> be_module;
};

class RGWTopicMetadataObject : public RGWMetadataObject {
  rgw_pubsub_topic topic;
  rgw::sal::Driver* driver;

 public:
  RGWTopicMetadataObject() = default;
  RGWTopicMetadataObject(rgw_pubsub_topic& topic, const obj_version& v,
                         real_time m, rgw::sal::Driver* driver)
      : RGWMetadataObject(v, m), topic(topic), driver(driver) {}

  void dump(Formatter* f) const override { topic.dump(f); }

  rgw_pubsub_topic& get_topic_info() { return topic; }

  rgw::sal::Driver* get_driver() { return driver; }
};
class RGWTopicMetadataHandler : public RGWMetadataHandler_GenericMetaBE {
 public:
  RGWTopicMetadataHandler(rgw::sal::Driver* driver,
                          RGWSI_Topic_RADOS* role_svc);

  std::string get_type() final { return "topic"; }

  RGWMetadataObject* get_meta_obj(JSONObj* jo, const obj_version& objv,
                                  const ceph::real_time& mtime);

  int do_get(RGWSI_MetaBackend_Handler::Op* op, std::string& entry,
             RGWMetadataObject** obj, optional_yield y,
             const DoutPrefixProvider* dpp) final;

  int do_remove(RGWSI_MetaBackend_Handler::Op* op, std::string& entry,
                RGWObjVersionTracker& objv_tracker, optional_yield y,
                const DoutPrefixProvider* dpp) final;

  int do_put(RGWSI_MetaBackend_Handler::Op* op, std::string& entr,
             RGWMetadataObject* obj, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider* dpp,
             RGWMDLogSyncType type, bool from_remote_zone) override;

 private:
  rgw::sal::Driver* driver;
  RGWSI_Topic_RADOS* topic_svc;
};

std::string get_topic_key(const std::string& topic_name,
                          const std::string& tenant);

void parse_topic_entry(const std::string& topic_entry,
                       std::string* tenant_name,
                       std::string* topic_name);

std::string get_bucket_topic_mapping_oid(const rgw_pubsub_topic& topic);