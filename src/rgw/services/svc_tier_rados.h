// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include "rgw/rgw_service.h"

#include "svc_rados.h"

#define MP_META_SUFFIX ".meta"

class RGWMPObj {
  string oid;
  string prefix;
  string meta;
  string upload_id;
public:
  RGWMPObj() {}
  RGWMPObj(const string& _oid, const string& _upload_id) {
    init(_oid, _upload_id, _upload_id);
  }
  void init(const string& _oid, const string& _upload_id) {
    init(_oid, _upload_id, _upload_id);
  }
  void init(const string& _oid, const string& _upload_id, const string& part_unique_str) {
    if (_oid.empty()) {
      clear();
      return;
    }
    oid = _oid;
    upload_id = _upload_id;
    prefix = oid + ".";
    meta = prefix + upload_id + MP_META_SUFFIX;
    prefix.append(part_unique_str);
  }
  string& get_meta() { return meta; }
  string get_part(int num) {
    char buf[16];
    snprintf(buf, 16, ".%d", num);
    string s = prefix;
    s.append(buf);
    return s;
  }
  string get_part(string& part) {
    string s = prefix;
    s.append(".");
    s.append(part);
    return s;
  }
  string& get_upload_id() {
    return upload_id;
  }
  string& get_key() {
    return oid;
  }
  bool from_meta(string& meta) {
    int end_pos = meta.rfind('.'); // search for ".meta"
    if (end_pos < 0)
      return false;
    int mid_pos = meta.rfind('.', end_pos - 1); // <key>.<upload_id>
    if (mid_pos < 0)
      return false;
    oid = meta.substr(0, mid_pos);
    upload_id = meta.substr(mid_pos + 1, end_pos - mid_pos - 1);
    init(oid, upload_id, upload_id);
    return true;
  }
  void clear() {
    oid = "";
    prefix = "";
    meta = "";
    upload_id = "";
  }
};

class MultipartMetaFilter : public RGWAccessListFilter {
public:
  MultipartMetaFilter() {}
  bool filter(string& name, string& key) override {
    int len = name.size();
    if (len < 6)
      return false;

    size_t pos = name.find(MP_META_SUFFIX, len - 5);
    if (pos == string::npos)
      return false;

    pos = name.rfind('.', pos - 1);
    if (pos == string::npos)
      return false;

    key = name.substr(0, pos);

    return true;
  }
};

class RGWSI_Tier_RADOS : public RGWServiceInstance
{
  RGWSI_Zone *zone_svc{nullptr};

public:
  RGWSI_Tier_RADOS(CephContext *cct): RGWServiceInstance(cct) {}

  void init(RGWSI_Zone *_zone_svc) {
    zone_svc = _zone_svc;
  }

  static inline bool raw_obj_to_obj(const rgw_bucket& bucket, const rgw_raw_obj& raw_obj, rgw_obj *obj) {
    ssize_t pos = raw_obj.oid.find('_');
    if (pos < 0) {
      return false;
    }

    if (!rgw_obj_key::parse_raw_oid(raw_obj.oid.substr(pos + 1), &obj->key)) {
      return false;
    }
    obj->bucket = bucket;

    return true;
  }
};

