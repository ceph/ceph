// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include <iomanip>

#include "rgw_service.h"

extern const std::string MP_META_SUFFIX;

class RGWMPObj {
  std::string oid;
  std::string prefix;
  std::string meta;
  std::string upload_id;
public:
  RGWMPObj() {}
  RGWMPObj(const std::string& _oid, const std::string& _upload_id) {
    init(_oid, _upload_id, _upload_id);
  }
  RGWMPObj(const std::string& _oid, std::optional<std::string> _upload_id) {
    if (_upload_id) {
      init(_oid, *_upload_id, *_upload_id);
    } else {
      from_meta(_oid);
    }
  }
  void init(const std::string& _oid, const std::string& _upload_id) {
    init(_oid, _upload_id, _upload_id);
  }
  void init(const std::string& _oid, const std::string& _upload_id, const std::string& part_unique_str) {
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
  const std::string& get_meta() const { return meta; }
  std::string get_part(int num) const {
    char buf[16];
    snprintf(buf, 16, ".%d", num);
    std::string s = prefix;
    s.append(buf);
    return s;
  }
  std::string get_part(const std::string& part) const {
    std::string s = prefix;
    s.append(".");
    s.append(part);
    return s;
  }
  const std::string& get_upload_id() const {
    return upload_id;
  }
  const std::string& get_key() const {
    return oid;
  }
  bool from_meta(const std::string& meta) {
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
  friend std::ostream& operator<<(std::ostream& out, const RGWMPObj& obj) {
    return out << "RGWMPObj:{ prefix=" << std::quoted(obj.prefix) <<
      ", meta=" << std::quoted(obj.meta) << " }";
  }
}; // class RGWMPObj

/**
 * A filter to a) test whether an object name is a multipart meta
 * object, and b) filter out just the key used to determine the bucket
 * index shard.
 *
 * Objects for multipart meta have names adorned with an upload id and
 * other elements -- specifically a ".", MULTIPART_UPLOAD_ID_PREFIX,
 * unique id, and MP_META_SUFFIX. This filter will return true when
 * the name provided is such. It will also extract the key used for
 * bucket index shard calculation from the adorned name.
 */
/**
 * @param name [in] The object name as it appears in the bucket index.
 * @param key [out] An output parameter that will contain the bucket
 *        index key if this entry is in the form of a multipart meta object.
 * @return true if the name provided is in the form of a multipart meta
 *         object, false otherwise
 */
bool MultipartMetaFilter(const std::string& name, std::string& key);

class RGWSI_Tier_RADOS : public RGWServiceInstance
{
  RGWSI_Zone *zone_svc{nullptr};

public:
  RGWSI_Tier_RADOS(CephContext *cct): RGWServiceInstance(cct) {}

  void init(RGWSI_Zone *_zone_svc) {
    zone_svc = _zone_svc;
  }

  static inline bool raw_obj_to_obj(const rgw_bucket& bucket, const rgw_raw_obj& raw_obj, rgw_obj *obj) {
    ssize_t pos = raw_obj.oid.find('_', bucket.marker.length());
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

