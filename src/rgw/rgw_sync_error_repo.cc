// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "rgw_sync_error_repo.h"
#include "rgw_coroutine.h"
#include "rgw_sal.h"
#include "services/svc_rados.h"
#include "cls/cmpomap/client.h"

ceph::real_time rgw_error_repo_decode_value(const bufferlist& bl)
{
  uint64_t value;
  try {
    using ceph::decode;
    decode(value, bl);
  } catch (const buffer::error&) {
    value = 0; // empty buffer = 0
  }
  return ceph::real_clock::zero() + ceph::timespan(value);
}

int rgw_error_repo_write(librados::ObjectWriteOperation& op,
                         const std::string& key,
                         ceph::real_time timestamp)
{
  // overwrite the existing timestamp if value is greater
  const uint64_t value = timestamp.time_since_epoch().count();
  using namespace cls::cmpomap;
  const bufferlist zero = u64_buffer(0); // compare against 0 for missing keys
  return cmp_set_vals(op, Mode::U64, Op::GT, {{key, u64_buffer(value)}}, zero);
}

int rgw_error_repo_remove(librados::ObjectWriteOperation& op,
                          const std::string& key,
                          ceph::real_time timestamp)
{
  // remove the omap key if value >= existing
  const uint64_t value = timestamp.time_since_epoch().count();
  using namespace cls::cmpomap;
  return cmp_rm_keys(op, Mode::U64, Op::GTE, {{key, u64_buffer(value)}});
}

class RGWErrorRepoWriteCR : public RGWSimpleCoroutine {
  RGWSI_RADOS::Obj obj;
  std::string key;
  ceph::real_time timestamp;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 public:
  RGWErrorRepoWriteCR(RGWSI_RADOS* rados, const rgw_raw_obj& raw_obj,
                      const std::string& key, ceph::real_time timestamp)
    : RGWSimpleCoroutine(rados->ctx()),
      obj(rados->obj(raw_obj)),
      key(key), timestamp(timestamp)
  {}

  int send_request(const DoutPrefixProvider *dpp) override {
    librados::ObjectWriteOperation op;
    int r = rgw_error_repo_write(op, key, timestamp);
    if (r < 0) {
      return r;
    }
    r = obj.open(dpp);
    if (r < 0) {
      return r;
    }

    cn = stack->create_completion_notifier();
    return obj.aio_operate(cn->completion(), &op);
  }

  int request_complete() override {
    return cn->completion()->get_return_value();
  }
};

RGWCoroutine* rgw_error_repo_write_cr(RGWSI_RADOS* rados,
                                      const rgw_raw_obj& obj,
                                      const std::string& key,
                                      ceph::real_time timestamp)
{
  return new RGWErrorRepoWriteCR(rados, obj, key, timestamp);
}


class RGWErrorRepoRemoveCR : public RGWSimpleCoroutine {
  RGWSI_RADOS::Obj obj;
  std::string key;
  ceph::real_time timestamp;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 public:
  RGWErrorRepoRemoveCR(RGWSI_RADOS* rados, const rgw_raw_obj& raw_obj,
                       const std::string& key, ceph::real_time timestamp)
    : RGWSimpleCoroutine(rados->ctx()),
      obj(rados->obj(raw_obj)),
      key(key), timestamp(timestamp)
  {}

  int send_request(const DoutPrefixProvider *dpp) override {
    librados::ObjectWriteOperation op;
    int r = rgw_error_repo_remove(op, key, timestamp);
    if (r < 0) {
      return r;
    }
    r = obj.open(dpp);
    if (r < 0) {
      return r;
    }

    cn = stack->create_completion_notifier();
    return obj.aio_operate(cn->completion(), &op);
  }

  int request_complete() override {
    return cn->completion()->get_return_value();
  }
};

RGWCoroutine* rgw_error_repo_remove_cr(RGWSI_RADOS* rados,
                                       const rgw_raw_obj& obj,
                                       const std::string& key,
                                       ceph::real_time timestamp)
{
  return new RGWErrorRepoRemoveCR(rados, obj, key, timestamp);
}
