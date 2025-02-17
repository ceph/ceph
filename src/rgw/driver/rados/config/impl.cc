// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "impl.h"

#include "common/async/yield_context.h"
#include "common/errno.h"
#include "rgw_string.h"
#include "rgw_zone.h"

namespace rgw::rados {

// default pool names
constexpr std::string_view default_zone_root_pool = "rgw.root";
constexpr std::string_view default_zonegroup_root_pool = "rgw.root";
constexpr std::string_view default_realm_root_pool = "rgw.root";
constexpr std::string_view default_period_root_pool = "rgw.root";

static rgw_pool default_pool(std::string_view name,
                             std::string_view default_name)
{
  return std::string{name_or_default(name, default_name)};
}

ConfigImpl::ConfigImpl(const ceph::common::ConfigProxy& conf)
  : realm_pool(default_pool(conf->rgw_realm_root_pool,
                            default_realm_root_pool)),
    period_pool(default_pool(conf->rgw_period_root_pool,
                             default_period_root_pool)),
    zonegroup_pool(default_pool(conf->rgw_zonegroup_root_pool,
                                default_zonegroup_root_pool)),
    zone_pool(default_pool(conf->rgw_zone_root_pool,
                           default_zone_root_pool))
{
}

int ConfigImpl::read(const DoutPrefixProvider* dpp, optional_yield y,
                     const rgw_pool& pool, const std::string& oid,
                     bufferlist& bl, RGWObjVersionTracker* objv)
{
  librados::IoCtx ioctx;
  int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
  if (r < 0) {
    return r;
  }
  librados::ObjectReadOperation op;
  if (objv) {
    objv->prepare_op_for_read(&op);
  }
  op.read(0, 0, &bl, nullptr);
  return rgw_rados_operate(dpp, ioctx, oid, std::move(op), nullptr, y);
}

int ConfigImpl::write(const DoutPrefixProvider* dpp, optional_yield y,
                      const rgw_pool& pool, const std::string& oid,
                      Create create, const bufferlist& bl,
                      RGWObjVersionTracker* objv)
{
  librados::IoCtx ioctx;
  int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  switch (create) {
    case Create::MustNotExist: op.create(true); break;
    case Create::MayExist: op.create(false); break;
    case Create::MustExist: op.assert_exists(); break;
  }
  if (objv) {
    objv->prepare_op_for_write(&op);
  }
  op.write_full(bl);

  r = rgw_rados_operate(dpp, ioctx, oid, std::move(op), y);
  if (r >= 0 && objv) {
    objv->apply_write();
  }
  return r;
}

int ConfigImpl::remove(const DoutPrefixProvider* dpp, optional_yield y,
                       const rgw_pool& pool, const std::string& oid,
                       RGWObjVersionTracker* objv)
{
  librados::IoCtx ioctx;
  int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  if (objv) {
    objv->prepare_op_for_write(&op);
  }
  op.remove();

  r = rgw_rados_operate(dpp, ioctx, oid, std::move(op), y);
  if (r >= 0 && objv) {
    objv->apply_write();
  }
  return r;
}

int ConfigImpl::notify(const DoutPrefixProvider* dpp, optional_yield y,
                       const rgw_pool& pool, const std::string& oid,
                       bufferlist& bl, uint64_t timeout_ms)
{
  librados::IoCtx ioctx;
  int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
  if (r < 0) {
    return r;
  }
  return rgw_rados_notify(dpp, ioctx, oid, bl, timeout_ms, nullptr, y);
}

} // namespace rgw::rados
