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

#include "common/dout.h"
#include "common/errno.h"
#include "rgw_realm_watcher.h"
#include "rgw_zone.h"
#include "driver/rados/config/store.h"

#include "impl.h"

namespace rgw::rados {

// realm oids
constexpr std::string_view realm_names_oid_prefix = "realms_names.";
constexpr std::string_view realm_info_oid_prefix = "realms.";
constexpr std::string_view realm_control_oid_suffix = ".control";
constexpr std::string_view default_realm_info_oid = "default.realm";

static std::string realm_info_oid(std::string_view realm_id)
{
  return string_cat_reserve(realm_info_oid_prefix, realm_id);
}
static std::string realm_name_oid(std::string_view realm_id)
{
  return string_cat_reserve(realm_names_oid_prefix, realm_id);
}
static std::string realm_control_oid(std::string_view realm_id)
{
  return string_cat_reserve(realm_info_oid_prefix, realm_id,
                            realm_control_oid_suffix);
}
static std::string default_realm_oid(const ceph::common::ConfigProxy& conf)
{
  return std::string{name_or_default(conf->rgw_default_realm_info_oid,
                                     default_realm_info_oid)};
}


int RadosConfigStore::write_default_realm_id(const DoutPrefixProvider* dpp,
                                             optional_yield y, bool exclusive,
                                             std::string_view realm_id,
                                             std::string_view realm_name)
{
  const auto& pool = impl->realm_pool;
  const auto oid = default_realm_oid(dpp->get_cct()->_conf);
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = realm_id;
  default_info.default_name = realm_name;

  return impl->write(dpp, y, pool, oid, create, default_info, nullptr);
}

int RadosConfigStore::read_default_realm_id(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            std::string& realm_id,
                                            std::string& realm_name)
{
  const auto& pool = impl->realm_pool;
  const auto oid = default_realm_oid(dpp->get_cct()->_conf);

  RGWDefaultSystemMetaObjInfo default_info;
  int r = impl->read(dpp, y, pool, oid, default_info, nullptr);
  if (r >= 0) {
    realm_id = default_info.default_id;
    realm_name = default_info.default_name;
  }
  return r;
}

int RadosConfigStore::delete_default_realm_id(const DoutPrefixProvider* dpp,
                                              optional_yield y)
{
  const auto& pool = impl->realm_pool;
  const auto oid = default_realm_oid(dpp->get_cct()->_conf);

  return impl->remove(dpp, y, pool, oid, nullptr);
}


class RadosRealmWriter : public sal::RealmWriter {
  ConfigImpl* impl;
  RGWObjVersionTracker objv;
  std::string realm_id;
  std::string realm_name;
 public:
  RadosRealmWriter(ConfigImpl* impl, RGWObjVersionTracker objv,
                   std::string_view realm_id, std::string_view realm_name)
    : impl(impl), objv(std::move(objv)),
      realm_id(realm_id), realm_name(realm_name)
  {
  }

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWRealm& info) override
  {
    if (realm_id != info.get_id() || realm_name != info.get_name()) {
      return -EINVAL; // can't modify realm id or name directly
    }

    const auto& pool = impl->realm_pool;
    const auto info_oid = realm_info_oid(info.get_id());
    return impl->write(dpp, y, pool, info_oid, Create::MustExist, info, &objv);
  }

  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWRealm& info, std::string_view new_name) override
  {
    if (realm_id != info.get_id() || realm_name != info.get_name()) {
      return -EINVAL; // can't modify realm id or name directly
    }
    if (new_name.empty()) {
      ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
      return -EINVAL;
    }

    const auto& pool = impl->realm_pool;
    const auto name = RGWNameToId{info.get_id()};
    const auto info_oid = realm_info_oid(info.get_id());
    const auto old_oid = realm_name_oid(info.get_name());
    const auto new_oid = realm_name_oid(new_name);

    // link the new name
    RGWObjVersionTracker new_objv;
    new_objv.generate_new_write_ver(dpp->get_cct());
    int r = impl->write(dpp, y, pool, new_oid, Create::MustNotExist,
                        name, &new_objv);
    if (r < 0) {
      return r;
    }

    // write the info with updated name
    info.set_name(std::string{new_name});
    r = impl->write(dpp, y, pool, info_oid, Create::MustExist, info, &objv);
    if (r < 0) {
      // on failure, unlink the new name
      (void) impl->remove(dpp, y, pool, new_oid, &new_objv);
      return r;
    }

    // unlink the old name
    (void) impl->remove(dpp, y, pool, old_oid, nullptr);

    realm_name = new_name;
    return 0;
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    const auto& pool = impl->realm_pool;
    const auto info_oid = realm_info_oid(realm_id);
    int r = impl->remove(dpp, y, pool, info_oid, &objv);
    if (r < 0) {
      return r;
    }
    const auto name_oid = realm_name_oid(realm_name);
    (void) impl->remove(dpp, y, pool, name_oid, nullptr);
    const auto control_oid = realm_control_oid(realm_id);
    (void) impl->remove(dpp, y, pool, control_oid, nullptr);
    return 0;
  }
}; // RadosRealmWriter


int RadosConfigStore::create_realm(const DoutPrefixProvider* dpp,
                                   optional_yield y, bool exclusive,
                                   const RGWRealm& info,
                                   std::unique_ptr<sal::RealmWriter>* writer)
{
  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->realm_pool;
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;

  // write the realm info
  const auto info_oid = realm_info_oid(info.get_id());
  RGWObjVersionTracker objv;
  objv.generate_new_write_ver(dpp->get_cct());

  int r = impl->write(dpp, y, pool, info_oid, create, info, &objv);
  if (r < 0) {
    return r;
  }

  // write the realm name
  const auto name_oid = realm_name_oid(info.get_name());
  const auto name = RGWNameToId{info.get_id()};
  RGWObjVersionTracker name_objv;
  name_objv.generate_new_write_ver(dpp->get_cct());

  r = impl->write(dpp, y, pool, name_oid, create, name, &name_objv);
  if (r < 0) {
    (void) impl->remove(dpp, y, pool, info_oid, &objv);
    return r;
  }

  // create control object for watch/notify
  const auto control_oid = realm_control_oid(info.get_id());
  bufferlist empty_bl;
  r = impl->write(dpp, y, pool, control_oid, Create::MayExist,
                  empty_bl, nullptr);
  if (r < 0) {
    (void) impl->remove(dpp, y, pool, name_oid, &name_objv);
    (void) impl->remove(dpp, y, pool, info_oid, &objv);
    return r;
  }

  if (writer) {
    *writer = std::make_unique<RadosRealmWriter>(
        impl.get(), std::move(objv), info.get_id(), info.get_name());
  }
  return 0;
}

int RadosConfigStore::read_realm_by_id(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::string_view realm_id,
                                       RGWRealm& info,
                                       std::unique_ptr<sal::RealmWriter>* writer)
{
  const auto& pool = impl->realm_pool;
  const auto info_oid = realm_info_oid(realm_id);
  RGWObjVersionTracker objv;
  int r = impl->read(dpp, y, pool, info_oid, info, &objv);
  if (r < 0) {
    return r;
  }

  if (writer) {
    *writer = std::make_unique<RadosRealmWriter>(
        impl.get(), std::move(objv), info.get_id(), info.get_name());
  }
  return 0;
}

int RadosConfigStore::read_realm_by_name(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         std::string_view realm_name,
                                         RGWRealm& info,
                                         std::unique_ptr<sal::RealmWriter>* writer)
{
  const auto& pool = impl->realm_pool;

  // look up realm id by name
  RGWNameToId name;
  const auto name_oid = realm_name_oid(realm_name);
  int r = impl->read(dpp, y, pool, name_oid, name, nullptr);
  if (r < 0) {
    return r;
  }

  const auto info_oid = realm_info_oid(name.obj_id);
  RGWObjVersionTracker objv;
  r = impl->read(dpp, y, pool, info_oid, info, &objv);
  if (r < 0) {
    return r;
  }

  if (writer) {
    *writer = std::make_unique<RadosRealmWriter>(
        impl.get(), std::move(objv), info.get_id(), info.get_name());
  }
  return 0;
}

int RadosConfigStore::read_default_realm(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         RGWRealm& info,
                                         std::unique_ptr<sal::RealmWriter>* writer)
{
  const auto& pool = impl->realm_pool;

  // read default realm id
  RGWDefaultSystemMetaObjInfo default_info;
  const auto default_oid = default_realm_oid(dpp->get_cct()->_conf);
  int r = impl->read(dpp, y, pool, default_oid, default_info, nullptr);
  if (r < 0) {
    return r;
  }

  const auto info_oid = realm_info_oid(default_info.default_id);
  RGWObjVersionTracker objv;
  r = impl->read(dpp, y, pool, info_oid, info, &objv);
  if (r < 0) {
    return r;
  }

  if (writer) {
    *writer = std::make_unique<RadosRealmWriter>(
        impl.get(), std::move(objv), info.get_id(), info.get_name());
  }
  return 0;
}

int RadosConfigStore::read_realm_id(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string_view realm_name,
                                    std::string& realm_id)
{
  const auto& pool = impl->realm_pool;
  RGWNameToId name;

  // look up realm id by name
  const auto name_oid = realm_name_oid(realm_name);
  int r = impl->read(dpp, y, pool, name_oid, name, nullptr);
  if (r < 0) {
    return r;
  }
  realm_id = std::move(name.obj_id);
  return 0;
}

int RadosConfigStore::realm_notify_new_period(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              const RGWPeriod& period)
{
  const auto& pool = impl->realm_pool;
  const auto control_oid = realm_control_oid(period.get_realm());

  bufferlist bl;
  using ceph::encode;
  // push the period to dependent zonegroups/zones
  encode(RGWRealmNotify::ZonesNeedPeriod, bl);
  encode(period, bl);
  // reload the gateway with the new period
  encode(RGWRealmNotify::Reload, bl);

  constexpr uint64_t timeout_ms = 0;
  return impl->notify(dpp, y, pool, control_oid, bl, timeout_ms);
}

int RadosConfigStore::list_realm_names(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const std::string& marker,
                                       std::span<std::string> entries,
                                       sal::ListResult<std::string>& result)
{
  const auto& pool = impl->realm_pool;
  constexpr auto prefix = [] (std::string oid) -> std::string {
      if (!oid.starts_with(realm_names_oid_prefix)) {
        return {};
      }
      return oid.substr(realm_names_oid_prefix.size());
    };
  return impl->list(dpp, y, pool, marker, prefix, entries, result);
}

} // namespace rgw::rados
