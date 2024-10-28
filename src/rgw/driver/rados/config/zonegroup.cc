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
#include "rgw_zone.h"
#include "driver/rados/config/store.h"

#include "impl.h"

namespace rgw::rados {

// zonegroup oids
constexpr std::string_view zonegroup_names_oid_prefix = "zonegroups_names.";
constexpr std::string_view zonegroup_info_oid_prefix = "zonegroup_info.";
constexpr std::string_view default_zonegroup_info_oid = "default.zonegroup";

static std::string zonegroup_info_oid(std::string_view zonegroup_id)
{
  return string_cat_reserve(zonegroup_info_oid_prefix, zonegroup_id);
}
static std::string zonegroup_name_oid(std::string_view zonegroup_id)
{
  return string_cat_reserve(zonegroup_names_oid_prefix, zonegroup_id);
}
static std::string default_zonegroup_oid(const ceph::common::ConfigProxy& conf,
                                         std::string_view realm_id)
{
  const auto prefix = name_or_default(conf->rgw_default_zonegroup_info_oid,
                                      default_zonegroup_info_oid);
  return fmt::format("{}.{}", prefix, realm_id);
}


int RadosConfigStore::write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                 optional_yield y,
                                                 bool exclusive,
                                                 std::string_view realm_id,
                                                 std::string_view zonegroup_id)
{
  const auto& pool = impl->zonegroup_pool;
  const auto oid = default_zonegroup_oid(dpp->get_cct()->_conf, realm_id);
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = zonegroup_id;

  return impl->write(dpp, y, pool, oid, create, default_info, nullptr);
}

int RadosConfigStore::read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                optional_yield y,
                                                std::string_view realm_id,
                                                std::string& zonegroup_id)
{
  const auto& pool = impl->zonegroup_pool;
  const auto oid = default_zonegroup_oid(dpp->get_cct()->_conf, realm_id);

  RGWDefaultSystemMetaObjInfo default_info;
  int r = impl->read(dpp, y, pool, oid, default_info, nullptr);
  if (r >= 0) {
    zonegroup_id = default_info.default_id;
  }
  return r;
}

int RadosConfigStore::delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                  optional_yield y,
                                                  std::string_view realm_id)
{
  const auto& pool = impl->zonegroup_pool;
  const auto oid = default_zonegroup_oid(dpp->get_cct()->_conf, realm_id);
  return impl->remove(dpp, y, pool, oid, nullptr);
}


class RadosZoneGroupWriter : public sal::ZoneGroupWriter {
  ConfigImpl* impl;
  RGWObjVersionTracker objv;
  std::string zonegroup_id;
  std::string zonegroup_name;
 public:
  RadosZoneGroupWriter(ConfigImpl* impl, RGWObjVersionTracker objv,
                       std::string_view zonegroup_id,
                       std::string_view zonegroup_name)
    : impl(impl), objv(std::move(objv)),
      zonegroup_id(zonegroup_id), zonegroup_name(zonegroup_name)
  {
  }

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWZoneGroup& info) override
  {
    if (zonegroup_id != info.get_id() || zonegroup_name != info.get_name()) {
      return -EINVAL; // can't modify zonegroup id or name directly
    }

    const auto& pool = impl->zonegroup_pool;
    const auto info_oid = zonegroup_info_oid(info.get_id());
    return impl->write(dpp, y, pool, info_oid, Create::MustExist, info, &objv);
  }

  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWZoneGroup& info, std::string_view new_name) override
  {
    if (zonegroup_id != info.get_id() || zonegroup_name != info.get_name()) {
      return -EINVAL; // can't modify zonegroup id or name directly
    }
    if (new_name.empty()) {
      ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
      return -EINVAL;
    }

    const auto& pool = impl->zonegroup_pool;
    const auto name = RGWNameToId{info.get_id()};
    const auto info_oid = zonegroup_info_oid(info.get_id());
    const auto old_oid = zonegroup_name_oid(info.get_name());
    const auto new_oid = zonegroup_name_oid(new_name);

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

    zonegroup_name = new_name;
    return 0;
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    const auto& pool = impl->zonegroup_pool;
    const auto info_oid = zonegroup_info_oid(zonegroup_id);
    int r = impl->remove(dpp, y, pool, info_oid, &objv);
    if (r < 0) {
      return r;
    }
    const auto name_oid = zonegroup_name_oid(zonegroup_name);
    (void) impl->remove(dpp, y, pool, name_oid, nullptr);
    return 0;
  }
}; // RadosZoneGroupWriter


int RadosConfigStore::create_zonegroup(const DoutPrefixProvider* dpp,
                                       optional_yield y, bool exclusive,
                                       const RGWZoneGroup& info,
                                       std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_name().empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
    return -EINVAL;
  }

  const auto& pool = impl->zonegroup_pool;
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;

  // write the zonegroup info
  const auto info_oid = zonegroup_info_oid(info.get_id());
  RGWObjVersionTracker objv;
  objv.generate_new_write_ver(dpp->get_cct());

  int r = impl->write(dpp, y, pool, info_oid, create, info, &objv);
  if (r < 0) {
    return r;
  }

  // write the zonegroup name
  const auto name_oid = zonegroup_name_oid(info.get_name());
  const auto name = RGWNameToId{info.get_id()};
  RGWObjVersionTracker name_objv;
  name_objv.generate_new_write_ver(dpp->get_cct());

  r = impl->write(dpp, y, pool, name_oid, create, name, &name_objv);
  if (r < 0) {
    (void) impl->remove(dpp, y, pool, info_oid, &objv);
    return r;
  }

  if (writer) {
    *writer = std::make_unique<RadosZoneGroupWriter>(
        impl.get(), std::move(objv), info.get_id(), info.get_name());
  }
  return 0;
}

int RadosConfigStore::read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           std::string_view zonegroup_id,
                                           RGWZoneGroup& info,
                                           std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  const auto& pool = impl->zonegroup_pool;
  const auto info_oid = zonegroup_info_oid(zonegroup_id);
  RGWObjVersionTracker objv;

  int r = impl->read(dpp, y, pool, info_oid, info, &objv);
  if (r < 0) {
    return r;
  }

  if (writer) {
    *writer = std::make_unique<RadosZoneGroupWriter>(
        impl.get(), std::move(objv), info.get_id(), info.get_name());
  }
  return 0;
}

int RadosConfigStore::read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string_view zonegroup_name,
                                             RGWZoneGroup& info,
                                             std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  const auto& pool = impl->zonegroup_pool;

  // look up zonegroup id by name
  RGWNameToId name;
  const auto name_oid = zonegroup_name_oid(zonegroup_name);
  int r = impl->read(dpp, y, pool, name_oid, name, nullptr);
  if (r < 0) {
    return r;
  }

  const auto info_oid = zonegroup_info_oid(name.obj_id);
  RGWObjVersionTracker objv;
  r = impl->read(dpp, y, pool, info_oid, info, &objv);
  if (r < 0) {
    return r;
  }

  if (writer) {
    *writer = std::make_unique<RadosZoneGroupWriter>(
        impl.get(), std::move(objv), info.get_id(), info.get_name());
  }
  return 0;
}

int RadosConfigStore::read_default_zonegroup(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string_view realm_id,
                                             RGWZoneGroup& info,
                                             std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  const auto& pool = impl->zonegroup_pool;

  // read default zonegroup id
  RGWDefaultSystemMetaObjInfo default_info;
  const auto default_oid = default_zonegroup_oid(dpp->get_cct()->_conf, realm_id);
  int r = impl->read(dpp, y, pool, default_oid, default_info, nullptr);
  if (r < 0) {
    return r;
  }

  const auto info_oid = zonegroup_info_oid(default_info.default_id);
  RGWObjVersionTracker objv;
  r = impl->read(dpp, y, pool, info_oid, info, &objv);
  if (r < 0) {
    return r;
  }

  if (writer) {
    *writer = std::make_unique<RadosZoneGroupWriter>(
        impl.get(), std::move(objv), info.get_id(), info.get_name());
  }
  return 0;
}

int RadosConfigStore::list_zonegroup_names(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           const std::string& marker,
                                           std::span<std::string> entries,
                                           sal::ListResult<std::string>& result)
{
  const auto& pool = impl->zonegroup_pool;
  constexpr auto prefix = [] (std::string oid) -> std::string {
      if (!oid.starts_with(zonegroup_names_oid_prefix)) {
        return {};
      }
      return oid.substr(zonegroup_names_oid_prefix.size());
    };
  return impl->list(dpp, y, pool, marker, prefix, entries, result);
}

} // namespace rgw::rados
