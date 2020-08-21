// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_remote.h"
#include "rgw_rest_conn.h"
#include "rgw_user.h"

#include "services/svc_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

bool RGWRemoteCtl::get_access_key(const string& dest_id,
                                  std::optional<rgw_user> uid,
                                  std::optional<string> access_key,
                                  std::optional<string> secret,
                                  RGWAccessKey *result,
                                  optional_yield y) const
{
  RGWUserInfo info;
  int r;

  if (access_key) {
    result->id = *access_key;
    if (secret) {
      result->key = *secret;
      return true;
    }
    r = ctl.user->get_info_by_access_key(*access_key,
                                  &info,
                                  y);
  } else if (uid) {
    r = ctl.user->get_info_by_uid(*uid,
                                  &info,
                                  y);
  } else {
    return false;
  }

  if (r < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): could not find user info for connection to dest=" << dest_id << " (r=" << r << ")" << dendl;
    return false;
  }

  if (info.access_keys.empty()) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): user (uid=" << info.user_id << ") has no access keys for dest=" << dest_id << dendl;
    return false;
  }

  auto akiter = info.access_keys.begin();
  *result = akiter->second;

  return true;
}

void RGWRemoteCtl::init()
{
  auto& zone_conn_map = svc.zone->get_zone_conn_map();

  const auto& zonegroup = svc.zone->get_zonegroup();

  const auto& zone_id = svc.zone->zone_id();

  for (const auto& ziter : zonegroup.zones) {
    const rgw_zone_id& id = ziter.first;
    const RGWZone& z = ziter.second;
    if (id == zone_id) {
      continue;
    }
    if (z.endpoints.empty()) {
      ldout(cct, 0) << "WARNING: can't generate connection for zone " << id << " id " << z.name << ": no endpoints defined" << dendl;
      continue;
    }
    auto& conns = conns_map[id];
    auto citer = zone_conn_map.find(id);
    if (citer == zone_conn_map.end()) {
      ldout(cct, 20) << "generating connection object for zone " << z.name << " id " << z.id << dendl;
      conns.data = new RGWRESTConn(cct, svc.zone, z.id, z.endpoints);
    } else {
      conns.data = citer->second;
    }

    if (z.sip_config) {
      auto endpoints = z.sip_config->endpoints.value_or(z.endpoints);
      RGWAccessKey access_key;

      if (!get_access_key(z.name,
                          z.sip_config->uid,
                          z.sip_config->access_key,
                          z.sip_config->secret,
                          &access_key,
                          null_yield)) {
        ldout(cct, 0) << "NOTICE: using default access key for sip connection to zone " << z.name << dendl;
        access_key = svc.zone->get_zone_params().system_key;
      }

      ldout(cct, 20) << __func__ << "(): remote sip connection for zone=" << z.name << ": using access_key=" <<  access_key.id << dendl;
      conns.sip = new RGWRESTConn(cct, svc.zone, z.id, z.endpoints, access_key);
    } else {
      conns.sip = conns.data;
    }
  }
}


std::optional<RGWRemoteCtl::Conns> RGWRemoteCtl::zone_conns(const rgw_zone_id& zone_id)
{
  auto citer = conns_map.find(zone_id);
  if (citer == conns_map.end()) {
    return nullopt;
  }

   return citer->second;
}

std::optional<RGWRemoteCtl::Conns> RGWRemoteCtl::zone_conns(const string& name)
{
  rgw_zone_id id;
  if (!svc.zone->find_zone_id_by_name(name, &id)) {
    return nullopt;
  }

  return zone_conns(id);
}
