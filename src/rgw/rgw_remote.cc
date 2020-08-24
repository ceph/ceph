// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_remote.h"
#include "rgw_rest_conn.h"
#include "rgw_user.h"

#include "services/svc_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

RGWRemoteCtl::RGWRemoteCtl(RGWSI_Zone *_zone_svc,
                           RGWUserCtl *_user_ctl) : cct(_zone_svc->ctx())
{
  svc.zone = _zone_svc;
  ctl.user = _user_ctl;
}

RGWRemoteCtl::~RGWRemoteCtl()
{
  for (auto& conn : alloc_conns) {
    delete conn;
  }
}

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
  auto& zone_data_notify_set = svc.zone->get_zone_data_notify_set();

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
    ldout(cct, 20) << "generating connection object for zone " << z.name << " id " << z.id << dendl;
    if (z.data_access_conf) {
      conns.data = add_conn(create_conn(z.name, z.id, z.endpoints, *z.data_access_conf));
    } else {
      conns.data = add_conn(new RGWRESTConn(cct, svc.zone, z.id, z.endpoints));
    }

    if (z.sip_conf) {
      conns.sip = add_conn(create_conn(z.name, z.id, z.endpoints, z.sip_conf->rest_conf));
    } else {
      conns.sip = conns.data;
    }

    zone_meta_notify_to_map[z.id] = conns.data;

    if (zone_data_notify_set.find(z.id) != zone_data_notify_set.end()) {
      zone_data_notify_to_map[z.id] = conns.data;
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

RGWRESTConn *RGWRemoteCtl::create_conn(const string& zone_name,
                                       const rgw_zone_id& zone_id,
                                       const std::list<string>& def_endpoints,
                                       const RGWDataProvider::RESTConfig& conf,
                                       std::optional<string> api_name)
{
  auto endpoints = conf.endpoints.value_or(def_endpoints);
  RGWAccessKey access_key;

  if (!get_access_key(zone_name,
                      conf.uid,
                      conf.access_key,
                      conf.secret,
                      &access_key,
                      null_yield)) {
    ldout(cct, 0) << "NOTICE: using default access key for connection to zone " << zone_name << dendl;
    access_key = svc.zone->get_zone_params().system_key;
  }

  ldout(cct, 20) << __func__ << "(): remote sip connection for zone=" << zone_name << ": using access_key=" <<  access_key.id << dendl;
  return new RGWRESTConn(cct, svc.zone, zone_id.id, def_endpoints, access_key);
}

RGWRESTConn *RGWRemoteCtl::create_conn(const string& remote_id,
                                       const list<string>& endpoints,
                                       const RGWAccessKey& key,
                                       std::optional<string> api_name)
{
  return new RGWRESTConn(cct, svc.zone, remote_id, endpoints, key, api_name);
}

bool RGWRemoteCtl::get_redirect_zone_endpoint(string *endpoint)
{
  const auto& zone_public_config = svc.zone->get_zone();

  if (zone_public_config.redirect_zone.empty()) {
    return false;
  }

  auto iter = conns_map.find(zone_public_config.redirect_zone);
  if (iter == conns_map.end()) {
    ldout(cct, 0) << "ERROR: cannot find entry for redirect zone: " << zone_public_config.redirect_zone << dendl;
    return false;
  }

  RGWRESTConn *conn = iter->second.data;

  int ret = conn->get_url(*endpoint);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: redirect zone, conn->get_endpoint() returned ret=" << ret << dendl;
    return false;
  }

  return true;
}

