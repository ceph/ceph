
#pragma once

#include "rgw_zone.h"

class CephContext;
class RGWRESTConn;
class RGWSI_Zone;
class RGWUserCtl;


class RGWRemoteCtl
{
  std::vector<RGWRESTConn *> alloc_conns;

  RGWRESTConn *add_conn(RGWRESTConn *conn) {
    alloc_conns.push_back(conn);
    return conn;
  }

public:
  struct Conns {
    RGWRESTConn *data{nullptr};
    RGWRESTConn *sip{nullptr};
  };

private:
  CephContext *cct;

  struct Svc {
    RGWSI_Zone *zone{nullptr};
  } svc;

  struct Ctl {
    RGWUserCtl *user{nullptr};
  } ctl;

  std::map<rgw_zone_id, Conns> conns_map;
  map<rgw_zone_id, RGWRESTConn *> zone_meta_notify_to_map;
  map<rgw_zone_id, RGWRESTConn *> zone_data_notify_to_map;

  bool get_access_key(const string& dest_id,
                      std::optional<rgw_user> uid,
                      std::optional<string> access_key,
                      std::optional<string> secret,
                      RGWAccessKey *result,
                      optional_yield y) const;

  void init_conn(const RGWDataProvider& z, bool need_notify);

public:
  RGWRemoteCtl(RGWSI_Zone *_zone_svc,
               RGWUserCtl *_user_ctl);
  ~RGWRemoteCtl();

  void init();

  map<rgw_zone_id, RGWRESTConn *>& get_zone_meta_notify_to_map() {
    return zone_meta_notify_to_map;
  }

  map<rgw_zone_id, RGWRESTConn *>& get_zone_data_notify_to_map() {
    return zone_data_notify_to_map;
  }

  std::optional<Conns> zone_conns(const rgw_zone_id& zone_id);
  std::optional<Conns> zone_conns(const string& name);

  RGWRESTConn *create_conn(const string& zone_name,
                           const rgw_zone_id& zone_id,
                           const std::list<string>& def_endpoints,
                           const RGWDataProvider::RESTConfig& conf,
                           std::optional<string> api_name);

  RGWRESTConn *create_conn(const string& remote_id,
                           const list<string>& endpoint,
                           const RGWAccessKey& key,
                           std::optional<string> api_name);

  bool get_redirect_zone_endpoint(string *endpoint);
};

