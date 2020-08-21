
#pragma once

#include "rgw_zone.h"

class CephContext;
class RGWRESTConn;
class RGWSI_Zone;
class RGWUserCtl;


class RGWRemoteCtl
{
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

  bool get_access_key(const string& dest_id,
                      std::optional<rgw_user> uid,
                      std::optional<string> access_key,
                      std::optional<string> secret,
                      RGWAccessKey *result,
                      optional_yield y) const;

public:
  RGWRemoteCtl(RGWSI_Zone *_zone_svc,
               RGWUserCtl *_user_ctl) {
    svc.zone = _zone_svc;
    ctl.user = _user_ctl;
  }

  void init();

  std::optional<Conns> zone_conns(const rgw_zone_id& zone_id);
  std::optional<Conns> zone_conns(const string& name);
};

