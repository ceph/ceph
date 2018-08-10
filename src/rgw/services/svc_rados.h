#ifndef CEPH_RGW_SERVICES_ZONE_H
#define CEPH_RGW_SERVICES_ZONE_H


#include "rgw/rgw_service.h"


class RGWS_RADOS : public RGWService
{
public:
  RGWS_RADOS(CephContext *cct) : RGWService(cct, "rados") {}

  std::vector<std::string> deps();
  int create_instance(JSONFormattable& conf, RGWServiceInstanceRef *instance);
};


#endif
