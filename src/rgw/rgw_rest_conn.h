#ifndef CEPH_RGW_REST_REQ_H
#define CEPH_RGW_REST_REQ_H

#include "rgw_rest_client.h"

class CephContext;
class RGWRados;
class RGWRegion;

class RGWRegionConnection
{
  CephContext *cct;
  map<int, string> endpoints;
  RGWAccessKey key;
  atomic_t counter;
public:

  RGWRegionConnection(CephContext *_cct, RGWRados *store, RGWRegion& upstream);
  int get_url(string& endpoint);

  int create_bucket(const string& uid, const string& bucket);

};

#endif
