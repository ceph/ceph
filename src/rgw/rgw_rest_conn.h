#ifndef CEPH_RGW_REST_CONN_H
#define CEPH_RGW_REST_CONN_H

#include "rgw_rest_client.h"

class CephContext;
class RGWRados;
class RGWRegion;
class RGWGetObjData;

class RGWRegionConnection
{
  CephContext *cct;
  map<int, string> endpoints;
  RGWAccessKey key;
  string region;
  atomic_t counter;
public:

  RGWRegionConnection(CephContext *_cct, RGWRados *store, RGWRegion& upstream);
  int get_url(string& endpoint);

  /* sync request */
  int forward(const string& uid, req_info& info, size_t max_response, bufferlist *inbl, bufferlist *outbl);

  /* async request */
  int put_obj_init(const string& uid, rgw_obj& obj, uint64_t obj_size,
                   map<string, bufferlist>& attrs, RGWRESTStreamRequest **req);
  int complete_request(RGWRESTStreamRequest *req);
};

#endif
