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
  string region;
  atomic_t counter;
public:

  RGWRegionConnection(CephContext *_cct, RGWRados *store, RGWRegion& upstream);
  int get_url(string& endpoint);

  int forward(const string& uid, req_info& info, size_t max_response, bufferlist *inbl, bufferlist *outbl);
  int put_obj(const string& uid, rgw_obj& obj, uint64_t obj_size, void (*get_data)(uint64_t ofs, uint64_t len, bufferlist& bl, void *));
};

#endif
