// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_CONN_H
#define CEPH_RGW_REST_CONN_H

#include "rgw_rest_client.h"

class CephContext;
class RGWRados;
class RGWGetObjData;

class RGWRESTConn
{
  CephContext *cct;
  map<int, string> endpoints;
  RGWAccessKey key;
  string region;
  atomic_t counter;
public:

  RGWRESTConn(CephContext *_cct, RGWRados *store, list<string>& endpoints);
  int get_url(string& endpoint);

  /* sync request */
  int forward(const rgw_user& uid, req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl);

  /* async request */
  int put_obj_init(const rgw_user& uid, rgw_obj& obj, uint64_t obj_size,
                   map<string, bufferlist>& attrs, RGWRESTStreamWriteRequest **req);
  int complete_request(RGWRESTStreamWriteRequest *req, string& etag, time_t *mtime);

  int get_obj(const rgw_user& uid, req_info *info /* optional */, rgw_obj& obj, bool prepend_metadata, RGWGetDataCB *cb, RGWRESTStreamReadRequest **req);
  int complete_request(RGWRESTStreamReadRequest *req, string& etag, time_t *mtime, map<string, string>& attrs);
};

#endif
