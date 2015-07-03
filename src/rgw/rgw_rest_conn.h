// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_CONN_H
#define CEPH_RGW_REST_CONN_H

#include "rgw_rest_client.h"
#include "common/ceph_json.h"


class CephContext;
class RGWRados;
class RGWGetObjData;

template <class T>
static int parse_decode_json(CephContext *cct, T& t, bufferlist& bl)
{
  JSONParser p;
  int ret = p.parse(bl.c_str(), bl.length());
  if (ret < 0) {
    return ret;
  }

  try {
    decode_json_obj(t, &p);
  } catch (JSONDecoder::err& e) {
    return -EINVAL;
  }
  return 0;
}

struct rgw_http_param_pair {
  const char *key;
  const char *val;
};

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
  int forward(const string& uid, req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl);

  /* async request */
  int put_obj_init(const string& uid, rgw_obj& obj, uint64_t obj_size,
                   map<string, bufferlist>& attrs, RGWRESTStreamWriteRequest **req);
  int complete_request(RGWRESTStreamWriteRequest *req, string& etag, time_t *mtime);

  int get_obj(const string& uid, req_info *info /* optional */, rgw_obj& obj, bool prepend_metadata, RGWGetDataCB *cb, RGWRESTStreamReadRequest **req);
  int complete_request(RGWRESTStreamReadRequest *req, string& etag, time_t *mtime, map<string, string>& attrs);

  int get_resource(const string& resource,
                   list<pair<string, string> > *extra_params,
                   map<string, string>* extra_headers,
                   bufferlist& bl);

  template <class T>
  int get_json_resource(const string& resource, list<pair<string, string> > *params, T& t);
  template <class T>
  int get_json_resource(const string& resource, const rgw_http_param_pair *pp, T& t);
};


template<class T>
int RGWRESTConn::get_json_resource(const string& resource, list<pair<string, string> > *params, T& t)
{
  bufferlist bl;
  int ret = get_resource(resource, params, NULL, bl);
  if (ret < 0) {
    return ret;
  }

  ret = parse_decode_json(cct, t, bl);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

template<class T>
int RGWRESTConn::get_json_resource(const string& resource,  const rgw_http_param_pair *pp, T& t)
{
  list<pair<string, string> > params;

  while (pp && pp->key) {
    string k = pp->key;
    string v = (pp->val ? pp->val : "");
    params.push_back(make_pair(k, v));
    ++pp;
  }

  return get_json_resource(resource, &params, t);
}

#endif
