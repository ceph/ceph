// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_CONN_H
#define CEPH_RGW_REST_CONN_H

#include "rgw_rados.h"
#include "rgw_rest_client.h"
#include "common/ceph_json.h"
#include "common/RefCountedObj.h"


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
  string get_url();
  const string& get_region() {
    return region;
  }
  RGWAccessKey& get_key() {
    return key;
  }

  CephContext *get_ctx() {
    return cct;
  }

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
                   bufferlist& bl, RGWHTTPManager *mgr = NULL);

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

class RGWStreamIntoBufferlist : public RGWGetDataCB {
  bufferlist& bl;
public:
  RGWStreamIntoBufferlist(bufferlist& _bl) : bl(_bl) {}
  int handle_data(bufferlist& inbl, off_t bl_ofs, off_t bl_len) {
    bl.claim_append(inbl);
    return bl_len;
  }
};

class RGWRESTReadResource : public RefCountedObject {
  CephContext *cct;
  RGWRESTConn *conn;
  string resource;
  list<pair<string, string> > params;
  map<string, string> headers;
  bufferlist bl;
  RGWStreamIntoBufferlist cb;

  RGWHTTPManager *mgr;
  RGWRESTStreamReadRequest req;

public:
  RGWRESTReadResource(RGWRESTConn *_conn,
		      const string& _resource,
		      const rgw_http_param_pair *pp,
		      list<pair<string, string> > *extra_headers,
		      RGWHTTPManager *_mgr);

  void set_user_info(void *user_info) {
    req.set_user_info(user_info);
  }
  void *get_user_info() {
    return req.get_user_info();
  }

  template <class T>
  int decode_resource(T *dest);

  int read();

  int aio_read();

  string to_str() {
    return req.to_str();
  }

  int get_http_status() {
    return req.get_http_status();
  }

  template <class T>
  int wait(T *dest);

  template <class T>
  int fetch(T *dest);
};


template <class T>
int RGWRESTReadResource::decode_resource(T *dest)
{
  int ret = parse_decode_json(cct, *dest, bl);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

template <class T>
int RGWRESTReadResource::fetch(T *dest)
{
  int ret = read();
  if (ret < 0) {
    return ret;
  }

  ret = decode_resource(dest);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

template <class T>
int RGWRESTReadResource::wait(T *dest)
{
  int ret = req.wait();
  put();
  if (ret < 0) {
    return ret;
  }

  ret = decode_resource(dest);
  if (ret < 0) {
    return ret;
  }
  return 0;
}
#endif
