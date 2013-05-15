// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rados.h"
#include "rgw_rest_conn.h"

#define dout_subsys ceph_subsys_rgw

RGWRESTConn::RGWRESTConn(CephContext *_cct, RGWRados *store, list<string>& remote_endpoints) : cct(_cct)
{
  list<string>::iterator iter;
  int i;
  for (i = 0, iter = remote_endpoints.begin(); iter != remote_endpoints.end(); ++iter, ++i) {
    endpoints[i] = *iter;
  }
  key = store->zone.system_key;
  region = store->region.name;
}

int RGWRESTConn::get_url(string& endpoint)
{
  if (endpoints.empty()) {
    ldout(cct, 0) << "ERROR: endpoints not configured for upstream zone" << dendl;
    return -EIO;
  }

  int i = counter.inc();
  endpoint = endpoints[i % endpoints.size()];

  return 0;
}

int RGWRESTConn::forward(const rgw_user& uid, req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;
  string uid_str = uid.to_str();
  list<pair<string, string> > params;
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "uid", uid_str));
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "region", region));
  if (objv) {
    params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "tag", objv->tag));
    char buf[16];
    snprintf(buf, sizeof(buf), "%lld", (long long)objv->ver);
    params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "ver", buf));
  }
  RGWRESTSimpleRequest req(cct, url, NULL, &params);
  return req.forward_request(key, info, max_response, inbl, outbl);
}

class StreamObjData : public RGWGetDataCB {
  rgw_obj obj;
public:
    StreamObjData(rgw_obj& _obj) : obj(_obj) {}
};

int RGWRESTConn::put_obj_init(const rgw_user& uid, rgw_obj& obj, uint64_t obj_size,
                                      map<string, bufferlist>& attrs, RGWRESTStreamWriteRequest **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  string uid_str = uid.to_str();
  list<pair<string, string> > params;
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "uid", uid_str));
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "region", region));
  *req = new RGWRESTStreamWriteRequest(cct, url, NULL, &params);
  return (*req)->put_obj_init(key, obj, obj_size, attrs);
}

int RGWRESTConn::complete_request(RGWRESTStreamWriteRequest *req, string& etag, time_t *mtime)
{
  int ret = req->complete(etag, mtime);
  delete req;

  return ret;
}

int RGWRESTConn::get_obj(const rgw_user& uid, req_info *info /* optional */, rgw_obj& obj, bool prepend_metadata,
                                 RGWGetDataCB *cb, RGWRESTStreamReadRequest **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  string uid_str = uid.to_str();
  list<pair<string, string> > params;
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "uid", uid_str));
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "region", region));
  if (prepend_metadata) {
    params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "prepend-metadata", region));
  }
  *req = new RGWRESTStreamReadRequest(cct, url, cb, NULL, &params);
  map<string, string> extra_headers;
  if (info) {
    map<string, string, ltstr_nocase>& orig_map = info->env->get_map();

    /* add original headers that start with HTTP_X_AMZ_ */
#define SEARCH_AMZ_PREFIX "HTTP_X_AMZ_"
    for (map<string, string>::iterator iter = orig_map.lower_bound(SEARCH_AMZ_PREFIX); iter != orig_map.end(); ++iter) {
      const string& name = iter->first;
      if (name == "HTTP_X_AMZ_DATE") /* dont forward date from original request */
        continue;
      if (name.compare(0, sizeof(SEARCH_AMZ_PREFIX) - 1, "HTTP_X_AMZ_") != 0)
        break;
      extra_headers[iter->first] = iter->second;
    }
  }
  return (*req)->get_obj(key, extra_headers, obj);
}

int RGWRESTConn::complete_request(RGWRESTStreamReadRequest *req, string& etag, time_t *mtime, map<string, string>& attrs)
{
  int ret = req->complete(etag, mtime, attrs);
  delete req;

  return ret;
}


