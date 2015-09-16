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
  zone_group = store->zonegroup.get_id();
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

string RGWRESTConn::get_url()
{
  string endpoint;
  if (endpoints.empty()) {
    ldout(cct, 0) << "WARNING: endpoints not configured for upstream zone" << dendl; /* we'll catch this later */
    return endpoint;
  }

  int i = counter.inc();
  endpoint = endpoints[i % endpoints.size()];

  return endpoint;
}

int RGWRESTConn::forward(const string& uid, req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;
  list<pair<string, string> > params;
  if (!uid.empty())
    params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "uid", uid));
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "region", zone_group));
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

int RGWRESTConn::put_obj_init(const string& uid, rgw_obj& obj, uint64_t obj_size,
                                      map<string, bufferlist>& attrs, RGWRESTStreamWriteRequest **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  list<pair<string, string> > params;
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "uid", uid));
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "region", zone_group));
  *req = new RGWRESTStreamWriteRequest(cct, url, NULL, &params);
  return (*req)->put_obj_init(key, obj, obj_size, attrs);
}

int RGWRESTConn::complete_request(RGWRESTStreamWriteRequest *req, string& etag, time_t *mtime)
{
  int ret = req->complete(etag, mtime);
  delete req;

  return ret;
}

static void set_date_header(const time_t *t, map<string, string>& headers, const string& header_name)
{
  if (!t) {
    return;
  }
  stringstream s;
  utime_t tm = utime_t(*t, 0);
  tm.asctime(s);
  headers["HTTP_IF_MODIFIED_SINCE"] = s.str();
}


int RGWRESTConn::get_obj(const string& uid, req_info *info /* optional */, rgw_obj& obj,
                         const time_t *mod_ptr, const time_t *unmod_ptr,
                         bool prepend_metadata, RGWGetDataCB *cb, RGWRESTStreamReadRequest **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  list<pair<string, string> > params;
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "uid", uid));
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "region", zone_group));
  if (prepend_metadata) {
    params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "prepend-metadata", zone_group));
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

  set_date_header(mod_ptr, extra_headers, "HTTP_IF_MODIFIED_SINCE");
  set_date_header(unmod_ptr, extra_headers, "HTTP_IF_UNMODIFIED_SINCE");

  return (*req)->get_obj(key, extra_headers, obj);
}

int RGWRESTConn::complete_request(RGWRESTStreamReadRequest *req, string& etag, time_t *mtime, map<string, string>& attrs)
{
  int ret = req->complete(etag, mtime, attrs);
  delete req;

  return ret;
}

int RGWRESTConn::get_resource(const string& resource,
		     list<pair<string, string> > *extra_params,
		     map<string, string> *extra_headers,
		     bufferlist& bl,
		     RGWHTTPManager *mgr)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  list<pair<string, string> > params;

  if (extra_params) {
    list<pair<string, string> >::iterator iter = extra_params->begin();
    for (; iter != extra_params->end(); ++iter) {
      params.push_back(*iter);
    }
  }

  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "zonegroup", zone_group));

  RGWStreamIntoBufferlist cb(bl);

  RGWRESTStreamReadRequest req(cct, url, &cb, NULL, &params);

  map<string, string> headers;
  if (extra_headers) {
    for (map<string, string>::iterator iter = extra_headers->begin();
	 iter != extra_headers->end(); ++iter) {
      headers[iter->first] = iter->second;
    }
  }

  ret = req.get_resource(key, headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 0) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  string etag;
  map<string, string> attrs;
  return req.complete(etag, NULL, attrs);
}

RGWRESTReadResource::RGWRESTReadResource(RGWRESTConn *_conn,
                                         const string& _resource,
		                         const rgw_http_param_pair *pp,
                                         list<pair<string, string> > *extra_headers,
                                         RGWHTTPManager *_mgr) : cct(_conn->get_ctx()), conn(_conn), resource(_resource), cb(bl),
                                                                 mgr(_mgr), req(cct, conn->get_url(), &cb, NULL, NULL) {
  while (pp && pp->key) {
    string k = pp->key;
    string v = (pp->val ? pp->val : "");
    params.push_back(make_pair(k, v));
    ++pp;
  }
  init_common(extra_headers);
}

RGWRESTReadResource::RGWRESTReadResource(RGWRESTConn *_conn,
                                         const string& _resource,
		                         list<pair<string, string> >& _params,
                                         list<pair<string, string> > *extra_headers,
                                         RGWHTTPManager *_mgr) : cct(_conn->get_ctx()), conn(_conn), resource(_resource), cb(bl),
                                                                 mgr(_mgr), req(cct, conn->get_url(), &cb, NULL, NULL) {
  for (list<pair<string, string> >::iterator iter = _params.begin(); iter != _params.end(); ++iter) {
    params.push_back(*iter);
  }
  init_common(extra_headers);
}

void RGWRESTReadResource::init_common(list<pair<string, string> > *extra_headers)
{
  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "zonegroup", conn->get_zonegroup()));

  if (extra_headers) {
    for (list<pair<string, string> >::iterator iter = extra_headers->begin();
      iter != extra_headers->end(); ++iter) {
      headers[iter->first] = iter->second;
    }
  }

  req.set_params(&params);
}

int RGWRESTReadResource::read()
{
  int ret = req.get_resource(conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 0) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  string etag;
  map<string, string> attrs;
  return req.complete(etag, NULL, attrs);
}

int RGWRESTReadResource::aio_read()
{
  get();
  int ret = req.get_resource(conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    put();
    ldout(cct, 0) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

RGWRESTPostResource::RGWRESTPostResource(RGWRESTConn *_conn,
                                         const string& _resource,
		                         const rgw_http_param_pair *pp,
                                         list<pair<string, string> > *extra_headers,
                                         RGWHTTPManager *_mgr) : cct(_conn->get_ctx()), conn(_conn), resource(_resource), cb(bl),
                                                                 mgr(_mgr), req(cct, "POST", conn->get_url(), &cb, NULL, NULL) {
  while (pp && pp->key) {
    string k = pp->key;
    string v = (pp->val ? pp->val : "");
    params.push_back(make_pair(k, v));
    ++pp;
  }

  params.push_back(pair<string, string>(RGW_SYS_PARAM_PREFIX "zonegroup", conn->get_zonegroup()));

  if (extra_headers) {
    for (list<pair<string, string> >::iterator iter = extra_headers->begin();
      iter != extra_headers->end(); ++iter) {
      headers[iter->first] = iter->second;
    }
  }

  req.set_params(&params);
}

int RGWRESTPostResource::send(bufferlist& outbl)
{
  req.set_outbl(outbl);
  int ret = req.get_resource(conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 0) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  string etag;
  map<string, string> attrs;
  return req.complete(etag, NULL, attrs);
}

int RGWRESTPostResource::aio_send(bufferlist& outbl)
{
  req.set_outbl(outbl);
  get();
  int ret = req.get_resource(conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    put();
    ldout(cct, 0) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

