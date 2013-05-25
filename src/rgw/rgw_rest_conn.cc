#include "rgw_rados.h"
#include "rgw_rest_conn.h"

#define dout_subsys ceph_subsys_rgw

RGWRegionConnection::RGWRegionConnection(CephContext *_cct, RGWRados *store, RGWRegion& upstream) : cct(_cct)
{
  list<string>::iterator iter;
  int i;
  for (i = 0, iter = upstream.endpoints.begin(); iter != upstream.endpoints.end(); ++iter, ++i) {
    endpoints[i] = *iter;
  }
  key = store->zone.system_key;
  region = store->region.name;
}

int RGWRegionConnection::get_url(string& endpoint)
{
  if (endpoints.empty()) {
    ldout(cct, 0) << "ERROR: endpoints not configured for upstream zone" << dendl;
    return -EIO;
  }

  int i = counter.inc();
  endpoint = endpoints[i % endpoints.size()];

  return 0;
}

int RGWRegionConnection::forward(const string& uid, req_info& info, bufferlist *inbl)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;
  list<pair<string, string> > params;
  params.push_back(make_pair<string, string>(RGW_SYS_PARAM_PREFIX "uid", uid));
  params.push_back(make_pair<string, string>(RGW_SYS_PARAM_PREFIX "region", region));
  RGWRESTClient client(cct, url, NULL, &params);
  return client.forward_request(key, info, inbl);
}

int RGWRegionConnection::create_bucket(const string& uid, const string& bucket)
{
  list<pair<string, string> > params;
  params.push_back(make_pair<string, string>("uid", uid));
  params.push_back(make_pair<string, string>("bucket", bucket));
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;
  RGWRESTClient client(cct, url, NULL, &params);
  return client.execute(key, "PUT", "/admin/bucket");
}
