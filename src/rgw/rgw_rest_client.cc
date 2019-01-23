// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_http_errors.h"
#include "rgw_rados.h"

#include "common/ceph_crypto_cms.h"
#include "common/armor.h"
#include "common/strtol.h"
#include "include/str_list.h"
#include "rgw_crypt_sanitize.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int RGWHTTPSimpleRequest::get_status()
{
  int retcode = get_req_retcode();
  if (retcode < 0) {
    return retcode;
  }
  return status;
}

int RGWHTTPSimpleRequest::handle_header(const string& name, const string& val) 
{
  if (name == "CONTENT_LENGTH") {
    string err;
    long len = strict_strtol(val.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(cct, 0) << "ERROR: failed converting content length (" << val << ") to int " << dendl;
      return -EINVAL;
    }

    max_response = len;
  }

  return 0;
}

int RGWHTTPSimpleRequest::receive_header(void *ptr, size_t len)
{
  unique_lock guard(out_headers_lock);

  char line[len + 1];

  char *s = (char *)ptr, *end = (char *)ptr + len;
  char *p = line;
  ldout(cct, 10) << "receive_http_header" << dendl;

  while (s != end) {
    if (*s == '\r') {
      s++;
      continue;
    }
    if (*s == '\n') {
      *p = '\0';
      ldout(cct, 10) << "received header:" << line << dendl;
      // TODO: fill whatever data required here
      char *l = line;
      char *tok = strsep(&l, " \t:");
      if (tok && l) {
        while (*l == ' ')
          l++;
 
        if (strcmp(tok, "HTTP") == 0 || strncmp(tok, "HTTP/", 5) == 0) {
          http_status = atoi(l);
          if (http_status == 100) /* 100-continue response */
            continue;
          status = rgw_http_error_to_errno(http_status);
        } else {
          /* convert header field name to upper case  */
          char *src = tok;
          char buf[len + 1];
          size_t i;
          for (i = 0; i < len && *src; ++i, ++src) {
            switch (*src) {
              case '-':
                buf[i] = '_';
                break;
              default:
                buf[i] = toupper(*src);
            }
          }
          buf[i] = '\0';
          out_headers[buf] = l;
          int r = handle_header(buf, l);
          if (r < 0)
            return r;
        }
      }
    }
    if (s != end)
      *p++ = *s++;
  }
  return 0;
}

static void get_new_date_str(string& date_str)
{
  date_str = rgw_to_asctime(ceph_clock_now());
}

static void get_gmt_date_str(string& date_str)
{
  auto now_time = ceph::real_clock::now();
  time_t rawtime = ceph::real_clock::to_time_t(now_time);

  char buffer[80];

  struct tm timeInfo;
  gmtime_r(&rawtime, &timeInfo);
  strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S %z", &timeInfo);  
  
  date_str = buffer;
}

int RGWRESTSimpleRequest::execute(RGWAccessKey& key, const char *_method, const char *resource)
{
  method = _method;
  string new_url = url;
  string new_resource = resource;

  if (new_url[new_url.size() - 1] == '/' && resource[0] == '/') {
    new_url = new_url.substr(0, new_url.size() - 1);
  } else if (resource[0] != '/') {
    new_resource = "/";
    new_resource.append(resource);
  }
  new_url.append(new_resource);
  url = new_url;

  string date_str;
  get_new_date_str(date_str);
  headers.push_back(pair<string, string>("HTTP_DATE", date_str));

  string canonical_header;
  map<string, string> meta_map;
  map<string, string> sub_resources;

  rgw_create_s3_canonical_header(method.c_str(), NULL, NULL, date_str.c_str(),
				 meta_map, meta_map, url.c_str(), sub_resources,
				 canonical_header);

  string digest;
  try {
    digest = rgw::auth::s3::get_v2_signature(cct, key.key, canonical_header);
  } catch (int ret) {
    return ret;
  }

  string auth_hdr = "AWS " + key.id + ":" + digest;

  ldout(cct, 15) << "generated auth header: " << auth_hdr << dendl;

  headers.push_back(pair<string, string>("AUTHORIZATION", auth_hdr));
  int r = process();
  if (r < 0)
    return r;

  return status;
}

int RGWHTTPSimpleRequest::send_data(void *ptr, size_t len, bool* pause)
{
  if (!send_iter)
    return 0;

  if (len > send_iter->get_remaining())
    len = send_iter->get_remaining();

  send_iter->copy(len, (char *)ptr);

  return len;
}

int RGWHTTPSimpleRequest::receive_data(void *ptr, size_t len, bool *pause)
{
  size_t cp_len, left_len;

  left_len = max_response > response.length() ? (max_response - response.length()) : 0;
  if (left_len == 0)
    return 0; /* don't read extra data */

  cp_len = (len > left_len) ? left_len : len;
  bufferptr p((char *)ptr, cp_len);

  response.append(p);

  return 0;
}

static void append_param(string& dest, const string& name, const string& val)
{
  if (dest.empty()) {
    dest.append("?");
  } else {
    dest.append("&");
  }
  string url_name;
  url_encode(name, url_name);
  dest.append(url_name);

  if (!val.empty()) {
    string url_val;
    url_encode(val, url_val);
    dest.append("=");
    dest.append(url_val);
  }
}

static void do_get_params_str(const param_vec_t& params, map<string, string>& extra_args, string& dest)
{
  map<string, string>::iterator miter;
  for (miter = extra_args.begin(); miter != extra_args.end(); ++miter) {
    append_param(dest, miter->first, miter->second);
  }
  for (auto iter = params.begin(); iter != params.end(); ++iter) {
    append_param(dest, iter->first, iter->second);
  }
}

void RGWHTTPSimpleRequest::get_params_str(map<string, string>& extra_args, string& dest)
{
  do_get_params_str(params, extra_args, dest);
}

void RGWHTTPSimpleRequest::get_out_headers(map<string, string> *pheaders)
{
  unique_lock guard(out_headers_lock);
  pheaders->swap(out_headers);
  out_headers.clear();
}

static int sign_request(CephContext *cct, RGWAccessKey& key, RGWEnv& env, req_info& info)
{
  /* don't sign if no key is provided */
  if (key.key.empty()) {
    return 0;
  }

  if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
    for (const auto& i: env.get_map()) {
      ldout(cct, 20) << "> " << i.first << " -> " << rgw::crypt_sanitize::x_meta_map{i.first, i.second} << dendl;
    }
  }

  string canonical_header;
  if (!rgw_create_s3_canonical_header(info, NULL, canonical_header, false)) {
    ldout(cct, 0) << "failed to create canonical s3 header" << dendl;
    return -EINVAL;
  }

  ldout(cct, 10) << "generated canonical header: " << canonical_header << dendl;

  string digest;
  try {
    digest = rgw::auth::s3::get_v2_signature(cct, key.key, canonical_header);
  } catch (int ret) {
    return ret;
  }

  string auth_hdr = "AWS " + key.id + ":" + digest;
  ldout(cct, 15) << "generated auth header: " << auth_hdr << dendl;
  
  env.set("AUTHORIZATION", auth_hdr);

  return 0;
}

int RGWRESTSimpleRequest::forward_request(RGWAccessKey& key, req_info& info, size_t max_response, bufferlist *inbl, bufferlist *outbl)
{

  string date_str;
  get_new_date_str(date_str);

  RGWEnv new_env;
  req_info new_info(cct, &new_env);
  new_info.rebuild_from(info);

  new_env.set("HTTP_DATE", date_str.c_str());

  int ret = sign_request(cct, key, new_env, new_info);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to sign request" << dendl;
    return ret;
  }

  for (const auto& kv: new_env.get_map()) {
    headers.emplace_back(kv);
  }

  map<string, string>& meta_map = new_info.x_meta_map;
  for (const auto& kv: meta_map) {
    headers.emplace_back(kv);
  }

  string params_str;
  get_params_str(info.args.get_params(), params_str);

  string new_url = url;
  string& resource = new_info.request_uri;
  string new_resource = resource;
  if (new_url[new_url.size() - 1] == '/' && resource[0] == '/') {
    new_url = new_url.substr(0, new_url.size() - 1);
  } else if (resource[0] != '/') {
    new_resource = "/";
    new_resource.append(resource);
  }
  new_url.append(new_resource + params_str);

  bufferlist::iterator bliter;

  if (inbl) {
    bliter = inbl->begin();
    send_iter = &bliter;

    set_send_length(inbl->length());
  }

  method = new_info.method;
  url = new_url;

  int r = process();
  if (r < 0){
    if (r == -EINVAL){
      // curl_easy has errored, generally means the service is not available
      r = -ERR_SERVICE_UNAVAILABLE;
    }
    return r;
  }

  response.append((char)0); /* NULL terminate response */

  if (outbl) {
    outbl->claim(response);
  }

  return status;
}

class RGWRESTStreamOutCB : public RGWGetDataCB {
  RGWRESTStreamS3PutObj *req;
public:
  explicit RGWRESTStreamOutCB(RGWRESTStreamS3PutObj *_req) : req(_req) {}
  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override; /* callback for object iteration when sending data */
};

int RGWRESTStreamOutCB::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  dout(20) << "RGWRESTStreamOutCB::handle_data bl.length()=" << bl.length() << " bl_ofs=" << bl_ofs << " bl_len=" << bl_len << dendl;
  if (!bl_ofs && bl_len == bl.length()) {
    req->add_send_data(bl);
    return 0;
  }

  bufferptr bp(bl.c_str() + bl_ofs, bl_len);
  bufferlist new_bl;
  new_bl.push_back(bp);

  req->add_send_data(new_bl);
  return 0;
}

RGWRESTStreamS3PutObj::~RGWRESTStreamS3PutObj()
{
  delete out_cb;
}

static void grants_by_type_add_one_grant(map<int, string>& grants_by_type, int perm, ACLGrant& grant)
{
  string& s = grants_by_type[perm];

  if (!s.empty())
    s.append(", ");

  string id_type_str;
  ACLGranteeType& type = grant.get_type();
  switch (type.get_type()) {
    case ACL_TYPE_GROUP:
      id_type_str = "uri";
      break;
    case ACL_TYPE_EMAIL_USER:
      id_type_str = "emailAddress";
      break;
    default:
      id_type_str = "id";
  }
  rgw_user id;
  grant.get_id(id);
  s.append(id_type_str + "=\"" + id.to_str() + "\"");
}

struct grant_type_to_header {
  int type;
  const char *header;
};

struct grant_type_to_header grants_headers_def[] = {
  { RGW_PERM_FULL_CONTROL, "x-amz-grant-full-control"},
  { RGW_PERM_READ,         "x-amz-grant-read"},
  { RGW_PERM_WRITE,        "x-amz-grant-write"},
  { RGW_PERM_READ_ACP,     "x-amz-grant-read-acp"},
  { RGW_PERM_WRITE_ACP,    "x-amz-grant-write-acp"},
  { 0, NULL}
};

static bool grants_by_type_check_perm(map<int, string>& grants_by_type, int perm, ACLGrant& grant, int check_perm)
{
  if ((perm & check_perm) == check_perm) {
    grants_by_type_add_one_grant(grants_by_type, check_perm, grant);
    return true;
  }
  return false;
}

static void grants_by_type_add_perm(map<int, string>& grants_by_type, int perm, ACLGrant& grant)
{
  struct grant_type_to_header *t;

  for (t = grants_headers_def; t->header; t++) {
    if (grants_by_type_check_perm(grants_by_type, perm, grant, t->type))
      return;
  }
}

static void add_grants_headers(map<int, string>& grants, RGWEnv& env, map<string, string>& meta_map)
{
  struct grant_type_to_header *t;

  for (t = grants_headers_def; t->header; t++) {
    map<int, string>::iterator iter = grants.find(t->type);
    if (iter != grants.end()) {
      env.set(t->header,iter->second);
      meta_map[t->header] = iter->second;
    }
  }
}

void RGWRESTGenerateHTTPHeaders::init(const string& _method, const string& _url, const string& resource, const param_vec_t& params)
{
  string params_str;
  map<string, string>& args = new_info->args.get_params();
  do_get_params_str(params, args, params_str);

  /* merge params with extra args so that we can sign correctly */
  for (auto iter = params.begin(); iter != params.end(); ++iter) {
    new_info->args.append(iter->first, iter->second);
  }

  url = _url + resource + params_str;

  string date_str;
  get_gmt_date_str(date_str);

  new_env->set("HTTP_DATE", date_str.c_str());

  method = _method;
  new_info->method = method.c_str();

  new_info->script_uri = "/";
  new_info->script_uri.append(resource);
  new_info->request_uri = new_info->script_uri;
}

static bool is_x_amz(const string& s) {
  return boost::algorithm::starts_with(s, "x-amz-");
}

void RGWRESTGenerateHTTPHeaders::set_extra_headers(const map<string, string>& extra_headers)
{
  for (auto iter : extra_headers) {
    const string& name = lowercase_dash_http_attr(iter.first);
    new_env->set(name, iter.second.c_str());
    if (is_x_amz(name)) {
      new_info->x_meta_map[name] = iter.second;
    }
  }
}

int RGWRESTGenerateHTTPHeaders::set_obj_attrs(map<string, bufferlist>& rgw_attrs)
{
  map<string, string> new_attrs;

  /* merge send headers */
  for (auto& attr: rgw_attrs) {
    bufferlist& bl = attr.second;
    const string& name = attr.first;
    string val = bl.c_str();
    if (name.compare(0, sizeof(RGW_ATTR_META_PREFIX) - 1, RGW_ATTR_META_PREFIX) == 0) {
      string header_name = RGW_AMZ_META_PREFIX;
      header_name.append(name.substr(sizeof(RGW_ATTR_META_PREFIX) - 1));
      new_attrs[header_name] = val;
    }
  }

  RGWAccessControlPolicy policy;
  int ret = rgw_policy_from_attrset(cct, rgw_attrs, &policy);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: couldn't get policy ret=" << ret << dendl;
    return ret;
  }

  set_http_attrs(new_attrs);
  set_policy(policy);

  return 0;
}

static std::set<string> keep_headers = { "content-type",
                                         "content-encoding",
                                         "content-disposition",
                                         "content-language" };

void RGWRESTGenerateHTTPHeaders::set_http_attrs(const map<string, string>& http_attrs)
{
  /* merge send headers */
  for (auto& attr: http_attrs) {
    const string& val = attr.second;
    const string& name = lowercase_dash_http_attr(attr.first);
    if (is_x_amz(name)) {
      new_env->set(name, val);
      new_info->x_meta_map[name] = val;
    } else {
      new_env->set(attr.first, val); /* Ugh, using the uppercase representation,
                                       as the signing function calls info.env.get("CONTENT_TYPE").
                                       This needs to be cleaned up! */
    }
  }
}

void RGWRESTGenerateHTTPHeaders::set_policy(RGWAccessControlPolicy& policy)
{
  /* update acl headers */
  RGWAccessControlList& acl = policy.get_acl();
  multimap<string, ACLGrant>& grant_map = acl.get_grant_map();
  multimap<string, ACLGrant>::iterator giter;
  map<int, string> grants_by_type;
  for (giter = grant_map.begin(); giter != grant_map.end(); ++giter) {
    ACLGrant& grant = giter->second;
    ACLPermission& perm = grant.get_permission();
    grants_by_type_add_perm(grants_by_type, perm.get_permissions(), grant);
  }
  add_grants_headers(grants_by_type, *new_env, new_info->x_meta_map);
}

int RGWRESTGenerateHTTPHeaders::sign(RGWAccessKey& key)
{
  int ret = sign_request(cct, key, *new_env, *new_info);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to sign request" << dendl;
    return ret;
  }

  return 0;
}

void RGWRESTStreamS3PutObj::send_init(rgw_obj& obj)
{
  string resource_str;
  string resource;
  string new_url = url;

  if (host_style == VirtualStyle) {
    resource_str = obj.get_oid();
    new_url = obj.bucket.name + "."  + new_url;
  } else {
    resource_str = obj.bucket.name + "/" + obj.get_oid();
  }

  //do not encode slash in object key name
  url_encode(resource_str, resource, false);

  if (new_url[new_url.size() - 1] != '/')
    new_url.append("/");

  method = "PUT";
  headers_gen.init(method, new_url, resource, params);

  url = headers_gen.get_url();
}

int RGWRESTStreamS3PutObj::send_ready(RGWAccessKey& key, map<string, bufferlist>& rgw_attrs, bool send)
{
  headers_gen.set_obj_attrs(rgw_attrs);

  return send_ready(key, send);
}

int RGWRESTStreamS3PutObj::send_ready(RGWAccessKey& key, const map<string, string>& http_attrs,
                                      RGWAccessControlPolicy& policy, bool send)
{
  headers_gen.set_http_attrs(http_attrs);
  headers_gen.set_policy(policy);

  return send_ready(key, send);
}

int RGWRESTStreamS3PutObj::send_ready(RGWAccessKey& key, bool send)
{
  headers_gen.sign(key);

  for (const auto& kv: new_env.get_map()) {
    headers.emplace_back(kv);
  }

  out_cb = new RGWRESTStreamOutCB(this);

  if (send) {
    int r = RGWHTTP::send(this);
    if (r < 0)
      return r;
  }

  return 0;
}

int RGWRESTStreamS3PutObj::put_obj_init(RGWAccessKey& key, rgw_obj& obj, uint64_t obj_size, map<string, bufferlist>& attrs, bool send)
{
  send_init(obj);
  return send_ready(key, attrs, send);
}

void set_str_from_headers(map<string, string>& out_headers, const string& header_name, string& str)
{
  map<string, string>::iterator iter = out_headers.find(header_name);
  if (iter != out_headers.end()) {
    str = iter->second;
  } else {
    str.clear();
  }
}

static int parse_rgwx_mtime(CephContext *cct, const string& s, ceph::real_time *rt)
{
  string err;
  vector<string> vec;

  get_str_vec(s, ".", vec);

  if (vec.empty()) {
    return -EINVAL;
  }

  long secs = strict_strtol(vec[0].c_str(), 10, &err);
  long nsecs = 0;
  if (!err.empty()) {
    ldout(cct, 0) << "ERROR: failed converting mtime (" << s << ") to real_time " << dendl;
    return -EINVAL;
  }

  if (vec.size() > 1) {
    nsecs = strict_strtol(vec[1].c_str(), 10, &err);
    if (!err.empty()) {
      ldout(cct, 0) << "ERROR: failed converting mtime (" << s << ") to real_time " << dendl;
      return -EINVAL;
    }
  }

  *rt = utime_t(secs, nsecs).to_real_time();

  return 0;
}

static void send_prepare_convert(const rgw_obj& obj, string *resource)
{
  string urlsafe_bucket, urlsafe_object;
  url_encode(obj.bucket.get_key(':', 0), urlsafe_bucket);
  url_encode(obj.key.name, urlsafe_object);
  *resource = urlsafe_bucket + "/" + urlsafe_object;
}

int RGWRESTStreamRWRequest::send_request(RGWAccessKey& key, map<string, string>& extra_headers, const rgw_obj& obj, RGWHTTPManager *mgr)
{
  string resource;
  send_prepare_convert(obj, &resource);

  return send_request(&key, extra_headers, resource, mgr);
}

int RGWRESTStreamRWRequest::send_prepare(RGWAccessKey& key, map<string, string>& extra_headers, const rgw_obj& obj)
{
  string resource;
  send_prepare_convert(obj, &resource);

  return do_send_prepare(&key, extra_headers, resource);
}

int RGWRESTStreamRWRequest::send_prepare(RGWAccessKey *key, map<string, string>& extra_headers, const string& resource,
                                           bufferlist *send_data)
{
  string new_resource;
  //do not encode slash
  url_encode(resource, new_resource, false);

  return do_send_prepare(key, extra_headers, new_resource, send_data);
}

int RGWRESTStreamRWRequest::do_send_prepare(RGWAccessKey *key, map<string, string>& extra_headers, const string& resource,
                                         bufferlist *send_data)
{
  string new_url = url;
  if (new_url[new_url.size() - 1] != '/')
    new_url.append("/");
  
  RGWEnv new_env;
  req_info new_info(cct, &new_env);
  
  string new_resource;
  string bucket_name;
  string old_resource = resource;

  if (resource[0] == '/') {
    new_resource = resource.substr(1);
  } else {
    new_resource = resource;
  }

  size_t pos = new_resource.find("/");
  bucket_name = new_resource.substr(0, pos);

  //when dest is a bucket with out other params, uri should end up with '/'
  if(pos == string::npos && params.size() == 0 && host_style == VirtualStyle) {
    new_resource.append("/");
  }

  if (host_style == VirtualStyle) {
    new_url = bucket_name + "." + new_url;
    if(pos == string::npos) {
      new_resource = "";
    } else {
      new_resource = new_resource.substr(pos+1);
    }
  }

  RGWRESTGenerateHTTPHeaders headers_gen(cct, &new_env, &new_info);

  headers_gen.init(method, new_url, new_resource, params);

  headers_gen.set_http_attrs(extra_headers);

  if (key) {
#if 0
    new_info.init_meta_info(nullptr);
#endif

    int ret = headers_gen.sign(*key);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to sign request" << dendl;
      return ret;
    }
  }

  for (const auto& kv: new_env.get_map()) {
    headers.emplace_back(kv);
  }

  if (send_data) {
    set_send_length(send_data->length());
    set_outbl(*send_data);
    set_send_data_hint(true);
  }
  

  method = new_info.method;
  url = headers_gen.get_url();

  return 0;
}

int RGWRESTStreamRWRequest::send_request(RGWAccessKey *key, map<string, string>& extra_headers, const string& resource,
                                         RGWHTTPManager *mgr, bufferlist *send_data)
{
  int ret = send_prepare(key, extra_headers, resource, send_data);
  if (ret < 0) {
    return ret;
  }

  return send(mgr);
}


int RGWRESTStreamRWRequest::send(RGWHTTPManager *mgr)
{
  if (!mgr) {
    return RGWHTTP::send(this);
  }

  int r = mgr->add_request(this);
  if (r < 0)
    return r;

  return 0;
}

int RGWRESTStreamRWRequest::complete_request(string *etag,
                                             real_time *mtime,
                                             uint64_t *psize,
                                             map<string, string> *pattrs,
                                             map<string, string> *pheaders)
{
  int ret = wait();
  if (ret < 0) {
    return ret;
  }

  unique_lock guard(out_headers_lock);

  if (etag) {
    set_str_from_headers(out_headers, "ETAG", *etag);
  }
  if (status >= 0) {
    if (mtime) {
      string mtime_str;
      set_str_from_headers(out_headers, "RGWX_MTIME", mtime_str);
      if (!mtime_str.empty()) {
        int ret = parse_rgwx_mtime(cct, mtime_str, mtime);
        if (ret < 0) {
          return ret;
        }
      } else {
        *mtime = real_time();
      }
    }
    if (psize) {
      string size_str;
      set_str_from_headers(out_headers, "RGWX_OBJECT_SIZE", size_str);
      string err;
      *psize = strict_strtoll(size_str.c_str(), 10, &err);
      if (!err.empty()) {
        ldout(cct, 0) << "ERROR: failed parsing embedded metadata object size (" << size_str << ") to int " << dendl;
        return -EIO;
      }
    }
  }

  for (auto iter = out_headers.begin(); pattrs && iter != out_headers.end(); ++iter) {
    const string& attr_name = iter->first;
    if (attr_name.compare(0, sizeof(RGW_HTTP_RGWX_ATTR_PREFIX) - 1, RGW_HTTP_RGWX_ATTR_PREFIX) == 0) {
      string name = attr_name.substr(sizeof(RGW_HTTP_RGWX_ATTR_PREFIX) - 1);
      const char *src = name.c_str();
      char buf[name.size() + 1];
      char *dest = buf;
      for (; *src; ++src, ++dest) {
        switch(*src) {
          case '_':
            *dest = '-';
            break;
          default:
            *dest = tolower(*src);
        }
      }
      *dest = '\0';
      (*pattrs)[buf] = iter->second;
    }
  }

  if (pheaders) {
    *pheaders = std::move(out_headers);
  }
  return status;
}

int RGWHTTPStreamRWRequest::handle_header(const string& name, const string& val)
{
  if (name == "RGWX_EMBEDDED_METADATA_LEN") {
    string err;
    long len = strict_strtol(val.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(cct, 0) << "ERROR: failed converting embedded metadata len (" << val << ") to int " << dendl;
      return -EINVAL;
    }

    cb->set_extra_data_len(len);
  }
  return 0;
}

int RGWHTTPStreamRWRequest::receive_data(void *ptr, size_t len, bool *pause)
{
  size_t orig_len = len;

  if (cb) {
    in_data.append((const char *)ptr, len);

    size_t orig_in_data_len = in_data.length();

    int ret = cb->handle_data(in_data, pause);
    if (ret < 0)
      return ret;
    if (ret == 0) {
      in_data.clear();
    } else {
      /* partial read */
      ceph_assert(in_data.length() <= orig_in_data_len);
      len = ret;
      bufferlist bl;
      size_t left_to_read = orig_in_data_len - len;
      if (in_data.length() > left_to_read) {
        in_data.splice(0, in_data.length() - left_to_read, &bl);
      }
    }
  }
  ofs += len;
  return orig_len;
}

void RGWHTTPStreamRWRequest::set_stream_write(bool s) {
  Mutex::Locker wl(write_lock);
  stream_writes = s;
}

void RGWHTTPStreamRWRequest::unpause_receive()
{
  Mutex::Locker req_locker(get_req_lock());
  if (!read_paused) {
    _set_read_paused(false);
  }
}

void RGWHTTPStreamRWRequest::add_send_data(bufferlist& bl)
{
  Mutex::Locker req_locker(get_req_lock());
  Mutex::Locker wl(write_lock);
  outbl.claim_append(bl);
  _set_write_paused(false);
}

uint64_t RGWHTTPStreamRWRequest::get_pending_send_size()
{
  Mutex::Locker wl(write_lock);
  return outbl.length();
}

void RGWHTTPStreamRWRequest::finish_write()
{
  Mutex::Locker req_locker(get_req_lock());
  Mutex::Locker wl(write_lock);
  write_stream_complete = true;
  _set_write_paused(false);
}

int RGWHTTPStreamRWRequest::send_data(void *ptr, size_t len, bool *pause)
{
  uint64_t out_len;
  uint64_t send_size;
  {
    Mutex::Locker wl(write_lock);

    if (outbl.length() == 0) {
      if ((stream_writes && !write_stream_complete) ||
          (write_ofs < send_len)) {
        *pause = true;
      }
      return 0;
    }

    len = std::min(len, (size_t)outbl.length());

    bufferlist bl;
    outbl.splice(0, len, &bl);
    send_size = bl.length();
    if (send_size > 0) {
      memcpy(ptr, bl.c_str(), send_size);
      write_ofs += send_size;
    }

    out_len = outbl.length();
  }
  /* don't need to be under write_lock here, avoid deadlocks in case notify callback
   * needs to lock */
  if (write_drain_cb) {
    write_drain_cb->notify(out_len);
  }
  return send_size;
}

class StreamIntoBufferlist : public RGWGetDataCB {
  bufferlist& bl;
public:
  explicit StreamIntoBufferlist(bufferlist& _bl) : bl(_bl) {}
  int handle_data(bufferlist& inbl, off_t bl_ofs, off_t bl_len) override {
    bl.claim_append(inbl);
    return bl_len;
  }
};

