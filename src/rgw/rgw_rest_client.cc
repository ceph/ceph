#include "rgw_common.h"
#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_http_errors.h"

#include "common/ceph_crypto_cms.h"
#include "common/armor.h"

#define dout_subsys ceph_subsys_rgw

int RGWRESTClient::receive_header(void *ptr, size_t len)
{
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
          status = atoi(l);
        } else {
          /* convert header field name to upper case  */
          char *src = tok;
          char buf[len + 1];
          size_t i;
          for (i = 0; i < len && *src; ++i, ++src) {
            buf[i] = toupper(*src);
          }
          buf[i] = '\0';
          out_headers[buf] = l;
        }
      }
    }
    if (s != end)
      *p++ = *s++;
  }
  return 0;
}

static void get_new_date_str(CephContext *cct, string& date_str)
{
  utime_t tm = ceph_clock_now(cct);
  stringstream s;
  tm.asctime(s);
  date_str = s.str();
}

int RGWRESTClient::execute(RGWAccessKey& key, const char *method, const char *resource)
{
  string new_url = url;
  string new_resource = resource;

  if (new_url[new_url.size() - 1] == '/' && resource[0] == '/') {
    new_url = new_url.substr(0, new_url.size() - 1);
  } else if (resource[0] != '/') {
    new_resource = "/";
    new_resource.append(resource);
  }
  new_url.append(new_resource);

  string date_str;
  get_new_date_str(cct, date_str);
  headers.push_back(make_pair<string, string>("HTTP_DATE", date_str));

  string canonical_header;
  map<string, string> meta_map;
  map<string, string> sub_resources;
  rgw_create_s3_canonical_header(method, NULL, NULL, date_str.c_str(),
                            meta_map, new_url.c_str(), sub_resources,
                            canonical_header);

  string digest;
  int ret = rgw_get_s3_header_digest(canonical_header, key.key, digest);
  if (ret < 0) {
    return ret;
  }

  string auth_hdr = "AWS " + key.id + ":" + digest;

  ldout(cct, 15) << "generated auth header: " << auth_hdr << dendl;

  headers.push_back(make_pair<string, string>("AUTHORIZATION", auth_hdr));
  int r = process(method, new_url.c_str());
  if (r < 0)
    return r;

  return rgw_http_error_to_errno(status);
}

int RGWRESTClient::send_data(void *ptr, size_t len)
{
  if (!send_iter)
    return 0;

  if (len > send_iter->get_remaining())
    len = send_iter->get_remaining();

  send_iter->copy(len, (char *)ptr);

  return len;
}

int RGWRESTClient::receive_data(void *ptr, size_t len)
{
  if (response.length() > max_response)
    return 0; /* don't read extra data */

  bufferptr p((char *)ptr, len);

  response.append(p);

  return 0;

}
void RGWRESTClient::append_param(string& dest, const string& name, const string& val)
{
  if (dest.empty()) {
    dest.append("?");
  } else {
    dest.append("&");
  }
  dest.append(name);

  if (!val.empty()) {
    dest.append("=");
    dest.append(val);
  }
}

void RGWRESTClient::get_params_str(map<string, string>& extra_args, string& dest)
{
  map<string, string>::iterator miter;
  for (miter = extra_args.begin(); miter != extra_args.end(); ++miter) {
    append_param(dest, miter->first, miter->second);
  }
  list<pair<string, string> >::iterator iter;
  for (iter = params.begin(); iter != params.end(); ++iter) {
    append_param(dest, iter->first, iter->second);
  }
}

int RGWRESTClient::sign_request(RGWAccessKey& key, RGWEnv& env, req_info& info)
{
  map<string, string>& m = env.get_map();

  map<string, string>::iterator i;
  for (i = m.begin(); i != m.end(); ++i) {
    ldout(cct, 0) << "> " << i->first << " -> " << i->second << dendl;
  }

  string canonical_header;
  if (!rgw_create_s3_canonical_header(info, NULL, canonical_header, false)) {
    ldout(cct, 0) << "failed to create canonical s3 header" << dendl;
    return -EINVAL;
  }

  ldout(cct, 10) << "generated canonical header: " << canonical_header << dendl;

  string digest;
  int ret = rgw_get_s3_header_digest(canonical_header, key.key, digest);
  if (ret < 0) {
    return ret;
  }

  string auth_hdr = "AWS " + key.id + ":" + digest;
  ldout(cct, 15) << "generated auth header: " << auth_hdr << dendl;
  
  m["AUTHORIZATION"] = auth_hdr;

  return 0;
}

int RGWRESTClient::forward_request(RGWAccessKey& key, req_info& info, size_t max_response, bufferlist *inbl, bufferlist *outbl)
{

  string date_str;
  get_new_date_str(cct, date_str);

  RGWEnv new_env;
  req_info new_info(cct, &new_env);
  new_info.rebuild_from(info);

  new_env.set("HTTP_DATE", date_str.c_str());

  int ret = sign_request(key, new_env, new_info);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to sign request" << dendl;
    return ret;
  }

  map<string, string>& m = new_env.get_map();
  map<string, string>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    headers.push_back(make_pair<string, string>(iter->first, iter->second));
  }

  string params_str;
  map<string, string>& args = new_info.args.get_params();
  get_params_str(args, params_str);

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

  int r = process(new_info.method, new_url.c_str());
  if (r < 0)
    return r;

  response.append((char)0); /* NULL terminate response */

  if (outbl) {
    outbl->claim(response);
  }

  return rgw_http_error_to_errno(status);
}

int RGWRESTClient::put_obj(RGWAccessKey& key, rgw_obj& obj, uint64_t obj_size, void (*get_data)(uint64_t ofs, uint64_t len, bufferlist& bl, void *))
{
  string resource = obj.bucket.name + "/" + obj.object;
  string new_url = url;
  if (new_url[new_url.size() - 1] != '/')
    new_url.append("/");

  string date_str;
  get_new_date_str(cct, date_str);

  RGWEnv new_env;
  req_info new_info(cct, &new_env);
  
  string params_str;
  map<string, string>& args = new_info.args.get_params();
  get_params_str(args, params_str);

  new_url.append(resource + params_str);

  new_env.set("HTTP_DATE", date_str.c_str());

  new_info.method = "PUT";

  new_info.script_uri = "/";
  new_info.script_uri.append(resource);
  new_info.request_uri = new_info.script_uri;

  int ret = sign_request(key, new_env, new_info);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to sign request" << dendl;
    return ret;
  }

  map<string, string>& m = new_env.get_map();
  map<string, string>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    headers.push_back(make_pair<string, string>(iter->first, iter->second));
  }

  int r = process(new_info.method, new_url.c_str());
  if (r < 0)
    return r;

  return 0;
}

