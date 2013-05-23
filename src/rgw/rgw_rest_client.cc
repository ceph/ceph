#include "rgw_common.h"
#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_http_errors.h"

#include "common/ceph_crypto_cms.h"
#include "common/armor.h"

#define dout_subsys ceph_subsys_rgw

int RGWRESTClient::read_header(void *ptr, size_t len)
{
  char line[len + 1];

  char *s = (char *)ptr, *end = (char *)ptr + len;
  char *p = line;
  ldout(cct, 10) << "read_http_header" << dendl;

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

  utime_t tm = ceph_clock_now(cct);
  stringstream s;
  tm.gmtime(s);
  string date_str = s.str();
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

