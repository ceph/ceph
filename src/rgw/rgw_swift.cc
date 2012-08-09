#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <curl/curl.h>
#include <curl/easy.h>

#include "rgw_common.h"
#include "rgw_swift.h"
#include "rgw_swift_auth.h"
#include "rgw_user.h"

#define dout_subsys ceph_subsys_rgw

static size_t read_http_header(void *ptr, size_t size, size_t nmemb, void *_info)
{
  size_t len = size * nmemb;
  char line[len + 1];
  struct rgw_swift_auth_info *info = (struct rgw_swift_auth_info *)_info;

  char *s = (char *)ptr, *end = (char *)ptr + len;
  char *p = line;
  dout(10) << "read_http_header" << dendl;

  while (s != end) {
    if (*s == '\r') {
      s++;
      continue;
    }
    if (*s == '\n') {
      *p = '\0';
      dout(10) << "os_auth:" << line << dendl;
      // TODO: fill whatever data required here
      char *l = line;
      char *tok = strsep(&l, " \t:");
      if (tok) {
        while (l && *l == ' ')
          l++;
 
        if (strcmp(tok, "HTTP") == 0) {
          info->status = atoi(l);
        } else if (strcasecmp(tok, "X-Auth-Groups") == 0) {
          info->auth_groups = strdup(l);
          char *s = strchr(l, ',');
          if (s) {
            *s = '\0';
            info->user = strdup(l);
          }
        } else if (strcasecmp(tok, "X-Auth-Ttl") == 0) {
          info->ttl = atoll(l);
        }
      }
    }
    if (s != end)
      *p++ = *s++;
  }
  return len;
}

static int rgw_swift_validate_token(const char *token, struct rgw_swift_auth_info *info)
{
  CURL *curl_handle;
  string auth_url = "http://127.0.0.1:11000/token";
  char url_buf[auth_url.size() + 1 + strlen(token) + 1];
  sprintf(url_buf, "%s/%s", auth_url.c_str(), token);

  dout(10) << "rgw_swift_validate_token url=" << url_buf << dendl;

  curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_URL, url_buf);
  curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);

  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, read_http_header);

  curl_easy_setopt(curl_handle,   CURLOPT_WRITEHEADER, info);

  curl_easy_perform(curl_handle);
  curl_easy_cleanup(curl_handle);

  return 0;
}

bool rgw_verify_swift_token(RGWRados *store, req_state *s)
{
  if (!s->os_auth_token)
    return false;

  if (strncmp(s->os_auth_token, "AUTH_rgwtk", 10) == 0) {
    int ret = rgw_swift_verify_signed_token(s->cct, store, s->os_auth_token, s->user);
    if (ret < 0)
      return false;

    return  true;
  }

  struct rgw_swift_auth_info info;

  memset(&info, 0, sizeof(info));

  info.status = 401; // start with access denied, validate_token might change that

  int ret = rgw_swift_validate_token(s->os_auth_token, &info);
  if (ret < 0)
    return ret;

  if (!info.user) {
    dout(5) << "swift auth didn't authorize a user" << dendl;
    return false;
  }

  s->os_user = info.user;
  s->os_groups = info.auth_groups;

  string swift_user = s->os_user;

  dout(10) << "swift user=" << s->os_user << dendl;

  if (rgw_get_user_info_by_swift(store, swift_user, s->user) < 0) {
    dout(0) << "NOTICE: couldn't map swift user" << dendl;
    return false;
  }

  dout(10) << "user_id=" << s->user.user_id << dendl;

  return true;
}
