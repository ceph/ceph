#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <curl/curl.h>
#include <curl/types.h>
#include <curl/easy.h>

#include "rgw_common.h"
#include "rgw_os.h"


static size_t read_http_header(void *ptr, size_t size, size_t nmemb, void *_info)
{
  size_t len = size * nmemb;
  char line[len + 1];
  struct rgw_os_auth_info *info = (struct rgw_os_auth_info *)_info;

  char *s = (char *)ptr, *end = (char *)ptr + len;
  char *p = line;
  RGW_LOG(10) << "read_http_header" << std::endl;

  while (s != end) {
    if (*s == '\r') {
      s++;
      continue;
    }
    if (*s == '\n') {
      *p = '\0';
      RGW_LOG(10) << "os_auth:" << line << std::endl;
      // TODO: fill whatever data required here
    }
    if (s != end)
      *p++ = *s++;
  }
  return len;
}

static int rgw_os_validate_token(const char *token, struct rgw_os_auth_info *info)
{
  CURL *curl_handle;
  string auth_url = "http://127.0.0.1:11000/token";
  char url_buf[auth_url.size() + 1 + strlen(token) + 1];
  sprintf(url_buf, "%s/%s", auth_url.c_str(), token);

  RGW_LOG(10) << "rgw_os_validate_token url=" << url_buf << std::endl;

  curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_URL, url_buf);
  curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);

  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, read_http_header);

  curl_easy_setopt(curl_handle,   CURLOPT_WRITEHEADER, info);

  curl_easy_perform(curl_handle);
  curl_easy_cleanup(curl_handle);

  return 0;
}

bool rgw_verify_os_token(req_state *s)
{
  struct rgw_os_auth_info info;

  int ret = rgw_os_validate_token(s->os_auth_token, &info);

  return true; // for now
}
