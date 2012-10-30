#include <curl/curl.h>
#include <curl/easy.h>

#include "rgw_common.h"
#include "rgw_http_client.h"

#define dout_subsys ceph_subsys_rgw

static size_t read_http_header(void *ptr, size_t size, size_t nmemb, void *_info)
{
  RGWHTTPClient *client = (RGWHTTPClient *)_info;
  size_t len = size * nmemb;
  int ret = client->read_header(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->read_header() returned ret=" << ret << dendl;
  }

  return len;
}

int RGWHTTPClient::process(const string& url)
{
  CURL *curl_handle;

  curl_handle = curl_easy_init();
  curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, read_http_header);
  curl_easy_setopt(curl_handle,   CURLOPT_WRITEHEADER, (void *)this);
  curl_easy_perform(curl_handle);
  curl_easy_cleanup(curl_handle);

  return 0;
}


