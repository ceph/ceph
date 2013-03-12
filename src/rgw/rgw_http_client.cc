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

static size_t read_http_data(void *ptr, size_t size, size_t nmemb, void *_info)
{
  RGWHTTPClient *client = (RGWHTTPClient *)_info;
  size_t len = size * nmemb;
  int ret = client->read_data(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->read_data() returned ret=" << ret << dendl;
  }

  return len;
}

int RGWHTTPClient::process(const string& url)
{
  int ret = 0;
  CURL *curl_handle;

  char error_buf[CURL_ERROR_SIZE];

  curl_handle = curl_easy_init();

  dout(20) << "sending request to " << url << dendl;

  curl_slist *h = NULL;

  list<pair<string, string> >::iterator iter;
  for (iter = headers.begin(); iter != headers.end(); ++iter) {
    pair<string, string>& p = *iter;
    string val = p.first;
    val.append(": ");
    val.append(p.second);
    h = curl_slist_append(h, val.c_str());
  }

  curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, read_http_header);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEHEADER, (void *)this);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, read_http_data);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)this);
  curl_easy_setopt(curl_handle, CURLOPT_ERRORBUFFER, (void *)error_buf);
  if (h) {
    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, (void *)h);
  }
  CURLcode status = curl_easy_perform(curl_handle);
  if (status) {
    dout(0) << "curl_easy_performed returned error: " << error_buf << dendl;
    ret = -EINVAL;
  }
  curl_easy_cleanup(curl_handle);
  curl_slist_free_all(h);

  return ret;
}


