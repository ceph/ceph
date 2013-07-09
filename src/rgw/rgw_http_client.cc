#include <curl/curl.h>
#include <curl/easy.h>
#include <curl/multi.h>

#include "rgw_common.h"
#include "rgw_http_client.h"

#define dout_subsys ceph_subsys_rgw

static size_t receive_http_header(void *ptr, size_t size, size_t nmemb, void *_info)
{
  RGWHTTPClient *client = static_cast<RGWHTTPClient *>(_info);
  size_t len = size * nmemb;
  int ret = client->receive_header(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_header() returned ret=" << ret << dendl;
  }

  return len;
}

static size_t receive_http_data(void *ptr, size_t size, size_t nmemb, void *_info)
{
  RGWHTTPClient *client = static_cast<RGWHTTPClient *>(_info);
  size_t len = size * nmemb;
  int ret = client->receive_data(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_data() returned ret=" << ret << dendl;
  }

  return len;
}

static size_t send_http_data(void *ptr, size_t size, size_t nmemb, void *_info)
{
  RGWHTTPClient *client = static_cast<RGWHTTPClient *>(_info);
  int ret = client->send_data(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_data() returned ret=" << ret << dendl;
  }

  return ret;
}

int RGWHTTPClient::process(const char *method, const char *url)
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

    if (strncmp(val.c_str(), "HTTP_", 5) == 0) {
      val = val.substr(5);
    }
    val.append(": ");
    val.append(p.second);
    h = curl_slist_append(h, val.c_str());
  }

  curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, method);
  curl_easy_setopt(curl_handle, CURLOPT_URL, url);
  curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, receive_http_header);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEHEADER, (void *)this);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, receive_http_data);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)this);
  curl_easy_setopt(curl_handle, CURLOPT_ERRORBUFFER, (void *)error_buf);
  if (h) {
    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, (void *)h);
  }
  curl_easy_setopt(curl_handle, CURLOPT_READFUNCTION, send_http_data);
  curl_easy_setopt(curl_handle, CURLOPT_READDATA, (void *)this);
  curl_easy_setopt(curl_handle, CURLOPT_UPLOAD, 1L); 
  if (has_send_len) {
    curl_easy_setopt(curl_handle, CURLOPT_INFILESIZE, (void *)send_len); 
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

struct multi_req_data {
  CURL *easy_handle;
  CURLM *multi_handle;
  curl_slist *h;

  multi_req_data() : easy_handle(NULL), multi_handle(NULL), h(NULL) {}
  ~multi_req_data() {
    if (multi_handle)
      curl_multi_cleanup(multi_handle);

    if (easy_handle)
      curl_easy_cleanup(easy_handle);

    if (h)
      curl_slist_free_all(h);
  }
};

int RGWHTTPClient::init_async(const char *method, const char *url, void **handle)
{
  CURL *easy_handle;
  CURLM *multi_handle;
  multi_req_data *req_data = new multi_req_data;
  *handle = (void *)req_data;

  char error_buf[CURL_ERROR_SIZE];

  multi_handle = curl_multi_init();
  easy_handle = curl_easy_init();

  req_data->multi_handle = multi_handle;
  req_data->easy_handle = easy_handle;

  CURLMcode mstatus = curl_multi_add_handle(multi_handle, easy_handle);
  if (mstatus) {
    dout(0) << "ERROR: failed on curl_multi_add_handle, status=" << mstatus << dendl;
    delete req_data;
    return -EIO;
  }

  dout(20) << "sending request to " << url << dendl;

  curl_slist *h = NULL;

  list<pair<string, string> >::iterator iter;
  for (iter = headers.begin(); iter != headers.end(); ++iter) {
    pair<string, string>& p = *iter;
    string val = p.first;

    if (strncmp(val.c_str(), "HTTP_", 5) == 0) {
      val = val.substr(5);
    }
    val.append(": ");
    val.append(p.second);
    h = curl_slist_append(h, val.c_str());
  }

  req_data->h = h;

  curl_easy_setopt(easy_handle, CURLOPT_CUSTOMREQUEST, method);
  curl_easy_setopt(easy_handle, CURLOPT_URL, url);
  curl_easy_setopt(easy_handle, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(easy_handle, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(easy_handle, CURLOPT_HEADERFUNCTION, receive_http_header);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEHEADER, (void *)this);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, receive_http_data);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEDATA, (void *)this);
  curl_easy_setopt(easy_handle, CURLOPT_ERRORBUFFER, (void *)error_buf);
  if (h) {
    curl_easy_setopt(easy_handle, CURLOPT_HTTPHEADER, (void *)h);
  }
  curl_easy_setopt(easy_handle, CURLOPT_READFUNCTION, send_http_data);
  curl_easy_setopt(easy_handle, CURLOPT_READDATA, (void *)this);
  curl_easy_setopt(easy_handle, CURLOPT_UPLOAD, 1L); 
  if (has_send_len) {
    curl_easy_setopt(easy_handle, CURLOPT_INFILESIZE, (void *)send_len); 
  }

  return 0;
}

#if HAVE_CURL_MULTI_WAIT

static int do_curl_wait(CephContext *cct, CURLM *handle)
{
  int num_fds;
  int ret = curl_multi_wait(handle, NULL, 0, cct->_conf->rgw_curl_wait_timeout_ms, &num_fds);
  if (ret) {
    dout(0) << "ERROR: curl_multi_wait() returned " << ret << dendl;
    return -EIO;
  }
  return 0;
}

#else

static int do_curl_wait(CephContext *cct, CURLM *handle)
{
  fd_set fdread;
  fd_set fdwrite;
  fd_set fdexcep;
  int maxfd = -1;
 
  FD_ZERO(&fdread);
  FD_ZERO(&fdwrite);
  FD_ZERO(&fdexcep);

  /* get file descriptors from the transfers */ 
  int ret = curl_multi_fdset(handle, &fdread, &fdwrite, &fdexcep, &maxfd);
  if (ret) {
    dout(0) << "ERROR: curl_multi_fdset returned " << ret << dendl;
    return -EIO;
  }

  /* forcing a strict timeout, as the returned fdsets might not reference all fds we wait on */
  uint64_t to = cct->_conf->rgw_curl_wait_timeout_ms;
#define RGW_CURL_TIMEOUT 1000
  if (!to)
    to = RGW_CURL_TIMEOUT;
  struct timeval timeout;
  timeout.tv_sec = to / 1000;
  timeout.tv_usec = to % 1000;

  ret = select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout);
  if (ret < 0) {
    ret = -errno;
    dout(0) << "ERROR: select returned " << ret << dendl;
    return ret;
  }

  return 0;
}

#endif

int RGWHTTPClient::process_request(void *handle, bool wait_for_data, bool *done)
{
  multi_req_data *req_data = (multi_req_data *)handle;
  int still_running;
  int mstatus;

  do {
    if (wait_for_data) {
      int ret = do_curl_wait(cct, req_data->multi_handle);
      if (ret < 0) {
        return ret;
      }
    }

    mstatus = curl_multi_perform(req_data->multi_handle, &still_running);
    dout(20) << "curl_multi_perform returned: " << mstatus << dendl;
    switch (mstatus) {
      case CURLM_OK:
      case CURLM_CALL_MULTI_PERFORM:
        break;
      default:
        return -EINVAL;
    }
    int msgs_left;
    CURLMsg *msg;
    while ((msg = curl_multi_info_read(req_data->multi_handle, &msgs_left))) {
      if (msg->msg == CURLMSG_DONE) {
        switch (msg->data.result) {
          case CURLE_OK:
            break;
          default:
            dout(20) << "ERROR: msg->data.result=" << msg->data.result << dendl;
            return -EIO;
        }
      }
    }
  } while (mstatus == CURLM_CALL_MULTI_PERFORM);

  *done = (still_running == 0);

  return 0;
}

int RGWHTTPClient::complete_request(void *handle)
{
  bool done;
  int ret;
  do {
    ret = process_request(handle, true, &done);
  } while (!done && !ret);
  multi_req_data *req_data = (multi_req_data *)handle;
  delete req_data;

  return ret;
}
