// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <curl/curl.h>
#include <curl/easy.h>
#include <curl/multi.h>

#include "rgw_common.h"
#include "rgw_http_client.h"
#include "rgw_http_errors.h"
#include "common/RefCountedObj.h"

#include "rgw_coroutine.h"

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

static curl_slist *headers_to_slist(list<pair<string, string> >& headers)
{
  curl_slist *h = NULL;

  list<pair<string, string> >::iterator iter;
  for (iter = headers.begin(); iter != headers.end(); ++iter) {
    pair<string, string>& p = *iter;
    string val = p.first;

    if (strncmp(val.c_str(), "HTTP_", 5) == 0) {
      val = val.substr(5);
    }

    /* we need to convert all underscores into dashes as some web servers forbid them
     * in the http header field names
     */
    for (size_t i = 0; i < val.size(); i++) {
      if (val[i] == '_') {
        val[i] = '-';
      }
    }

    val.append(": ");
    val.append(p.second);
    h = curl_slist_append(h, val.c_str());
  }

  return h;
}

int RGWHTTPClient::process(const char *method, const char *url)
{
  int ret = 0;
  CURL *curl_handle;

  char error_buf[CURL_ERROR_SIZE];

  last_method = (method ? method : "");
  last_url = (url ? url : "");

  curl_handle = curl_easy_init();

  dout(20) << "sending request to " << url << dendl;

  curl_slist *h = headers_to_slist(headers);

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
  if (!verify_ssl) {
    curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYHOST, 0L);
    dout(20) << "ssl verification is set to off" << dendl;
  }

  CURLcode status = curl_easy_perform(curl_handle);
  if (status) {
    dout(0) << "curl_easy_perform returned error: " << error_buf << dendl;
    ret = -EINVAL;
  }
  curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_status);
  curl_easy_cleanup(curl_handle);
  curl_slist_free_all(h);

  return ret;
}

struct rgw_http_req_data : public RefCountedObject {
  CURL *easy_handle;
  curl_slist *h;
  uint64_t id;
  int ret;
  atomic_t done;
  RGWHTTPClient *client;
  RGWHTTPManager *mgr;
  char error_buf[CURL_ERROR_SIZE];

  Mutex lock;
  Cond cond;

  rgw_http_req_data() : easy_handle(NULL), h(NULL), id(-1), ret(0), client(NULL),
                        mgr(NULL), lock("rgw_http_req_data::lock") {
    memset(error_buf, 0, sizeof(error_buf));
  }

  int wait() {
    Mutex::Locker l(lock);
    cond.Wait(lock);
    return ret;
  }

  void finish(int r) {
    Mutex::Locker l(lock);
    ret = r;
    cond.Signal();
    done.set(1);
    if (easy_handle)
      curl_easy_cleanup(easy_handle);

    if (h)
      curl_slist_free_all(h);

    easy_handle = NULL;
    h = NULL;
  }

  bool is_done() {
    return done.read() != 0;
  }

  int get_retcode() {
    Mutex::Locker l(lock);
    return ret;
  }
};

string RGWHTTPClient::to_str()
{
  string method_str = (last_method.empty() ? "<no-method>" : last_method);
  string url_str = (last_url.empty() ? "<no-url>" : last_url);
  return method_str + " " + url_str;
}

int RGWHTTPClient::get_req_retcode()
{
  if (!req_data) {
    return -EINVAL;
  }

  return req_data->get_retcode();
}

int RGWHTTPClient::init_request(const char *method, const char *url, rgw_http_req_data *_req_data)
{
  assert(!req_data);
  _req_data->get();
  req_data = _req_data;

  CURL *easy_handle;

  easy_handle = curl_easy_init();

  req_data->easy_handle = easy_handle;

  dout(20) << "sending request to " << url << dendl;

  curl_slist *h = headers_to_slist(headers);

  req_data->h = h;

  last_method = (method ? method : "");
  last_url = (url ? url : "");

  curl_easy_setopt(easy_handle, CURLOPT_CUSTOMREQUEST, method);
  curl_easy_setopt(easy_handle, CURLOPT_URL, url);
  curl_easy_setopt(easy_handle, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(easy_handle, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(easy_handle, CURLOPT_HEADERFUNCTION, receive_http_header);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEHEADER, (void *)this);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, receive_http_data);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEDATA, (void *)this);
  curl_easy_setopt(easy_handle, CURLOPT_ERRORBUFFER, (void *)req_data->error_buf);
  if (h) {
    curl_easy_setopt(easy_handle, CURLOPT_HTTPHEADER, (void *)h);
  }
  curl_easy_setopt(easy_handle, CURLOPT_READFUNCTION, send_http_data);
  curl_easy_setopt(easy_handle, CURLOPT_READDATA, (void *)this);
  curl_easy_setopt(easy_handle, CURLOPT_UPLOAD, 1L); 
  if (has_send_len) {
    curl_easy_setopt(easy_handle, CURLOPT_INFILESIZE, (void *)send_len); 
  }
  curl_easy_setopt(easy_handle, CURLOPT_PRIVATE, (void *)req_data);

  return 0;
}

int RGWHTTPClient::wait()
{
  if (!req_data->is_done()) {
    return req_data->wait();
  }

  return req_data->ret;
}

RGWHTTPClient::~RGWHTTPClient()
{
  if (req_data) {
    req_data->put();
  }
}


#ifdef HAVE_CURL_MULTI_WAIT

static int do_curl_wait(CephContext *cct, CURLM *handle, int signal_fd)
{
  int num_fds;
  struct curl_waitfd wait_fd;

  wait_fd.fd = signal_fd;
  wait_fd.events = CURL_WAIT_POLLIN;
  wait_fd.revents = 0;

  int ret = curl_multi_wait(handle, &wait_fd, 1, cct->_conf->rgw_curl_wait_timeout_ms, &num_fds);
  if (ret) {
    dout(0) << "ERROR: curl_multi_wait() returned " << ret << dendl;
    return -EIO;
  }

  if (wait_fd.revents > 0) {
    uint32_t buf;
    ret = read(signal_fd, (void *)&buf, sizeof(buf));
    if (ret < 0) {
      ret = -errno;
      dout(0) << "ERROR: " << __func__ << "(): read() returned " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

#else

static int do_curl_wait(CephContext *cct, CURLM *handle, int signal_fd)
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
    generic_dout(0) << "ERROR: curl_multi_fdset returned " << ret << dendl;
    return -EIO;
  }

  if (signal_fd >= maxfd) {
    maxfd = signal_fd + 1;
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

  if (signal_fd > 0 && FD_ISSET(signal_fd, &fdread)) {
    uint32_t buf;
    ret = read(signal_fd, (void *)&buf, sizeof(buf));
    if (ret < 0) {
      ret = -errno;
      dout(0) << "ERROR: " << __func__ << "(): read() returned " << ret << dendl;
      return ret;
    }
  }

  return 0;
}

#endif

void *RGWHTTPManager::ReqsThread::entry()
{
  manager->reqs_thread_entry();
  return NULL;
}

RGWHTTPManager::RGWHTTPManager(CephContext *_cct, RGWCompletionManager *_cm) : cct(_cct),
                                                    completion_mgr(_cm), is_threaded(false),
                                                    reqs_lock("RGWHTTPManager::reqs_lock"), num_reqs(0), max_threaded_req(0),
                                                    reqs_thread(NULL)
{
  multi_handle = (void *)curl_multi_init();
  thread_pipe[0] = -1;
  thread_pipe[1] = -1;
}

RGWHTTPManager::~RGWHTTPManager() {
  stop();
  if (multi_handle)
    curl_multi_cleanup((CURLM *)multi_handle);
}

void RGWHTTPManager::register_request(rgw_http_req_data *req_data)
{
  RWLock::WLocker rl(reqs_lock);
  req_data->id = num_reqs;
  reqs[num_reqs] = req_data;
  num_reqs++;
  ldout(cct, 20) << __func__ << " mgr=" << this << " req_data->id=" << req_data->id << ", easy_handle=" << req_data->easy_handle << dendl;
}

void RGWHTTPManager::complete_request(rgw_http_req_data *req_data)
{
  RWLock::WLocker rl(reqs_lock);
  _complete_request(req_data);
}

void RGWHTTPManager::_complete_request(rgw_http_req_data *req_data)
{
  map<uint64_t, rgw_http_req_data *>::iterator iter = reqs.find(req_data->id);
  if (iter != reqs.end()) {
    reqs.erase(iter);
  }
  if (completion_mgr) {
    completion_mgr->complete(req_data->client->get_user_info());
  }
  req_data->put();
}

void RGWHTTPManager::finish_request(rgw_http_req_data *req_data, int ret)
{
  req_data->finish(ret);
  complete_request(req_data);
}

void RGWHTTPManager::_finish_request(rgw_http_req_data *req_data, int ret)
{
  req_data->finish(ret);
  _complete_request(req_data);
}

int RGWHTTPManager::link_request(rgw_http_req_data *req_data)
{
  ldout(cct, 20) << __func__ << " req_data=" << req_data << " req_data->id=" << req_data->id << ", easy_handle=" << req_data->easy_handle << dendl;
  CURLMcode mstatus = curl_multi_add_handle((CURLM *)multi_handle, req_data->easy_handle);
  if (mstatus) {
    dout(0) << "ERROR: failed on curl_multi_add_handle, status=" << mstatus << dendl;
    return -EIO;
  }
  return 0;
}

void RGWHTTPManager::link_pending_requests()
{
  reqs_lock.get_read();
  if (max_threaded_req == num_reqs) {
    reqs_lock.unlock();
    return;
  }
  reqs_lock.unlock();

  RWLock::WLocker wl(reqs_lock);

  map<uint64_t, rgw_http_req_data *>::iterator iter = reqs.find(max_threaded_req);

  list<std::pair<rgw_http_req_data *, int> > remove_reqs;

  for (; iter != reqs.end(); ++iter) {
    rgw_http_req_data *req_data = iter->second;
    int r = link_request(req_data);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to link http request" << dendl;
      remove_reqs.push_back(std::make_pair(iter->second, r));
    } else {
      max_threaded_req = iter->first + 1;
    }
  }

  for (auto piter : remove_reqs) {
    rgw_http_req_data *req_data = piter.first;
    int r = piter.second;

    _finish_request(req_data, r);
  }
}

int RGWHTTPManager::add_request(RGWHTTPClient *client, const char *method, const char *url)
{
  rgw_http_req_data *req_data = new rgw_http_req_data;

  int ret = client->init_request(method, url, req_data);
  if (ret < 0) {
    req_data->put();
    return ret;
  }

  req_data->mgr = this;
  req_data->client = client;

  register_request(req_data);

  if (!is_threaded) {
    ret = link_request(req_data);
    return ret;
  }
  ret = signal_thread();
  if (ret < 0) {
    finish_request(req_data, ret);
  }

  return ret;
}

int RGWHTTPManager::process_requests(bool wait_for_data, bool *done)
{
  assert(!is_threaded);

  int still_running;
  int mstatus;

  do {
    if (wait_for_data) {
      int ret = do_curl_wait(cct, (CURLM *)multi_handle, -1);
      if (ret < 0) {
        return ret;
      }
    }

    mstatus = curl_multi_perform((CURLM *)multi_handle, &still_running);
    switch (mstatus) {
      case CURLM_OK:
      case CURLM_CALL_MULTI_PERFORM:
        break;
      default:
        dout(20) << "curl_multi_perform returned: " << mstatus << dendl;
        return -EINVAL;
    }
    int msgs_left;
    CURLMsg *msg;
    while ((msg = curl_multi_info_read((CURLM *)multi_handle, &msgs_left))) {
      if (msg->msg == CURLMSG_DONE) {
	CURL *e = msg->easy_handle;
	rgw_http_req_data *req_data;
	curl_easy_getinfo(e, CURLINFO_PRIVATE, (void **)&req_data);

	long http_status;
	curl_easy_getinfo(e, CURLINFO_RESPONSE_CODE, (void **)&http_status);

	int status = rgw_http_error_to_errno(http_status);
	int result = msg->data.result;
	finish_request(req_data, status);
        switch (result) {
          case CURLE_OK:
            break;
          default:
            dout(20) << "ERROR: msg->data.result=" << result << dendl;
            return -EIO;
        }
      }
    }
  } while (mstatus == CURLM_CALL_MULTI_PERFORM);

  *done = (still_running == 0);

  return 0;
}

int RGWHTTPManager::complete_requests()
{
  bool done;
  int ret;
  do {
    ret = process_requests(true, &done);
  } while (!done && !ret);

  return ret;
}

int RGWHTTPManager::set_threaded()
{
  is_threaded = true;
  reqs_thread = new ReqsThread(this);
  reqs_thread->create("http_manager");

  int r = pipe(thread_pipe);
  if (r < 0) {
    r = -errno;
    ldout(cct, 0) << "ERROR: pipe() returned errno=" << r << dendl;
    return r;
  }
  return 0;
}

void RGWHTTPManager::stop()
{
  if (is_threaded) {
    going_down.set(1);
    reqs_thread->join();
    delete reqs_thread;
  }
}

int RGWHTTPManager::signal_thread()
{
  uint32_t buf = 0;
  int ret = write(thread_pipe[1], (void *)&buf, sizeof(buf));
  if (ret < 0) {
    ret = -errno;
    ldout(cct, 0) << "ERROR: " << __func__ << ": write() returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

void *RGWHTTPManager::reqs_thread_entry()
{
  int still_running;
  int mstatus;

  ldout(cct, 20) << __func__ << ": start" << dendl;

  while (!going_down.read()) {
    int ret = do_curl_wait(cct, (CURLM *)multi_handle, thread_pipe[0]);
    if (ret < 0) {
      dout(0) << "ERROR: do_curl_wait() returned: " << ret << dendl;
      return NULL;
    }

    link_pending_requests();

    mstatus = curl_multi_perform((CURLM *)multi_handle, &still_running);
    switch (mstatus) {
      case CURLM_OK:
      case CURLM_CALL_MULTI_PERFORM:
        break;
      default:
        dout(10) << "curl_multi_perform returned: " << mstatus << dendl;
	break;
    }
    int msgs_left;
    CURLMsg *msg;
    while ((msg = curl_multi_info_read((CURLM *)multi_handle, &msgs_left))) {
      if (msg->msg == CURLMSG_DONE) {
	int result = msg->data.result;
	CURL *e = msg->easy_handle;
	rgw_http_req_data *req_data;
	curl_easy_getinfo(e, CURLINFO_PRIVATE, (void **)&req_data);
	curl_multi_remove_handle((CURLM *)multi_handle, e);

	long http_status;
	curl_easy_getinfo(e, CURLINFO_RESPONSE_CODE, (void **)&http_status);

	int status = rgw_http_error_to_errno(http_status);
        if (result != CURLE_OK && http_status == 0) {
          status = -EAGAIN;
        }
        int id = req_data->id;
	finish_request(req_data, status);
        switch (result) {
          case CURLE_OK:
            break;
          default:
            dout(20) << "ERROR: msg->data.result=" << result << " req_data->id=" << id << " http_status=" << http_status << dendl;
	    break;
        }
      }
    }
  }

  RWLock::WLocker rl(reqs_lock);
  map<uint64_t, rgw_http_req_data *>::iterator iter = reqs.begin();
  for (; iter != reqs.end(); ++iter) {
    _finish_request(iter->second, -ECANCELED);
  }

  reqs.clear();

  if (completion_mgr) {
    completion_mgr->go_down();
  }
  
  return 0;
}


