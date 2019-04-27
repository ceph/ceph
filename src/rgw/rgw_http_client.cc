// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "common/errno.h"

#include <boost/utility/string_ref.hpp>

#include <curl/curl.h>
#include <curl/easy.h>
#include <curl/multi.h>

#include "rgw_common.h"
#include "rgw_http_client.h"
#include "rgw_http_errors.h"
#include "common/async/completion.h"
#include "common/RefCountedObj.h"

#include "rgw_coroutine.h"
#include "rgw_tools.h"

#include <atomic>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

RGWHTTPManager *rgw_http_manager;

struct RGWCurlHandle;

static void do_curl_easy_cleanup(RGWCurlHandle *curl_handle);

struct rgw_http_req_data : public RefCountedObject {
  RGWCurlHandle *curl_handle{nullptr};
  curl_slist *h{nullptr};
  uint64_t id;
  int ret{0};
  std::atomic<bool> done = { false };
  RGWHTTPClient *client{nullptr};
  rgw_io_id control_io_id;
  void *user_info{nullptr};
  bool registered{false};
  RGWHTTPManager *mgr{nullptr};
  char error_buf[CURL_ERROR_SIZE];
  bool write_paused{false};
  bool read_paused{false};

  Mutex lock;
  Cond cond;

  using Signature = void(boost::system::error_code);
  using Completion = ceph::async::Completion<Signature>;
  std::unique_ptr<Completion> completion;

  rgw_http_req_data() : id(-1), lock("rgw_http_req_data::lock") {
    memset(error_buf, 0, sizeof(error_buf));
  }

  template <typename ExecutionContext, typename CompletionToken>
  auto async_wait(ExecutionContext& ctx, CompletionToken&& token) {
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto& handler = init.completion_handler;
    completion = Completion::create(ctx.get_executor(), std::move(handler));
    return init.result.get();
  }

  int wait(optional_yield y) {
    Mutex::Locker l(lock);
    if (done) {
      return ret;
    }
#ifdef HAVE_BOOST_CONTEXT
    if (y) {
      auto& context = y.get_io_context();
      auto& yield = y.get_yield_context();
      boost::system::error_code ec;
      async_wait(context, yield[ec]);
      return -ec.value();
    }
    // work on asio threads should be asynchronous, so warn when they block
    if (is_asio_thread) {
      dout(20) << "WARNING: blocking http request" << dendl;
    }
#endif
    cond.Wait(lock);
    return ret;
  }

  void set_state(int bitmask);

  void finish(int r) {
    Mutex::Locker l(lock);
    ret = r;
    if (curl_handle)
      do_curl_easy_cleanup(curl_handle);

    if (h)
      curl_slist_free_all(h);

    curl_handle = NULL;
    h = NULL;
    done = true;
    if (completion) {
      boost::system::error_code ec(-ret, boost::system::system_category());
      Completion::post(std::move(completion), ec);
    } else {
      cond.Signal();
    }
  }

  bool _is_done() {
    return done;
  }

  bool is_done() {
    Mutex::Locker l(lock);
    return done;
  }

  int get_retcode() {
    Mutex::Locker l(lock);
    return ret;
  }

  RGWHTTPManager *get_manager() {
    Mutex::Locker l(lock);
    return mgr;
  }

  CURL *get_easy_handle() const;
};

struct RGWCurlHandle {
  int uses;
  mono_time lastuse;
  CURL* h;

  explicit RGWCurlHandle(CURL* h) : uses(0), h(h) {};
  CURL* operator*() {
    return this->h;
  }
};

void rgw_http_req_data::set_state(int bitmask) {
  /* no need to lock here, moreover curl_easy_pause() might trigger
   * the data receive callback :/
   */
  CURLcode rc = curl_easy_pause(**curl_handle, bitmask);
  if (rc != CURLE_OK) {
    dout(0) << "ERROR: curl_easy_pause() returned rc=" << rc << dendl;
  }
}

#define MAXIDLE 5
class RGWCurlHandles : public Thread {
public:
  Mutex cleaner_lock;
  std::vector<RGWCurlHandle*>saved_curl;
  int cleaner_shutdown;
  Cond cleaner_cond;

  RGWCurlHandles() :
    cleaner_lock{"RGWCurlHandles::cleaner_lock"},
    cleaner_shutdown{0} {
  }

  RGWCurlHandle* get_curl_handle();
  void release_curl_handle_now(RGWCurlHandle* curl);
  void release_curl_handle(RGWCurlHandle* curl);
  void flush_curl_handles();
  void* entry();
  void stop();
};

RGWCurlHandle* RGWCurlHandles::get_curl_handle() {
  RGWCurlHandle* curl = 0;
  CURL* h;
  {
    Mutex::Locker lock(cleaner_lock);
    if (!saved_curl.empty()) {
      curl = *saved_curl.begin();
      saved_curl.erase(saved_curl.begin());
    }
  }
  if (curl) {
  } else if ((h = curl_easy_init())) {
    curl = new RGWCurlHandle{h};
  } else {
    // curl = 0;
  }
  return curl;
}

void RGWCurlHandles::release_curl_handle_now(RGWCurlHandle* curl)
{
  curl_easy_cleanup(**curl);
  delete curl;
}

void RGWCurlHandles::release_curl_handle(RGWCurlHandle* curl)
{
  if (cleaner_shutdown) {
    release_curl_handle_now(curl);
  } else {
    curl_easy_reset(**curl);
    Mutex::Locker lock(cleaner_lock);
    curl->lastuse = mono_clock::now();
    saved_curl.insert(saved_curl.begin(), 1, curl);
  }
}

void* RGWCurlHandles::entry()
{
  RGWCurlHandle* curl;
  Mutex::Locker lock(cleaner_lock);

  for (;;) {
    if (cleaner_shutdown) {
      if (saved_curl.empty())
        break;
    } else {
      utime_t release = ceph_clock_now() + utime_t(MAXIDLE,0);
      cleaner_cond.WaitUntil(cleaner_lock, release);
    }
    mono_time now = mono_clock::now();
    while (!saved_curl.empty()) {
      auto cend = saved_curl.end();
      --cend;
      curl = *cend;
      if (!cleaner_shutdown && now - curl->lastuse < std::chrono::seconds(MAXIDLE))
        break;
      saved_curl.erase(cend);
      release_curl_handle_now(curl);
    }
  }
  return nullptr;
}

void RGWCurlHandles::stop()
{
  Mutex::Locker lock(cleaner_lock);
  cleaner_shutdown = 1;
  cleaner_cond.Signal();
}

void RGWCurlHandles::flush_curl_handles()
{
  stop();
  join();
  if (!saved_curl.empty()) {
    dout(0) << "ERROR: " << __func__ << " failed final cleanup" << dendl;
  }
  saved_curl.shrink_to_fit();
}

CURL *rgw_http_req_data::get_easy_handle() const
{
  return **curl_handle;
}

static RGWCurlHandles *handles;

static RGWCurlHandle *do_curl_easy_init()
{
  return handles->get_curl_handle();
}

static void do_curl_easy_cleanup(RGWCurlHandle *curl_handle)
{
  handles->release_curl_handle(curl_handle);
}

// XXX make this part of the token cache?  (but that's swift-only;
//	and this especially needs to integrates with s3...)

void rgw_setup_saved_curl_handles()
{
  handles = new RGWCurlHandles();
  handles->create("rgw_curl");
}

void rgw_release_all_curl_handles()
{
  handles->flush_curl_handles();
  delete handles;
}

void RGWIOProvider::assign_io(RGWIOIDProvider& io_id_provider, int io_type)
{
  if (id == 0) {
    id = io_id_provider.get_next();
  }
}

/*
 * the following set of callbacks will be called either on RGWHTTPManager::process(),
 * or via the RGWHTTPManager async processing.
 */
size_t RGWHTTPClient::receive_http_header(void * const ptr,
                                          const size_t size,
                                          const size_t nmemb,
                                          void * const _info)
{
  rgw_http_req_data *req_data = static_cast<rgw_http_req_data *>(_info);
  size_t len = size * nmemb;

  Mutex::Locker l(req_data->lock);
  
  if (!req_data->registered) {
    return len;
  }

  int ret = req_data->client->receive_header(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_header() returned ret=" << ret << dendl;
  }

  return len;
}

size_t RGWHTTPClient::receive_http_data(void * const ptr,
                                        const size_t size,
                                        const size_t nmemb,
                                        void * const _info)
{
  rgw_http_req_data *req_data = static_cast<rgw_http_req_data *>(_info);
  size_t len = size * nmemb;

  bool pause = false;

  RGWHTTPClient *client;

  {
    Mutex::Locker l(req_data->lock);
    if (!req_data->registered) {
      return len;
    }

    client = req_data->client;
  }

  size_t& skip_bytes = client->receive_pause_skip;

  if (skip_bytes >= len) {
    skip_bytes -= len;
    return len;
  }

  int ret = client->receive_data((char *)ptr + skip_bytes, len - skip_bytes, &pause);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_data() returned ret=" << ret << dendl;
  }

  if (pause) {
    dout(20) << "RGWHTTPClient::receive_http_data(): pause" << dendl;
    skip_bytes = len;
    Mutex::Locker l(req_data->lock);
    req_data->read_paused = true;
    return CURL_WRITEFUNC_PAUSE;
  }

  skip_bytes = 0;

  return len;
}

size_t RGWHTTPClient::send_http_data(void * const ptr,
                                     const size_t size,
                                     const size_t nmemb,
                                     void * const _info)
{
  rgw_http_req_data *req_data = static_cast<rgw_http_req_data *>(_info);

  RGWHTTPClient *client;

  {
    Mutex::Locker l(req_data->lock);
  
    if (!req_data->registered) {
      return 0;
    }

    client = req_data->client;
  }

  bool pause = false;

  int ret = client->send_data(ptr, size * nmemb, &pause);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_data() returned ret=" << ret << dendl;
  }

  if (ret == 0 &&
      pause) {
    Mutex::Locker l(req_data->lock);
    req_data->write_paused = true;
    return CURL_READFUNC_PAUSE;
  }

  return ret;
}

Mutex& RGWHTTPClient::get_req_lock()
{
  return req_data->lock;
}

void RGWHTTPClient::_set_write_paused(bool pause)
{
  ceph_assert(req_data->lock.is_locked());
  
  RGWHTTPManager *mgr = req_data->mgr;
  if (pause == req_data->write_paused) {
    return;
  }
  if (pause) {
    mgr->set_request_state(this, SET_WRITE_PAUSED);
  } else {
    mgr->set_request_state(this, SET_WRITE_RESUME);
  }
}

void RGWHTTPClient::_set_read_paused(bool pause)
{
  ceph_assert(req_data->lock.is_locked());
  
  RGWHTTPManager *mgr = req_data->mgr;
  if (pause == req_data->read_paused) {
    return;
  }
  if (pause) {
    mgr->set_request_state(this, SET_READ_PAUSED);
  } else {
    mgr->set_request_state(this, SET_READ_RESUME);
  }
}

static curl_slist *headers_to_slist(param_vec_t& headers)
{
  curl_slist *h = NULL;

  param_vec_t::iterator iter;
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
    
    val = camelcase_dash_http_attr(val);

    // curl won't send headers with empty values unless it ends with a ; instead
    if (p.second.empty()) {
      val.append(1, ';');
    } else {
      val.append(": ");
      val.append(p.second);
    }
    h = curl_slist_append(h, val.c_str());
  }

  return h;
}

static bool is_upload_request(const string& method)
{
  return method == "POST" || method == "PUT";
}

/*
 * process a single simple one off request
 */
int RGWHTTPClient::process(optional_yield y)
{
  return RGWHTTP::process(this, y);
}

string RGWHTTPClient::to_str()
{
  string method_str = (method.empty() ? "<no-method>" : method);
  string url_str = (url.empty() ? "<no-url>" : url);
  return method_str + " " + url_str;
}

int RGWHTTPClient::get_req_retcode()
{
  if (!req_data) {
    return -EINVAL;
  }

  return req_data->get_retcode();
}

/*
 * init request, will be used later with RGWHTTPManager
 */
int RGWHTTPClient::init_request(rgw_http_req_data *_req_data)
{
  ceph_assert(!req_data);
  _req_data->get();
  req_data = _req_data;

  req_data->curl_handle = do_curl_easy_init();

  CURL *easy_handle = req_data->get_easy_handle();

  dout(20) << "sending request to " << url << dendl;

  curl_slist *h = headers_to_slist(headers);

  req_data->h = h;

  curl_easy_setopt(easy_handle, CURLOPT_CUSTOMREQUEST, method.c_str());
  curl_easy_setopt(easy_handle, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy_handle, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(easy_handle, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(easy_handle, CURLOPT_HEADERFUNCTION, receive_http_header);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEHEADER, (void *)req_data);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, receive_http_data);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEDATA, (void *)req_data);
  curl_easy_setopt(easy_handle, CURLOPT_ERRORBUFFER, (void *)req_data->error_buf);
  curl_easy_setopt(easy_handle, CURLOPT_LOW_SPEED_TIME, cct->_conf->rgw_curl_low_speed_time);
  curl_easy_setopt(easy_handle, CURLOPT_LOW_SPEED_LIMIT, cct->_conf->rgw_curl_low_speed_limit);
  if (h) {
    curl_easy_setopt(easy_handle, CURLOPT_HTTPHEADER, (void *)h);
  }
  curl_easy_setopt(easy_handle, CURLOPT_READFUNCTION, send_http_data);
  curl_easy_setopt(easy_handle, CURLOPT_READDATA, (void *)req_data);
  if (send_data_hint || is_upload_request(method)) {
    curl_easy_setopt(easy_handle, CURLOPT_UPLOAD, 1L);
  }
  if (has_send_len) {
    curl_easy_setopt(easy_handle, CURLOPT_INFILESIZE, (void *)send_len); 
  }
  if (!verify_ssl) {
    curl_easy_setopt(easy_handle, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(easy_handle, CURLOPT_SSL_VERIFYHOST, 0L);
    dout(20) << "ssl verification is set to off" << dendl;
  }
  curl_easy_setopt(easy_handle, CURLOPT_PRIVATE, (void *)req_data);

  return 0;
}

bool RGWHTTPClient::is_done()
{
  return req_data->is_done();
}

/*
 * wait for async request to complete
 */
int RGWHTTPClient::wait(optional_yield y)
{
  return req_data->wait(y);
}

void RGWHTTPClient::cancel()
{
  if (req_data) {
    RGWHTTPManager *http_manager = req_data->mgr;
    if (http_manager) {
      http_manager->remove_request(this);
    }
  }
}

RGWHTTPClient::~RGWHTTPClient()
{
  cancel();
  if (req_data) {
    req_data->put();
  }
}


int RGWHTTPHeadersCollector::receive_header(void * const ptr, const size_t len)
{
  const boost::string_ref header_line(static_cast<const char *>(ptr), len);

  /* We're tokening the line that way due to backward compatibility. */
  const size_t sep_loc = header_line.find_first_of(" \t:");

  if (boost::string_ref::npos == sep_loc) {
    /* Wrongly formatted header? Just skip it. */
    return 0;
  }

  header_name_t name(header_line.substr(0, sep_loc));
  if (0 == relevant_headers.count(name)) {
    /* Not interested in this particular header. */
    return 0;
  }

  const auto value_part = header_line.substr(sep_loc + 1);

  /* Skip spaces and tabs after the separator. */
  const size_t val_loc_s = value_part.find_first_not_of(' ');
  const size_t val_loc_e = value_part.find_first_of("\r\n");

  if (boost::string_ref::npos == val_loc_s ||
      boost::string_ref::npos == val_loc_e) {
    /* Empty value case. */
    found_headers.emplace(name, header_value_t());
  } else {
    found_headers.emplace(name, header_value_t(
        value_part.substr(val_loc_s, val_loc_e - val_loc_s)));
  }

  return 0;
}

int RGWHTTPTransceiver::send_data(void* ptr, size_t len, bool* pause)
{
  int length_to_copy = 0;
  if (post_data_index < post_data.length()) {
    length_to_copy = min(post_data.length() - post_data_index, len);
    memcpy(ptr, post_data.data() + post_data_index, length_to_copy);
    post_data_index += length_to_copy;
  }
  return length_to_copy;
}


static int clear_signal(int fd)
{
  // since we're in non-blocking mode, we can try to read a lot more than
  // one signal from signal_thread() to avoid later wakeups. non-blocking reads
  // are also required to support the curl_multi_wait bug workaround
  std::array<char, 256> buf;
  int ret = ::read(fd, (void *)buf.data(), buf.size());
  if (ret < 0) {
    ret = -errno;
    return ret == -EAGAIN ? 0 : ret; // clear EAGAIN
  }
  return 0;
}

#if HAVE_CURL_MULTI_WAIT

static std::once_flag detect_flag;
static bool curl_multi_wait_bug_present = false;

static int detect_curl_multi_wait_bug(CephContext *cct, CURLM *handle,
                                      int write_fd, int read_fd)
{
  int ret = 0;

  // write to write_fd so that read_fd becomes readable
  uint32_t buf = 0;
  ret = ::write(write_fd, &buf, sizeof(buf));
  if (ret < 0) {
    ret = -errno;
    ldout(cct, 0) << "ERROR: " << __func__ << "(): write() returned " << ret << dendl;
    return ret;
  }

  // pass read_fd in extra_fds for curl_multi_wait()
  int num_fds;
  struct curl_waitfd wait_fd;

  wait_fd.fd = read_fd;
  wait_fd.events = CURL_WAIT_POLLIN;
  wait_fd.revents = 0;

  ret = curl_multi_wait(handle, &wait_fd, 1, 0, &num_fds);
  if (ret != CURLM_OK) {
    ldout(cct, 0) << "ERROR: curl_multi_wait() returned " << ret << dendl;
    return -EIO;
  }

  // curl_multi_wait should flag revents when extra_fd is readable. if it
  // doesn't, the bug is present and we can't rely on revents
  if (wait_fd.revents == 0) {
    curl_multi_wait_bug_present = true;
    ldout(cct, 0) << "WARNING: detected a version of libcurl which contains a "
        "bug in curl_multi_wait(). enabling a workaround that may degrade "
        "performance slightly." << dendl;
  }

  return clear_signal(read_fd);
}

static bool is_signaled(const curl_waitfd& wait_fd)
{
  if (wait_fd.fd < 0) {
    // no fd to signal
    return false;
  }

  if (curl_multi_wait_bug_present) {
    // we can't rely on revents, so we always return true if a wait_fd is given.
    // this means we'll be trying a non-blocking read on this fd every time that
    // curl_multi_wait() wakes up
    return true;
  }

  return wait_fd.revents > 0;
}

static int do_curl_wait(CephContext *cct, CURLM *handle, int signal_fd)
{
  int num_fds;
  struct curl_waitfd wait_fd;

  wait_fd.fd = signal_fd;
  wait_fd.events = CURL_WAIT_POLLIN;
  wait_fd.revents = 0;

  int ret = curl_multi_wait(handle, &wait_fd, 1, cct->_conf->rgw_curl_wait_timeout_ms, &num_fds);
  if (ret) {
    ldout(cct, 0) << "ERROR: curl_multi_wait() returned " << ret << dendl;
    return -EIO;
  }

  if (is_signaled(wait_fd)) {
    ret = clear_signal(signal_fd);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): read() returned " << ret << dendl;
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
    ldout(cct, 0) << "ERROR: curl_multi_fdset returned " << ret << dendl;
    return -EIO;
  }

  if (signal_fd > 0) {
    FD_SET(signal_fd, &fdread);
    if (signal_fd >= maxfd) {
      maxfd = signal_fd + 1;
    }
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
    ldout(cct, 0) << "ERROR: select returned " << ret << dendl;
    return ret;
  }

  if (signal_fd > 0 && FD_ISSET(signal_fd, &fdread)) {
    ret = clear_signal(signal_fd);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): read() returned " << ret << dendl;
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

/*
 * RGWHTTPManager has two modes of operation: threaded and non-threaded.
 */
RGWHTTPManager::RGWHTTPManager(CephContext *_cct, RGWCompletionManager *_cm) : cct(_cct),
                                                    completion_mgr(_cm), is_started(false),
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
  req_data->registered = true;
  reqs[num_reqs] = req_data;
  num_reqs++;
  ldout(cct, 20) << __func__ << " mgr=" << this << " req_data->id=" << req_data->id << ", curl_handle=" << req_data->curl_handle << dendl;
}

bool RGWHTTPManager::unregister_request(rgw_http_req_data *req_data)
{
  RWLock::WLocker rl(reqs_lock);
  if (!req_data->registered) {
    return false;
  }
  req_data->get();
  req_data->registered = false;
  unregistered_reqs.push_back(req_data);
  ldout(cct, 20) << __func__ << " mgr=" << this << " req_data->id=" << req_data->id << ", curl_handle=" << req_data->curl_handle << dendl;
  return true;
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
  {
    Mutex::Locker l(req_data->lock);
    req_data->mgr = nullptr;
  }
  if (completion_mgr) {
    completion_mgr->complete(NULL, req_data->control_io_id, req_data->user_info);
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

void RGWHTTPManager::_set_req_state(set_state& ss)
{
  ss.req->set_state(ss.bitmask);
}
/*
 * hook request to the curl multi handle
 */
int RGWHTTPManager::link_request(rgw_http_req_data *req_data)
{
  ldout(cct, 20) << __func__ << " req_data=" << req_data << " req_data->id=" << req_data->id << ", curl_handle=" << req_data->curl_handle << dendl;
  CURLMcode mstatus = curl_multi_add_handle((CURLM *)multi_handle, req_data->get_easy_handle());
  if (mstatus) {
    dout(0) << "ERROR: failed on curl_multi_add_handle, status=" << mstatus << dendl;
    return -EIO;
  }
  return 0;
}

/*
 * unhook request from the curl multi handle, and finish request if it wasn't finished yet as
 * there will be no more processing on this request
 */
void RGWHTTPManager::_unlink_request(rgw_http_req_data *req_data)
{
  if (req_data->curl_handle) {
    curl_multi_remove_handle((CURLM *)multi_handle, req_data->get_easy_handle());
  }
  if (!req_data->_is_done()) {
    _finish_request(req_data, -ECANCELED);
  }
}

void RGWHTTPManager::unlink_request(rgw_http_req_data *req_data)
{
  RWLock::WLocker wl(reqs_lock);
  _unlink_request(req_data);
}

void RGWHTTPManager::manage_pending_requests()
{
  reqs_lock.get_read();
  if (max_threaded_req == num_reqs &&
      unregistered_reqs.empty() &&
      reqs_change_state.empty()) {
    reqs_lock.unlock();
    return;
  }
  reqs_lock.unlock();

  RWLock::WLocker wl(reqs_lock);

  if (!unregistered_reqs.empty()) {
    for (auto& r : unregistered_reqs) {
      _unlink_request(r);
      r->put();
    }

    unregistered_reqs.clear();
  }

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

  if (!reqs_change_state.empty()) {
    for (auto siter : reqs_change_state) {
      _set_req_state(siter);
    }
    reqs_change_state.clear();
  }

  for (auto piter : remove_reqs) {
    rgw_http_req_data *req_data = piter.first;
    int r = piter.second;

    _finish_request(req_data, r);
  }
}

int RGWHTTPManager::add_request(RGWHTTPClient *client)
{
  rgw_http_req_data *req_data = new rgw_http_req_data;

  int ret = client->init_request(req_data);
  if (ret < 0) {
    req_data->put();
    req_data = NULL;
    return ret;
  }

  req_data->mgr = this;
  req_data->client = client;
  req_data->control_io_id = client->get_io_id(RGWHTTPClient::HTTPCLIENT_IO_CONTROL);
  req_data->user_info = client->get_io_user_info();

  register_request(req_data);

  if (!is_started) {
    ret = link_request(req_data);
    if (ret < 0) {
      req_data->put();
      req_data = NULL;
    }
    return ret;
  }
  ret = signal_thread();
  if (ret < 0) {
    finish_request(req_data, ret);
  }

  return ret;
}

int RGWHTTPManager::remove_request(RGWHTTPClient *client)
{
  rgw_http_req_data *req_data = client->get_req_data();

  if (!is_started) {
    unlink_request(req_data);
    return 0;
  }
  if (!unregister_request(req_data)) {
    return 0;
  }
  int ret = signal_thread();
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWHTTPManager::set_request_state(RGWHTTPClient *client, RGWHTTPRequestSetState state)
{
  rgw_http_req_data *req_data = client->get_req_data();

  ceph_assert(req_data->lock.is_locked());

  /* can only do that if threaded */
  if (!is_started) {
    return -EINVAL;
  }

  bool suggested_wr_paused = req_data->write_paused;
  bool suggested_rd_paused = req_data->read_paused;

  switch (state) {
    case SET_WRITE_PAUSED:
      suggested_wr_paused = true;
      break;
    case SET_WRITE_RESUME:
      suggested_wr_paused = false;
      break;
    case SET_READ_PAUSED:
      suggested_rd_paused = true;
      break;
    case SET_READ_RESUME:
      suggested_rd_paused = false;
      break;
    default:
      /* shouldn't really be here */
      return -EIO;
  }
  if (suggested_wr_paused == req_data->write_paused &&
      suggested_rd_paused == req_data->read_paused) {
    return 0;
  }

  req_data->write_paused = suggested_wr_paused;
  req_data->read_paused = suggested_rd_paused;

  int bitmask = CURLPAUSE_CONT;

  if (req_data->write_paused) {
    bitmask |= CURLPAUSE_SEND;
  }

  if (req_data->read_paused) {
    bitmask |= CURLPAUSE_RECV;
  }

  reqs_change_state.push_back(set_state(req_data, bitmask));
  int ret = signal_thread();
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWHTTPManager::start()
{
  if (pipe_cloexec(thread_pipe) < 0) {
    int e = errno;
    ldout(cct, 0) << "ERROR: pipe(): " << cpp_strerror(e) << dendl;
    return -e;
  }

  // enable non-blocking reads
  if (::fcntl(thread_pipe[0], F_SETFL, O_NONBLOCK) < 0) {
    int e = errno;
    ldout(cct, 0) << "ERROR: fcntl(): " << cpp_strerror(e) << dendl;
    TEMP_FAILURE_RETRY(::close(thread_pipe[0]));
    TEMP_FAILURE_RETRY(::close(thread_pipe[1]));
    return -e;
  }

#ifdef HAVE_CURL_MULTI_WAIT
  // on first initialization, use this pipe to detect whether we're using a
  // buggy version of libcurl
  std::call_once(detect_flag, detect_curl_multi_wait_bug, cct,
                 static_cast<CURLM*>(multi_handle),
                 thread_pipe[1], thread_pipe[0]);
#endif

  is_started = true;
  reqs_thread = new ReqsThread(this);
  reqs_thread->create("http_manager");
  return 0;
}

void RGWHTTPManager::stop()
{
  if (is_stopped) {
    return;
  }

  is_stopped = true;

  if (is_started) {
    going_down = true;
    signal_thread();
    reqs_thread->join();
    delete reqs_thread;
    TEMP_FAILURE_RETRY(::close(thread_pipe[1]));
    TEMP_FAILURE_RETRY(::close(thread_pipe[0]));
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

  while (!going_down) {
    int ret = do_curl_wait(cct, (CURLM *)multi_handle, thread_pipe[0]);
    if (ret < 0) {
      dout(0) << "ERROR: do_curl_wait() returned: " << ret << dendl;
      return NULL;
    }

    manage_pending_requests();

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
          case CURLE_OPERATION_TIMEDOUT:
            dout(0) << "WARNING: curl operation timed out, network average transfer speed less than " 
              << cct->_conf->rgw_curl_low_speed_limit << " Bytes per second during " << cct->_conf->rgw_curl_low_speed_time << " seconds." << dendl;
          default:
            dout(20) << "ERROR: msg->data.result=" << result << " req_data->id=" << id << " http_status=" << http_status << dendl;
            dout(20) << "ERROR: curl error: " << curl_easy_strerror((CURLcode)result) << dendl;
	    break;
        }
      }
    }
  }


  RWLock::WLocker rl(reqs_lock);
  for (auto r : unregistered_reqs) {
    _unlink_request(r);
  }

  unregistered_reqs.clear();

  auto all_reqs = std::move(reqs);
  for (auto iter : all_reqs) {
    _unlink_request(iter.second);
  }

  reqs.clear();

  if (completion_mgr) {
    completion_mgr->go_down();
  }
  
  return 0;
}

void rgw_http_client_init(CephContext *cct)
{
  curl_global_init(CURL_GLOBAL_ALL);
  rgw_http_manager = new RGWHTTPManager(cct);
  rgw_http_manager->start();
}

void rgw_http_client_cleanup()
{
  rgw_http_manager->stop();
  delete rgw_http_manager;
  curl_global_cleanup();
}


int RGWHTTP::send(RGWHTTPClient *req) {
  if (!req) {
    return 0;
  }
  int r = rgw_http_manager->add_request(req);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWHTTP::process(RGWHTTPClient *req, optional_yield y) {
  if (!req) {
    return 0;
  }
  int r = send(req);
  if (r < 0) {
    return r;
  }

  return req->wait(y);
}

