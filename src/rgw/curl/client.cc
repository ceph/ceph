#include "client.h"

#include <string>
#include <unordered_map>
#include <variant>

#include <boost/asio/append.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/system/system_error.hpp>

#include <curl/curl.h>

#include "common/async/service.h"

#if !CURL_AT_LEAST_VERSION(7, 17, 1)
#error "requires libcurl >= 7.17.1 for CURLOPT_OPENSOCKETFUNCTION"
#endif

namespace rgw::curl {

boost::system::error_category& easy_category()
{
  static struct category : boost::system::error_category {
    const char* name() const noexcept override { return "curl easy"; }
    std::string message(int code) const override {
      return ::curl_easy_strerror(static_cast<CURLcode>(code));
    }
  } instance;
  return instance;
}

boost::system::error_category& multi_category()
{
  static struct category : boost::system::error_category {
    const char* name() const noexcept override { return "curl multi"; }
    std::string message(int code) const override {
      return ::curl_multi_strerror(static_cast<CURLMcode>(code));
    }
  } instance;
  return instance;
}


void easy_deleter::operator()(CURL* p) { ::curl_easy_cleanup(p); }

easy_ptr easy_init(error_code& ec)
{
  auto easy = easy_ptr{::curl_easy_init()};
  if (!easy) {
    ec.assign(CURLE_OUT_OF_MEMORY, easy_category());
  }
  return easy;
}

easy_ptr easy_init()
{
  error_code ec;
  auto easy = easy_init(ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_easy_init");
  }
  return easy;
}

template <typename T>
void easy_setopt(CURL* easy, CURLoption option, T&& value, error_code& ec)
{
  CURLcode code = ::curl_easy_setopt(easy, option, std::forward<T>(value));
  if (code != CURLE_OK) {
    ec.assign(code, easy_category());
  }
}

template <typename T>
void easy_setopt(CURL* easy, CURLoption option, T&& value)
{
  error_code ec;
  easy_setopt(easy, option, std::forward<T>(value), ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_easy_setopt");
  }
}


void multi_deleter::operator()(CURLM* p) { ::curl_multi_cleanup(p); }

multi_ptr multi_init(error_code& ec)
{
  auto multi = multi_ptr{::curl_multi_init()};
  if (!multi) {
    ec.assign(CURLE_OUT_OF_MEMORY, multi_category());
  }
  return multi;
}

multi_ptr multi_init()
{
  error_code ec;
  auto multi = multi_init(ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_multi_init");
  }
  return multi;
}

template <typename T>
void multi_setopt(CURLM* multi, CURLMoption option, T&& value, error_code& ec)
{
  CURLMcode mcode = ::curl_multi_setopt(multi, option, std::forward<T>(value));
  if (mcode != CURLM_OK) {
    ec.assign(mcode, multi_category());
  }
}

template <typename T>
void multi_setopt(CURLM* multi, CURLMoption option, T&& value)
{
  error_code ec;
  multi_setopt(multi, option, std::forward<T>(value), ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_multi_setopt");
  }
}


void slist_deleter::operator()(curl_slist* p) { ::curl_slist_free_all(p); }

void slist_append(slist_ptr& p, const char* str, error_code& ec)
{
  if (curl_slist* tmp = ::curl_slist_append(p.get(), str); tmp) {
    (void) p.release(); // will be freed by curl_slist_free_all(tmp)
    p.reset(tmp);
  } else {
    ec.assign(CURLE_OUT_OF_MEMORY, easy_category());
  }
}

void slist_append(slist_ptr& p, const char* str)
{
  error_code ec;
  slist_append(p, str, ec);
  if (ec) {
    throw boost::system::system_error(ec, "curl_slist_append");
  }
}


// libcurl callback functions
static curl_socket_t opensocket_callback(void* user, curlsocktype purpose,
                                         curl_sockaddr* address);
static int closesocket_callback(void* user, curl_socket_t fd);
static int socket_callback(CURL* easy, curl_socket_t fd, int what,
                           void* user, void* socket);
static int timer_callback(CURLM* multi, long timeout_ms, void* user);


namespace ip = boost::asio::ip;

// This implementation uses the 'multi_socket' flavor of the libcurl multi API:
// https://curl.se/libcurl/c/libcurl-multi.html
//
// By providing our own CURLMOPT_TIMERFUNCTION AND CURLMOPT_SOCKETFUNCTION, we
// can do all necessary polling and waiting on the given asio executor. As we
// get wakeups from asio, we call curl_multi_socket_action() to do its
// non-blocking socket i/o from within that executor.
//
// This also overrides CURLOPT_OPENSOCKETFUNCTION on each request to manage the
// pool of asio sockets.
class Client::Impl :
    public boost::intrusive_ref_counter<Impl, boost::thread_unsafe_counter>,
    public ceph::async::service_list_base_hook
{
 public:
  explicit Impl(executor_type ex)
    : svc(boost::asio::use_service<ceph::async::service<Impl>>(
          boost::asio::query(ex, boost::asio::execution::context))),
      ex(ex),
      timer(ex),
      multi(multi_init())
  {
    multi_setopt(multi.get(), CURLMOPT_TIMERFUNCTION, timer_callback);
    multi_setopt(multi.get(), CURLMOPT_TIMERDATA, this);
    multi_setopt(multi.get(), CURLMOPT_SOCKETFUNCTION, socket_callback);
    multi_setopt(multi.get(), CURLMOPT_SOCKETDATA, this);

    // register for service_shutdown() notifications
    svc.add(*this);
  }
  ~Impl()
  {
    svc.remove(*this);
  }

  executor_type get_executor() const noexcept { return ex; }

  void async_perform_impl(handler_type handler, CURL* easy)
  {
    // configure the easy handle and add it to the multi handle
    error_code ec;
    add_handle(easy, ec);
    if (ec) {
      boost::asio::post(boost::asio::bind_executor(get_executor(),
          boost::asio::append(std::move(handler), ec)));
      return;
    }

    // arrange for per-op cancellation
    auto slot = boost::asio::get_associated_cancellation_slot(handler);
    if (slot.is_connected()) {
      slot.template emplace<op_cancellation>(this, easy);
    }

    // register the completion handler
    handlers.emplace(easy, std::move(handler));

    // kick things off if they haven't started yet
    socket_action(CURL_SOCKET_TIMEOUT, 0);
  }

  void cancel(error_code ec)
  {
    auto h = handlers.begin();
    while (h != handlers.end()) {
      auto handler = std::move(h->second);
      ::curl_multi_remove_handle(multi.get(), h->first);
      h = handlers.erase(h);
      boost::asio::dispatch(boost::asio::append(std::move(handler), ec));
    }
    timer.cancel();
  }

  void service_shutdown()
  {
    auto h = handlers.begin();
    while (h != handlers.end()) {
      auto handler = std::move(h->second);
      ::curl_multi_remove_handle(multi.get(), h->first);
      h = handlers.erase(h);
    }
  }

 private:
  ceph::async::service<Impl>& svc;
  executor_type ex;
  boost::asio::steady_timer timer;
  multi_ptr multi;

  using client_socket = std::variant<ip::tcp::socket, ip::udp::socket>;
  using client_socket_map = std::unordered_map<curl_socket_t, client_socket>;
  client_socket_map sockets;

  using handler_map = std::unordered_map<CURL*, handler_type>;
  handler_map handlers;

  // handler for per-op cancellation
  struct op_cancellation {
    Impl* impl;
    CURL* easy;

    op_cancellation(Impl* impl, CURL* easy)
      : impl(impl), easy(easy) {}

    void operator()(boost::asio::cancellation_type_t type) {
      if (type == boost::asio::cancellation_type::none) {
        return;
      }
      auto h = impl->handlers.find(easy);
      if (h == impl->handlers.end()) {
        return;
      }
      auto ec = make_error_code(boost::asio::error::operation_aborted);
      auto handler = std::move(h->second);
      ::curl_multi_remove_handle(impl->multi.get(), easy);
      impl->handlers.erase(h);
      boost::asio::dispatch(boost::asio::append(std::move(handler), ec));
    }
  };

  void add_handle(CURL* easy, error_code& ec)
  {
    // attach socket callbacks to the easy handle
    easy_setopt(easy, CURLOPT_OPENSOCKETFUNCTION, opensocket_callback, ec);
    if (ec) {
      return;
    }
    easy_setopt(easy, CURLOPT_OPENSOCKETDATA, this, ec);
    if (ec) {
      return;
    }
    easy_setopt(easy, CURLOPT_CLOSESOCKETFUNCTION, closesocket_callback, ec);
    if (ec) {
      return;
    }
    easy_setopt(easy, CURLOPT_CLOSESOCKETDATA, this, ec);
    if (ec) {
      return;
    }

    // register the easy handle with the multi handle
    CURLMcode mcode = ::curl_multi_add_handle(multi.get(), easy);
    if (mcode != CURLM_OK) {
      ec.assign(mcode, multi_category());
      return;
    }
  }

  void socket_action(curl_socket_t fd, int events)
  {
    // process the socket action as many times as necessary
    CURLMcode mcode = CURLM_OK;
    int count = 0;
    do {
      mcode = ::curl_multi_socket_action(multi.get(), fd, events, &count);
    } while (mcode == CURLM_CALL_MULTI_PERFORM);

    if (mcode != CURLM_OK) {
      // on error, all transfers must be aborted
      cancel(error_code{mcode, multi_category()});
      return;
    }

    // handle any completions
    while (CURLMsg* msg = ::curl_multi_info_read(multi.get(), &count)) {
      if (msg->msg == CURLMSG_DONE) {
        error_code ec;
        if (msg->data.result != CURLE_OK) {
          ec.assign(msg->data.result, easy_category());
        }
        auto h = handlers.find(msg->easy_handle);
        if (h != handlers.end()) {
          auto handler = std::move(h->second);
          ::curl_multi_remove_handle(multi.get(), h->first);
          handlers.erase(h);
          boost::asio::dispatch(boost::asio::append(std::move(handler), ec));
        }
      }
    }
  }

  // construct and open a tcp or udp socket
  template <typename Protocol>
  curl_socket_t open_socket(const Protocol& proto)
  {
    auto socket = typename Protocol::socket{get_executor()};
    error_code ec;
    socket.open(proto, ec);
    if (ec) {
      return CURL_SOCKET_BAD;
    }
    curl_socket_t fd = socket.native_handle();
    sockets.emplace(std::piecewise_construct, std::forward_as_tuple(fd),
                    std::forward_as_tuple(std::move(socket)));
    return fd;
  }

  friend curl_socket_t opensocket_callback(void* user, curlsocktype purpose,
                                           curl_sockaddr* address)
  {
    auto impl = static_cast<Impl*>(user);

    if (address->socktype == SOCK_STREAM) {
      auto proto = address->family == AF_INET ? ip::tcp::v4() : ip::tcp::v6();
      return impl->open_socket(proto);
    }
    if (address->socktype == SOCK_DGRAM) {
      auto proto = address->family == AF_INET ? ip::udp::v4() : ip::udp::v6();
      return impl->open_socket(proto);
    }
    return CURL_SOCKET_BAD;
  }

  friend int closesocket_callback(void* user, curl_socket_t fd)
  {
    auto impl = static_cast<Impl*>(user);

    impl->sockets.erase(fd);
    return 0;
  }

  struct socket_wait_handler {
    boost::intrusive_ptr<Impl> impl;
    curl_socket_t fd;
    int mask;

    socket_wait_handler(boost::intrusive_ptr<Impl> impl,
                        curl_socket_t fd, int mask)
      : impl(std::move(impl)), fd(fd), mask(mask) {}

    // callback for async_wait()
    void operator()(error_code ec)
    {
      if (ec) {
        mask |= CURL_CSELECT_ERR;
      }
      impl->socket_action(fd, mask);
    }

    // variant visitor for udp/tcp sockets
    void operator()(auto& socket)
    {
      auto wait_flag = (mask == CURL_CSELECT_IN)
          ? socket.wait_read : socket.wait_write;
      socket.async_wait(wait_flag, std::move(*this));
    }
  };

  friend int socket_callback(CURL* easy, curl_socket_t fd, int what,
                             void* user, void* socket)
  {
    auto impl = static_cast<Impl*>(user);

    if (what == CURL_POLL_REMOVE) {
      return 0;
    }

    auto i = impl->sockets.find(fd);
    if (i == impl->sockets.end()) {
      return -1;
    }

    if (what & CURL_POLL_IN) {
      std::visit(socket_wait_handler{impl, fd, CURL_CSELECT_IN}, i->second);
    }
    if (what & CURL_POLL_OUT) {
      std::visit(socket_wait_handler{impl, fd, CURL_CSELECT_OUT}, i->second);
    }
    return 0;
  }

  friend int timer_callback(CURLM* multi, long timeout_ms, void* user)
  {
    boost::intrusive_ptr impl = static_cast<Impl*>(user);
    if (timeout_ms == -1) {
      impl->timer.cancel();
      return 0;
    }
    impl->timer.expires_after(std::chrono::milliseconds(timeout_ms));
    impl->timer.async_wait([impl] (error_code ec) {
          if (!ec) {
            impl->socket_action(CURL_SOCKET_TIMEOUT, 0);
          }
        });
    return 0;
  }
};


Client::Client(executor_type ex)
  : impl(new Impl(ex))
{
}

Client::~Client()
{
  cancel();
}

auto Client::get_executor() const noexcept -> executor_type
{
  return impl->get_executor();
}

void Client::async_perform_impl(handler_type handler, CURL* easy)
{
  impl->async_perform_impl(std::move(handler), easy);
}

void Client::cancel()
{
  impl->cancel(boost::asio::error::operation_aborted);
}

} // namespace rgw::curl
