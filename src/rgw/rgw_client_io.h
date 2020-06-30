// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_CLIENT_IO_H
#define CEPH_RGW_CLIENT_IO_H

#include <exception>
#include <string>
#include <string_view>
#include <streambuf>
#include <istream>
#include <stdlib.h>
#include <system_error>

#include "include/types.h"
#include "rgw_common.h"


class RGWRestfulIO;

namespace rgw {
namespace io {

using Exception = std::system_error;

/* The minimal and simplest subset of methods that a client of RadosGW can be
 * interacted with. */
class BasicClient {
protected:
  virtual int init_env(CephContext *cct) = 0;

public:
  virtual ~BasicClient() = default;

  /* Initialize the BasicClient and inject CephContext. */
  int init(CephContext *cct);

  /* Return the RGWEnv describing the environment that a given request lives in.
   * The method does not throw exceptions. */
  virtual RGWEnv& get_env() noexcept = 0;

  /* Complete request.
   * On success returns number of bytes generated for a direct client of RadosGW.
   * On failure throws rgw::io::Exception containing errno. */
  virtual size_t complete_request() = 0;
}; /* rgw::io::Client */


class Accounter {
public:
  virtual ~Accounter() = default;

  /* Enable or disable the accounting of both sent and received data. Changing
   * the state does not affect the counters. */
  virtual void set_account(bool enabled) = 0;

  /* Return number of bytes sent to a direct client of RadosGW (direct means
   * eg. a web server instance in the case of using FastCGI front-end) when
   * the accounting was enabled. */
  virtual uint64_t get_bytes_sent() const = 0;

  /* Return number of bytes received from a direct client of RadosGW (direct
   * means eg. a web server instance in the case of using FastCGI front-end)
   * when the accounting was enabled. */
  virtual uint64_t get_bytes_received() const = 0;
}; /* rgw::io::Accounter */


/* Interface abstracting restful interactions with clients, usually through
 * the HTTP protocol. The methods participating in the response generation
 * process should be called in the specific order:
 *   1. send_100_continue() - at most once,
 *   2. send_status() - exactly once,
 *   3. Any of:
 *      a. send_header(),
 *      b. send_content_length() XOR send_chunked_transfer_encoding()
 *         Please note that only one of those two methods must be called
           at most once.
 *   4. complete_header() - exactly once,
 *   5. send_body()
 *   6. complete_request() - exactly once.
 * There are no restrictions on flush() - it may be called in any moment.
 *
 * Receiving data from a client isn't a subject to any further call order
 * restrictions besides those imposed by BasicClient. That is, get_env()
 * and recv_body can be mixed. */
class RestfulClient : public BasicClient {
  template<typename T> friend class DecoratedRestfulClient;

public:
  /* Generate the 100 Continue message.
   * On success returns number of bytes generated for a direct client of RadosGW.
   * On failure throws rgw::io::Exception containing errno. */
  virtual size_t send_100_continue() = 0;

  /* Generate the response's status part taking the HTTP status code as @status
   * and its name pointed in @status_name.
   * On success returns number of bytes generated for a direct client of RadosGW.
   * On failure throws rgw::io::Exception containing errno. */
  virtual size_t send_status(int status, const char *status_name) = 0;

  /* Generate header. On success returns number of bytes generated for a direct
   * client of RadosGW. On failure throws rgw::io::Exception containing errno.
   *
   * std::string_view is being used because of length it internally carries. */
  virtual size_t send_header(const std::string_view& name,
                             const std::string_view& value) = 0;

  /* Inform a client about a content length. Takes number of bytes as @len.
   * On success returns number of bytes generated for a direct client of RadosGW.
   * On failure throws rgw::io::Exception containing errno.
   *
   * CALL LIMITATIONS:
   *  - The method must be called EXACTLY ONCE.
   *  - The method is interchangeable with send_chunked_transfer_encoding(). */
  virtual size_t send_content_length(uint64_t len) = 0;

  /* Inform a client that the chunked transfer encoding will be used.
   * On success returns number of bytes generated for a direct client of RadosGW.
   * On failure throws rgw::io::Exception containing errno.
   *
   * CALL LIMITATIONS:
   *  - The method must be called EXACTLY ONCE.
   *  - The method is interchangeable with send_content_length(). */
  virtual size_t send_chunked_transfer_encoding() {
    /* This is a null implementation. We don't send anything here, even the HTTP
     * header. The intended behaviour should be provided through a decorator or
     * directly by a given front-end. */
    return 0;
  }

  /* Generate completion (the CRLF sequence separating headers and body in
   * the case of HTTP) of headers. On success returns number of generated bytes
   * for a direct client of RadosGW. On failure throws rgw::io::Exception with
   * errno. */
  virtual size_t complete_header() = 0;

  /* Receive no more than @max bytes from a request's body and store it in
   * buffer pointed by @buf. On success returns number of bytes received from
   * a direct client of RadosGW that has been stored in @buf. On failure throws
   * rgw::io::Exception containing errno. */
  virtual size_t recv_body(char* buf, size_t max) = 0;

  /* Generate a part of response's body by taking exactly @len bytes from
   * the buffer pointed by @buf. On success returns number of generated bytes
   * of response's body. On failure throws rgw::io::Exception. */
  virtual size_t send_body(const char* buf, size_t len) = 0;

  /* Flushes all already generated data to a direct client of RadosGW.
   * On failure throws rgw::io::Exception containing errno. */
  virtual void flush() = 0;
} /* rgw::io::RestfulClient */;


/* Abstract decorator over any implementation of rgw::io::RestfulClient
 * which could be provided both as a pointer-to-object or the object itself. */
template <typename DecorateeT>
class DecoratedRestfulClient : public RestfulClient {
  template<typename T> friend class DecoratedRestfulClient;
  friend RGWRestfulIO;

  typedef typename std::remove_pointer<DecorateeT>::type DerefedDecorateeT;

  static_assert(std::is_base_of<RestfulClient, DerefedDecorateeT>::value,
                "DecorateeT must be a subclass of rgw::io::RestfulClient");

  DecorateeT decoratee;

  /* There is an indirection layer over accessing decoratee to share the same
   * code base between dynamic and static decorators. The difference is about
   * what we store internally: pointer to a decorated object versus the whole
   * object itself. */
  template <typename T = void,
            typename std::enable_if<
    ! std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  DerefedDecorateeT& get_decoratee() {
    return decoratee;
  }

protected:
  template <typename T = void,
            typename std::enable_if<
    std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  DerefedDecorateeT& get_decoratee() {
    return *decoratee;
  }

  /* Dynamic decorators (those storing a pointer instead of the decorated
   * object itself) can be reconfigured on-the-fly. HOWEVER: there are no
   * facilities for orchestrating such changes. Callers must take care of
   * atomicity and thread-safety. */
  template <typename T = void,
            typename std::enable_if<
    std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  void set_decoratee(DerefedDecorateeT& new_dec) {
    decoratee = &new_dec;
  }

  int init_env(CephContext *cct) override {
    return get_decoratee().init_env(cct);
  }

public:
  explicit DecoratedRestfulClient(DecorateeT&& decoratee)
    : decoratee(std::forward<DecorateeT>(decoratee)) {
  }

  size_t send_status(const int status,
                     const char* const status_name) override {
    return get_decoratee().send_status(status, status_name);
  }

  size_t send_100_continue() override {
    return get_decoratee().send_100_continue();
  }

  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override {
    return get_decoratee().send_header(name, value);
  }

  size_t send_content_length(const uint64_t len) override {
    return get_decoratee().send_content_length(len);
  }

  size_t send_chunked_transfer_encoding() override {
    return get_decoratee().send_chunked_transfer_encoding();
  }

  size_t complete_header() override {
    return get_decoratee().complete_header();
  }

  size_t recv_body(char* const buf, const size_t max) override {
    return get_decoratee().recv_body(buf, max);
  }

  size_t send_body(const char* const buf,
                   const size_t len) override {
    return get_decoratee().send_body(buf, len);
  }

  void flush() override {
    return get_decoratee().flush();
  }

  RGWEnv& get_env() noexcept override {
    return get_decoratee().get_env();
  }

  size_t complete_request() override {
    return get_decoratee().complete_request();
  }
} /* rgw::io::DecoratedRestfulClient */;


/* Interface that should be provided by a front-end class wanting to to use
 * the low-level buffering offered by i.e. StaticOutputBufferer. */
class BuffererSink {
public:
  virtual ~BuffererSink() = default;

  /* Send exactly @len bytes from the memory location pointed by @buf.
   * On success returns @len. On failure throws rgw::io::Exception. */
  virtual size_t write_data(const char *buf, size_t len) = 0;
};

/* Utility class providing RestfulClient's implementations with facilities
 * for low-level buffering without relying on dynamic memory allocations.
 * The buffer is carried entirely on stack. This narrows down applicability
 * to these situations where buffers are relatively small. This perfectly
 * fits the needs of composing an HTTP header. Without that a front-end
 * might need to issue a lot of small IO operations leading to increased
 * overhead on syscalls and fragmentation of a message if the Nagle's
 * algorithm won't be able to form a single TCP segment (usually when
 * running on extremely fast network interfaces like the loopback). */
template <size_t BufferSizeV = 4096>
class StaticOutputBufferer : public std::streambuf {
  static_assert(BufferSizeV >= sizeof(std::streambuf::char_type),
                "Buffer size must be bigger than a single char_type.");

  using std::streambuf::int_type;

  int_type overflow(const int_type c) override {
    *pptr() = c;
    pbump(sizeof(std::streambuf::char_type));

    if (! sync()) {
      /* No error, the buffer has been successfully synchronized. */
      return c;
    } else {
      return std::streambuf::traits_type::eof();
    }
  }

  int sync() override {
    const auto len = static_cast<size_t>(std::streambuf::pptr() -
                                         std::streambuf::pbase());
    std::streambuf::pbump(-len);
    sink.write_data(std::streambuf::pbase(), len);
    /* Always return success here. In case of failure write_data() will throw
     * rgw::io::Exception. */
    return 0;
  }

  BuffererSink& sink;
  std::streambuf::char_type buffer[BufferSizeV];

public:
  explicit StaticOutputBufferer(BuffererSink& sink)
    : sink(sink) {
    constexpr size_t len = sizeof(buffer) - sizeof(std::streambuf::char_type);
    std::streambuf::setp(buffer, buffer + len);
  }
};

} /* namespace io */
} /* namespace rgw */


/* We're doing this nasty thing only because of extensive usage of templates
 * to implement the static decorator pattern. C++ templates de facto enforce
 * mixing interfaces with implementation. Additionally, those classes derive
 * from RGWRestfulIO defined here. I believe that including in the middle of
 * file is still better than polluting it directly. */
#include "rgw_client_io_filters.h"


/* RGWRestfulIO: high level interface to interact with RESTful clients. What
 * differentiates it from rgw::io::RestfulClient is providing more specific APIs
 * like rgw::io::Accounter or the AWS Auth v4 stuff implemented by filters
 * while hiding the pipelined architecture from clients.
 *
 * rgw::io::Accounter came in as a part of rgw::io::AccountingFilter. */
class RGWRestfulIO : public rgw::io::AccountingFilter<rgw::io::RestfulClient*> {
  std::vector<std::shared_ptr<DecoratedRestfulClient>> filters;

public:
  ~RGWRestfulIO() override = default;

  RGWRestfulIO(CephContext *_cx, rgw::io::RestfulClient* engine)
    : AccountingFilter<rgw::io::RestfulClient*>(_cx, std::move(engine)) {
  }

  void add_filter(std::shared_ptr<DecoratedRestfulClient> new_filter) {
    new_filter->set_decoratee(this->get_decoratee());
    this->set_decoratee(*new_filter);
    filters.emplace_back(std::move(new_filter));
  }
}; /* RGWRestfulIO */


/* Type conversions to work around lack of req_state type hierarchy matching
 * (e.g.) REST backends (may be replaced w/dynamic typed req_state). */
static inline rgw::io::RestfulClient* RESTFUL_IO(struct req_state* s) {
  ceph_assert(dynamic_cast<rgw::io::RestfulClient*>(s->cio) != nullptr);

  return static_cast<rgw::io::RestfulClient*>(s->cio);
}

static inline rgw::io::Accounter* ACCOUNTING_IO(struct req_state* s) {
  auto ptr = dynamic_cast<rgw::io::Accounter*>(s->cio);
  ceph_assert(ptr != nullptr);

  return ptr;
}

static inline RGWRestfulIO* AWS_AUTHv4_IO(const req_state* const s) {
  ceph_assert(dynamic_cast<RGWRestfulIO*>(s->cio) != nullptr);

  return static_cast<RGWRestfulIO*>(s->cio);
}


class RGWClientIOStreamBuf : public std::streambuf {
protected:
  RGWRestfulIO &rio;
  size_t const window_size;
  size_t const putback_size;
  std::vector<char> buffer;

public:
  RGWClientIOStreamBuf(RGWRestfulIO &rio, size_t ws, size_t ps = 1)
    : rio(rio),
      window_size(ws),
      putback_size(ps),
      buffer(ws + ps)
  {
    setg(nullptr, nullptr, nullptr);
  }

  std::streambuf::int_type underflow() override {
    if (gptr() < egptr()) {
      return traits_type::to_int_type(*gptr());
    }

    char * const base = buffer.data();
    char * start;

    if (nullptr != eback()) {
      /* We need to skip moving bytes on first underflow. In such case
       * there is simply no previous data we should preserve for unget()
       * or something similar. */
      std::memmove(base, egptr() - putback_size, putback_size);
      start = base + putback_size;
    } else {
      start = base;
    }

    size_t read_len = 0;
    try {
      read_len = rio.recv_body(base, window_size);
    } catch (rgw::io::Exception&) {
      return traits_type::eof();
    }
    if (0 == read_len) {
      return traits_type::eof();
    }

    setg(base, start, start + read_len);

    return traits_type::to_int_type(*gptr());
  }
};

class RGWClientIOStream : private RGWClientIOStreamBuf, public std::istream {
/* Inheritance from RGWClientIOStreamBuf is a kind of shadow, undirect
 * form of composition here. We cannot do that explicitly because istream
 * ctor is being called prior to construction of any member of this class. */

public:
  explicit RGWClientIOStream(RGWRestfulIO &s)
    : RGWClientIOStreamBuf(s, 1, 2),
      istream(static_cast<RGWClientIOStreamBuf *>(this)) {
  }
};

#endif /* CEPH_RGW_CLIENT_IO_H */
