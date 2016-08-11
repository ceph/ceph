// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_CLIENT_IO_H
#define CEPH_RGW_CLIENT_IO_H

#include <exception>
#include <string>
#include <streambuf>
#include <istream>
#include <stdlib.h>

#include <boost/utility/string_ref.hpp>

#include "include/types.h"
#include "rgw_common.h"

class RGWClientIO {
protected:
  virtual void init_env(CephContext *cct) = 0;

public:
  virtual ~RGWClientIO() {}

  void init(CephContext *cct);
  virtual RGWEnv& get_env() noexcept = 0;
  virtual int complete_request() = 0;
}; /* RGWClient IO */


class RGWClientIOAccounter {
public:
  virtual ~RGWClientIOAccounter() {}

  virtual void set_account(bool enabled) = 0;

  virtual uint64_t get_bytes_sent() const = 0;
  virtual uint64_t get_bytes_received() const = 0;
};


class RGWStreamIOEngine : public RGWClientIO {
  friend class RGWStreamIOFacade;
  friend class RGWStreamIOLegacyWrapper;

public:
  class Exception : public std::exception {
    int err;
  public:
    Exception(const int err)
      : err(err) {
    }

    int value() {
      return err;
    }
  };

  virtual std::size_t send_status(int status, const char *status_name) = 0;
  virtual std::size_t send_100_continue() = 0;

  /* Send header to client. On success returns number of bytes sent to the direct
   * client of RadosGW. On failure throws int containing errno. boost::string_ref
   * is being used because of length it internally carries. */
  virtual std::size_t send_header(const boost::string_ref& name,
                                  const boost::string_ref& value) = 0;

  virtual std::size_t send_content_length(uint64_t len) = 0;
  virtual std::size_t complete_header() = 0;

  /* Receive body. On success Returns number of bytes sent to the direct
   * client of RadosGW. On failure throws int containing errno. */
  virtual std::size_t recv_body(char* buf, std::size_t max) = 0;
  virtual std::size_t send_body(const char* buf, std::size_t len) = 0;

  virtual void flush() = 0;
};


class RGWStreamIO;

class RGWStreamIOFacade {
protected:
  RGWStreamIO& engine;

public:
  RGWStreamIOFacade(RGWStreamIO* const engine)
    : engine(*engine) {
  }

  /* High-level utilities to move out to a client's facade. Those functions
   * are not intended for overriding by a front-end glue code. That's the
   * reason why they aren't virtuals. */
  int write(const char *buf, int len);
  int read(char *buf, int max, int *actual);
};


/* HTTP IO: compatibility layer */
class RGWStreamIO : public RGWClientIO,
                    public RGWStreamIOFacade,
                    public RGWClientIOAccounter {
  bool _account;
  size_t bytes_sent;
  size_t bytes_received;

  SHA256 *sha256_hash;

  bool account() const {
    return _account;
  }

protected:
  RGWEnv env;

public:
  virtual int send_status(int status, const char *status_name) = 0;
  virtual int send_100_continue() = 0;
  virtual std::size_t send_header(const boost::string_ref& name,
                                  const boost::string_ref& value) noexcept = 0;
  virtual int send_content_length(uint64_t len) = 0;
  virtual int complete_header() = 0;

  virtual int recv_body(char* buf, std::size_t max) = 0;
  virtual int send_body(const char* buf, std::size_t len) = 0;
  virtual void flush() = 0;

  virtual ~RGWStreamIO() {}

  RGWStreamIO()
    : RGWStreamIOFacade(this),
      _account(false),
      bytes_sent(0),
      bytes_received(0),
      sha256_hash(nullptr) {
  }

  int write(const char *buf, int len);

  int read(char *buf, int max, int *actual);
  int read(char *buf, int max, int *actual, bool hash);

  std::string grab_aws4_sha256_hash();

  RGWEnv& get_env() noexcept override {
    return env;
  }

  void set_account(bool _accnt) override {
    _account = _accnt;
  }

  uint64_t get_bytes_sent() const override {
    return bytes_sent;
  }

  uint64_t get_bytes_received() const override {
    return bytes_received;
  }
}; /* RGWStreamIO */


/* A class for preserving interface compatibility with RGWStreamIO clients
 * while allowing front-end migration to the new API. We don't multi-inherit
 * from RGWDecoratedStreamIO<> to avoid using virtual inheritance in engine.
 * Should be removed after converting all clients. */
class RGWStreamIOLegacyWrapper : public RGWStreamIO {
  RGWStreamIOEngine * const engine;

  RGWStreamIOEngine& get_decoratee() {
    return *engine;
  }

#define EXCPT_TO_RC(code)                                       \
  try {                                                         \
    return code;                                                \
  } catch (RGWStreamIOEngine::Exception& e) {                   \
    return e.value();                                           \
  }

#define EXCPT_TO_VOID(code)                                     \
  try {                                                         \
    return code;                                                \
  } catch (RGWStreamIOEngine::Exception& e) {                   \
    return;                                                     \
  }

protected:
  void init_env(CephContext *cct) override {
    EXCPT_TO_VOID(get_decoratee().init_env(cct));
  }

public:
  RGWStreamIOLegacyWrapper(RGWStreamIOEngine * const engine)
    : engine(engine) {
  }

  int send_status(const int status, const char* const status_name) override {
    EXCPT_TO_RC(get_decoratee().send_status(status, status_name));
  }

  std::size_t send_header(const boost::string_ref& name,
                          const boost::string_ref& value) noexcept override {
    EXCPT_TO_RC(get_decoratee().send_header(name, value));
  }

  int send_100_continue() override {
    EXCPT_TO_RC(get_decoratee().send_100_continue());
  }

  int send_content_length(const uint64_t len) override {
    EXCPT_TO_RC(get_decoratee().send_content_length(len));
  }

  int complete_header() override {
    EXCPT_TO_RC(get_decoratee().complete_header());
  }

  int recv_body(char* buf, const std::size_t max) override {
    EXCPT_TO_RC(get_decoratee().recv_body(buf, max));
  }

  int send_body(const char* const buf, const std::size_t len) override {
    EXCPT_TO_RC(get_decoratee().send_body(buf, len));
  }


  void flush() override {
    EXCPT_TO_VOID(get_decoratee().flush());
  }

  RGWEnv& get_env() noexcept override {
    return get_decoratee().get_env();
  }

  int complete_request() override {
    EXCPT_TO_RC(get_decoratee().complete_request());
  }
};


class RGWClientIOStreamBuf : public std::streambuf {
protected:
  RGWStreamIO &sio;
  std::size_t const window_size;
  std::size_t const putback_size;
  std::vector<char> buffer;

public:
  RGWClientIOStreamBuf(RGWStreamIO &s, std::size_t ws, std::size_t ps = 1)
    : sio(s),
      window_size(ws),
      putback_size(ps),
      buffer(ws + ps)
  {
    setg(nullptr, nullptr, nullptr);
  }

  std::streambuf::int_type underflow() {
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

    int read_len;
    int ret = sio.read(base, window_size, &read_len);
    if (ret < 0 || 0 == read_len) {
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
  explicit RGWClientIOStream(RGWStreamIO &s)
    : RGWClientIOStreamBuf(s, 1, 2),
      istream(static_cast<RGWClientIOStreamBuf *>(this)) {
  }
};

#endif /* CEPH_RGW_CLIENT_IO_H */
