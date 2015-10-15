// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_CLIENT_IO_H
#define CEPH_RGW_CLIENT_IO_H

#include <streambuf>
#include <istream>
#include <stdlib.h>

#include "include/types.h"

#include "rgw_common.h"

class RGWClientIO {
  bool account;

  size_t bytes_sent;
  size_t bytes_received;

protected:
  RGWEnv env;

  virtual void init_env(CephContext *cct) = 0;

  virtual int write_data(const char *buf, int len) = 0;
  virtual int read_data(char *buf, int max) = 0;

public:
  virtual ~RGWClientIO() {}
  RGWClientIO() : account(false), bytes_sent(0), bytes_received(0) {}

  void init(CephContext *cct);
  int print(const char *format, ...);
  int write(const char *buf, int len);
  virtual void flush() = 0;
  int read(char *buf, int max, int *actual);

  virtual int send_status(const char *status, const char *status_name) = 0;
  virtual int send_100_continue() = 0;
  virtual int complete_header() = 0;
  virtual int complete_request() = 0;
  virtual int send_content_length(uint64_t len) = 0;

  RGWEnv& get_env() { return env; }

  void set_account(bool _account) {
    account = _account;
  }

  uint64_t get_bytes_sent() { return bytes_sent; }
  uint64_t get_bytes_received() { return bytes_received; }
};


class RGWClientIOStreamBuf : public std::streambuf {
protected:
  RGWClientIO &cio;
  std::size_t const window_size;
  std::size_t const putback_size;
  std::vector<char> buffer;

public:
  RGWClientIOStreamBuf(RGWClientIO &c, std::size_t ws, std::size_t ps = 1)
    : cio(c),
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
    int ret = cio.read(base, window_size, &read_len);
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
  RGWClientIOStream(RGWClientIO &c)
    : RGWClientIOStreamBuf(c, 1, 2),
      istream(static_cast<RGWClientIOStreamBuf *>(this)) {
  }
};

#endif
