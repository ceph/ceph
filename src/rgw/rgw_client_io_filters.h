// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_CLIENT_IO_DECOIMPL_H
#define CEPH_RGW_CLIENT_IO_DECOIMPL_H

#include <type_traits>

#include <boost/optional.hpp>

#include "rgw_common.h"
#include "rgw_client_io.h"

namespace rgw {
namespace io {

template <typename T>
class AccountingFilter : public DecoratedRestfulClient<T>,
                         public Accounter {
  bool enabled;
  uint64_t total_sent;
  uint64_t total_received;
  CephContext *cct;

public:
  template <typename U>
  AccountingFilter(CephContext *cct, U&& decoratee)
    : DecoratedRestfulClient<T>(std::forward<U>(decoratee)),
      enabled(false),
      total_sent(0),
      total_received(0), cct(cct) {
  }

  size_t send_status(const int status,
                     const char* const status_name) override {
    const auto sent = DecoratedRestfulClient<T>::send_status(status,
                                                             status_name);
    lsubdout(cct, rgw, 30) << "AccountingFilter::send_status: e="
        << (enabled ? "1" : "0") << ", sent=" << sent << ", total="
        << total_sent << dendl;
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  size_t send_100_continue() override {
    const auto sent = DecoratedRestfulClient<T>::send_100_continue();
    lsubdout(cct, rgw, 30) << "AccountingFilter::send_100_continue: e="
        << (enabled ? "1" : "0") << ", sent=" << sent << ", total="
        << total_sent << dendl;
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override {
    const auto sent = DecoratedRestfulClient<T>::send_header(name, value);
    lsubdout(cct, rgw, 30) << "AccountingFilter::send_header: e="
        << (enabled ? "1" : "0") << ", sent=" << sent << ", total="
        << total_sent << dendl;
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  size_t send_content_length(const uint64_t len) override {
    const auto sent = DecoratedRestfulClient<T>::send_content_length(len);
    lsubdout(cct, rgw, 30) << "AccountingFilter::send_content_length: e="
        << (enabled ? "1" : "0") << ", sent=" << sent << ", total="
        << total_sent << dendl;
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  size_t send_chunked_transfer_encoding() override {
    const auto sent = DecoratedRestfulClient<T>::send_chunked_transfer_encoding();
    lsubdout(cct, rgw, 30) << "AccountingFilter::send_chunked_transfer_encoding: e="
        << (enabled ? "1" : "0") << ", sent=" << sent << ", total="
        << total_sent << dendl;
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  size_t complete_header() override {
    const auto sent = DecoratedRestfulClient<T>::complete_header();
    lsubdout(cct, rgw, 30) << "AccountingFilter::complete_header: e="
        << (enabled ? "1" : "0") << ", sent=" << sent << ", total="
        << total_sent << dendl;
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  size_t recv_body(char* buf, size_t max) override {
    const auto received = DecoratedRestfulClient<T>::recv_body(buf, max);
    lsubdout(cct, rgw, 30) << "AccountingFilter::recv_body: e="
        << (enabled ? "1" : "0") << ", received=" << received << dendl;
    if (enabled) {
      total_received += received;
    }
    return received;
  }

  size_t send_body(const char* const buf,
                   const size_t len) override {
    const auto sent = DecoratedRestfulClient<T>::send_body(buf, len);
    lsubdout(cct, rgw, 30) << "AccountingFilter::send_body: e="
        << (enabled ? "1" : "0") << ", sent=" << sent << ", total="
        << total_sent << dendl;
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  size_t complete_request() override {
    const auto sent = DecoratedRestfulClient<T>::complete_request();
    lsubdout(cct, rgw, 30) << "AccountingFilter::complete_request: e="
        << (enabled ? "1" : "0") << ", sent=" << sent << ", total="
        << total_sent << dendl;
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  uint64_t get_bytes_sent() const override {
    return total_sent;
  }

  uint64_t get_bytes_received() const override {
    return total_received;
  }

  void set_account(bool enabled) override {
    this->enabled = enabled;
    lsubdout(cct, rgw, 30) << "AccountingFilter::set_account: e="
        << (enabled ? "1" : "0") << dendl;
  }
};


/* Filter for in-memory buffering incoming data and calculating the content
 * length header if it isn't present. */
template <typename T>
class BufferingFilter : public DecoratedRestfulClient<T> {
  template<typename Td> friend class DecoratedRestfulClient;
protected:
  ceph::bufferlist data;

  bool has_content_length;
  bool buffer_data;
  CephContext *cct;

public:
  template <typename U>
  BufferingFilter(CephContext *cct, U&& decoratee)
    : DecoratedRestfulClient<T>(std::forward<U>(decoratee)),
      has_content_length(false),
      buffer_data(false), cct(cct) {
  }

  size_t send_content_length(const uint64_t len) override;
  size_t send_chunked_transfer_encoding() override;
  size_t complete_header() override;
  size_t send_body(const char* buf, size_t len) override;
  size_t complete_request() override;
};

template <typename T>
size_t BufferingFilter<T>::send_body(const char* const buf,
                                     const size_t len)
{
  if (buffer_data) {
    data.append(buf, len);

    lsubdout(cct, rgw, 30) << "BufferingFilter<T>::send_body: defer count = "
        << len << dendl;
    return 0;
  }

  return DecoratedRestfulClient<T>::send_body(buf, len);
}

template <typename T>
size_t BufferingFilter<T>::send_content_length(const uint64_t len)
{
  has_content_length = true;
  return DecoratedRestfulClient<T>::send_content_length(len);
}

template <typename T>
size_t BufferingFilter<T>::send_chunked_transfer_encoding()
{
  has_content_length = true;
  return DecoratedRestfulClient<T>::send_chunked_transfer_encoding();
}

template <typename T>
size_t BufferingFilter<T>::complete_header()
{
  if (! has_content_length) {
    /* We will dump everything in complete_request(). */
    buffer_data = true;
    lsubdout(cct, rgw, 30) << "BufferingFilter<T>::complete_header: has_content_length="
        << (has_content_length ? "1" : "0") << dendl;
    return 0;
  }

  return DecoratedRestfulClient<T>::complete_header();
}

template <typename T>
size_t BufferingFilter<T>::complete_request()
{
  size_t sent = 0;

  if (! has_content_length) {
    /* It is not correct to count these bytes here,
     * because they can only be part of the header.
     * Therefore force count to 0.
     */
    sent += DecoratedRestfulClient<T>::send_content_length(data.length());
    sent += DecoratedRestfulClient<T>::complete_header();
    lsubdout(cct, rgw, 30) <<
        "BufferingFilter::complete_request: !has_content_length: IGNORE: sent="
        << sent << dendl;
    sent = 0;
  }

  if (buffer_data) {
    /* We are sending each buffer separately to avoid extra memory shuffling
     * that would occur on data.c_str() to provide a continuous memory area. */
    for (const auto& ptr : data.buffers()) {
      sent += DecoratedRestfulClient<T>::send_body(ptr.c_str(),
                                                   ptr.length());
    }
    data.clear();
    buffer_data = false;
    lsubdout(cct, rgw, 30) << "BufferingFilter::complete_request: buffer_data: sent="
        << sent << dendl;
  }

  return sent + DecoratedRestfulClient<T>::complete_request();
}

template <typename T> static inline
BufferingFilter<T> add_buffering(
CephContext *cct,
T&& t) {
  return BufferingFilter<T>(cct, std::forward<T>(t));
}


template <typename T>
class ChunkingFilter : public DecoratedRestfulClient<T> {
  template<typename Td> friend class DecoratedRestfulClient;
protected:
  bool chunking_enabled;

public:
  template <typename U>
  explicit ChunkingFilter(U&& decoratee)
    : DecoratedRestfulClient<T>(std::forward<U>(decoratee)),
      chunking_enabled(false) {
  }

  size_t send_chunked_transfer_encoding() override {
    chunking_enabled = true;
    return DecoratedRestfulClient<T>::send_header("Transfer-Encoding",
                                                  "chunked");
  }

  size_t send_body(const char* buf,
                   const size_t len) override {
    if (! chunking_enabled) {
      return DecoratedRestfulClient<T>::send_body(buf, len);
    } else {
      static constexpr char HEADER_END[] = "\r\n";
      /* https://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.6.1 */
      // TODO: we have no support for sending chunked-encoding
      // extensions/trailing headers.
      char chunk_size[32];
      const auto chunk_size_len = snprintf(chunk_size, sizeof(chunk_size),
                                           "%zx\r\n", len);
      size_t sent = 0;

      sent += DecoratedRestfulClient<T>::send_body(chunk_size, chunk_size_len);
      sent += DecoratedRestfulClient<T>::send_body(buf, len);
      sent += DecoratedRestfulClient<T>::send_body(HEADER_END,
                                                   sizeof(HEADER_END) - 1);
      return sent;
    }
  }

  size_t complete_request() override {
    size_t sent = 0;

    if (chunking_enabled) {
      static constexpr char CHUNKED_RESP_END[] = "0\r\n\r\n";
      sent += DecoratedRestfulClient<T>::send_body(CHUNKED_RESP_END,
                                                   sizeof(CHUNKED_RESP_END) - 1);
    }

    return sent + DecoratedRestfulClient<T>::complete_request();
  }
};

template <typename T> static inline
ChunkingFilter<T> add_chunking(T&& t) {
  return ChunkingFilter<T>(std::forward<T>(t));
}


/* Class that controls and inhibits the process of sending Content-Length HTTP
 * header where RFC 7230 requests so. The cases worth our attention are 204 No
 * Content as well as 304 Not Modified. */
template <typename T>
class ConLenControllingFilter : public DecoratedRestfulClient<T> {
protected:
  enum class ContentLengthAction {
    FORWARD,
    INHIBIT,
    UNKNOWN
  } action;

public:
  template <typename U>
  explicit ConLenControllingFilter(U&& decoratee)
    : DecoratedRestfulClient<T>(std::forward<U>(decoratee)),
      action(ContentLengthAction::UNKNOWN) {
  }

  size_t send_status(const int status,
                     const char* const status_name) override {
    if ((204 == status || 304 == status) &&
        ! g_conf()->rgw_print_prohibited_content_length) {
      action = ContentLengthAction::INHIBIT;
    } else {
      action = ContentLengthAction::FORWARD;
    }

    return DecoratedRestfulClient<T>::send_status(status, status_name);
  }

  size_t send_content_length(const uint64_t len) override {
    switch(action) {
    case ContentLengthAction::FORWARD:
      return DecoratedRestfulClient<T>::send_content_length(len);
    case ContentLengthAction::INHIBIT:
      return 0;
    case ContentLengthAction::UNKNOWN:
    default:
      return -EINVAL;
    }
  }
};

template <typename T> static inline
ConLenControllingFilter<T> add_conlen_controlling(T&& t) {
  return ConLenControllingFilter<T>(std::forward<T>(t));
}


/* Filter that rectifies the wrong behaviour of some clients of the RGWRestfulIO
 * interface. Should be removed after fixing those clients. */
template <typename T>
class ReorderingFilter : public DecoratedRestfulClient<T> {
protected:
  enum class ReorderState {
    RGW_EARLY_HEADERS,  /* Got headers sent before calling send_status. */
    RGW_STATUS_SEEN,    /* Status has been seen. */
    RGW_DATA            /* Header has been completed. */
  } phase;

  boost::optional<uint64_t> content_length;

  std::vector<std::pair<std::string, std::string>> headers;

  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override {
    switch (phase) {
    case ReorderState::RGW_EARLY_HEADERS:
    case ReorderState::RGW_STATUS_SEEN:
      headers.emplace_back(std::make_pair(std::string(name.data(), name.size()),
                                          std::string(value.data(), value.size())));
      return 0;
    case ReorderState::RGW_DATA:
      return DecoratedRestfulClient<T>::send_header(name, value);
    }

    return -EIO;
  }

public:
  template <typename U>
  explicit ReorderingFilter(U&& decoratee)
    : DecoratedRestfulClient<T>(std::forward<U>(decoratee)),
      phase(ReorderState::RGW_EARLY_HEADERS) {
  }

  size_t send_status(const int status,
                     const char* const status_name) override {
    phase = ReorderState::RGW_STATUS_SEEN;

    return DecoratedRestfulClient<T>::send_status(status, status_name);
  }

  size_t send_content_length(const uint64_t len) override {
    if (ReorderState::RGW_EARLY_HEADERS == phase) {
      /* Oh great, someone tries to send content length before status. */
      content_length = len;
      return 0;
    } else {
      return DecoratedRestfulClient<T>::send_content_length(len);
    }
  }

  size_t complete_header() override {
    size_t sent = 0;

    /* Change state in order to immediately send everything we get. */
    phase = ReorderState::RGW_DATA;

    /* Sent content length if necessary. */
    if (content_length) {
      sent += DecoratedRestfulClient<T>::send_content_length(*content_length);
    }

    /* Header data in buffers are already counted. */
    for (const auto& kv : headers) {
      sent += DecoratedRestfulClient<T>::send_header(kv.first, kv.second);
    }
    headers.clear();

    return sent + DecoratedRestfulClient<T>::complete_header();
  }
};

template <typename T> static inline
ReorderingFilter<T> add_reordering(T&& t) {
  return ReorderingFilter<T>(std::forward<T>(t));
}

} /* namespace io */
} /* namespace rgw */
#endif /* CEPH_RGW_CLIENT_IO_DECOIMPL_H */
