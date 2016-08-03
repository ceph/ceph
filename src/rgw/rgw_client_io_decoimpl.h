// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_CLIENT_IO_DECOIMPL_H
#define CEPH_RGW_CLIENT_IO_DECOIMPL_H

#include <type_traits>

#include <boost/optional.hpp>

#include "rgw_common.h"
#include "rgw_client_io.h"

/* Abstract decorator over any implementation of RGWStreamIOEngine. */
template <typename DecorateeT>
class RGWDecoratedStreamIO : public RGWStreamIOEngine {
  template<typename T> friend class RGWDecoratedStreamIO;

  typedef typename std::remove_pointer<DecorateeT>::type DerefedDecorateeT;

  static_assert(std::is_base_of<RGWStreamIOEngine, DerefedDecorateeT>::value,
                "DecorateeT must be a subclass of RGWStreamIOEngine");

  DecorateeT decoratee;

  /* There is an indirection layer over accessing decoratee to share the same
   * code base between dynamic and static decorators. The difference is about
   * what we store internally: pointer to a decorated object versus the whole
   * object itself. */
  template <typename T = void,
            typename std::enable_if<
    std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  DerefedDecorateeT& get_decoratee() {
    return *decoratee;
  }

  template <typename T = void,
            typename std::enable_if<
    ! std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  DerefedDecorateeT& get_decoratee() {
    return decoratee;
  }

protected:
  void init_env(CephContext *cct) override {
    return get_decoratee().init_env(cct);
  }

  int read_data(char* const buf, const int max) override {
    return get_decoratee().read_data(buf, max);
  }

  int write_data(const char* const buf, const int len) override {
    return get_decoratee().write_data(buf, len);
  }

public:
  RGWDecoratedStreamIO(const DecorateeT& decoratee)
    : decoratee(decoratee) {
  }

  int send_status(const int status, const char* const status_name) override {
    return get_decoratee().send_status(status, status_name);
  }

  int send_100_continue() override {
    return get_decoratee().send_100_continue();
  }

  int send_content_length(const uint64_t len) override {
    return get_decoratee().send_content_length(len);
  }

  int complete_header() override {
    return get_decoratee().complete_header();
  }

  void flush() override {
    return get_decoratee().flush();
  }

  RGWEnv& get_env() override {
    return get_decoratee().get_env();
  }

  int complete_request() override {
    return get_decoratee().complete_request();
  }
};


template <typename T>
class RGWStreamIOAccountingEngine : public RGWDecoratedStreamIO<T>,
                                    public RGWClientIOAccounter {
  bool enabled;
  uint64_t total_sent;
  uint64_t total_received;

protected:
  int read_data(char* const buf, const int max) override {
    const auto received = RGWDecoratedStreamIO<T>::read_data(buf, max);
    if (enabled) {
      total_received += received;
    }
    return received;
  }

  int write_data(const char* const buf, const int len) override {
    const auto sent = RGWDecoratedStreamIO<T>::write_data(buf, len);
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

public:
  template <typename U>
  RGWStreamIOAccountingEngine(U&& decoratee)
    : RGWDecoratedStreamIO<T>(std::move(decoratee)),
      enabled(false),
      total_sent(0),
      total_received(0) {
  }

  int send_status(const int status, const char* const status_name) override {
    const auto sent = RGWDecoratedStreamIO<T>::send_status(status, status_name);
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  int send_100_continue() override {
    const auto sent = RGWDecoratedStreamIO<T>::send_100_continue();
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  int send_content_length(const uint64_t len) override {
    const auto sent = RGWDecoratedStreamIO<T>::send_content_length(len);
    if (enabled) {
      total_sent += sent;
    }
    return sent;
  }

  int complete_header() override {
    const auto sent = RGWDecoratedStreamIO<T>::complete_header();
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
  }
};


/* Filter for in-memory buffering incoming data and calculating the content
 * length header if it isn't present. */
template <typename T>
class RGWStreamIOBufferingEngine : public RGWDecoratedStreamIO<T> {
  template<typename Td> friend class RGWDecoratedStreamIO;
protected:
  ceph::bufferlist data;

  bool has_content_length;
  bool buffer_data;

  int write_data(const char* buf, const int len) override;

public:
  template <typename U>
  RGWStreamIOBufferingEngine(U&& decoratee)
    : RGWDecoratedStreamIO<T>(std::move(decoratee)),
      has_content_length(false),
      buffer_data(false) {
  }

  int send_content_length(const uint64_t len) override;
  int complete_header() override;
  int complete_request() override;
};

template <typename T>
int RGWStreamIOBufferingEngine<T>::write_data(const char* buf,
                                              const int len)
{
  if (buffer_data) {
    data.append(buf, len);
    return 0;
  }

  return RGWDecoratedStreamIO<T>::write_data(buf, len);
}

template <typename T>
int RGWStreamIOBufferingEngine<T>::send_content_length(const uint64_t len)
{
  has_content_length = true;
  return RGWDecoratedStreamIO<T>::send_content_length(len);
}

template <typename T>
int RGWStreamIOBufferingEngine<T>::complete_header()
{
  if (! has_content_length) {
    /* We will dump everything in complete_request(). */
    buffer_data = true;
    return 0;
  }

  return RGWDecoratedStreamIO<T>::complete_header();
}

template <typename T>
int RGWStreamIOBufferingEngine<T>::complete_request()
{
  size_t sent = 0;

  if (! has_content_length) {
    sent += RGWDecoratedStreamIO<T>::send_content_length(data.length());
    sent += RGWDecoratedStreamIO<T>::complete_header();
  }

  if (buffer_data) {
    /* We are sending each buffer separately to avoid extra memory shuffling
     * that would occur on data.c_str() to provide a continuous memory area. */
    for (const auto& ptr : data.buffers()) {
      sent += RGWDecoratedStreamIO<T>::write_data(ptr.c_str(),
                                                  ptr.length());
    }
    data.clear();
    buffer_data = false;
  }

  return sent + RGWDecoratedStreamIO<T>::complete_request();
}


template <typename T>
class RGWStreamIOChunkingEngine : public RGWDecoratedStreamIO<T> {
  template<typename Td> friend class RGWDecoratedStreamIO;
protected:
  bool has_content_length;
  bool chunking_enabled;

  int write_data(const char* const buf, const int len) override {
    if (! chunking_enabled) {
      return RGWDecoratedStreamIO<T>::write_data(buf, len);
    } else {
      constexpr char HEADER_END[] = "\r\n";
      char sizebuf[32];
      snprintf(sizebuf, sizeof(buf), "%" PRIx64 "\r\n", len);

      RGWDecoratedStreamIO<T>::write_data(sizebuf, strlen(sizebuf));
      RGWDecoratedStreamIO<T>::write_data(buf, len);
      return RGWDecoratedStreamIO<T>::write_data(HEADER_END, sizeof(HEADER_END) - 1);
    }
  }

public:
  template <typename U>
  RGWStreamIOChunkingEngine(U&& decoratee)
    : RGWDecoratedStreamIO<T>(std::move(decoratee)),
      has_content_length(false),
      chunking_enabled(false) {
  }

  using RGWDecoratedStreamIO<T>::send_content_length;
  int send_content_length(const uint64_t len) override {
    has_content_length = true;
    return RGWDecoratedStreamIO<T>::send_content_length(len);
  }

  int complete_header() override {
    size_t sent = 0;

    if (! has_content_length) {
      constexpr char TRANSFER_CHUNKED[] = "Transfer-Enconding: chunked\r\n";

      sent += RGWDecoratedStreamIO<T>::write_data(TRANSFER_CHUNKED,
                                                  sizeof(TRANSFER_CHUNKED) - 1);
      chunking_enabled = true;
    }

    return sent + RGWDecoratedStreamIO<T>::complete_header();
  }
};

template <typename T>
RGWStreamIOChunkingEngine<T> add_chunking(T&& t) {
  return RGWStreamIOChunkingEngine<T>(std::move(t));
}


/* Class that controls and inhibits the process of sending Content-Length HTTP
 * header where RFC 7230 requests so. The cases worth our attention are 204 No
 * Content as well as 304 Not Modified. */
template <typename T>
class RGWStreamIOConLenControllingEngine : public RGWDecoratedStreamIO<T> {
protected:
  enum class ContentLengthAction {
    FORWARD,
    INHIBIT,
    UNKNOWN
  } action;

public:
  template <typename U>
  RGWStreamIOConLenControllingEngine(U&& decoratee)
    : RGWDecoratedStreamIO<T>(std::move(decoratee)),
      action(ContentLengthAction::UNKNOWN) {
  }

  int send_status(const int status, const char* const status_name) override {
    if (204 == status || 304 == status) {
      action = ContentLengthAction::INHIBIT;
    } else {
      action = ContentLengthAction::FORWARD;
    }

    return RGWDecoratedStreamIO<T>::send_status(status, status_name);
  }

  int send_content_length(const uint64_t len) override {
    switch(action) {
    case ContentLengthAction::FORWARD:
      return RGWDecoratedStreamIO<T>::send_content_length(len);
    case ContentLengthAction::INHIBIT:
      return 0;
    case ContentLengthAction::UNKNOWN:
    default:
      return -EINVAL;
    }
  }
};

template <typename T>
RGWStreamIOConLenControllingEngine<T> add_conlen_controlling(T&& t) {
  return RGWStreamIOConLenControllingEngine<T>(std::move(t));
}


/* Filter that rectifies the wrong behaviour of some clients of the RGWStreamIO
 * interface. Should be removed after fixing those clients. */
template <typename T>
class RGWStreamIOReorderingEngine : public RGWDecoratedStreamIO<T> {
protected:
  enum class ReorderState {
    RGW_EARLY_HEADERS,  /* Got headers sent before calling send_status. */
    RGW_STATUS_SEEN,    /* Status has been seen. */
    RGW_DATA            /* Header has been completed. */
  } phase;

  boost::optional<uint64_t> content_length;

  ceph::bufferlist early_header_data;
  ceph::bufferlist header_data;

  int write_data(const char* const buf, const int len) override {
    switch (phase) {
    case ReorderState::RGW_EARLY_HEADERS:
      early_header_data.append(buf, len);
      return len;
    case ReorderState::RGW_STATUS_SEEN:
      header_data.append(buf, len);
      return len;
    case ReorderState::RGW_DATA:
      return RGWDecoratedStreamIO<T>::write_data(buf, len);
    }

    return -EIO;
  }

public:
  template <typename U>
  RGWStreamIOReorderingEngine(U&& decoratee)
    : RGWDecoratedStreamIO<T>(std::move(decoratee)),
      phase(ReorderState::RGW_EARLY_HEADERS) {
  }

  int send_status(const int status, const char* const status_name) override {
    phase = ReorderState::RGW_STATUS_SEEN;

    return RGWDecoratedStreamIO<T>::send_status(status, status_name);
  }

  int send_content_length(const uint64_t len) override {
    if (ReorderState::RGW_EARLY_HEADERS == phase) {
      /* Oh great, someone tries to send content length before status. */
      content_length = len;
      return 0;
    } else {
      return RGWDecoratedStreamIO<T>::send_content_length(len);
    }
  }

  int complete_header() override {
    size_t sent = 0;

    /* Change state in order to immediately send everything we get. */
    phase = ReorderState::RGW_DATA;

    /* Sent content length if necessary. */
    if (content_length) {
      ssize_t rc = RGWDecoratedStreamIO<T>::send_content_length(*content_length);
      if (rc < 0) {
        return rc;
      } else {
        sent += rc;
      }
    }

    /* Header data in buffers are already counted. */
    if (header_data.length()) {
      ssize_t rc = RGWDecoratedStreamIO<T>::write_data(header_data.c_str(),
                                                       header_data.length());
      if (rc < 0) {
        return rc;
      } else {
        sent += rc;
      }

      header_data.clear();
    }

    if (early_header_data.length()) {
      ssize_t rc = RGWDecoratedStreamIO<T>::write_data(early_header_data.c_str(),
                                                       early_header_data.length());
      if (rc < 0) {
        return rc;
      } else {
        sent += rc;
      }

      early_header_data.clear();
    }

    return sent + RGWDecoratedStreamIO<T>::complete_header();
  }
};

template <typename T>
RGWStreamIOReorderingEngine<T> add_reordering(T&& t) {
  return RGWStreamIOReorderingEngine<T>(std::move(t));
}

#endif /* CEPH_RGW_CLIENT_IO_DECOIMPL_H */
