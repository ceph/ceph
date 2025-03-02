#pragma once

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/intrusive_ptr.hpp>

#include "common/ceph_time.h"

namespace rgw {

// a WaitHandler that closes a stream if the timeout expires
template <typename Stream>
struct timeout_handler {
  // this handler may outlive the timer/stream, so we need to hold a reference
  // to keep the stream alive
  boost::intrusive_ptr<Stream> stream;

  explicit timeout_handler(boost::intrusive_ptr<Stream> stream) noexcept
      : stream(std::move(stream)) {}

  void operator()(boost::system::error_code ec) {
    if (!ec) { // wait was not canceled
      boost::system::error_code ec_ignored;
      stream->get_socket().cancel();
      stream->get_socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec_ignored);
    }
  }
};

// a timeout timer for stream operations
template <typename Clock, typename Executor, typename Stream>
class basic_timeout_timer {
 public:
  using clock_type = Clock;
  using duration = typename clock_type::duration;
  using executor_type = Executor;

  explicit basic_timeout_timer(const executor_type& ex, duration dur,
                               boost::intrusive_ptr<Stream> stream)
      : timer(ex), dur(dur), stream(std::move(stream))
  {}

  basic_timeout_timer(const basic_timeout_timer&) = delete;
  basic_timeout_timer& operator=(const basic_timeout_timer&) = delete;

  void start() {
    if (dur.count() > 0) {
      timer.expires_after(dur);
      timer.async_wait(timeout_handler{stream});
    }
  }

  void cancel() {
    if (dur.count() > 0) {
      timer.cancel();
    }
  }

 private:
  using Timer = boost::asio::basic_waitable_timer<clock_type,
        boost::asio::wait_traits<clock_type>, executor_type>;
  Timer timer;
  duration dur;
  boost::intrusive_ptr<Stream> stream;
};

} // namespace rgw
