#pragma once

#include <boost/asio/basic_waitable_timer.hpp>

#include "common/ceph_time.h"

namespace rgw {

// a WaitHandler that closes a stream if the timeout expires
template <typename Stream>
struct timeout_handler {
  Stream* stream;

  explicit timeout_handler(Stream* stream) noexcept : stream(stream) {}

  void operator()(boost::system::error_code ec) {
    if (!ec) { // wait was not canceled
      boost::system::error_code ec_ignored;
      stream->close(ec_ignored);
    }
  }
};

// a timeout timer for stream operations
template <typename Clock, typename Executor>
class basic_timeout_timer {
 public:
  using clock_type = Clock;
  using duration = typename clock_type::duration;
  using executor_type = Executor;

  explicit basic_timeout_timer(const executor_type& ex) : timer(ex) {}

  basic_timeout_timer(const basic_timeout_timer&) = delete;
  basic_timeout_timer& operator=(const basic_timeout_timer&) = delete;

  template <typename Stream>
  void expires_after(Stream& stream, duration dur) {
    timer.expires_after(dur);
    timer.async_wait(timeout_handler{&stream});
  }

  void cancel() {
    timer.cancel();
  }

 private:
  using Timer = boost::asio::basic_waitable_timer<clock_type,
        boost::asio::wait_traits<clock_type>, executor_type>;
  Timer timer;
};

} // namespace rgw
