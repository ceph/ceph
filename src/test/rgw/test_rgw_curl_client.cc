#include "curl/client.h"
#include <gtest/gtest.h>
#include <chrono>
#include <exception>
#include <optional>
#include <string>
#include <boost/asio.hpp>
#include <curl/curl.h>
#include <fmt/format.h>

namespace rgw::curl {

namespace asio = boost::asio;
namespace ip = asio::ip;

using namespace std::chrono_literals;

template <typename T>
auto capture(std::optional<T>& opt)
{
  return [&opt] (T value) { opt = std::move(value); };
}

template <typename T>
auto capture(asio::cancellation_signal& signal, std::optional<T>& opt)
{
  return asio::bind_cancellation_slot(signal.slot(), capture(opt));
}

void rethrow(std::exception_ptr eptr)
{
  if (eptr) std::rethrow_exception(eptr);
}

auto accept_connection(ip::tcp::acceptor& acceptor, size_t keepalive_count)
    -> asio::awaitable<void>
{
  auto socket = co_await acceptor.async_accept(asio::use_awaitable);

  for (size_t i = 0; i < keepalive_count; i++) {
    std::string request;
    co_await asio::async_read_until(socket, asio::dynamic_buffer(request),
                                    "\r\n\r\n", asio::use_awaitable);

    static constexpr std::string_view response =
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    co_await asio::async_write(socket, asio::buffer(response),
                               asio::use_awaitable);
  }
}

auto perform(Client& client, const char* url)
    -> asio::awaitable<void>
{
  auto easy = easy_init();
  ::curl_easy_setopt(easy.get(), CURLOPT_URL, url);

  co_await client.async_perform(easy.get(), asio::use_awaitable);
}

TEST(Client, one)
{
  asio::io_context sctx;
  const auto localhost = ip::make_address("127.0.0.1");
  auto acceptor = ip::tcp::acceptor{sctx, ip::tcp::endpoint{localhost, 0}};
  acceptor.listen(1);

  std::optional<std::exception_ptr> septr;
  asio::co_spawn(sctx, accept_connection(acceptor, 1), capture(septr));

  sctx.poll(); // server spawn, block on accept
  ASSERT_FALSE(sctx.stopped());
  EXPECT_FALSE(septr);

  const std::string url = fmt::format("http://127.0.0.1:{}",
                                      acceptor.local_endpoint().port());

  // step through the client on a separate io_context
  asio::io_context cctx;
  auto client = Client{cctx.get_executor()};

  std::optional<std::exception_ptr> ceptr;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(ceptr));

  cctx.poll(); // client spawn + write, block on wait_read
  ASSERT_FALSE(cctx.stopped());
  EXPECT_FALSE(ceptr);

  sctx.poll(); // server accept + read + write
  ASSERT_TRUE(sctx.stopped());
  ASSERT_TRUE(septr);
  EXPECT_FALSE(*septr);

  cctx.poll(); // client wait_read completes, timer is canceled
  ASSERT_TRUE(cctx.stopped());
  ASSERT_TRUE(ceptr);
  EXPECT_FALSE(*ceptr);
}

TEST(Client, cancel_destroy)
{
  asio::io_context sctx;
  const auto localhost = ip::make_address("127.0.0.1");
  auto acceptor = ip::tcp::acceptor{sctx, ip::tcp::endpoint{localhost, 0}};
  acceptor.listen(1);

  std::optional<std::exception_ptr> septr;
  asio::co_spawn(sctx, accept_connection(acceptor, 1), capture(septr));

  sctx.poll();
  ASSERT_FALSE(sctx.stopped());
  EXPECT_FALSE(septr);

  const std::string url = fmt::format("http://127.0.0.1:{}",
                                      acceptor.local_endpoint().port());

  // step through the client on a separate io_context
  asio::io_context cctx;
  auto cr = [&cctx] (const char* url) -> asio::awaitable<void> {
    auto client = Client{cctx.get_executor()};
    co_await perform(client, url);
  };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> ceptr;
  asio::co_spawn(cctx, cr(url.c_str()), capture(signal, ceptr));

  cctx.poll();
  ASSERT_FALSE(cctx.stopped());
  EXPECT_FALSE(ceptr);

  // cancel before the server runs
  signal.emit(asio::cancellation_type::terminal);

  sctx.poll();
  ASSERT_TRUE(sctx.stopped());
  ASSERT_TRUE(septr);
  EXPECT_FALSE(*septr);

  cctx.poll();
  ASSERT_TRUE(cctx.stopped());
  ASSERT_TRUE(ceptr);
  ASSERT_TRUE(*ceptr);
  try {
    std::rethrow_exception(*ceptr);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(Client, two)
{
  asio::io_context sctx;
  const auto localhost = ip::make_address("127.0.0.1");
  auto acceptor = ip::tcp::acceptor{sctx, ip::tcp::endpoint{localhost, 0}};
  acceptor.listen(2);

  // accept a single request then close the connection
  std::optional<std::exception_ptr> septr1;
  asio::co_spawn(sctx, accept_connection(acceptor, 1), capture(septr1));

  EXPECT_EQ(1, sctx.poll()); // server spawn, block on accept
  ASSERT_FALSE(sctx.stopped());
  EXPECT_FALSE(septr1);

  const std::string url = fmt::format("http://127.0.0.1:{}",
                                      acceptor.local_endpoint().port());

  // step through the client on a separate io_context
  asio::io_context cctx;
  auto client = Client{cctx.get_executor()};

  std::optional<std::exception_ptr> ceptr1;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(ceptr1));

  std::optional<std::exception_ptr> ceptr2;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(ceptr2));

  cctx.poll();
  ASSERT_FALSE(cctx.stopped());
  EXPECT_FALSE(ceptr1);
  EXPECT_FALSE(ceptr2);

  sctx.poll();
  ASSERT_TRUE(sctx.stopped());
  ASSERT_TRUE(septr1);
  EXPECT_FALSE(*septr1);

  cctx.poll();
  ASSERT_FALSE(cctx.stopped());
  ASSERT_TRUE(ceptr1);
  EXPECT_FALSE(*ceptr1);

  // accept a second connection
  std::optional<std::exception_ptr> septr2;
  asio::co_spawn(sctx, accept_connection(acceptor, 1), capture(septr2));

  sctx.restart();
  sctx.poll();
  ASSERT_TRUE(sctx.stopped());
  ASSERT_TRUE(septr2);
  EXPECT_FALSE(*septr2);

  cctx.poll();
  ASSERT_TRUE(cctx.stopped());
  ASSERT_TRUE(ceptr2);
  EXPECT_FALSE(*ceptr2);
}

TEST(Client, two_keepalive)
{
  asio::io_context sctx;
  const auto localhost = ip::make_address("127.0.0.1");
  auto acceptor = ip::tcp::acceptor{sctx, ip::tcp::endpoint{localhost, 0}};
  acceptor.listen(1);

  // accept two requests on the same connection
  std::optional<std::exception_ptr> septr;
  asio::co_spawn(sctx, accept_connection(acceptor, 2), capture(septr));

  EXPECT_EQ(1, sctx.poll()); // server spawn, block on accept
  ASSERT_FALSE(sctx.stopped());
  EXPECT_FALSE(septr);

  const std::string url = fmt::format("http://127.0.0.1:{}",
                                      acceptor.local_endpoint().port());

  // step through the client on a separate io_context
  asio::io_context cctx;
  auto client = Client{cctx.get_executor()};

  std::optional<std::exception_ptr> ceptr1;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(ceptr1));

  cctx.poll();
  ASSERT_FALSE(cctx.stopped());
  EXPECT_FALSE(ceptr1);

  sctx.poll();
  ASSERT_FALSE(sctx.stopped());
  ASSERT_FALSE(septr);

  cctx.poll();
  ASSERT_TRUE(cctx.stopped());
  ASSERT_TRUE(ceptr1);
  EXPECT_FALSE(*ceptr1);

  std::optional<std::exception_ptr> ceptr2;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(ceptr2));

  cctx.restart();
  cctx.poll();
  ASSERT_FALSE(cctx.stopped());
  EXPECT_FALSE(ceptr2);

  sctx.poll();
  ASSERT_TRUE(sctx.stopped());
  ASSERT_TRUE(septr);
  EXPECT_FALSE(*septr);

  cctx.run_for(10ms);
  ASSERT_TRUE(cctx.stopped());
  ASSERT_TRUE(ceptr2);
  EXPECT_FALSE(*ceptr2);
}

TEST(Client, two_signal_one)
{
  asio::io_context sctx;
  const auto localhost = ip::make_address("127.0.0.1");
  auto acceptor = ip::tcp::acceptor{sctx, ip::tcp::endpoint{localhost, 0}};
  acceptor.listen(2);

  std::optional<std::exception_ptr> septr1;
  asio::co_spawn(sctx, accept_connection(acceptor, 1), capture(septr1));
  std::optional<std::exception_ptr> septr2;
  asio::co_spawn(sctx, accept_connection(acceptor, 1), capture(septr2));

  sctx.poll(); // server spawn, block on accept
  ASSERT_FALSE(sctx.stopped());
  EXPECT_FALSE(septr1);
  EXPECT_FALSE(septr2);

  const std::string url = fmt::format("http://127.0.0.1:{}",
                                      acceptor.local_endpoint().port());

  // step through the client on a separate io_context
  asio::io_context cctx;
  auto client = Client{cctx.get_executor()};

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> ceptr1;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(signal, ceptr1));

  std::optional<std::exception_ptr> ceptr2;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(ceptr2));

  cctx.poll();
  ASSERT_FALSE(cctx.stopped());
  EXPECT_FALSE(ceptr1);
  EXPECT_FALSE(ceptr2);

  // cancel before server runs
  signal.emit(asio::cancellation_type::terminal);

  sctx.poll();
  ASSERT_TRUE(sctx.stopped());
  ASSERT_TRUE(septr1);
  EXPECT_FALSE(*septr1);
  ASSERT_TRUE(septr2);
  EXPECT_FALSE(*septr2);

  cctx.poll();
  ASSERT_TRUE(cctx.stopped());
  ASSERT_TRUE(ceptr1);
  ASSERT_TRUE(*ceptr1);
  try {
    std::rethrow_exception(*ceptr1);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
  ASSERT_TRUE(ceptr2);
  EXPECT_FALSE(*ceptr2);
}

TEST(Client, two_cancel)
{
  asio::io_context sctx;
  const auto localhost = ip::make_address("127.0.0.1");
  auto acceptor = ip::tcp::acceptor{sctx, ip::tcp::endpoint{localhost, 0}};
  acceptor.listen(2);

  std::optional<std::exception_ptr> septr1;
  asio::co_spawn(sctx, accept_connection(acceptor, 1), capture(septr1));
  std::optional<std::exception_ptr> septr2;
  asio::co_spawn(sctx, accept_connection(acceptor, 1), capture(septr2));

  sctx.poll(); // server spawn, block on accept
  ASSERT_FALSE(sctx.stopped());
  EXPECT_FALSE(septr1);
  EXPECT_FALSE(septr2);

  const std::string url = fmt::format("http://127.0.0.1:{}",
                                      acceptor.local_endpoint().port());

  // step through the client on a separate io_context
  asio::io_context cctx;
  auto client = Client{cctx.get_executor()};

  std::optional<std::exception_ptr> ceptr1;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(ceptr1));

  std::optional<std::exception_ptr> ceptr2;
  asio::co_spawn(cctx, perform(client, url.c_str()), capture(ceptr2));

  cctx.poll();
  ASSERT_FALSE(cctx.stopped());
  EXPECT_FALSE(ceptr1);
  EXPECT_FALSE(ceptr2);

  // cancel before server runs
  client.cancel();

  sctx.poll();
  ASSERT_TRUE(sctx.stopped());
  ASSERT_TRUE(septr1);
  EXPECT_FALSE(*septr1);
  ASSERT_TRUE(septr2);
  EXPECT_FALSE(*septr2);

  cctx.poll();
  ASSERT_TRUE(cctx.stopped());
  ASSERT_TRUE(ceptr1);
  ASSERT_TRUE(*ceptr1);
  try {
    std::rethrow_exception(*ceptr1);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
  ASSERT_TRUE(ceptr2);
  ASSERT_TRUE(*ceptr2);
  try {
    std::rethrow_exception(*ceptr2);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

auto client_load(Client& client, const char* url, size_t count)
    -> asio::awaitable<void>
{
  for (size_t i = 0; i < count; i++) {
    co_await perform(client, url);
  }
}

TEST(ClientLoad, single_thread)
{
  constexpr size_t max_connections = 8;
  constexpr size_t requests_per_connection = 256;

  asio::io_context ctx;
  const auto localhost = ip::make_address("127.0.0.1");
  auto acceptor = ip::tcp::acceptor{ctx, ip::tcp::endpoint{localhost, 0}};
  acceptor.listen(max_connections);

  auto client = Client{ctx.get_executor()};
  const std::string url = fmt::format("http://127.0.0.1:{}",
                                      acceptor.local_endpoint().port());

  for (size_t i = 0; i < max_connections; i++) {
    asio::co_spawn(ctx, accept_connection(acceptor,
        requests_per_connection), rethrow);
    asio::co_spawn(ctx, client_load(client, url.c_str(),
        requests_per_connection), rethrow);
  }

  ctx.run();
}

TEST(ClientLoad, multi_thread)
{
  constexpr size_t num_threads = 8;
  constexpr size_t max_connections = 8;
  constexpr size_t requests_per_connection = 256;

  auto ctx = asio::static_thread_pool{num_threads};
  const auto localhost = ip::make_address("127.0.0.1");
  auto acceptor = ip::tcp::acceptor{ctx, ip::tcp::endpoint{localhost, 0}};
  acceptor.listen(max_connections);

  for (size_t i = 0; i < max_connections; i++) {
    asio::co_spawn(ctx, accept_connection(acceptor,
        requests_per_connection), rethrow);
  }

  const std::string url = fmt::format("http://127.0.0.1:{}",
                                      acceptor.local_endpoint().port());

  auto ex = asio::make_strand(ctx);
  auto client = Client{ex};
  for (size_t i = 0; i < max_connections; i++) {
    asio::co_spawn(ex, client_load(client, url.c_str(),
        requests_per_connection), rethrow);
  }

  ctx.join();
}

} // namespace rgw::curl
