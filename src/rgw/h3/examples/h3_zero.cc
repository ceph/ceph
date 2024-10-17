// h3_zero
//
// a very simple example API where all request paths refer to a single resource
// which has only a content length
//
// HEAD: send response header with the current content length
// GET: send response body full of zeroes up to the current content length
// PUT: read/discard the entire request body but record the content length

#include <atomic>
#include <charconv>
#include <chrono>
#include <iostream>
#include <span>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/program_options.hpp>

#include <h3/h3.h>
#include "observer.h"

namespace asio = boost::asio;
namespace ip = asio::ip;
namespace http = boost::beast::http;

class Data {
  std::atomic<std::size_t> size = 0;
 public:
  std::size_t get_size() const { return size; }
  void set_size(std::size_t value) { size = value; }
};

auto error_response(boost::intrusive_ptr<rgw::h3::Connection> conn,
                    rgw::h3::StreamIO& stream,
                    std::string_view status)
    -> asio::awaitable<void, rgw::h3::Listener::executor_type>
{
  http::fields response;
  response.set(":status", {status.data(), status.size()});
  response.set(http::field::server, "h3_zero");
  response.set(http::field::content_length, "0");

  try {
    constexpr bool fin = true; // no body to follow
    co_await conn->write_response(stream, response, fin);
  } catch (const std::exception& e) {
    std::cerr << "failed to write error response: " << e.what() << '\n';
  }
}

auto handle_head(boost::intrusive_ptr<rgw::h3::Connection> conn,
                 rgw::h3::StreamIO& stream, const Data& data)
    -> asio::awaitable<void, rgw::h3::Listener::executor_type>
{
  const std::size_t size = data.get_size();
  std::cout << "HEAD sending content-length:" << size << '\n';

  http::fields response;
  response.set(":status", "200");
  response.set(http::field::server, "h3_zero");
  response.set(http::field::content_length, std::to_string(size));

  constexpr bool fin = true; // no body to follow
  co_await conn->write_response(stream, response, fin);
}

auto handle_get(boost::intrusive_ptr<rgw::h3::Connection> conn,
                rgw::h3::StreamIO& stream, const Data& data)
    -> asio::awaitable<void, rgw::h3::Listener::executor_type>
{
  const std::size_t size = data.get_size();
  std::cout << "GET sending " << size << " bytes of response body...\n";

  http::fields response;
  response.set(":status", "200");
  response.set(http::field::server, "h3_zero");
  response.set(http::field::content_length, std::to_string(size));

  bool fin = (size == 0); // no body?
  co_await conn->write_response(stream, response, fin);

  std::vector<unsigned char> zeroes;
  zeroes.resize(std::min<std::size_t>(size, 64*1024), '\0');

  std::size_t remaining = size;
  while (remaining) {
    std::size_t count = std::min(zeroes.size(), remaining);
    auto buffer = std::span(zeroes.begin(), count);
    fin = (count == remaining); // last buffer?
    std::size_t bytes = co_await conn->write_body(stream, buffer, fin);
    remaining -= bytes;
  }
  std::cout << "GET successful\n";
}

auto handle_put(boost::intrusive_ptr<rgw::h3::Connection> conn,
                rgw::h3::StreamIO& stream, const http::fields& request,
                Data& data)
    -> asio::awaitable<void, rgw::h3::Listener::executor_type>
{
  auto content_length = request.find(http::field::content_length);
  if (content_length == request.end()) {
    std::cout << "PUT returning error: 411 Length Required\n";
    co_return co_await error_response(std::move(conn), stream, "411");
  }

  // parse the content length string
  auto str = content_length->value();
  std::size_t length = 0;
  auto [ptr, ec] = std::from_chars(str.begin(), str.end(), length);
  if (ec != std::errc{} || ptr != str.end()) {
    std::cout << "PUT returning error: 400 Bad Request\n";
    co_return co_await error_response(std::move(conn), stream, "400");
  }

  std::cout << "PUT reading " << length << " bytes...\n";

  // read and discard all of the request body
  std::vector<unsigned char> zeroes;
  zeroes.resize(std::min<std::size_t>(length, 64*1024), '\0');

  std::size_t remaining = length;
  while (remaining) {
    std::size_t count = std::min(zeroes.size(), remaining);
    auto buffer = std::span(zeroes.begin(), count);
    std::size_t bytes = co_await conn->read_body(stream, buffer);
    remaining -= bytes;
  }

  // update the object size on success
  data.set_size(length);

  std::cout << "PUT successful\n";

  http::fields response;
  response.set(":status", "200");
  response.set(http::field::server, "h3_zero");
  response.set(http::field::content_length, "0");

  constexpr bool fin = true; // no body to follow
  co_await conn->write_response(stream, response, fin);
}

auto handle_request(boost::intrusive_ptr<rgw::h3::Connection> conn,
                    uint64_t stream_id, http::fields request, Data& data)
    -> asio::awaitable<void, rgw::h3::Listener::executor_type>
{
  auto stream = rgw::h3::StreamIO{stream_id};

  // request must contain :method and :path
  auto method = request.find(":method");
  auto path = request.find(":path");
  if (method == request.end() || path == request.end()) {
    std::cout << "request missing :method or :path, "
        "returning error: 400 Bad Request\n";
    co_return co_await error_response(std::move(conn), stream, "400");
  }

  try {
    if (method->value() == "HEAD") {
      co_return co_await handle_head(conn, stream, data);
    } else if (method->value() == "GET") {
      co_return co_await handle_get(conn, stream, data);
    } else if (method->value() == "PUT") {
      co_return co_await handle_put(conn, stream, request, data);
    }
  } catch (const std::exception& e) {
    std::cerr << "failed to handle " << method->value()
        << " request: " << e.what() << '\n';
    co_return;
  }

  std::cout << "method:" << method->value()
      << " returning error: 405 Method Not Allowed\n";
  co_await error_response(std::move(conn), stream, "405");
}


// StreamHandler that spawns handle_request()
struct ZeroServer {
  Data& data;

  void operator()(boost::intrusive_ptr<rgw::h3::Connection> conn,
                  uint64_t stream_id, http::fields request,
                  ip::udp::endpoint myaddr, ip::udp::endpoint peeraddr)
  {
    std::cout << "request from " << peeraddr << '\n';

    auto ex = conn->get_executor();
    asio::co_spawn(ex, handle_request(std::move(conn), stream_id,
                                      std::move(request), data),
                   [] (std::exception_ptr eptr) {
                     if (eptr) std::rethrow_exception(eptr);
                   });
  }
};

// library entrypoints
extern "C" {
auto create_h3_config(const rgw::h3::Options&)
    -> std::unique_ptr<rgw::h3::Config>;
auto create_h3_listener(rgw::h3::Observer& observer,
                        rgw::h3::Config& config,
                        rgw::h3::Listener::executor_type ex,
                        ip::udp::socket socket,
                        rgw::h3::StreamHandler& on_new_stream)
    -> std::unique_ptr<rgw::h3::Listener>;
} // extern "C"

namespace po = boost::program_options;

int main(int argc, char** argv)
{
  // library options
  rgw::h3::Options o;

  ip::port_type port = 8000;
  std::string ssl_certificate;
  std::string ssl_private_key;
  size_t thread_count = 1;
  uint64_t conn_idle_timeout_ms = 5000;
  uint64_t max_ack_delay_ms = 25;

  po::options_description options("Options");
  options.add_options()
      ("help,h", "print usage")
      ("verbose,v", "verbose output")
      ("port,p", po::value(&port)->default_value(8000),
       "udp port number")
      ("cert,c", po::value(&ssl_certificate)->required(),
       "path to ssl certificate (required)")
      ("key,k", po::value(&ssl_private_key)->required(),
       "path to ssl private key (required)")
      ("threads,t", po::value(&thread_count)->default_value(1),
       "number of worker threads")
      ;

  po::options_description library_options("Library options");
  library_options.add_options()
      ("conn-idle-timeout-ms", po::value(&conn_idle_timeout_ms),
       "connection idle timeout in milliseconds")
      ("conn-max-streams", po::value(&o.conn_max_streams_bidi),
       "maximum number of streams per connection")
      ("conn-max-data", po::value(&o.conn_max_data),
       "flow control limit per connection")
      ("stream-max-data-local", po::value(&o.stream_max_data_bidi_local),
       "local flow control limit per stream")
      ("stream-max-data-remote", po::value(&o.stream_max_data_bidi_remote),
       "remote flow control limit per stream")
      ("ack-delay-exp", po::value(&o.ack_delay_exponent), "ack delay exponent")
      ("max-ack-delay-ms", po::value(&max_ack_delay_ms),
       "maximum ack delay in milliseconds")
      ("cc-alg", po::value(&o.cc_algorithm),
       "congestion control algorithm: cubic (default), reno, bbr, bbr2")
      ;
  options.add(library_options);

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, options), vm);
    po::notify(vm);
  } catch (const std::exception& e) {
    std::cout << e.what() << "\n\n";
    std::cout << options << '\n';
    return EXIT_FAILURE;
  }

  if (vm.count("help")) {
    std::cout << options << '\n';
    return EXIT_FAILURE;
  }

  o.ssl_certificate = ssl_certificate;
  o.ssl_private_key = ssl_private_key;
  if (vm.count("verbose")) {
    o.log_callback = [] (const char* msg, void*) { std::cout << msg << '\n'; };
  }
  o.conn_idle_timeout = std::chrono::milliseconds(conn_idle_timeout_ms);
  o.max_ack_delay = std::chrono::milliseconds(max_ack_delay_ms);

  // configure the library
  std::unique_ptr<rgw::h3::Config> config;
  try {
    config = create_h3_config(o);
  } catch (const std::exception& e) {
    std::cout << "Configuration failed: " << e.what() << '\n';
    return EXIT_FAILURE;
  }

  // bind a udp socket
  auto ctx = asio::thread_pool{thread_count};
  const auto endpoint = ip::udp::endpoint{ip::address_v4::any(), port};
  ip::udp::socket socket{ctx, endpoint};
  socket.non_blocking(true);
  socket.set_option(ip::udp::socket::reuse_address{true});
  std::cout << "Bound udp socket to " << socket.local_endpoint() << '\n';

  // create the listener
  StderrObserver observer;
  Data data;
  rgw::h3::StreamHandler on_new_stream = ZeroServer{data};
  std::unique_ptr<rgw::h3::Listener> listener;
  try {
    listener = create_h3_listener(observer, *config, asio::make_strand(ctx),
                                  std::move(socket), on_new_stream);
  } catch (const std::exception& e) {
    std::cout << "Failed to start listener: " << e.what() << '\n';
    return EXIT_FAILURE;
  }

  std::cout << "Listening for connections\n";
  listener->async_listen();

  // run the server
  try {
    ctx.join();
  } catch (const std::exception& e) {
    std::cout << "Exiting with exception: " << e.what() << '\n';
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
