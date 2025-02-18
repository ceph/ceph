#include "web_server.h"
#include "common/debug.h"
#include "common/hostname.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "exporter/DaemonMetricCollector.h"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>   // SSL/TLS
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/thread/thread.hpp>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ceph_exporter

namespace beast = boost::beast;   // from <boost/beast.hpp>
namespace http = beast::http;     // from <boost/beast/http.hpp>
namespace net = boost::asio;      // from <boost/asio.hpp>
namespace ssl = boost::asio::ssl; // from <boost/asio/ssl.hpp>
using tcp = boost::asio::ip::tcp; // from <boost/asio/ip/tcp.hpp>

//common io context for the web servers
std::shared_ptr<net::io_context> global_ioc;

// Base class for common functionality
class web_connection {
public:
  virtual ~web_connection() = default;
  virtual void start() = 0; // Pure virtual function to start the connection

protected:
  beast::flat_buffer buffer_{8192};
  http::request<http::dynamic_body> request_;
  http::response<http::string_body> response_;
  net::steady_timer deadline_;

  web_connection(net::any_io_executor executor, std::chrono::seconds timeout)
      : deadline_(executor, timeout) {}

  // Common request processing logic
  void process_request() {
    std::cout << "Hello, World!" << std::endl;
    response_.version(request_.version());
    response_.keep_alive(request_.keep_alive());

    switch (request_.method()) {
    case http::verb::get:
      response_.result(http::status::ok);
      std::cout << "Hello, World in switch case!" << std::endl;
      create_response();
      break;

    default:
      response_.result(http::status::method_not_allowed);
      response_.set(http::field::content_type, "text/plain");
      std::string body("Invalid request-method '" + std::string(request_.method_string()) + "'\n");
      response_.body() = body;
      break;
    }
    write_response();
  }

  std::map<std::string, std::string> parse_query_string(const std::string& query) {
    std::map<std::string, std::string> params;
    std::istringstream query_stream(query);
    std::string key_value;

    while (std::getline(query_stream, key_value, '&')) { // Split on '&'
        size_t pos = key_value.find('=');
        if (pos != std::string::npos) {
            std::string key = key_value.substr(0, pos);
            std::string value = key_value.substr(pos + 1);
            params[key] = value;
        }
    }
    return params;
  }

  std::string extract_query_string(const std::string& url) {
    size_t pos = url.find('?');  // Find start of query string
    if (pos != std::string::npos) {
        return url.substr(pos + 1);  // Extract everything after '?'
    }
    return "";  // No query string found
  }

  // Construct a response message based on the request target
  void create_response() {
    std::cout << "Hello, World create_response!" << std::endl;
    std::cout << "requested string: " << request_.target() << std::endl;
    if (request_.target() == "/") {
        response_.result(http::status::moved_permanently);
        response_.set(http::field::location, "/metrics");
    } else if ((request_.target() == "/metrics") || (request_.target().compare(0, std::string("/metrics?").size(), "/metrics?") == 0)) {
      std::cout << "request string: " << request_.target() << std::endl;
      response_.set(http::field::content_type, "text/plain; charset=utf-8");

      auto filters = parse_query_string(extract_query_string(request_.target()));
      auto is_only = (filters.find("exclude_perf_counter_prefix") != filters.end());
      auto is_exclude = (filters.find("only_perf_counter_prefix") != filters.end());
      if (is_only && is_exclude) {
        response_.body() = "Please pass either exclude_perf_counter_prefix or only_perf_counter_prefix and not both";
      } else {
        DaemonMetricCollector &collector = collector_instance();
        std::string metrics = collector.get_metrics(filters);
        response_.body() = metrics;
      }
    } else {
      response_.result(http::status::method_not_allowed);
      response_.set(http::field::content_type, "text/plain");
      response_.body() = "File not found \n";
    }
  }

  // Asynchronously transmit the response message
  virtual void write_response() = 0;

  // Check whether we have spent enough time on this connection
  void check_deadline(std::shared_ptr<web_connection> self) {
    deadline_.async_wait([self](beast::error_code ec) {
      if (!ec) {
          self->close_connection(ec);
      }
    });
  }

  // Bad requests error mgmt (http req->https srv and https req ->http srv)
  void handle_bad_request(beast::error_code ec) {
    response_.version(request_.version());
    response_.keep_alive(request_.keep_alive());
    response_.result(http::status::method_not_allowed);
    response_.set(http::field::content_type, "text/plain");
    std::string body = "Ceph exporter.\nRequest Error: " + ec.message();
    response_.body() = body;

    write_response();
  }

  virtual void close_connection(beast::error_code& ec) = 0;
};

// Derived class for HTTP connections
class http_connection : public web_connection, public std::enable_shared_from_this<http_connection> {
public:
  explicit http_connection(tcp::socket socket)
      : web_connection(socket.get_executor(), std::chrono::seconds(60)), socket_(std::move(socket)) {}

  void start() override {
      read_request(shared_from_this());
      check_deadline(shared_from_this());
  }

private:
  tcp::socket socket_;

  void read_request(std::shared_ptr<http_connection> self) {
    http::async_read(socket_, buffer_, request_,
                        [self](beast::error_code ec, std::size_t bytes_transferred) {
                          boost::ignore_unused(bytes_transferred);
                          if (ec) {
                              dout(1) << "ERROR: " << ec.message() << dendl;
                              self->handle_bad_request(ec);
                              return;
                          }
                          self->process_request();
                        });
  }

  void write_response() override {
    auto self = shared_from_this();
    response_.prepare_payload();
    http::async_write(socket_, response_,
                      [self](beast::error_code ec, std::size_t) {
                        self->socket_.shutdown(tcp::socket::shutdown_send, ec);
                        self->deadline_.cancel();
                        if (ec) {
                            dout(1) << "ERROR: " << ec.message() << dendl;
                            return;
                        }
                      });
  }

  void close_connection(beast::error_code& ec) override {
      socket_.close(ec);
  }
};

// Derived class for HTTPS connections
class https_connection : public web_connection, public std::enable_shared_from_this<https_connection> {
public:
  explicit https_connection(ssl::stream<tcp::socket> socket)
    : web_connection(socket.get_executor(), std::chrono::seconds(60)), socket_(std::move(socket)) {}

  void start() override {
    auto self = shared_from_this();
    socket_.async_handshake(ssl::stream_base::server,
                            [self](beast::error_code ec) {
                              if (!ec) {
                                self->read_request(self);
                              } else {
                                dout(1) << "ERROR: SSL Handshake failed: " << ec.message() << dendl;
                                self->handle_bad_request(ec);
                              }
                            });
    check_deadline(self);
  }

private:
  ssl::stream<tcp::socket> socket_;

  void read_request(std::shared_ptr<https_connection> self) {
    http::async_read(socket_, buffer_, request_,
                      [self](beast::error_code ec, std::size_t bytes_transferred) {
                        boost::ignore_unused(bytes_transferred);
                        if (ec) {
                            dout(1) << "ERROR: " << ec.message() << dendl;
                            return;
                        }
                        self->process_request();
                      });
  }

  void write_response() override {
    auto self = shared_from_this();
    response_.prepare_payload();
    http::async_write(socket_, response_,
                      [self](beast::error_code ec, std::size_t) {
                        self->socket_.async_shutdown([self](beast::error_code ec) {
                          self->deadline_.cancel();
                          if (ec) {
                            dout(1) << "ERROR: " << ec.message() << dendl;
                          }
                        });
                      });
  }

  void close_connection(beast::error_code& ec) override {
      socket_.lowest_layer().close(ec);
  }

};

void http_server(tcp::acceptor &acceptor, tcp::socket &socket) {
  acceptor.async_accept(socket, [&](beast::error_code ec) {
    if (!ec) {
      std::make_shared<http_connection>(std::move(socket))->start();
    }
    http_server(acceptor, socket);
  });
}

void https_server(tcp::acceptor &acceptor, ssl::context &ssl_ctx) {
  acceptor.async_accept([&](beast::error_code ec, tcp::socket socket) {
    if (!ec) {
      std::make_shared<https_connection>(ssl::stream<tcp::socket>(std::move(socket), ssl_ctx))->start();
    }
    https_server(acceptor, ssl_ctx);
  });
}

void run_http_server(const std::string& exporter_addr, short unsigned int port) {
  tcp::acceptor acceptor{*global_ioc, {net::ip::make_address(exporter_addr), port}};
  tcp::socket socket{*global_ioc};

  http_server(acceptor, socket);

  dout(1) << "HTTP server running on " << exporter_addr << ":" << port << dendl;
  global_ioc->run();
}

void run_https_server(const std::string& exporter_addr, short unsigned int port, const std::string& cert_file, const std::string& key_file) {
  ssl::context ssl_ctx(ssl::context::tlsv13);

  ssl_ctx.use_certificate_chain_file(cert_file);
  ssl_ctx.use_private_key_file(key_file, ssl::context::pem);

  tcp::acceptor acceptor{*global_ioc, {net::ip::make_address(exporter_addr), port}};
  https_server(acceptor, ssl_ctx);

  dout(1) << "HTTPS server running on " << exporter_addr << ":" << port << dendl;
  global_ioc->run();
}

void stop_web_server() {
  if (global_ioc) {
    global_ioc->stop();
    dout(1) << "Ceph exporter web server stopped" << dendl;
  }
}

void web_server_thread_entrypoint() {
  try {
    std::string exporter_addr = g_conf().get_val<std::string>("exporter_addr");
    short unsigned int port = g_conf().get_val<int64_t>("exporter_http_port");
    std::string cert_file = g_conf().get_val<std::string>("exporter_cert_file");
    std::string key_file = g_conf().get_val<std::string>("exporter_key_file");

    // Initialize global_ioc
    global_ioc = std::make_shared<net::io_context>(1);

    if (cert_file.empty() && key_file.empty()) {
      run_http_server(exporter_addr, port);
    } else {
      try {
          run_https_server(exporter_addr, port, cert_file, key_file);
      } catch (const std::exception &e) {
          derr << "Failed to start HTTPS server: " << e.what() << dendl;
          exit(EXIT_FAILURE);
      }
    }
  } catch (std::exception const &e) {
      derr << "Error: " << e.what() << dendl;
      exit(EXIT_FAILURE);
  }
}
