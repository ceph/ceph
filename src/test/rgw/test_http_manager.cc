// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include "rgw_rados.h"
#include "rgw_http_client.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include <unistd.h>
#include <curl/curl.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <thread>
#include <gtest/gtest.h>

using namespace std;

namespace {
  using tcp = boost::asio::ip::tcp;

  // if we have a racing where another thread manages to bind and listen the
  // port picked by this acceptor, try again.
  static constexpr int MAX_BIND_RETRIES = 60;

  tcp::acceptor try_bind(boost::asio::io_context& ioctx) {
    using tcp = boost::asio::ip::tcp;
    tcp::endpoint endpoint(tcp::v4(), 0);
    tcp::acceptor acceptor(ioctx);
    acceptor.open(endpoint.protocol());
    for (int retries = 0;; retries++) {
      try {
	acceptor.bind(endpoint);
	// yay!
	break;
      } catch (const boost::system::system_error& e) {
	if (retries == MAX_BIND_RETRIES) {
	  throw;
	}
	if (e.code() != boost::system::errc::address_in_use) {
	  throw;
	}
      }
      // backoff a little bit
      sleep(1);
    }
    return acceptor;
  }
}

TEST(HTTPManager, ReadTruncated)
{
  using tcp = boost::asio::ip::tcp;
  boost::asio::io_context ioctx;
  auto acceptor = try_bind(ioctx);
  acceptor.listen();

  std::thread server{[&] {
    tcp::socket socket{ioctx};
    acceptor.accept(socket);
    std::string_view response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 1024\r\n"
        "\r\n"
        "short body";
    boost::asio::write(socket, boost::asio::buffer(response));
  }};
  const auto url = std::string{"http://127.0.0.1:"} + std::to_string(acceptor.local_endpoint().port());

  RGWHTTPClient client{g_ceph_context, "GET", url};
  const auto dpp = NoDoutPrefix{g_ceph_context, ceph_subsys_rgw};
  EXPECT_EQ(-EAGAIN, RGWHTTP::process(&dpp, &client, null_yield));

  server.join();
}

TEST(HTTPManager, Head)
{
  using tcp = boost::asio::ip::tcp;
  boost::asio::io_context ioctx;
  auto acceptor = try_bind(ioctx);
  acceptor.listen();

  std::thread server{[&] {
    tcp::socket socket{ioctx};
    acceptor.accept(socket);
    std::string_view response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 1024\r\n"
        "\r\n";
    boost::asio::write(socket, boost::asio::buffer(response));
  }};
  const auto url = std::string{"http://127.0.0.1:"} + std::to_string(acceptor.local_endpoint().port());

  RGWHTTPClient client{g_ceph_context, "HEAD", url};
  const auto dpp = NoDoutPrefix{g_ceph_context, ceph_subsys_rgw};
  EXPECT_EQ(0, RGWHTTP::process(&dpp, &client, null_yield));

  server.join();
}

TEST(HTTPManager, SignalThread)
{
  auto cct = g_ceph_context;
  RGWHTTPManager http(cct);

  ASSERT_EQ(0, http.start());

  // default pipe buffer size according to man pipe
  constexpr size_t max_pipe_buffer_size = 65536;
  // each signal writes 4 bytes to the pipe
  constexpr size_t max_pipe_signals = max_pipe_buffer_size / sizeof(uint32_t);
  // add_request and unregister_request
  constexpr size_t pipe_signals_per_request = 2;
  // number of http requests to fill the pipe buffer
  constexpr size_t max_requests = max_pipe_signals / pipe_signals_per_request;

  // send one extra request to test that we don't deadlock
  constexpr size_t num_requests = max_requests + 1;

  for (size_t i = 0; i < num_requests; i++) {
    RGWHTTPClient client{cct, "PUT", "http://127.0.0.1:80"};
    http.add_request(&client);
  }
}

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  rgw_http_client_init(cct->get());
  rgw_setup_saved_curl_handles();
  ::testing::InitGoogleTest(&argc, argv);
  int r = RUN_ALL_TESTS();
  rgw_release_all_curl_handles();
  rgw_http_client_cleanup();
  return r;
}
