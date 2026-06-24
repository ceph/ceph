// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw/rgw_asio_frontend_connection.h"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/intrusive_ptr.hpp>

#include <gtest/gtest.h>

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using rgw::asio::Connection;
using rgw::asio::ConnectionList;

// Testcases for ConnectionList::close() shutdown behavior.
//
// Verifies that close() uses socket.cancel() + ::shutdown() rather than
// socket.close(). Calling socket.close() from outside the socket's strand
// is a boost::asio thread-safety violation that can SIGSEGV or orphaned
// pending operations.

// Verify that ConnectionList::close() does NOT close the sockets (fd stays
// valid, is_open() remains true) but does shut down the transport (writes
// fail). This ensures we never call socket.close() from outside the strand.
TEST(BeastFrontendShutdown, CloseShutdownsButDoesNotCloseSocket)
{
  asio::io_context ioctx;
  ConnectionList connections;

  tcp::acceptor acceptor(ioctx, tcp::endpoint(tcp::v4(), 0));
  acceptor.listen(3);

  tcp::socket c0(ioctx), c1(ioctx), c2(ioctx);
  c0.connect(acceptor.local_endpoint());
  auto s0 = acceptor.accept();
  c1.connect(acceptor.local_endpoint());
  auto s1 = acceptor.accept();
  c2.connect(acceptor.local_endpoint());
  auto s2 = acceptor.accept();

  boost::intrusive_ptr<Connection> conns[] = {
    new Connection(std::move(s0)),
    new Connection(std::move(s1)),
    new Connection(std::move(s2)),
  };

  {
    auto g0 = connections.add(*conns[0]);
    auto g1 = connections.add(*conns[1]);
    auto g2 = connections.add(*conns[2]);

    ASSERT_EQ(3u, connections.size());

    boost::system::error_code ec;
    connections.close(ec);

    // After close(): sockets must still be "open" from boost::asio's
    // perspective (fd not released, descriptor_data not zeroed).
    // socket.close() would have set is_open()=false and fd=-1, which is the
    // thread-safety violation that causes SIGSEGV.
    for (auto& conn : conns) {
      EXPECT_TRUE(conn->socket.is_open());
      EXPECT_GE(conn->socket.native_handle(), 0);
    }

    // But the transport is shut down — writes must fail
    for (auto& conn : conns) {
      char buf[] = "test";
      asio::write(conn->socket, asio::buffer(buf), ec);
      EXPECT_TRUE(ec.failed())
          << "Write should fail after shutdown";
    }
  } // guards destroyed here — connections removed from list

  EXPECT_EQ(0u, connections.size());
}
