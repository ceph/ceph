// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_HTTP_PROCESSOR_INTERFACE_H
#define CEPH_LIBRBD_MIGRATION_HTTP_PROCESSOR_INTERFACE_H

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/message.hpp>

namespace librbd {
namespace migration {

struct HttpProcessorInterface {
  using EmptyBody = boost::beast::http::empty_body;
  using EmptyRequest = boost::beast::http::request<EmptyBody>;

  virtual ~HttpProcessorInterface() {
  }

  virtual void process_request(EmptyRequest& request) = 0;

};

} // namespace migration
} // namespace librbd

#endif // CEPH_LIBRBD_MIGRATION_HTTP_PROCESSOR_INTERFACE_H
