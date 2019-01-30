// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>

#include <boost/asio.hpp>

#include "include/function2.hpp"

#include "rgw/rgw_service.h"


class RGWSI_Finisher : public RGWServiceInstance
{
  friend struct RGWServices_Def;
public:

  using ShutdownCB = fu2::unique_function<void()>;

private:
  boost::asio::io_context::strand finish_strand;
  bool finalized{false};

  void shutdown() override;

  std::map<int, ShutdownCB> shutdown_cbs;
  std::atomic<int> handles_counter{0};

protected:
  void init() {}

public:
  RGWSI_Finisher(CephContext* cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc), finish_strand(ioc) {}
  ~RGWSI_Finisher();

  int register_caller(ShutdownCB&& cb);
  void unregister_caller(int handle);

  template<typename CompletionToken>
  void schedule(CompletionToken&& token) {
    boost::asio::defer(finish_strand, std::forward<CompletionToken>(token));
  }
};
