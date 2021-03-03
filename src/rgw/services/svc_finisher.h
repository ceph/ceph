// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <boost/asio/io_context.hpp>
#include "include/function2.hpp"

#include "rgw/rgw_service.h"

namespace ba = boost::asio;

class RGWSI_Finisher : public RGWServiceInstance
{
  friend struct RGWServices_Def;
private:
  bool finalized{false};
  std::optional<ba::io_context::strand> strand;

  void shutdown() override;

  std::unordered_map<int, fu2::unique_function<void()>> shutdown_cbs;
  std::atomic<int> handles_counter{0};

protected:
  void init(ba::io_context* ioctx) {
    strand.emplace(*ioctx);
  }
  int do_start(optional_yield y, const DoutPrefixProvider *dpp) override;

public:
  RGWSI_Finisher(CephContext *cct): RGWServiceInstance(cct) {}
  ~RGWSI_Finisher();

  int register_caller(fu2::unique_function<void()>&& cb);
  void unregister_caller(int handle);

  template<typename CT>
  auto schedule(CT&& ct) {
    boost::asio::async_completion<CT, void()> init(ct);
    boost::asio::defer(*strand, std::move(init.completion_handler));
    return init.result.get();
  }
};
