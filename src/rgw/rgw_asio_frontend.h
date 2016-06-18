// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_ASIO_FRONTEND_H
#define RGW_ASIO_FRONTEND_H

#include <memory>
#include "rgw_frontend.h"

class RGWAsioFrontend : public RGWFrontend {
  class Impl;
  std::unique_ptr<Impl> impl;
public:
  RGWAsioFrontend(const RGWProcessEnv& env);
  ~RGWAsioFrontend();

  int init() override;
  int run() override;
  void stop() override;
  void join() override;

  void pause_for_new_config() override;
  void unpause_with_new_config(RGWRados *store) override;
};

#endif // RGW_ASIO_FRONTEND_H
