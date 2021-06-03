// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "stop_signal.h"

#include <csignal>
#include <seastar/core/reactor.hh>

StopSignal::StopSignal()
{
  seastar::engine().handle_signal(SIGINT, [this] { signaled(); });
  seastar::engine().handle_signal(SIGTERM, [this] { signaled(); });
}

StopSignal::~StopSignal()
{
  seastar::engine().handle_signal(SIGINT, [] {});
  seastar::engine().handle_signal(SIGTERM, [] {});
}

seastar::future<> StopSignal::wait()
{
  return should_stop.wait([this] { return caught; });
}

void StopSignal::signaled()
{
  if (caught) {
    return;
  }
  caught = true;
  should_stop.signal();
}
