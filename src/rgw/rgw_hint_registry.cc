// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_hint_registry.h"

#include <mutex>

namespace rgw::hint_registry {

namespace {
std::mutex lock;
std::map<std::string, handler_t> handlers;
}

void add(const std::string& name, handler_t handler)
{
  std::lock_guard l{lock};
  handlers[name] = std::move(handler);
}

void remove(const std::string& name)
{
  std::lock_guard l{lock};
  handlers.erase(name);
}

std::optional<int> run(const DoutPrefixProvider* dpp, const std::string& name,
                       const std::map<std::string, std::string>& params,
                       std::map<std::string, std::string>* out)
{
  handler_t handler;
  {
    std::lock_guard l{lock};
    auto it = handlers.find(name);
    if (it == handlers.end()) {
      return std::nullopt;
    }
    handler = it->second;
  }
  return handler(dpp, params, out);
}

} // namespace rgw::hint_registry
