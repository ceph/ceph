// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>

class DoutPrefixProvider;

/* Hints RGW serves itself, beside those a driver serves:  a subsystem
 * outside the drivers -- the Kafka endpoint's connections -- registers
 * one by name while it runs, and the driver hint endpoint looks a hint
 * up here before passing it to the driver.  Same signature and gates as
 * Driver::driver_hint(). */
namespace rgw::hint_registry {

using handler_t = std::function<int(const DoutPrefixProvider* dpp,
                                    const std::map<std::string, std::string>& params,
                                    std::map<std::string, std::string>* out)>;

void add(const std::string& name, handler_t handler);
void remove(const std::string& name);

/* the registered hint's result;  nullopt where none is registered */
std::optional<int> run(const DoutPrefixProvider* dpp, const std::string& name,
                       const std::map<std::string, std::string>& params,
                       std::map<std::string, std::string>* out);

} // namespace rgw::hint_registry
