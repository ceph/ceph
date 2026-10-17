// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <set>
#include <string>
#include <vector>

#include "common/config_obs.h"
#include "common/config_proxy.h"
#include "osd/oob_direct_read.h"

#ifdef WITH_CRIMSON
#error "oob_direct_read_cacher.h needs the classic config"
#endif

/**
 * The classic OSD's config observer of osd_oob_direct_read. It needs the
 * classic config, so that crimson, which includes oob_direct_read.h
 * through ECListener.h, must not include this header.
 */
namespace ceph::osd::oob {

/**
 * osd_oob_direct_read as the config has it now. A read asks for it on
 * every op, and get_val() would take the config's lock each time: a
 * config observer keeps it here instead.
 */
class direct_read_cacher_t : public md_config_obs_t {
public:
  static constexpr const char* option = "osd_oob_direct_read";

  explicit direct_read_cacher_t(ConfigProxy& conf) : conf(conf) {
    conf.add_observer(this);
    mode = to_direct_read(conf.get_val<std::string>(option));
  }
  ~direct_read_cacher_t() override {
    conf.remove_observer(this);
  }
  direct_read_cacher_t(const direct_read_cacher_t&) = delete;
  direct_read_cacher_t& operator=(const direct_read_cacher_t&) = delete;

  direct_read_t get() const {
    return mode.load(std::memory_order_relaxed);
  }

  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {option};
  }
  void handle_conf_change(const ConfigProxy& c,
                          const std::set<std::string>& changed) override {
    if (changed.contains(option)) {
      mode = to_direct_read(c.get_val<std::string>(option));
    }
  }

private:
  ConfigProxy& conf;
  std::atomic<direct_read_t> mode{direct_read_t::off};
};

} // namespace ceph::osd::oob
