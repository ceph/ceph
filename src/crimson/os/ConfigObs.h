#pragma once

#include "common/config_obs.h"
#include "common/config_values.h"
#include "crimson/common/config_proxy.h"
#include "common/options.h"

using Config = ceph::common::ConfigProxy;
const uint64_t INVALID_VALUE = (uint64_t)(-1);
const std::string memstore_device_bytes ="memstore_device_bytes";
const std::string memstore_page_set = "memstore_page_set";
const std::string memstore_page_size = "memstore_page_size";


class ConfigObs : public ceph::md_config_obs_impl<Config> {
  uint64_t total = 0;
  bool page_set = false;
  uint64_t page_size = 0;
private:
  const char** get_tracked_conf_keys() const override {
    static const char* keys[] = {
      memstore_device_bytes.c_str(),
      memstore_page_set.c_str(),
      memstore_page_size.c_str(),
      nullptr,
    };
    return keys;
  }
  void handle_conf_change(const Config& conf,
                          const std::set <std::string> &changes) override{
    if (changes.count(memstore_device_bytes)) {
      Option::size_t bytes = conf.get_val<Option::size_t>(memstore_device_bytes);
      total = static_cast<uint64_t>(bytes.value);
    }
    if (changes.count(memstore_page_set)) {
      page_set = conf.get_val<bool>(memstore_page_set);
    }
    if (changes.count(memstore_page_size)) {
      Option::size_t size = conf.get_val<Option::size_t>(memstore_page_size);
      page_size = static_cast<uint64_t>(size.value);
    }
  }
public:
  ConfigObs() {
  }
  uint64_t get_device_bytes() const {return total;}
  bool get_page_set() const {return page_set;}
  uint64_t get_page_size() const {return page_size;}
};

