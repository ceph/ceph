// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TRACEPOINT_PROVIDER_H
#define CEPH_TRACEPOINT_PROVIDER_H

#include "common/ceph_context.h"
#include "common/config_obs.h"
#include "common/ceph_mutex.h"
#include <dlfcn.h>

class TracepointProvider : public md_config_obs_t {
public:
  struct Traits {
    const char *library;
    const char *config_key;

    Traits(const char *library, const char *config_key)
      : library(library), config_key(config_key) {
    }
  };

  class Singleton {
  public:
    Singleton(CephContext *cct, const char *library, const char *config_key)
      : tracepoint_provider(new TracepointProvider(cct, library, config_key)) {
    }
    ~Singleton() {
      delete tracepoint_provider;
    }

    inline bool is_enabled() const {
      return tracepoint_provider->m_handle != nullptr;
    }
  private:
    TracepointProvider *tracepoint_provider;
  };

  template <const Traits &traits>
  class TypedSingleton : public Singleton {
  public:
    explicit TypedSingleton(CephContext *cct)
      : Singleton(cct, traits.library, traits.config_key) {
    }
  };

  TracepointProvider(CephContext *cct, const char *library,
                     const char *config_key);
  ~TracepointProvider() override;

  TracepointProvider(const TracepointProvider&) = delete;
  TracepointProvider operator =(const TracepointProvider&) = delete;
  TracepointProvider(TracepointProvider&&) = delete;
  TracepointProvider operator =(TracepointProvider&&) = delete;

  template <const Traits &traits>
  static void initialize(CephContext *cct) {
#ifdef WITH_LTTNG
     cct->lookup_or_create_singleton_object<TypedSingleton<traits>>(
       traits.library, false, cct);
#endif
  }

protected:
  const char** get_tracked_conf_keys() const override {
    return m_config_keys;
  }
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set <std::string> &changed) override;

private:
  CephContext *m_cct;
  std::string m_library;
  mutable const char* m_config_keys[2];

  ceph::mutex m_lock = ceph::make_mutex("TracepointProvider::m_lock");
  void* m_handle = nullptr;

  void verify_config(const ConfigProxy& conf);
};

#endif // CEPH_TRACEPOINT_PROVIDER_H
