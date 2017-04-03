// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TRACEPOINT_PROVIDER_H
#define CEPH_TRACEPOINT_PROVIDER_H

#include "include/int_types.h"
#include "common/ceph_context.h"
#include "common/config_obs.h"
#include "common/Mutex.h"
#include <dlfcn.h>
#include <set>
#include <string>
#include <boost/noncopyable.hpp>

struct md_config_t;

class TracepointProvider : public md_config_obs_t, boost::noncopyable {
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

  template <const Traits &traits>
  static void initialize(CephContext *cct) {
#ifdef WITH_LTTNG
    TypedSingleton<traits> *singleton;
    cct->lookup_or_create_singleton_object(singleton, traits.library);
#endif
  }

protected:
  const char** get_tracked_conf_keys() const override {
    return m_config_keys;
  }
  void handle_conf_change(const struct md_config_t *conf,
                                  const std::set <std::string> &changed) override;

private:
  CephContext *m_cct;
  std::string m_library;
  mutable const char* m_config_keys[2];

  Mutex m_lock;
  void* m_handle = nullptr;

  void verify_config(const struct md_config_t *conf);
};

#endif // CEPH_TRACEPOINT_PROVIDER_H
