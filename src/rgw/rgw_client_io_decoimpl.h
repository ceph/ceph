// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_CLIENT_IO_DECOIMPL_H
#define CEPH_RGW_CLIENT_IO_DECOIMPL_H

#include <type_traits>

#include "rgw_common.h"
#include "rgw_client_io.h"

/* Abstract decorator over any implementation of RGWStreamIOEngine. */
template <typename DecorateeT>
class RGWDecoratedStreamIO : public RGWStreamIOEngine {
  template<typename T> friend class RGWDecoratedStreamIO;

  typedef typename std::remove_pointer<DecorateeT>::type DerefedDecorateeT;

  static_assert(std::is_base_of<RGWStreamIOEngine, DerefedDecorateeT>::value,
                "DecorateeT must be a subclass of RGWStreamIOEngine");

  DecorateeT decoratee;

  /* There is an indirection layer over accessing decoratee to share the same
   * code base between dynamic and static decorators. The difference is about
   * what we store internally: pointer to a decorated object versus the whole
   * object itself. */
  template <typename T = void,
            typename std::enable_if<
    std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  DerefedDecorateeT& get_decoratee() {
    return *decoratee;
  }

  template <typename T = void,
            typename std::enable_if<
    ! std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  DerefedDecorateeT& get_decoratee() {
    return decoratee;
  }

protected:
  void init_env(CephContext *cct) override {
    return get_decoratee().init_env(cct);
  }

  int read_data(char* const buf, const int max) override {
    return get_decoratee().read_data(buf, max);
  }

  int write_data(const char* const buf, const int len) override {
    return get_decoratee().write_data(buf, len);
  }

public:
  RGWDecoratedStreamIO(const DecorateeT& decoratee)
    : decoratee(decoratee) {
  }

  int send_status(const int status, const char* const status_name) override {
    return get_decoratee().send_status(status, status_name);
  }

  int send_100_continue() override {
    return get_decoratee().send_100_continue();
  }

  int send_content_length(const uint64_t len) override {
    return get_decoratee().send_content_length(len);
  }

  int complete_header() override {
    return get_decoratee().complete_header();
  }

  void flush() override {
    return get_decoratee().flush();
  }

  RGWEnv& get_env() override {
    return get_decoratee().get_env();
  }

  int complete_request() override {
    return get_decoratee().complete_request();
  }
};

#endif /* CEPH_RGW_CLIENT_IO_DECOIMPL_H */
