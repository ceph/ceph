// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_DOUT_H
#define CEPH_DOUT_H

#include <type_traits>

#include "include/ceph_assert.h"
#include "include/common_fwd.h"
#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
#include <seastar/util/log.hh>
#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"
#else
#include "global/global_context.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/likely.h"
#include "common/Clock.h"
#include "log/Log.h"
#endif

extern void dout_emergency(const char * const str);
extern void dout_emergency(const std::string &str);

// intentionally conflict with endl
class _bad_endl_use_dendl_t { public: _bad_endl_use_dendl_t(int) {} };
static const _bad_endl_use_dendl_t endl = 0;
inline std::ostream& operator<<(std::ostream& out, _bad_endl_use_dendl_t) {
  ceph_abort_msg("you are using the wrong endl.. use std::endl or dendl");
  return out;
}

class DoutPrefixProvider {
public:
  virtual std::ostream& gen_prefix(std::ostream& out) const = 0;
  virtual CephContext *get_cct() const = 0;
  virtual unsigned get_subsys() const = 0;
  virtual ~DoutPrefixProvider() {}
};

inline std::ostream &operator<<(
  std::ostream &lhs, const DoutPrefixProvider &dpp) {
  return dpp.gen_prefix(lhs);
}
#if FMT_VERSION >= 90000
template <> struct fmt::formatter<DoutPrefixProvider> : fmt::ostream_formatter {};
#endif

// a prefix provider with empty prefix
class NoDoutPrefix : public DoutPrefixProvider {
  CephContext *const cct;
  const unsigned subsys;
 public:
  NoDoutPrefix(CephContext *cct, unsigned subsys) : cct(cct), subsys(subsys) {}

  std::ostream& gen_prefix(std::ostream& out) const override { return out; }
  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override { return subsys; }
};

// a prefix provider with static (const char*) prefix
class DoutPrefix : public NoDoutPrefix {
  const char *const prefix;
 public:
  DoutPrefix(CephContext *cct, unsigned subsys, const char *prefix)
    : NoDoutPrefix(cct, subsys), prefix(prefix) {}

  std::ostream& gen_prefix(std::ostream& out) const override {
    return out << prefix;
  }
};

// a prefix provider that composes itself on top of another
class DoutPrefixPipe : public DoutPrefixProvider {
  const DoutPrefixProvider& dpp;
 public:
  DoutPrefixPipe(const DoutPrefixProvider& dpp) : dpp(dpp) {}

  std::ostream& gen_prefix(std::ostream& out) const override final {
    dpp.gen_prefix(out);
    add_prefix(out);
    return out;
  }
  CephContext *get_cct() const override { return dpp.get_cct(); }
  unsigned get_subsys() const override { return dpp.get_subsys(); }

  virtual void add_prefix(std::ostream& out) const = 0;
};

// helpers
namespace ceph::dout {

template<typename T>
struct dynamic_marker_t {
  T value;
  // constexpr ctor isn't needed as it's an aggregate type
  constexpr operator T() const { return value; }
};

template<typename T>
constexpr dynamic_marker_t<T> need_dynamic(T&& t) {
  return dynamic_marker_t<T>{ std::forward<T>(t) };
}

template<typename T>
struct is_dynamic : public std::false_type {};

template<typename T>
struct is_dynamic<dynamic_marker_t<T>> : public std::true_type {};

} // ceph::dout

// generic macros
#define dout_prefix *_dout

#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
#define dout_impl(cct, sub, v)                                          \
  do {                                                                  \
    if (crimson::common::local_conf()->subsys.should_gather(sub, v)) {  \
      seastar::logger& _logger = crimson::get_logger(sub);              \
      const auto _lv = v;                                               \
      std::ostringstream _out;                                          \
      std::ostream* _dout = &_out;
#define dendl_impl                              \
     "";                                        \
      _logger.log(crimson::to_log_level(_lv),   \
                  "{}", _out.str().c_str());    \
    }                                           \
  } while (0)
#else
#define dout_impl(cct, sub, v)						\
  do {									\
  const bool should_gather = [&](const auto cctX, auto sub_, auto v_) {	\
    /* The check is performed on `sub_` and `v_` to leverage the C++'s 	\
     * guarantee on _discarding_ one of blocks of `if constexpr`, which	\
     * includes also the checks for ill-formed code (`should_gather<>`	\
     * must not be feed with non-const expresions), BUT ONLY within	\
     * a template (thus the generic lambda) and under the restriction	\
     * it's dependant on a parameter of this template).			\
     * GCC prior to v14 was not enforcing these restrictions. */	\
    if constexpr (ceph::dout::is_dynamic<decltype(sub_)>::value ||	\
		  ceph::dout::is_dynamic<decltype(v_)>::value) {	\
      return cctX->_conf->subsys.should_gather(sub, v);			\
    } else {								\
      constexpr auto sub_helper = static_cast<decltype(sub_)>(sub);	\
      constexpr auto v_helper = static_cast<decltype(v_)>(v);		\
      /* The parentheses are **essential** because commas in angle	\
       * brackets are NOT ignored on macro expansion! A language's	\
       * limitation, sorry. */						\
      return (cctX->_conf->subsys.template should_gather<sub_helper,	\
							 v_helper>());	\
    }									\
  }(cct, sub, v);							\
									\
  if (should_gather) {							\
    ceph::logging::MutableEntry _dout_e(v, sub);                        \
    static_assert(std::is_convertible<decltype(&*cct), 			\
				      CephContext* >::value,		\
		  "provided cct must be compatible with CephContext*"); \
    auto _dout_cct = cct;						\
    std::ostream* _dout = &_dout_e.get_ostream();

#define dendl_impl std::flush;                                          \
    _dout_cct->_log->submit_entry(std::move(_dout_e));                  \
  }                                                                     \
  } while (0)
#endif	// WITH_SEASTAR

#define lsubdout(cct, sub, v)  dout_impl(cct, ceph_subsys_##sub, v) dout_prefix
#define ldout(cct, v)  dout_impl(cct, dout_subsys, v) dout_prefix
#define lderr(cct) dout_impl(cct, ceph_subsys_, -1) dout_prefix

#define ldpp_subdout(dpp, sub, v) 						\
  if (decltype(auto) pdpp = (dpp); pdpp) /* workaround -Wnonnull-compare for 'this' */ \
    dout_impl(pdpp->get_cct(), ceph_subsys_##sub, v) \
      pdpp->gen_prefix(*_dout)

#define ldpp_dout(dpp, v) 						\
  if (decltype(auto) pdpp = (dpp); pdpp) /* workaround -Wnonnull-compare for 'this' */ \
    dout_impl(pdpp->get_cct(), ceph::dout::need_dynamic(pdpp->get_subsys()), v) \
      pdpp->gen_prefix(*_dout)

#define lgeneric_subdout(cct, sub, v) dout_impl(cct, ceph_subsys_##sub, v) *_dout
#define lgeneric_dout(cct, v) dout_impl(cct, ceph_subsys_, v) *_dout
#define lgeneric_derr(cct) dout_impl(cct, ceph_subsys_, -1) *_dout

#define ldlog_p1(cct, sub, lvl)                 \
  (cct->_conf->subsys.should_gather((sub), (lvl)))

#define dendl dendl_impl

#endif
