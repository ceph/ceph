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

#include "global/global_context.h"
#include "common/config.h"
#include "common/likely.h"
#include "common/Clock.h"
#include "log/Log.h"

extern void dout_emergency(const char * const str);
extern void dout_emergency(const std::string &str);

// intentionally conflict with endl
class _bad_endl_use_dendl_t { public: _bad_endl_use_dendl_t(int) {} };
static const _bad_endl_use_dendl_t endl = 0;
inline std::ostream& operator<<(std::ostream& out, _bad_endl_use_dendl_t) {
  assert(0 && "you are using the wrong endl.. use std::endl or dendl");
  return out;
}

class DoutPrefixProvider {
public:
  virtual string gen_prefix() const = 0;
  virtual CephContext *get_cct() const = 0;
  virtual unsigned get_subsys() const = 0;
  virtual ~DoutPrefixProvider() {}
};

// helpers
namespace ceph::dout {

template<typename T>
struct dynamic_marker_t {
  T value;
  operator T() const { return value; }
};

template<typename T>
dynamic_marker_t<T> need_dynamic(T&& t) {
  return dynamic_marker_t<T>{ std::forward<T>(t) };
}

template<typename T>
struct is_dynamic : public std::false_type {};

template<typename T>
struct is_dynamic<dynamic_marker_t<T>> : public std::true_type {};

} // ceph::dout

// generic macros
#define dout_prefix *_dout

#define dout_impl(cct, sub, v)						\
  do {									\
  const bool should_gather = [&](const auto cctX) {			\
    if constexpr (ceph::dout::is_dynamic<decltype(sub)>::value ||	\
		  ceph::dout::is_dynamic<decltype(v)>::value) {		\
      return cctX->_conf->subsys.should_gather(sub, v);			\
    } else {								\
      /* The parentheses are **essential** because commas in angle	\
       * brackets are NOT ignored on macro expansion! A language's	\
       * limitation, sorry. */						\
      return (cctX->_conf->subsys.template should_gather<sub, v>());	\
    }									\
  }(cct);								\
									\
  if (should_gather) {							\
    static size_t _log_exp_length = 80; 				\
    ceph::logging::Entry *_dout_e = 					\
      cct->_log->create_entry(v, sub, &_log_exp_length);		\
    static_assert(std::is_convertible<decltype(&*cct), 			\
				      CephContext* >::value,		\
		  "provided cct must be compatible with CephContext*"); \
    auto _dout_cct = cct;						\
    std::ostream* _dout = &_dout_e->get_ostream();

#define lsubdout(cct, sub, v)  dout_impl(cct, ceph_subsys_##sub, v) dout_prefix
#define ldout(cct, v)  dout_impl(cct, dout_subsys, v) dout_prefix
#define lderr(cct) dout_impl(cct, ceph_subsys_, -1) dout_prefix

#define ldpp_dout(dpp, v) 						\
  if (dpp) 								\
    dout_impl(dpp->get_cct(), ceph::dout::need_dynamic(dpp->get_subsys()), v)				\
    (*_dout << dpp->gen_prefix())

#define lgeneric_subdout(cct, sub, v) dout_impl(cct, ceph_subsys_##sub, v) *_dout
#define lgeneric_dout(cct, v) dout_impl(cct, ceph_subsys_, v) *_dout
#define lgeneric_derr(cct) dout_impl(cct, ceph_subsys_, -1) *_dout

#define ldlog_p1(cct, sub, lvl)                 \
  (cct->_conf->subsys.should_gather((sub), (lvl)))

// NOTE: depend on magic value in _ASSERT_H so that we detect when
// /usr/include/assert.h clobbers our fancier version.
#define dendl_impl std::flush;				\
  _ASSERT_H->_log->submit_entry(_dout_e);		\
    }						\
  } while (0)

#define dendl dendl_impl

#endif
