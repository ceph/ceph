// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once


#include <map>
#include "dmclock/src/dmclock_server.h"

namespace ceph {
  namespace mclock {

    template<typename C>
    class MClockClientInfoMgr {
      mutable std::shared_mutex rw_mtx;
      std::map<C,crimson::dmclock::ClientInfo> map;

      using WriteGuard = std::unique_lock<decltype(rw_mtx)>;
      using ReadGuard = std::shared_lock<decltype(rw_mtx)>;

    public:

      void set(const C& client, uint64_t res, uint64_t wgt, uint64_t lim) {
	WriteGuard g(rw_mtx);
	auto r =
	  map.emplace(std::piecewise_construct,
		      std::forward_as_tuple(client),
		      std::forward_as_tuple(res, wgt, lim));
	if (!r.second) {
	  r.first->second.update(res, wgt, lim);
	}
      }

      inline crimson::dmclock::ClientInfo operator()(const C& client) const {
	return get(client);
      }
      
      const crimson::dmclock::ClientInfo* get(const C& client) const {
	ReadGuard g(rw_mtx);
	return &map.at(client);
      }

      void clear(const C& client) {
	WriteGuard g(rw_mtx);
	auto it = map.find(client);
	if (map.end() != it) {
	  map.erase(it);
	}
      }
    }; // class ClientInfoMgr
  }
}
