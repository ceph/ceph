// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <ostream>
#include <assert.h>


namespace crimson {
  namespace simple_scheduler {

    // since we send no additional data out
    struct NullData {
      // intentionally empty
    };

    inline std::ostream& operator<<(std::ostream& out, const NullData& n) {
      out << "NullData{intentionally empty}";
      return out;
    }

    struct ReqParams {
      friend std::ostream& operator<<(std::ostream& out, const ReqParams& rp) {
	out << "ReqParams{ EMPTY }";
	return out;
      }
    };

#if 0
    template<typename C>
    struct ReqParams {
      C        client;

      ReqParams(const C& _client) :
	client(_client)
      {
	// empty
      }

      ReqParams(const ReqParams& other) :
	client(other.client)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream& out, const ReqParams& rp) {
	out << "ReqParams{ client:" << rp.client << "}";
	return out;
      }
    }; // struct ReqParams
#endif
  }
}
