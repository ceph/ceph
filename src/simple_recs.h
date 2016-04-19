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
    };
    
    // S is server id type
    template<typename S>
    struct RespParams {
      S         server;

      RespParams(const S& _server) :
	server(_server)
      {
	// empty
      }

      RespParams(const S& _server, NullData ignore) :
	server(_server)
      {
	// empty
      }


      RespParams(const RespParams& other) :
	server(other.server)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream& out, const RespParams& rp) {
	out << "RespParams{ server:" << rp.server << "}";
	return out;
      }
    };
  }
}
