// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include "boost/variant.hpp"


namespace crimson {
  namespace priority_queue {
    enum class Mechanism { push, pull };

    // when we try to get the next request, we'll be in one of three
    // situations -- we'll have one to return, have one that can
    // fire in the future, or not have any

    template<typename Cli, typename Req, typename Time, typename AddInfo>
    struct PullReq {
      enum class Type { returning, future, none };

      struct Retn {
	Cli     client;
	Req     request;
	AddInfo add_info;
      };

      Type                      type;
      boost::variant<Retn,Time> data;
    };
  }; // namespace priority_queue
}; // namespace crimson
