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
    // NOTE: Change name to RespParams? Is it used elsewhere?
    struct NullData {
      friend std::ostream& operator<<(std::ostream& out, const NullData& n) {
	out << "NullData{ EMPTY }";
	return out;
      }
    }; // struct NullData


    struct ReqParams {
      friend std::ostream& operator<<(std::ostream& out, const ReqParams& rp) {
	out << "ReqParams{ EMPTY }";
	return out;
      }
    };

  }
}
