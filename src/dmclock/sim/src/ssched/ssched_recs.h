// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
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
