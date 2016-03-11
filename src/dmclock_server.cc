// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "dmclock_server.h"


namespace dmc = crimson::dmclock;


std::ostream& dmc::operator<<(std::ostream& out,
			      const dmc::ClientInfo& client) {
  out <<
    "{ r:" << client.reservation <<
    " w:" << client.weight <<
    " l:" << client.limit <<
    " 1/r:" << client.reservation_inv <<
    " 1/w:" << client.weight_inv <<
    " 1/l:" << client.limit_inv <<
    " }";
  return out;
}


std::ostream& dmc::operator<<(std::ostream& out,
			      const dmc::RequestTag& tag) {
  out <<
    "{ r:" << format_time(tag.reservation) <<
    " p:" << format_time(tag.proportion) <<
    " l:" << format_time(tag.limit) << " }";
  return out;
}
