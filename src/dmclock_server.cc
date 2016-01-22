// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#include "dmclock_server.h"


namespace dmc = crimson::dmclock;


std::ostream& dmc::operator<<(std::ostream& out,
			      const dmc::ClientInfo& client) {
  out << "{ w:" << client.weight <<
    " r:" << client.reservation <<
    " l:" << client.limit <<
    " 1/w:" << client.weight_inv <<
    " 1/r:" << client.reservation_inv <<
    " 1/l:" << client.limit_inv <<
    " }";
  return out;
}


std::ostream& dmc::operator<<(std::ostream& out,
			      const dmc::RequestTag& tag) {
  out << "{ p:" << formatTime(tag.proportion) <<
    " r:" << formatTime(tag.reservation) <<
    " l:" << formatTime(tag.limit) << " }";
  return out;
}
