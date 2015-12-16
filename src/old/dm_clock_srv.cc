// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include "dm_clock_srv.h"


#if 0
std::ostream& dmc::operator<<(std::ostream& out,
			      const dmc::ClientInfo_old& client) {
  if (client.isUnset()) {
    out << "unset";
  } else {
    out << "{w:" << client.weight << " r:" << client.reservation <<
      " l:" << client.limit << " t:" << client.prevTag << "}";
  }
  return out;
}
#endif


std::ostream& dmc::operator<<(std::ostream& out,
			      const dmc::ClientInfo& client) {
  out << "{w:" << client.weight <<
    " r:" << client.reservation <<
    " l:" << client.limit << "}";
  return out;
}


std::ostream& dmc::operator<<(std::ostream& out, const dmc::RequestTag& tag) {
  out << "{p:" << tag.proportion << " r:" << tag.reservation <<
    " l:" << tag.limit << "}";
  return out;
}
