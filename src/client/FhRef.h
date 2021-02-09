// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_FHREF_H
#define CEPH_CLIENT_FHREF_H

#include <boost/intrusive_ptr.hpp>
class Fh;
void intrusive_ptr_add_ref(Fh *fh);
void intrusive_ptr_release(Fh *fh);
typedef boost::intrusive_ptr<Fh> FhRef;
#endif
