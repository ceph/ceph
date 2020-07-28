// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_METAREQUESTREF_H
#define CEPH_CLIENT_METAREQUESTREF_H

#include <boost/intrusive_ptr.hpp>
class MetaRequest;
void intrusive_ptr_add_ref(MetaRequest *request);
void intrusive_ptr_release(MetaRequest *request);
typedef boost::intrusive_ptr<MetaRequest> MetaRequestRef;
#endif
