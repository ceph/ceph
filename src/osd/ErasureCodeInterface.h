// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_ERASURE_CODE_INTERFACE_H
#define CEPH_ERASURE_CODE_INTERFACE_H

#include <map>
#include <set>
#include <tr1/memory>
#include "include/buffer.h"

using namespace std;

namespace ceph {


  class ErasureCodeInterface {
  public:
    virtual ~ErasureCodeInterface() {}

    virtual int minimum_to_decode(const set<int> &want_to_read,
				  const set<int> &available_chunks,
				  set<int> *minimum) = 0;

    virtual int minimum_to_decode_with_cost(const set<int> &want_to_read,
					    const map<int, int> &available,
					    set<int> *minimum) = 0;

    virtual int encode(const set<int> &want_to_encode,
		       const bufferlist &in,
		       map<int, bufferlist> *encoded) = 0;

    virtual int decode(const set<int> &want_to_read,
		       const map<int, bufferlist> &chunks,
		       map<int, bufferlist> *decoded) = 0;
  };

  typedef std::tr1::shared_ptr<ErasureCodeInterface> ErasureCodeInterfaceRef;

}

#endif
