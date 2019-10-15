// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/ceph_assert.h"
#include "include/str_list.h"

#include "rgw/rgw_cksum.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw::cksum;
  using std::get;
  using std::string;

  CksumType t1 = CksumType::blake2bp;
  CksumType t2 = CksumType::sha256;

  std::string dolor =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod "
    "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim "
    "veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea "
    "commodo consequat. Duis aute irure dolor in reprehenderit in voluptate "
    "velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint "
    "occaecat cupidatat non proident, sunt in culpa qui officia deserunt "
    "mollit anim id est laborum.";
  
}

int main(int argc, char **argv)
{
  ceph::crypto::SHA512 hasher;
  sha512_digest_t hash;
  hasher.Update((const unsigned char *)dolor.c_str(), dolor.length());
  hasher.Final(hash.v);
  
  for (auto t : {t1, t2}) {
    DigestVariant dv = rgw::cksum::digest_factory(t);
    Digest* digest = get_digest(dv);

    if (digest) {
      digest->Update((const unsigned char *)dolor.c_str(), dolor.length());
    }

    if (digest) {
      buffer::list cksum_bl;
      auto cksum = rgw::cksum::finalize_digest(digest, t);
      std::cout << "type: " << to_string(t)
		<< "digest: " << cksum.to_string()
		<< std::endl;
    }
  }

  return 0;
}
