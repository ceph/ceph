// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_md5.h"

#include "common/ceph_crypto.h"

#include "rgw_putobj_hash.h"

namespace rgw {

class MD5WithFIPS : public ceph::crypto::ssl::MD5 {
 public:
  MD5WithFIPS() {
    // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
    SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  }
};

auto create_md5_putobj_pipe(sal::DataProcessor* next,
                            std::string& output)
    -> std::unique_ptr<sal::DataProcessor>
{
  using Pipe = putobj::HashPipe<MD5WithFIPS>;
  return std::make_unique<Pipe>(next, output);
}

} // namespace rgw
