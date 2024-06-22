// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <stdint.h>
#include <stdio.h>
#include "BLAKE3/c/blake3.h"

namespace rgw { namespace digest {

class Blake3 {
  private:
    blake3_hasher h;

  public:
    static constexpr uint16_t digest_size = BLAKE3_OUT_LEN /* 32 bytes */;

    Blake3() { Restart(); }

    void Restart() { blake3_hasher_init(&h); }

    void Update(const unsigned char *data, uint64_t len) {
	blake3_hasher_update(&h, data, len);
    }

    void Final(unsigned char* digest) {
	blake3_hasher_finalize(&h, digest, digest_size);
    }
}; /* Blake3 */

}} /* namespace */
