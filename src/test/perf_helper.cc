// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/* Copyright (c) 2011 Facebook
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "include/buffer.h"

using namespace ceph;

namespace PerfHelper {

/// Flush the CPU data cache by reading and writing 100MB of new data.
void flush_cache()
{
    int hundredMegs = 100 * 1024 * 1024;
    volatile char* block = new char[hundredMegs];
    for (int i = 0; i < hundredMegs; i++)
        block[i] = 1;
    delete[] block;
}

/// Used in functionCall().
uint64_t plus_one(uint64_t x)
{
    return x + 1;
}

/// Used in throwIntNL.
void throw_int()
{
    throw 0;
}

/// Used in throwExceptionNL.
void throw_end_of_buffer()
{
    throw buffer::end_of_buffer();
}
}
