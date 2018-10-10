// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_putobj_processor.h"

namespace rgw::putobj {

int HeadObjectProcessor::process(bufferlist&& data, uint64_t logical_offset)
{
  const bool flush = (data.length() == 0);

  // capture the first chunk for special handling
  if (data_offset < head_chunk_size) {
    if (flush) {
      // flush partial chunk
      return process_first_chunk(std::move(head_data), &processor);
    }

    auto remaining = head_chunk_size - data_offset;
    auto count = std::min<uint64_t>(data.length(), remaining);
    data.splice(0, count, &head_data);
    data_offset += count;

    if (data_offset == head_chunk_size) {
      // process the first complete chunk
      ceph_assert(head_data.length() == head_chunk_size);
      int r = process_first_chunk(std::move(head_data), &processor);
      if (r < 0) {
        return r;
      }
    }
    if (data.length() == 0) { // avoid flushing stripe processor
      return 0;
    }
  }
  ceph_assert(processor); // process_first_chunk() must initialize

  // send everything else through the processor
  auto write_offset = data_offset;
  data_offset += data.length();
  return processor->process(std::move(data), write_offset);
}

} // namespace rgw::putobj
