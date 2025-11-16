// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
      
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include "base.h"
#include "conversion.h"

namespace ceph::libfdb {

[[nodiscard]] bool transaction::commit()
{
 // JFW: IMPORTANT: TODO: I think to correctly handle this, we're supposed to do the fdb_transaction_on_error() 
 // dance; there are some special rules around commits and retries in particular, see fdb_transaction_commit()'s
 // and especially fdb_transaction_on_error()'s documentation. I *think* I got it about right, but this is something
 // that certainly could use some further attention when I (or someone else) have more FDB experience-- the extant
 // documentation and examples don't make the "right way" very clear:

 // We don't want to try to vivify for an "empty" commit:
 if(!*this) 
  return false;

 future_value fv = fdb_transaction_commit(raw_handle());

 if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {

  future_value ferror_result = fdb_transaction_on_error(raw_handle(), r);

  /* ...not handling this right now, so we will possibly get a second retry from the library;
  I think the usual retry behavior from the library should be ok most of the time for us. See
  docs for fdb_transaction_commit(). The notes for error codes are also pretty lacking (why even
  have them, then??):
    https://apple.github.io/foundationdb/api-error-codes.html#developer-guide-error-codes
  if(commit_unknown_result == ferror_result) { [...] } */

  if(0 != fdb_future_block_until_ready(ferror_result.raw_handle())) {
    // Destroy the futures AND be sure to invalidate the transaction-- according to the documentation,
    // this must happen in the specified order, which I think will match the ctor order, but for now
    // I've spelled it out):
    fv.destroy(), ferror_result.destroy(), destroy();

    // In their example, they use the first error as the message source, so I will do that also:
    throw libfdb_exception(r);
  }

  // Do NOT invalidate the transaction: application should RETRY the operation: 
  return false;
 }

 // Ok:
 // Note: the internal handle will still be valid, but (according to FDB's rules) will be in an usuable
 // state.
 return true;
}

} // namespace ceph::libfdb 

namespace ceph::libfdb::detail {

// JFW: challenge: how to access ceph::libfdb::from at the right time w/o this being in here? Need to
// delay name resolution:
std::pair<std::string, std::string> to_decoded_kv_pair(const FDBKeyValue kv)
{
 std::pair<std::string, std::string> r;

 r.first.assign((const char *)kv.key, static_cast<std::string::size_type>(kv.key_length));

 ceph::libfdb::from::convert(std::span<const std::uint8_t>(kv.value, kv.value_length), r.second);

 return r;
}

} // namespace ceph::libfdb::detail 

