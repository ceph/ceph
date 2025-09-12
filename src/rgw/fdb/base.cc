// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
      
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 International Business Machines Corp. (IBM)
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
 // JFW: This could all could use some further attention when I (or someone else) have more FDB 
 // experience-- the extant documentation and examples don't make the "right way" very clear; 
 // I think I have it about right. Consolidating this together so that all calls into the database
 // go through the same functions is underway in some of the newer operations, but we need to come
 // in and do the same here:

 // We don't want to try to vivify for an "empty" commit:
 if(!*this) 
  return false;

 future_value fv = fdb_transaction_commit(raw_handle());

 if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {

  future_value ferror_result = fdb_transaction_on_error(raw_handle(), r);

  /* ...not handling this right now, so we will possibly get a second retry from the library;
  I think the usual retry behavior from the library should be ok most of the time for us. See
  docs for fdb_transaction_commit(). The notes for error codes are also pretty lacking, but you can
  find at least SOME more information here:
    https://apple.github.io/foundationdb/api-error-codes.html#developer-guide-error-codes
  if(commit_unknown_result == ferror_result) { [...] } */

  if(0 != fdb_future_block_until_ready(ferror_result.raw_handle())) {
    // Destroy the futures AND be sure to invalidate the transaction-- according to the documentation,
    // this must happen in the specified order (which /should/ currently match the dtor order, but I am
    // not going to touch this any more right now):
    fv.destroy(), ferror_result.destroy(), destroy();

    // In their example, they use the first error as the message source, so I will do that also:
    throw libfdb_exception(r);
  }

  // Do NOT invalidate the transaction: application should RETRY the operation: 
  return false;
 }

 // Ok:
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

 try
  {
    ceph::libfdb::from::convert(std::span<const std::uint8_t>(kv.value, kv.value_length), r.second);
  }
 catch(const std::system_error& e)
  {
    // This is a bit bound to zpp_bits for the moment, but there's not a more direct way to distinguish this
    // from a different system_error. We could do that, by using zpp_bits' non-throwing modes and throwing a
    // special type, but this will do for now.
    throw ceph::libfdb::libfdb_exception(e.what());
  }

 return r;
}

} // namespace ceph::libfdb::detail 

