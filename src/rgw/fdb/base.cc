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

namespace ceph::libfdb {

database::database()
{
 std::call_once(ceph::libfdb::detail::database_system::fdb_was_initialized, ceph::libfdb::detail::database_system::initialize_fdb);

 if(fdb_error_t r = fdb_create_database(nullptr, &fdb_handle); 0 != r)
  throw fdb_exception(r);

 // may now set database options:
 ; // JFW
}

database::~database()
{
 // destroy specific database:
 if(nullptr != fdb_handle) {
  fdb_database_destroy(fdb_handle), fdb_handle = nullptr;
 }
}

transaction::transaction(database_handle& dbh)
 : dbh(dbh)
{
  if(fdb_error_t r = fdb_database_create_transaction(dbh->raw_handle(), &txn_handle); 0 != r) {
   throw fdb_exception(r);
  }
}

transaction::~transaction()
{
  if(nullptr == txn_handle) {
   return;
  }

/*JFW
  // If no writes have taken place we do not need to commit; if we don't commit, then we are implicitly
  // rolled-back. For now, this isn't tracked, but we could implement optional autocommits, etc.-- but 
  // what would work best IMO would be to offer the user a way of controlling it as they see fit, via
  // policy or runtime binding:
  commit();
*/
  fdb_transaction_destroy(txn_handle);
}

void transaction::commit()
{
 future_value fv = fdb_transaction_commit(raw_handle());

 // JFW: IMPORTANT: TODO: I think to correctly handle this, we're supposed to do the fdb_transaction_on_error() 
 // dance; for now:
 if(fdb_error_t r = fdb_future_block_until_ready(fv.raw_handle()); 0 != r) {
    throw fdb_exception(r);
 }
}

} // namespace ceph::libfdb

