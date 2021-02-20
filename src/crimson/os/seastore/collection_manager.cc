// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
//
#include "crimson/os/seastore/collection_manager.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"

namespace crimson::os::seastore::collection_manager {

CollectionManagerRef create_coll_manager(TransactionManager &trans_manager) {
  return CollectionManagerRef(new FlatCollectionManager(trans_manager));
}

}
