// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/transaction_interruptor.h"

#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

bool TransactionConflictCondition::is_conflicted() const
{
  return t.conflicted;
}

}
