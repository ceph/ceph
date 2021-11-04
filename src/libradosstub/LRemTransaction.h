// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "LRemCluster.h"

namespace librados {

struct LRemTransactionState {
  LRemCluster::ObjectLocator locator;
  int op_id = 0;
  int flags = 0;
  bool write = false;

  LRemTransactionState() : locator(std::string(), std::string()) {}
  LRemTransactionState(const LRemCluster::ObjectLocator& loc) : locator(loc) {}
  virtual ~LRemTransactionState() {}

  virtual void set_write(bool w) {
    /* can set but not reset */
    write |= w;
  }

  std::string& oid() {
    return locator.name;
  }

  std::string& nspace() {
    return locator.nspace;
  }
};

using LRemTransactionStateRef = std::shared_ptr<LRemTransactionState>;

static inline LRemTransactionStateRef make_op_transaction(const LRemCluster::ObjectLocator& locator) {
  return std::make_shared<LRemTransactionState>(locator);
}

}
