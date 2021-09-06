// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "TestCluster.h"

namespace librados {

struct TestTransactionState {
  TestCluster::ObjectLocator locator;
  int op_id = 0;
  int flags = 0;
  bool write = false;

  TestTransactionState(const TestCluster::ObjectLocator& loc) : locator(loc) {}

  void set_write(bool w) {
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

using TestTransactionStateRef = std::shared_ptr<TestTransactionState>;

static inline TestTransactionStateRef make_op_transaction(const TestCluster::ObjectLocator& locator) {
  return std::make_shared<TestTransactionState>(locator);
}

}
