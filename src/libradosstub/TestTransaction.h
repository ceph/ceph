// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

namespace librados {

struct TestTransactionState {
  int op_id = 0;
  int flags = 0;
};

using TestTransactionStateRef = std::shared_ptr<TestTransactionState>;

static inline TestTransactionStateRef make_op_transaction() {
  return std::make_shared<TestTransactionState>();
}

}
