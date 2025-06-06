// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_op_type.h"

class RGWGetAccountSummary : public RGWRESTOp {
  void add_entry(const std::string& key, int64_t value);
  public:
  int verify_permission(optional_yield y) override;
  void execute(optional_yield y) override;
  const char* name() const override { return "get_account_summary"; }
  RGWOpType get_type() override { return RGW_OP_GET_ACCOUNT_SUMMARY; }
};
