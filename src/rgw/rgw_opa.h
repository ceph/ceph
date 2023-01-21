// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_common.h"
#include "rgw_op.h"

/* authorize request using OPA */
int rgw_opa_authorize(RGWOp*& op,
                      req_state* s);
