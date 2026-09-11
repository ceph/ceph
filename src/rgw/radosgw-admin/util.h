// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <iostream>
#include <string>
#include <string_view>

#include "common/errno.h"

namespace rgw_admin {
inline int report_error(std::string_view what, int ret,
                        std::string_view err_msg = {})
{
  std::cerr << "ERROR: " << what << " with " << cpp_strerror(-ret);
  if (!err_msg.empty()) {
    std::cerr << ": " << err_msg;
  }
  std::cerr << std::endl;
  return -ret;
}
} // namespace rgw_admin