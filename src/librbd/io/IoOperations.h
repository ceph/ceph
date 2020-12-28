// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <ostream>

namespace librbd {
namespace io {

  std::string rbd_io_operations_to_string(uint64_t ops,
                                          std::ostream *err);
  uint64_t rbd_io_operations_from_string(const std::string& value,
                                         std::ostream *err);

} // namespace io
} // namespace librbd
