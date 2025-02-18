//
// Created by root on 1/9/25.
//

#pragma once

#include <string>
#include "common/hobject.h"
#include "osd_types.h"

namespace ECInject {

  // Error inject interfaces
  std::string read_error(const ghobject_t& o, const int64_t type, const int64_t when, const int64_t duration);
  std::string write_error(const ghobject_t& o, const int64_t type, const int64_t when, const int64_t duration);
  std::string clear_read_error(const ghobject_t& o, const int64_t type);
  std::string clear_write_error(const ghobject_t& o, const int64_t type);
  bool test_read_error0(const ghobject_t& o);
  bool test_read_error1(const ghobject_t& o);
  bool test_write_error0(const hobject_t& o,const osd_reqid_t& reqid);
  bool test_write_error1(const ghobject_t& o);
  bool test_write_error2(const hobject_t& o);
  bool test_write_error3(const hobject_t& o);

} // ECInject
