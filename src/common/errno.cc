#include "common/errno.h"
#include "include/compat.h"

#include <fmt/core.h>

std::string cpp_strerror(int err)
{
  char buf[128];
  char *errmsg;

  if (err < 0)
    err = -err;

  errmsg = ceph_strerror_r(err, buf, sizeof(buf));

  return fmt::format("({}) {}", err, errmsg);
}
