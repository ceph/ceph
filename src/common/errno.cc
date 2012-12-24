#include <sstream>
#include <string.h>

#include "errno.h"

std::string cpp_strerror(int err)
{
  char buf[128];

  if (err < 0)
    err = -err;
  std::ostringstream oss;
  oss << "(" << err << ") " << strerror_r(err, buf, sizeof(buf));

  return oss.str();
}
