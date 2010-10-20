#include "common/errno.h"

#include <sstream>
#include <string>
#include <string.h>

std::string cpp_strerror(int err)
{
  char buf[128];

  std::ostringstream oss;
  oss << "error " << err << ": " << strerror_r(err, buf, sizeof(buf));

  return oss.str();
}
