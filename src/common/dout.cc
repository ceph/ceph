#include "dout.h"

#include <iostream>
#include <sstream>

void dout_emergency(const char * const str)
{
  std::cerr << str;
  std::cerr.flush();
}

void dout_emergency(const std::string &str)
{
  std::cerr << str;
  std::cerr.flush();
}

#if FMT_VERSION >= 90000
fmt::format_context::iterator
fmt::formatter<DoutPrefixProvider>::format(const DoutPrefixProvider &dpp,
                                           fmt::format_context &ctx) const
{
  std::ostringstream out;
  dpp.gen_prefix(out);
  return fmt::formatter<std::string_view>::format(out.view(), ctx);
}
#endif
