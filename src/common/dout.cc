#include "dout.h"
#include "log/Log.h"

#include <iostream>

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

#if !defined(WITH_SEASTAR) || defined(WITH_ALIEN)

void DoutSubmitEntry(ceph::logging::Log &log, ceph::logging::Entry &&e) noexcept
{
    log.submit_entry(std::move(e));
}

#endif
