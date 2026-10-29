#include "ceph_releases.h"

#include <ostream>

#include "ceph_ver.h"
#include "include/ceph_features.h"

std::ostream& operator<<(std::ostream& os, const ceph_release_t r)
{
  return os << ceph_release_name(static_cast<int>(r));
}

ceph_release_t ceph_release()
{
  return ceph_release_t{CEPH_RELEASE};
}

uint64_t ceph_release_server_feature(ceph_release_t r)
{
  switch (r) {
  case ceph_release_t::kraken:
    return CEPH_FEATUREMASK_SERVER_KRAKEN;
  case ceph_release_t::luminous:
    return CEPH_FEATUREMASK_SERVER_LUMINOUS;
  case ceph_release_t::mimic:
    return CEPH_FEATUREMASK_SERVER_MIMIC;
  case ceph_release_t::nautilus:
    return CEPH_FEATUREMASK_SERVER_NAUTILUS;
  case ceph_release_t::octopus:
    return CEPH_FEATUREMASK_SERVER_OCTOPUS;
  case ceph_release_t::pacific:
    return CEPH_FEATUREMASK_SERVER_PACIFIC;
  case ceph_release_t::quincy:
    return CEPH_FEATUREMASK_SERVER_QUINCY;
  case ceph_release_t::reef:
    return CEPH_FEATUREMASK_SERVER_REEF;
  case ceph_release_t::squid:
    return CEPH_FEATUREMASK_SERVER_SQUID;
  case ceph_release_t::tentacle:
    return CEPH_FEATUREMASK_SERVER_TENTACLE;
  case ceph_release_t::umbrella:
    return CEPH_FEATUREMASK_SERVER_UMBRELLA;
  case ceph_release_t::vampire:
    return CEPH_FEATUREMASK_SERVER_VAMPIRE;
  default:
    return 0;
  }
}

ceph_release_t ceph_release_from_name(std::string_view s)
{
  ceph_release_t r = ceph_release_t::max;
  while (--r != ceph_release_t::unknown) {
    if (s == to_string(r)) {
      return r;
    }
  }
  return ceph_release_t::unknown;
}

bool can_upgrade_from(ceph_release_t from_release,
                      std::string_view from_release_name,
                      std::ostream& err)
{
  if (from_release == ceph_release_t::unknown) {
    // cannot tell, but i am optimistic
    return true;
  }
  const ceph_release_t cutoff{static_cast<uint8_t>(static_cast<uint8_t>(from_release) + 2)};
  const auto to_release = ceph_release();
  if (cutoff < to_release) {
    err << "recorded " << from_release_name << " "
        << to_integer<int>(from_release) << " (" << from_release << ") "
        << "is more than two releases older than installed "
        << to_integer<int>(to_release) << " (" << to_release << "); "
        << "you can only upgrade 2 releases at a time\n"
        << "you should first upgrade to ";
    auto release = from_release;
    while (++release <= cutoff) {
      err << to_integer<int>(release) << " (" << release << ")";
      if (release < cutoff) {
        err << " or ";
      } else {
        err << "\n";
      }
    }
    return false;
  } else {
    return true;
  }
}
