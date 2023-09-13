#include "ceph_releases.h"

#include <ostream>

#include "ceph_ver.h"

std::ostream& operator<<(std::ostream& os, const ceph_release_t r)
{
  return os << ceph_release_name(static_cast<int>(r));
}

ceph_release_t ceph_release()
{
  return ceph_release_t{CEPH_RELEASE};
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
