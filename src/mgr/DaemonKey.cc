#include "DaemonKey.h"

std::pair<DaemonKey, bool> DaemonKey::parse(const std::string& s)
{
  auto p = s.find('.');
  if (p == s.npos) {
    return {{}, false};
  } else {
    return {DaemonKey{s.substr(0, p), s.substr(p + 1)}, true};
  }
}

bool operator<(const DaemonKey& lhs, const DaemonKey& rhs)
{
  if (int cmp = lhs.type.compare(rhs.type); cmp < 0) {
    return true;
  } else if (cmp > 0) {
    return false;
  } else {
    return lhs.name < rhs.name;
  }
}

std::ostream& operator<<(std::ostream& os, const DaemonKey& key)
{
  return os << key.type << '.' << key.name;
}

namespace ceph {
std::string to_string(const DaemonKey& key)
{
  return key.type + '.' + key.name;
}
}

