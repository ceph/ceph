#include <iomanip>

#include "ethernet.h"

std::ostream& operator<<(std::ostream& os, ethernet_address ea) {
  auto& m = ea.mac;
  using u = uint32_t;
  os << std::hex << std::setw(2)
     << u(m[0]) << ":"
     << u(m[1]) << ":"
     << u(m[2]) << ":"
     << u(m[3]) << ":"
     << u(m[4]) << ":"
     << u(m[5]);
  return os;
}
