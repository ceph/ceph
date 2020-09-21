#include "zoned_types.h"

using ceph::decode;
using ceph::encode;

std::ostream& operator<<(std::ostream& out,
                         const zone_state_t& zone_state) {
  return out << " zone: 0x" << std::hex
	     << " dead bytes: 0x" << zone_state.get_num_dead_bytes()
	     << " write pointer: 0x"  << zone_state.get_write_pointer()
	     << " " << std::dec;
}

void zone_state_t::encode(ceph::buffer::list &bl) const {
  uint64_t v = static_cast<uint64_t>(num_dead_bytes) << 32 | write_pointer;
  ::encode(v, bl);
}

void zone_state_t::decode(ceph::buffer::list::const_iterator &p) {
  uint64_t v;
  ::decode(v, p);
  num_dead_bytes = v >> 32;
  write_pointer = v;  // discard left-most 32 bits
}
