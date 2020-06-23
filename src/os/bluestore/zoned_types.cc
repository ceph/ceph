#include "zoned_types.h"

std::ostream& operator<<(std::ostream& out,
                         const zone_state_t& zone_state) {
  return out << " zone: 0x" << std::hex << zone_state.get_zone_num()
	     << " dead bytes: 0x" << zone_state.get_num_dead_bytes()
	     << " write pointer: 0x"  << zone_state.get_write_pointer()
	     << " " << std::dec;
}
