#include "SMRZone.h"

SMRZone::SMRZone(uint64_t num, uint64_t size, zone_type zt, zone_state zs)
		: m_zone_number(num),
		m_zone_size(size),
		m_zone_write_pointer(0),
		m_remaining_bytes(size),
		m_zone_type(zt),
		m_zone_state(zs) {}
