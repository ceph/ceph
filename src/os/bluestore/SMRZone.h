// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_SMRZONE_H
#define CEPH_OS_BLUESTORE_SMRZONE_H

#include <string>
#include <map>
#include <mutex>
#include <ostream>


typedef enum zone_type { CONVENTIONAL, SEQ_WRITE_PREFERRED, SEQ_WRITE_ONLY } zone_type;
typedef enum zone_state { EMPTY, IMPLICIT_OPEN, EXPLICIT_OPEN , FULL, OFFLINE, READ_ONLY , CLOSED }
	    zone_state;

class SMRZone {
public:
  SMRZone() {}
  SMRZone(uint64_t num, uint64_t zone_size, zone_type zt, zone_state zs);

  ~SMRZone() {}

private:
  uint64_t m_zone_number;
  uint64_t m_zone_size;
  uint64_t m_zone_write_pointer;
  uint64_t m_remaining_bytes;	// deleted but not cleaned
  enum zone_type m_zone_type;
  enum zone_state m_zone_state;
}__attribute__((packed));


#endif
