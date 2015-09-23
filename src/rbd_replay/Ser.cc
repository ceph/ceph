// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "acconfig.h"
#include "Ser.hpp"
#include <arpa/inet.h>
#include <cstdlib>
#if defined(DARWIN) || defined(__FreeBSD__)
#include <machine/endian.h>
#else
#include <endian.h>
#endif

rbd_replay::Ser::Ser(std::ostream &out)
  : m_out(out) {
}

void rbd_replay::Ser::write_uint8_t(uint8_t data) {
  m_out.write(reinterpret_cast<char*>(&data), sizeof(data));
}

void rbd_replay::Ser::write_uint16_t(uint16_t data) {
  data = htons(data);
  m_out.write(reinterpret_cast<char*>(&data), sizeof(data));
}

void rbd_replay::Ser::write_uint32_t(uint32_t data) {
  data = htonl(data);
  m_out.write(reinterpret_cast<char*>(&data), sizeof(data));
}

void rbd_replay::Ser::write_uint64_t(uint64_t data) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
  data = (static_cast<uint64_t>(htonl(data)) << 32 | htonl(data >> 32));
#endif
  m_out.write(reinterpret_cast<char*>(&data), sizeof(data));
}

void rbd_replay::Ser::write_string(const std::string& data) {
  write_uint32_t(data.length());
  m_out.write(data.data(), data.length());
}

void rbd_replay::Ser::write_bool(bool data) {
  write_uint8_t(data ? 1 : 0);
}
