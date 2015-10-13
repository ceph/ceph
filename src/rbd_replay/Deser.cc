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
#include "Deser.hpp"
#include <arpa/inet.h>
#include <cstdlib>
#if defined(DARWIN) || defined(__FreeBSD__)
#include <machine/endian.h>
#else
#include <endian.h>
#endif

rbd_replay::Deser::Deser(std::istream &in)
  : m_in(in) {
}

uint8_t rbd_replay::Deser::read_uint8_t() {
  uint8_t data;
  m_in.read(reinterpret_cast<char*>(&data), sizeof(data));
  return data;
}

uint16_t rbd_replay::Deser::read_uint16_t() {
  uint16_t data;
  m_in.read(reinterpret_cast<char*>(&data), sizeof(data));
  return ntohs(data);
}

uint32_t rbd_replay::Deser::read_uint32_t() {
  uint32_t data;
  m_in.read(reinterpret_cast<char*>(&data), sizeof(data));
  return ntohl(data);
}

uint64_t rbd_replay::Deser::read_uint64_t() {
  uint64_t data;
  m_in.read(reinterpret_cast<char*>(&data), sizeof(data));
#if __BYTE_ORDER == __LITTLE_ENDIAN
  data = (static_cast<uint64_t>(ntohl(data)) << 32 | ntohl(data >> 32));
#endif
  return data;
}

std::string rbd_replay::Deser::read_string() {
  uint32_t length = read_uint32_t();
  char* data = reinterpret_cast<char*>(malloc(length));
  m_in.read(data, length);
  std::string s(data, length);
  free(data);
  return s;
}

bool rbd_replay::Deser::read_bool() {
  return read_uint8_t() != 0;
}

bool rbd_replay::Deser::eof() {
  return m_in.eof();
}
