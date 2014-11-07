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

#ifndef _INCLUDED_RBD_REPLAY_DESER_HPP
#define _INCLUDED_RBD_REPLAY_DESER_HPP

#include <iostream>
#include <stdint.h>

namespace rbd_replay {

/**
   Helper for deserializing data in an architecture-indepdendent way.
   Everything is read big-endian.
   @see Ser
*/
class Deser {
public:
  Deser(std::istream &in);

  uint8_t read_uint8_t();

  uint16_t read_uint16_t();

  uint32_t read_uint32_t();

  uint64_t read_uint64_t();

  std::string read_string();

  bool read_bool();

  bool eof();

private:
  std::istream &m_in;
};

}

#endif
