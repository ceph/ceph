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

#ifndef _INCLUDED_RBD_REPLAY_SER_HPP
#define _INCLUDED_RBD_REPLAY_SER_HPP

#include <iostream>
#include <stdint.h>

namespace rbd_replay {

/**
   Helper for serializing data in an architecture-indepdendent way.
   Everything is written big-endian.
   @see Deser
*/
class Ser {
public:
  Ser(std::ostream &out);

  void write_uint8_t(uint8_t);

  void write_uint16_t(uint16_t);

  void write_uint32_t(uint32_t);

  void write_uint64_t(uint64_t);

  void write_string(const std::string&);

  void write_bool(bool b);

private:
  std::ostream &m_out;
};

}

#endif
