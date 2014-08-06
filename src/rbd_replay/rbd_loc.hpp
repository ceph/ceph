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

#ifndef _INCLUDED_RBD_REPLAY_RBD_LOC_HPP
#define _INCLUDED_RBD_REPLAY_RBD_LOC_HPP

#include <string>

namespace rbd_replay {

struct rbd_loc {
  rbd_loc();

  rbd_loc(std::string pool, std::string image, std::string snap);

  bool parse(std::string name_string);

  std::string str() const;

  int compare(const rbd_loc& rhs) const;

  bool operator==(const rbd_loc& rhs) const;

  bool operator<(const rbd_loc& rhs) const;

  std::string pool;

  std::string image;

  std::string snap;
};

}

#endif
