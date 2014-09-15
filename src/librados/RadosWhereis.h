// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_LIBRADOS_RADOSWHEREIS_H
#define CEPH_LIBRADOS_RADOSWHEREIS_H

#include <stdio.h>

/**
 * @file RadosWhereis.h
 *
 * @brief Class providing a function to retrieve a vector of locations
 *
 * The 'locate' function fills a vector storing the IP, OSD- & PG-Seeds for
 * a given object in a given IO context (pool).
 */


namespace librados {
class IoCtx;
class RadosWhereis {
public:

  RadosWhereis(IoCtx &_io) : io(_io)
  {
  };

  virtual
  ~RadosWhereis()
  {
  };

  bool
  whereis(const std::string &oio,
         whereis_vector_t &locations
         );

private:
  IoCtx &io;
};
}
#endif
