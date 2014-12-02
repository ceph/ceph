/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
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
 * The 'whereis' function fills a vector storing the IP, OSD- & PG-Seeds for
 * a given object in a given IO context (pool).
 */


namespace librados {
  class IoCtx;

  class RadosWhereis {
  public:
    RadosWhereis(IoCtx &_io) : io(_io) { };
    virtual
    ~RadosWhereis() { };

    int
    whereis(const std::string &oio,
            std::vector<librados::whereis_t> &locations
            );

    static void dump(librados::whereis_t &location, bool reversedns, Formatter* formatter);

  private:
    IoCtx &io;
  };
}
#endif
