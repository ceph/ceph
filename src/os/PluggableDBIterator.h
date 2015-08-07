// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 SanDisk
 *
 * Author: Varada Kari<varada.kari@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it
 */

/*
 *  Contains the interfaces what an Iterator should implement in the backendDB
 *  plugin to use Iterator functionality on the backend.
 */
#ifndef PLUGGABLEDB_ITERATOR_H
#define PLUGGABLEDB_ITERATOR_H
#include <string>
using namespace std;

  class PluggableDBIterator {
  public:
    virtual int seek_to_first() = 0;
    virtual int seek_to_first(const string &prefix) = 0;
    virtual int seek_to_last() = 0;
    virtual int seek_to_last(const string &prefix) = 0;
    virtual int upper_bound(const string &prefix) = 0;
    virtual int lower_bound(const string &prefix) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual int prev() = 0;
    virtual string key() = 0;
    virtual string value() = 0;
    virtual int status() = 0;
    virtual ~PluggableDBIterator() { }
  };
#endif
