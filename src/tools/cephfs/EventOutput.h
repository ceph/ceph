// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * ceph - scalable distributed file system
 *
 * copyright (c) 2014 john spray <john.spray@inktank.com>
 *
 * this is free software; you can redistribute it and/or
 * modify it under the terms of the gnu lesser general public
 * license version 2.1, as published by the free software
 * foundation.  see file copying.
 */


#ifndef EVENT_OUTPUT_H
#define EVENT_OUTPUT_H

#include <string>

class JournalScanner;

/**
 * Different output formats for the results of a journal scan
 */
class EventOutput
{
  private:
    JournalScanner const &scan;
    std::string const path;

  public:
    EventOutput(JournalScanner const &scan_, std::string const &path_)
      : scan(scan_), path(path_) {}

    void summary() const;
    void list() const;
    int json() const;
    int binary() const;
};

#endif // EVENT_OUTPUT_H

