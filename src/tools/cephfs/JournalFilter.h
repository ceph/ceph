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


#ifndef JOURNAL_FILTER_H
#define JOURNAL_FILTER_H

#include "mds/mdstypes.h"
#include "mds/LogEvent.h"

/**
 * A set of conditions for narrowing down a search through the journal
 */
class JournalFilter
{
  private:

  /* Filtering by journal offset range */
  uint64_t range_start;
  uint64_t range_end;
  static const std::string range_separator;

  /* Filtering by file (sub) path */
  std::string path_expr;

  /* Filtering by inode */
  inodeno_t inode;

  /* Filtering by type */
  LogEvent::EventType event_type;

  /* Filtering by dirfrag */
  dirfrag_t frag;
  std::string frag_dentry;  //< optional, filter dentry name within fragment

  /* Filtering by metablob client name */
  entity_name_t client_name;

  public:
  JournalFilter() : 
    range_start(0),
    range_end(-1),
    inode(0),
    event_type(0) {}

  bool get_range(uint64_t &start, uint64_t &end) const;
  bool apply(uint64_t pos, LogEvent &le) const;
  int parse_args(
    std::vector<const char*> &argv, 
    std::vector<const char*>::iterator &arg);
};

#endif // JOURNAL_FILTER_H

