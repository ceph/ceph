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

#ifndef JOURNAL_SCANNER_H
#define JOURNAL_SCANNER_H

#include "include/rados/librados_fwd.hpp"

// For Journaler::Header, can't forward-declare nested classes
#include <osdc/Journaler.h>

#include "JournalFilter.h"

/**
 * A simple sequential reader for metadata journals.  Unlike
 * the MDS Journaler class, this is written to detect, record,
 * and read past corruptions and missing objects.  It is also
 * less efficient but more plainly written.
 */
class JournalScanner
{
  private:
  librados::IoCtx &io;

  // Input constraints
  const int rank;
  std::string type;
  JournalFilter const filter;

  void gap_advance();

  public:
  JournalScanner(
      librados::IoCtx &io_,
      int rank_,
      const std::string &type_,
      JournalFilter const &filter_) :
    io(io_),
    rank(rank_),
    type(type_),
    filter(filter_),
    is_mdlog(false),
    pointer_present(false),
    pointer_valid(false),
    header_present(false),
    header_valid(false),
    header(NULL) {};

  JournalScanner(
      librados::IoCtx &io_,
      int rank_,
      const std::string &type_) :
    io(io_),
    rank(rank_),
    type(type_),
    filter(type_),
    is_mdlog(false),
    pointer_present(false),
    pointer_valid(false),
    header_present(false),
    header_valid(false),
    header(NULL) {};

  ~JournalScanner();

  int set_journal_ino();
  int scan(bool const full=true);
  int scan_pointer();
  int scan_header();
  int scan_events();
  void report(std::ostream &out) const;

  std::string obj_name(uint64_t offset) const;
  std::string obj_name(inodeno_t ino, uint64_t offset) const;

  // The results of the scan
  inodeno_t ino;  // Corresponds to journal ino according their type
  struct EventRecord {
    EventRecord(std::unique_ptr<LogEvent> le, uint32_t rs) : log_event(std::move(le)), raw_size(rs) {}
    EventRecord(std::unique_ptr<PurgeItem> p, uint32_t rs) : pi(std::move(p)), raw_size(rs) {}
    std::unique_ptr<LogEvent> log_event;
    std::unique_ptr<PurgeItem> pi;
    uint32_t raw_size = 0;  //< Size from start offset including all encoding overhead
  };

  class EventError {
    public:
    int r;
    std::string description;
    EventError(int r_, const std::string &desc_)
      : r(r_), description(desc_) {}
  };

  typedef std::map<uint64_t, EventRecord> EventMap;
  typedef std::map<uint64_t, EventError> ErrorMap;
  typedef std::pair<uint64_t, uint64_t> Range;
  bool is_mdlog;
  bool pointer_present; //mdlog specific
  bool pointer_valid;   //mdlog specific
  bool header_present;
  bool header_valid;
  Journaler::Header *header;

  bool is_healthy() const;
  bool is_readable() const;
  std::vector<std::string> objects_valid;
  std::vector<uint64_t> objects_missing;
  std::vector<Range> ranges_invalid;
  std::vector<uint64_t> events_valid;
  EventMap events;

  // For events present in ::events (i.e. scanned successfully),
  // any subsequent errors handling them (e.g. replaying)
  ErrorMap errors;


  private:
  // Forbid copy construction because I have ptr members
  JournalScanner(const JournalScanner &rhs);
};

#endif // JOURNAL_SCANNER_H

