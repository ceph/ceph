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


#include <iostream>
#include <fstream>

#include "common/errno.h"
#include "mds/mdstypes.h"
#include "mds/events/EUpdate.h"
#include "mds/LogEvent.h"
#include "JournalScanner.h"

#include "EventOutput.h"


int EventOutput::binary() const
{
  // Binary output, files
  int r = ::mkdir(path.c_str(), 0755);
  if (r != 0) {
    r = -errno;
    if (r != -EEXIST) {
      std::cerr << "Error creating output directory: " << cpp_strerror(r) << std::endl;
      return r;
    }
  }

  for (JournalScanner::EventMap::const_iterator i = scan.events.begin(); i != scan.events.end(); ++i) {
    LogEvent *le = i->second.log_event;
    bufferlist le_bin;
    le->encode(le_bin, CEPH_FEATURES_SUPPORTED_DEFAULT);

    std::stringstream filename;
    filename << "0x" << std::hex << i->first << std::dec << "_" << le->get_type_str() << ".bin";
    std::string const file_path = path + std::string("/") + filename.str();
    std::ofstream bin_file(file_path.c_str(), std::ofstream::out | std::ofstream::binary);
    le_bin.write_stream(bin_file);
    bin_file.close();
    if (bin_file.fail()) {
      return -EIO;
    }
  }
  std::cerr << "Wrote output to binary files in directory '" << path << "'" << std::endl;

  return 0;
}

int EventOutput::json() const
{
  JSONFormatter jf(true);
  std::ofstream out_file(path.c_str(), std::ofstream::out);
  jf.open_array_section("journal");
  {
    for (JournalScanner::EventMap::const_iterator i = scan.events.begin(); i != scan.events.end(); ++i) {
      LogEvent *le = i->second.log_event;
      jf.open_object_section("log_event");
      {
        le->dump(&jf);
      }
      jf.close_section();  // log_event
    }
  }
  jf.close_section();  // journal
  jf.flush(out_file);
  out_file.close();

  if (out_file.fail()) {
    return -EIO;
  } else {
    std::cerr << "Wrote output to JSON file '" << path << "'" << std::endl;
    return 0;
  }
}

void EventOutput::list() const
{
  for (JournalScanner::EventMap::const_iterator i = scan.events.begin(); i != scan.events.end(); ++i) {
    std::vector<std::string> ev_paths;
    EMetaBlob const *emb = i->second.log_event->get_metablob();
    if (emb) {
      emb->get_paths(ev_paths);
    }

    std::string detail;
    if (i->second.log_event->get_type() == EVENT_UPDATE) {
      EUpdate *eu = reinterpret_cast<EUpdate*>(i->second.log_event);
      detail = eu->type;
    }

    std::cout << "0x"
      << std::hex << i->first << std::dec << " "
      << i->second.log_event->get_type_str() << ": "
      << " (" << detail << ")" << std::endl;
    for (std::vector<std::string>::iterator i = ev_paths.begin(); i != ev_paths.end(); ++i) {
        std::cout << "  " << *i << std::endl;
    }
  }
}

void EventOutput::summary() const
{
  std::map<std::string, int> type_count;
  for (JournalScanner::EventMap::const_iterator i = scan.events.begin(); i != scan.events.end(); ++i) {
    std::string const type = i->second.log_event->get_type_str();
    if (type_count.count(type) == 0) {
      type_count[type] = 0;
    }
    type_count[type] += 1;
  }

  std::cout << "Events by type:" << std::endl;
  for (std::map<std::string, int>::iterator i = type_count.begin(); i != type_count.end(); ++i) {
      std::cout << "  " << i->first << ": " << i->second << std::endl;
  }

  std::cout << "Errors: " << scan.errors.size() << std::endl;
  if (!scan.errors.empty()) {
    for (JournalScanner::ErrorMap::const_iterator i = scan.errors.begin();
         i != scan.errors.end(); ++i) {
      std::cout << "  0x" << std::hex << i->first << std::dec
                << ": " << i->second.r << " "
                << i->second.description << std::endl;
    }
  }
}
